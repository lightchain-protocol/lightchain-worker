package pipeline

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/ollama"
)

// SessionKeyGetter retrieves and stores session keys.
type SessionKeyGetter interface {
	GetKey(sessionID uint64) ([]byte, error)
	StoreKey(sessionID uint64, key []byte) error
}

// InferenceClient sends prompts to the AI model.
type InferenceClient interface {
	Generate(ctx context.Context, model, prompt string) (string, error)
	Chat(ctx context.Context, model string, messages []ollama.ChatMessage) (string, error)
}

// JobExecutionClient submits job lifecycle transactions on-chain.
type JobExecutionClient interface {
	AcknowledgeJob(ctx context.Context, jobID uint64) error
	// CompleteJob submits a completeJob TX with a single bytes32 response blob hash.
	// The contract enforces `blobhash(0) == responseBlobHash`, so the caller must
	// have submitted exactly one blob in the same (blob-carrying) transaction.
	CompleteJob(ctx context.Context, jobID uint64, responseBlobHash [32]byte, responseCiphertextHash [32]byte) error
	HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error)
	HasJobCompleted(ctx context.Context, jobID uint64) (bool, error)
	// GetSessionEncWorkerKey returns the most recent encrypted worker session key
	// for the given session — queries both SessionCreated and SessionKeyUpdated
	// events and returns the one from the highest-numbered block.
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)
	// GetJobBlobInfo returns the single prompt and response blob hashes for a
	// completed job along with the blocks they were submitted in. Used to build
	// conversation history.
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash common.Hash, responseHash common.Hash, submitBlock uint64, completionBlock uint64, err error)
}

// BlobFetcher fetches EIP-4844 blob data from the consensus layer.
type BlobFetcher interface {
	FetchBlob(ctx context.Context, versionedHash common.Hash, blockNumber uint64) ([]byte, error)
}

// BlobSubmitter submits blob transactions to the execution layer.
type BlobSubmitter interface {
	SubmitBlobTx(ctx context.Context, data []byte) ([][32]byte, error)
}

// HandlerConfig holds pipeline-specific configuration.
type HandlerConfig struct {
	AckTxTimeout    time.Duration
	ModelIDToName   map[string]string
	ChainID         *big.Int
	JobRegistryAddr common.Address
}

// JobHandler processes inference jobs received from Asynq.
type JobHandler struct {
	chainClient   JobExecutionClient
	blobFetcher   BlobFetcher
	blobSubmitter BlobSubmitter
	keyStore      SessionKeyGetter
	ollamaClient  InferenceClient
	redisClient   *redis.Client
	signingKey    *ecdsa.PrivateKey
	ecdhKey       *ecdh.PrivateKey
	jobCounter    *atomic.Int32
	logger        *slog.Logger
	cfg           HandlerConfig
	modelIDToName map[string]string
}

// NewJobHandler creates a handler wired with all dependencies.
func NewJobHandler(
	chainClient JobExecutionClient,
	blobFetcher BlobFetcher,
	blobSubmitter BlobSubmitter,
	keyStore SessionKeyGetter,
	ollamaClient InferenceClient,
	redisClient *redis.Client,
	signingKey *ecdsa.PrivateKey,
	ecdhKey *ecdh.PrivateKey,
	jobCounter *atomic.Int32,
	logger *slog.Logger,
	cfg HandlerConfig,
) *JobHandler {
	modelIDToName := make(map[string]string, len(cfg.ModelIDToName))
	for k, v := range cfg.ModelIDToName {
		modelIDToName[normalizeModelLookupKey(k)] = v
	}

	return &JobHandler{
		chainClient:   chainClient,
		blobFetcher:   blobFetcher,
		blobSubmitter: blobSubmitter,
		keyStore:      keyStore,
		ollamaClient:  ollamaClient,
		redisClient:   redisClient,
		signingKey:    signingKey,
		ecdhKey:       ecdhKey,
		jobCounter:    jobCounter,
		logger:        logger,
		cfg:           cfg,
		modelIDToName: modelIDToName,
	}
}

// HandleTask is the Asynq handler entry point.
func (h *JobHandler) HandleTask(ctx context.Context, task *asynq.Task) error {
	var payload JobPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal job payload: %w", err)
	}

	h.jobCounter.Add(1)
	defer h.jobCounter.Add(-1)

	h.logger.Info("processing job",
		"jobID", payload.JobID,
		"sessionID", payload.SessionID,
		"model", payload.ModelID,
		"correlationID", payload.CorrelationID,
	)

	if err := h.processJob(ctx, payload); err != nil {
		h.logger.Error("job failed",
			"jobID", payload.JobID,
			"error", err,
		)
		return err
	}

	h.logger.Info("job completed", "jobID", payload.JobID)
	return nil
}

// processJob executes the 8-stage inference pipeline.
func (h *JobHandler) processJob(ctx context.Context, p JobPayload) error {
	// Stage 1: ACK — acknowledge job on-chain
	if err := h.ensureAcknowledged(ctx, p.JobID); err != nil {
		return fmt.Errorf("stage 1 (ack): %w", err)
	}

	// Stage 2: Fetch the prompt blob from the consensus layer. Post-audit
	// each job carries a single bytes32 prompt blob hash.
	if p.PromptBlobHash == (common.Hash{}) {
		return fmt.Errorf("stage 2 (fetch blob): prompt blob hash is zero")
	}
	blobData, err := h.blobFetcher.FetchBlob(ctx, p.PromptBlobHash, p.BlockNumber)
	if err != nil {
		return fmt.Errorf("stage 2 (fetch blob %s): %w", p.PromptBlobHash.Hex(), err)
	}

	// Stage 3: Get session key (cache miss → chain fetch → store)
	sessionKey, err := h.getOrDeriveSessionKey(ctx, p.SessionID)
	if err != nil {
		return fmt.Errorf("stage 3 (session key): %w", err)
	}

	// Stage 4: Decrypt prompt.
	//
	// On decryption failure, try refreshing the session key from chain once —
	// the session key may have been rotated via updateSessionKey,
	// emitting SessionKeyUpdated with a new encWorkerKey. GetSessionEncWorkerKey
	// returns the newest event's key, so a refresh picks up the rotation.
	prompt, err := pkgcrypto.Decrypt(sessionKey, blobData)
	if err != nil {
		refreshed, refreshErr := h.refreshSessionKey(ctx, p.SessionID)
		if refreshErr != nil {
			return fmt.Errorf("stage 4 (decrypt prompt): %w (refresh failed: %v)", err, refreshErr)
		}
		sessionKey = refreshed
		prompt, err = pkgcrypto.Decrypt(sessionKey, blobData)
		if err != nil {
			return fmt.Errorf("stage 4 (decrypt prompt after key refresh): %w", err)
		}
	}

	// Stage 5: AI inference (with conversation history if prior jobs exist)
	modelName, err := h.resolveModelName(p.ModelID)
	if err != nil {
		return fmt.Errorf("stage 5 (resolve model): %w", err)
	}

	var response string
	var history []ollama.ChatMessage
	if len(p.PriorJobIDs) > 0 {
		var err error
		history, err = h.buildConversationHistory(ctx, p.PriorJobIDs, sessionKey)
		if err != nil {
			h.logger.Warn("failed to build conversation history, falling back to single prompt",
				"jobID", p.JobID,
				"error", err,
			)
			history = nil
		}
	}

	if len(history) > 0 {
		messages := append(history, ollama.ChatMessage{Role: "user", Content: string(prompt)})
		response, err = h.ollamaClient.Chat(ctx, modelName, messages)
		if err != nil {
			return fmt.Errorf("stage 5 (chat inference): %w", err)
		}
	} else {
		response, err = h.ollamaClient.Generate(ctx, modelName, string(prompt))
		if err != nil {
			return fmt.Errorf("stage 5 (inference): %w", err)
		}
	}

	// Stage 6: Encrypt response
	ciphertext, err := pkgcrypto.Encrypt(sessionKey, []byte(response))
	if err != nil {
		return fmt.Errorf("stage 6 (encrypt response): %w", err)
	}

	// Stage 7: Publish to Redis (non-fatal on error)
	h.publishToRedis(ctx, p.JobID, p.SessionID, p.CorrelationID, ciphertext)

	// Stage 8: Submit blob TX + complete job on-chain.
	// Post-audit the contract's completeJob takes a single bytes32
	// responseBlobHash and enforces `blobhash(0) == responseBlobHash`, so the
	// blob TX must carry exactly one blob.
	blobHashes, err := h.blobSubmitter.SubmitBlobTx(ctx, ciphertext)
	if err != nil {
		return fmt.Errorf("stage 8 (submit blob): %w", err)
	}
	if len(blobHashes) != 1 {
		return fmt.Errorf("stage 8 (submit blob): expected exactly 1 blob hash, got %d", len(blobHashes))
	}

	responseCiphertextHash := crypto.Keccak256Hash(ciphertext)
	if err := h.completeJob(ctx, p.JobID, blobHashes[0], responseCiphertextHash); err != nil {
		return fmt.Errorf("stage 8 (complete job): %w", err)
	}

	return nil
}

func (h *JobHandler) ensureAcknowledged(ctx context.Context, jobID uint64) error {
	acknowledged, err := h.chainClient.HasJobAcknowledged(ctx, jobID)
	if err != nil {
		return fmt.Errorf("check acknowledged state: %w", err)
	}
	if acknowledged {
		return nil
	}

	ackCtx, ackCancel := context.WithTimeout(ctx, h.cfg.AckTxTimeout)
	defer ackCancel()

	if err := h.chainClient.AcknowledgeJob(ackCtx, jobID); err != nil {
		acknowledged, checkErr := h.chainClient.HasJobAcknowledged(ctx, jobID)
		if checkErr != nil {
			return fmt.Errorf("acknowledge job: %w (recheck failed: %v)", err, checkErr)
		}
		if acknowledged {
			return nil
		}
		return fmt.Errorf("acknowledge job: %w", err)
	}

	return nil
}

func (h *JobHandler) completeJob(
	ctx context.Context,
	jobID uint64,
	responseBlobHash [32]byte,
	responseCiphertextHash [32]byte,
) error {
	if err := h.chainClient.CompleteJob(ctx, jobID, responseBlobHash, responseCiphertextHash); err != nil {
		completed, checkErr := h.chainClient.HasJobCompleted(ctx, jobID)
		if checkErr != nil {
			return fmt.Errorf("complete job: %w (recheck failed: %v)", err, checkErr)
		}
		if completed {
			return nil
		}
		return fmt.Errorf("complete job: %w", err)
	}

	return nil
}

// getOrDeriveSessionKey tries the cache first, then derives from chain on miss.
func (h *JobHandler) getOrDeriveSessionKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	key, err := h.keyStore.GetKey(sessionID)
	if err == nil {
		return key, nil
	}
	return h.deriveAndStoreSessionKey(ctx, sessionID)
}

// refreshSessionKey skips the local cache and derives the latest session key
// directly from chain events. Used on decryption failure to pick up a rotated
// session key.
func (h *JobHandler) refreshSessionKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	h.logger.Info("refreshing session key from chain", "sessionID", sessionID)
	return h.deriveAndStoreSessionKey(ctx, sessionID)
}

// deriveAndStoreSessionKey fetches the latest encrypted worker key for the
// session from chain, decrypts it with the worker's ECDH private key, and
// stores the result in the local keystore. The on-chain query returns the
// most recent SessionKeyUpdated event if one exists, falling back to
// SessionCreated — so this always yields the current live key.
func (h *JobHandler) deriveAndStoreSessionKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	encWorkerKey, err := h.chainClient.GetSessionEncWorkerKey(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("fetch enc worker key for session %d: %w", sessionID, err)
	}

	sessionKey, err := pkgcrypto.DecryptSessionKey(encWorkerKey, h.ecdhKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt session key for session %d: %w", sessionID, err)
	}

	if err := h.keyStore.StoreKey(sessionID, sessionKey); err != nil {
		// Log but don't fail — key is in memory for this job
		h.logger.Warn("failed to persist session key", "sessionID", sessionID, "error", err)
	}

	return sessionKey, nil
}

// responseMismatchSigArgs matches JobRegistry.disputeResponseMismatch signature
// verification: keccak256(abi.encode(chainid, address(this), jobId, sessionId, ciphertext)).
var responseMismatchSigArgs abi.Arguments

func init() {
	uint256Ty, err := abi.NewType("uint256", "", nil)
	if err != nil {
		panic(fmt.Sprintf("pipeline: abi.NewType(uint256): %v", err))
	}
	addressTy, err := abi.NewType("address", "", nil)
	if err != nil {
		panic(fmt.Sprintf("pipeline: abi.NewType(address): %v", err))
	}
	bytesTy, err := abi.NewType("bytes", "", nil)
	if err != nil {
		panic(fmt.Sprintf("pipeline: abi.NewType(bytes): %v", err))
	}
	responseMismatchSigArgs = abi.Arguments{
		{Type: uint256Ty}, // block.chainid
		{Type: addressTy}, // address(jobRegistry)
		{Type: uint256Ty}, // jobId
		{Type: uint256Ty}, // sessionId
		{Type: bytesTy},   // ciphertext
	}
}

// publishToRedis signs and publishes the response to the session's Redis channel.
// Errors are logged but non-fatal — the on-chain blob is the authoritative response.
func (h *JobHandler) publishToRedis(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	ciphertext []byte,
) {
	sig, err := signMismatchEvidence(h.cfg.ChainID, h.cfg.JobRegistryAddr, jobID, sessionID, ciphertext, h.signingKey)
	if err != nil {
		h.logger.Warn("failed to sign response for Redis", "jobID", jobID, "error", err)
		return
	}

	resp := pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeComplete,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      0,
		TotalChunks:   1,
		Payload:       ciphertext,
		Signature:     "0x" + hex.EncodeToString(sig),
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		h.logger.Warn("failed to marshal response for Redis", "jobID", jobID, "error", err)
		return
	}

	channel := fmt.Sprintf("session:%d:responses", sessionID)
	if err := h.redisClient.Publish(ctx, channel, data).Err(); err != nil {
		h.logger.Warn("failed to publish response to Redis", "jobID", jobID, "channel", channel, "error", err)
	}
}

// signMismatchEvidence produces an EIP-191 worker signature over the domain-separated
// payload that JobRegistry.disputeResponseMismatch verifies on-chain. Consumers who
// receive the signed PubSubMessage off-chain can use the signature as evidence when
// raising a mismatch dispute.
func signMismatchEvidence(
	chainID *big.Int,
	jobRegistryAddr common.Address,
	jobID, sessionID uint64,
	ciphertext []byte,
	signingKey *ecdsa.PrivateKey,
) ([]byte, error) {
	digest, err := responseMismatchDigest(chainID, jobRegistryAddr, jobID, sessionID, ciphertext)
	if err != nil {
		return nil, err
	}
	return crypto.Sign(accounts.TextHash(digest), signingKey)
}

// responseMismatchDigest computes keccak256(abi.encode(chainid, jobRegistryAddr,
// jobId, sessionId, ciphertext)) — the inner hash that JobRegistry wraps with
// EIP-191 before verifying in disputeResponseMismatch.
func responseMismatchDigest(
	chainID *big.Int,
	jobRegistryAddr common.Address,
	jobID, sessionID uint64,
	ciphertext []byte,
) ([]byte, error) {
	if chainID == nil {
		return nil, fmt.Errorf("responseMismatchDigest: chainID is nil")
	}
	encoded, err := responseMismatchSigArgs.Pack(
		new(big.Int).Set(chainID),
		jobRegistryAddr,
		new(big.Int).SetUint64(jobID),
		new(big.Int).SetUint64(sessionID),
		ciphertext,
	)
	if err != nil {
		return nil, fmt.Errorf("pack mismatch payload: %w", err)
	}
	return crypto.Keccak256(encoded), nil
}

// buildConversationHistory fetches prior jobs' prompt and response blobs,
// decrypts them, and assembles an ordered conversation.
//
// Security: PriorJobIDs comes from the trusted dispatcher (not user input).
// Additionally, blobs are decrypted with the current session's key — if a job
// ID belongs to a different session, decryption will fail, preventing
// cross-session data leakage.
func (h *JobHandler) buildConversationHistory(
	ctx context.Context,
	priorJobIDs []uint64,
	sessionKey []byte,
) ([]ollama.ChatMessage, error) {
	var messages []ollama.ChatMessage

	for _, jobID := range priorJobIDs {
		promptHash, responseHash, submitBlock, completionBlock, err := h.chainClient.GetJobBlobInfo(ctx, jobID)
		if err != nil {
			return nil, fmt.Errorf("get blob hashes for job %d: %w", jobID, err)
		}

		// Fetch and decrypt prompt. Post-audit each job carries a
		// single prompt blob; the on-chain tx submission lives in submitBlock.
		if promptHash != (common.Hash{}) {
			promptBlob, err := h.blobFetcher.FetchBlob(ctx, promptHash, submitBlock)
			if err != nil {
				return nil, fmt.Errorf("fetch prompt blob for job %d: %w", jobID, err)
			}
			promptText, err := pkgcrypto.Decrypt(sessionKey, promptBlob)
			if err != nil {
				return nil, fmt.Errorf("decrypt prompt for job %d: %w", jobID, err)
			}
			messages = append(messages, ollama.ChatMessage{Role: "user", Content: string(promptText)})
		}

		// Fetch and decrypt response (single blob, lives in completeJob TX block).
		if responseHash != (common.Hash{}) {
			responseBlob, err := h.blobFetcher.FetchBlob(ctx, responseHash, completionBlock)
			if err != nil {
				return nil, fmt.Errorf("fetch response blob for job %d: %w", jobID, err)
			}
			responseText, err := pkgcrypto.Decrypt(sessionKey, responseBlob)
			if err != nil {
				return nil, fmt.Errorf("decrypt response for job %d: %w", jobID, err)
			}
			messages = append(messages, ollama.ChatMessage{Role: "assistant", Content: string(responseText)})
		}
	}

	return messages, nil
}

func (h *JobHandler) resolveModelName(modelID string) (string, error) {
	if len(h.modelIDToName) == 0 {
		return modelID, nil
	}

	if modelName, ok := h.modelIDToName[normalizeModelLookupKey(modelID)]; ok {
		return modelName, nil
	}

	if looksLikeHexModelID(modelID) {
		return "", fmt.Errorf("no local Ollama model configured for queued model ID %q", modelID)
	}

	return modelID, nil
}

func normalizeModelLookupKey(modelID string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(modelID, "0x"), "0X"))
}

func looksLikeHexModelID(modelID string) bool {
	normalized := normalizeModelLookupKey(modelID)
	if len(normalized) != 64 {
		return false
	}

	for _, r := range normalized {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}

	return true
}

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
	CompleteJob(ctx context.Context, jobID uint64, responseBlobHashes [][32]byte, responseCiphertextHash [32]byte) error
	HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error)
	HasJobCompleted(ctx context.Context, jobID uint64) (bool, error)
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)
	GetJobBlobHashes(ctx context.Context, jobID uint64) (promptHashes []common.Hash, responseHashes []common.Hash, submitBlock uint64, completionBlock uint64, err error)
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
	AckTxTimeout  time.Duration
	ModelIDToName map[string]string
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

	// Stage 2: Fetch prompt blob(s) from the consensus layer
	hashes := p.EffectivePromptHashes()
	if len(hashes) == 0 {
		return fmt.Errorf("stage 2 (fetch blob): no prompt blob hashes")
	}
	var blobData []byte
	for _, hash := range hashes {
		data, err := h.blobFetcher.FetchBlob(ctx, hash, p.BlockNumber)
		if err != nil {
			return fmt.Errorf("stage 2 (fetch blob %s): %w", hash.Hex(), err)
		}
		blobData = append(blobData, data...)
	}

	// Stage 3: Get session key (cache miss → chain fetch → store)
	sessionKey, err := h.getOrDeriveSessionKey(ctx, p.SessionID)
	if err != nil {
		return fmt.Errorf("stage 3 (session key): %w", err)
	}

	// Stage 4: Decrypt prompt
	prompt, err := pkgcrypto.Decrypt(sessionKey, blobData)
	if err != nil {
		return fmt.Errorf("stage 4 (decrypt prompt): %w", err)
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

	// Stage 8: Submit blob TX + complete job on-chain
	blobHashes, err := h.blobSubmitter.SubmitBlobTx(ctx, ciphertext)
	if err != nil {
		return fmt.Errorf("stage 8 (submit blob): %w", err)
	}

	responseCiphertextHash := crypto.Keccak256Hash(ciphertext)
	if err := h.completeJob(ctx, p.JobID, blobHashes, responseCiphertextHash); err != nil {
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
	responseBlobHashes [][32]byte,
	responseCiphertextHash [32]byte,
) error {
	if err := h.chainClient.CompleteJob(ctx, jobID, responseBlobHashes, responseCiphertextHash); err != nil {
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

	// Cache miss — fetch encrypted key from chain and decrypt
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

// publishToRedis signs and publishes the response to the session's Redis channel.
// Errors are logged but non-fatal — the on-chain blob is the authoritative response.
func (h *JobHandler) publishToRedis(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	ciphertext []byte,
) {
	sig, err := signRedisResponse(ciphertext, jobID, h.signingKey)
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

func signRedisResponse(ciphertext []byte, jobID uint64, signingKey *ecdsa.PrivateKey) ([]byte, error) {
	digest := redisResponseDigest(ciphertext, jobID)
	return crypto.Sign(accounts.TextHash(digest), signingKey)
}

func redisResponseDigest(ciphertext []byte, jobID uint64) []byte {
	jobIDBig := new(big.Int).SetUint64(jobID)
	jobIDBytes := common.LeftPadBytes(jobIDBig.Bytes(), 32)

	payload := make([]byte, 0, len(ciphertext)+len(jobIDBytes))
	payload = append(payload, ciphertext...)
	payload = append(payload, jobIDBytes...)
	return crypto.Keccak256(payload)
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
		promptHashes, responseHashes, submitBlock, completionBlock, err := h.chainClient.GetJobBlobHashes(ctx, jobID)
		if err != nil {
			return nil, fmt.Errorf("get blob hashes for job %d: %w", jobID, err)
		}

		// Fetch and decrypt prompt (may span multiple blobs, lives in submitJob TX block).
		var promptBlob []byte
		for _, hash := range promptHashes {
			blob, err := h.blobFetcher.FetchBlob(ctx, hash, submitBlock)
			if err != nil {
				return nil, fmt.Errorf("fetch prompt blob for job %d: %w", jobID, err)
			}
			promptBlob = append(promptBlob, blob...)
		}
		if len(promptBlob) > 0 {
			promptText, err := pkgcrypto.Decrypt(sessionKey, promptBlob)
			if err != nil {
				return nil, fmt.Errorf("decrypt prompt for job %d: %w", jobID, err)
			}
			messages = append(messages, ollama.ChatMessage{Role: "user", Content: string(promptText)})
		}

		// Fetch and decrypt response (may span multiple blobs, lives in completeJob TX block).
		var responseBlob []byte
		for _, hash := range responseHashes {
			blob, err := h.blobFetcher.FetchBlob(ctx, hash, completionBlock)
			if err != nil {
				return nil, fmt.Errorf("fetch response blob for job %d: %w", jobID, err)
			}
			responseBlob = append(responseBlob, blob...)
		}
		if len(responseBlob) > 0 {
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

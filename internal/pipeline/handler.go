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
	BlobTxTimeout   time.Duration
	ModelIDToName   map[string]string
	ChainID         *big.Int
	JobRegistryAddr common.Address
}

// ResponsePublisher publishes encrypted responses for real-time delivery.
// Implementations: RedisResponsePublisher (direct Redis PUBLISH) and
// gateway-based publisher (POST to worker-gateway).
type ResponsePublisher interface {
	PublishResponse(ctx context.Context, jobID, sessionID uint64, correlationID string, signature string, ciphertext []byte)
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
	// checkpoints is optional. When nil, the handler runs without retry
	// caching — equivalent to pre-PR-2 behavior. Production wires a
	// Redis-backed store here; tests that exercise the cache path do the
	// same via miniredis.
	checkpoints *CheckpointStore
}

// NewJobHandler creates a handler wired with all dependencies. checkpoints
// may be nil — when nil, the handler skips the retry-safety cache (stages
// 2-6 re-run on every asynq retry, same as pre-checkpoint behavior).
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
	checkpoints *CheckpointStore,
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
		checkpoints:   checkpoints,
	}

	return &JobHandler{
		chainClient:       chainClient,
		blobFetcher:       blobFetcher,
		blobSubmitter:     blobSubmitter,
		keyStore:          keyStore,
		ollamaClient:      ollamaClient,
		redisClient:       redisClient,
		responsePublisher: rp,
		signingKey:        signingKey,
		ecdhKey:           ecdhKey,
		jobCounter:        jobCounter,
		logger:            logger,
		cfg:               cfg,
		modelIDToName:     modelIDToName,
	}
}

// RedisResponsePublisher publishes responses directly to Redis pub/sub.
type RedisResponsePublisher struct {
	client *redis.Client
	logger *slog.Logger
}

// PublishResponse signs and publishes the response to the session's Redis channel.
func (p *RedisResponsePublisher) PublishResponse(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	signature string,
	ciphertext []byte,
) {
	resp := pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeComplete,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      0,
		TotalChunks:   1,
		Payload:       ciphertext,
		Signature:     signature,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		p.logger.Warn("failed to marshal response for Redis", "jobID", jobID, "error", err)
		return
	}

	channel := fmt.Sprintf("session:%d:responses", sessionID)
	if err := p.client.Publish(ctx, channel, data).Err(); err != nil {
		p.logger.Warn("failed to publish response to Redis", "jobID", jobID, "channel", channel, "error", err)
	}
}

// HandleJobPayload processes a job from a raw JobPayload (used in gateway mode
// where jobs come via HTTP polling instead of Asynq).
func (h *JobHandler) HandleJobPayload(ctx context.Context, payload JobPayload) error {
	h.jobCounter.Add(1)
	defer h.jobCounter.Add(-1)

	h.logger.Info("processing job",
		"jobID", payload.JobID,
		"sessionID", payload.SessionID,
		"model", payload.ModelID,
		"correlationID", payload.CorrelationID,
	)

	if err := h.processJob(ctx, payload); err != nil {
		h.logger.Error("job failed", "jobID", payload.JobID, "error", err)
		return err
	}

	h.logger.Info("job completed", "jobID", payload.JobID)
	return nil
}

// HandleTask is the Asynq handler entry point.
func (h *JobHandler) HandleTask(ctx context.Context, task *asynq.Task) error {
	var payload JobPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal job payload: %w", err)
	}

	h.jobCounter.Add(1)
	defer h.jobCounter.Add(-1)

	// Asynq populates retry metadata in the context on every invocation.
	// retryCount is 0 on the first attempt and increments on each retry,
	// which lets us distinguish a fresh job from a retried one without
	// changing the task payload shape.
	retryCount, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)

	// Surface the dispatcher-configured asynq task deadline so operators
	// can correlate "context deadline exceeded" cascades with the upstream
	// timeout. The worker does not control this ceiling — the dispatcher
	// passes it via ctx — so we log it and warn loudly if it looks too
	// tight for the pipeline's own stage timeouts.
	deadline, hasDeadline := ctx.Deadline()
	budget := time.Duration(0)
	if hasDeadline {
		budget = time.Until(deadline)
	}

	h.logger.Info("processing job",
		"jobID", payload.JobID,
		"sessionID", payload.SessionID,
		"model", payload.ModelID,
		"correlationID", payload.CorrelationID,
		"attempt", retryCount+1,
		"maxRetry", maxRetry,
		"isRetry", retryCount > 0,
		"taskBudgetMs", budget.Milliseconds(),
		"hasDeadline", hasDeadline,
	)

	// Minimum realistic budget = ACK timeout + BLOB tx timeout + 10s buffer
	// for inference/encrypt/fetch/redis. If the dispatcher's task ctx is
	// tighter than this, stage 8 will hit ctx.Done before completing,
	// leaving nonces gapped and blob txs stuck. This is an operator-config
	// concern; log loud so it shows up in the first failing job, not after
	// a cascade.
	if hasDeadline {
		minBudget := h.cfg.AckTxTimeout + h.cfg.BlobTxTimeout + 10*time.Second
		if budget < minBudget {
			h.logger.Warn("task budget appears too small for the pipeline",
				"jobID", payload.JobID,
				"taskBudgetMs", budget.Milliseconds(),
				"minRecommendedMs", minBudget.Milliseconds(),
				"ackTxTimeoutMs", h.cfg.AckTxTimeout.Milliseconds(),
				"blobTxTimeoutMs", h.cfg.BlobTxTimeout.Milliseconds(),
				"hint", "raise the dispatcher's asynq task timeout",
			)
		}
	}

	if err := h.processJob(ctx, payload); err != nil {
		h.logger.Error("job failed",
			"jobID", payload.JobID,
			"attempt", retryCount+1,
			"maxRetry", maxRetry,
			"willRetry", retryCount < maxRetry,
			"error", err,
		)
		return err
	}

	h.logger.Info("job completed",
		"jobID", payload.JobID,
		"attempt", retryCount+1,
	)
	return nil
}

// processJob executes the 8-stage inference pipeline.
//
// A child logger tagged with job/session/correlation IDs is threaded through
// every helper so each stage-boundary log line is automatically correlated
// in Cloud Logging. Every stage emits at minimum a completion log with a
// stable `stage` attribute and `durationMs` so slow/stuck stages are easy
// to locate when debugging production traces.
func (h *JobHandler) processJob(ctx context.Context, p JobPayload) error {
	logger := h.logger.With(
		"jobID", p.JobID,
		"sessionID", p.SessionID,
		"correlationID", p.CorrelationID,
	)

	// Stage 1: ACK — acknowledge job on-chain
	stageStart := time.Now()
	if err := h.ensureAcknowledged(ctx, logger, p.JobID); err != nil {
		return fmt.Errorf("stage 1 (ack): %w", err)
	}
	logger.Info("stage 1 complete",
		"stage", "ack",
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	// Read checkpoint once after stage 1. The result determines whether
	// stages 2-6 (inference), 7 (redis publish), and 8a (blob submit) can
	// be skipped on a retry. Nil handler or Get error => treat as cache
	// miss and run the full pipeline — same as pre-checkpoint behavior.
	ckpt := h.readCheckpoint(ctx, logger, p.JobID)

	var ciphertext []byte

	if ckpt.HasCiphertext() {
		logger.Info("checkpoint hit, skipping stages 2-6",
			"stage", "checkpoint",
			"reason", "inference_cached",
			"ciphertextBytes", len(ckpt.Ciphertext),
		)
		ciphertext = ckpt.Ciphertext
	} else {
		// Stage 2: Fetch the prompt blob from the consensus layer. Post-audit
		// each job carries a single bytes32 prompt blob hash.
		if p.PromptBlobHash == (common.Hash{}) {
			return fmt.Errorf("stage 2 (fetch blob): prompt blob hash is zero")
		}
		stageStart = time.Now()
		logger.Info("stage 2 starting",
			"stage", "fetch_blob",
			"promptBlobHash", p.PromptBlobHash.Hex(),
			"blockNumber", p.BlockNumber,
		)
		blobData, err := h.blobFetcher.FetchBlob(ctx, p.PromptBlobHash, p.BlockNumber)
		if err != nil {
			return fmt.Errorf("stage 2 (fetch blob %s): %w", p.PromptBlobHash.Hex(), err)
		}
		logger.Info("stage 2 complete",
			"stage", "fetch_blob",
			"blobBytes", len(blobData),
			"durationMs", time.Since(stageStart).Milliseconds(),
		)

		// Stages 3-6 produce ciphertext; they're wrapped in a helper so the
		// old inlined code keeps its exact stage-boundary logging but the
		// checkpoint branching stays readable.
		producedCiphertext, err := h.runInferencePipeline(ctx, logger, p, blobData)
		if err != nil {
			return err
		}
		ciphertext = producedCiphertext

		// Persist canonical ciphertext. Under a concurrent retry on the
		// same jobID, SetCiphertextIfAbsent returns the earlier writer's
		// bytes — we overwrite our local with canonical so stage 7's
		// publish and stage 8b's hash match what the user already received.
		if h.checkpoints != nil {
			canonical, wasSet, cErr := h.checkpoints.SetCiphertextIfAbsent(ctx, p.JobID, ciphertext)
			switch {
			case cErr != nil:
				logger.Warn("failed to persist ciphertext to checkpoint",
					"stage", "checkpoint",
					"error", cErr,
				)
			case !wasSet:
				logger.Warn("checkpoint race lost, using canonical ciphertext",
					"stage", "checkpoint",
					"localBytes", len(ciphertext),
					"canonicalBytes", len(canonical),
				)
				ciphertext = canonical
			default:
				logger.Debug("ciphertext persisted to checkpoint",
					"stage", "checkpoint",
					"ciphertextBytes", len(ciphertext),
				)
			}
		}
	}

	// Stage 7: Publish to Redis (non-fatal on error). Skipped on retry if
	// the checkpoint records delivered=true so relay subscribers don't see
	// a duplicate complete frame.
	if ckpt.Delivered {
		logger.Info("checkpoint hit, skipping stage 7",
			"stage", "checkpoint",
			"reason", "delivered",
		)
	} else {
		stageStart = time.Now()
		h.publishToRedis(ctx, logger, p.JobID, p.SessionID, p.CorrelationID, ciphertext)
		logger.Info("stage 7 complete",
			"stage", "redis_publish",
			"durationMs", time.Since(stageStart).Milliseconds(),
		)
		if h.checkpoints != nil {
			if err := h.checkpoints.MarkDelivered(ctx, p.JobID); err != nil {
				logger.Warn("failed to mark delivered on checkpoint",
					"stage", "checkpoint",
					"error", err,
				)
			}
		}
	}

	// Stage 8a: Submit blob TX.
	// Post-audit the contract's completeJob takes a single bytes32
	// responseBlobHash and enforces `blobhash(0) == responseBlobHash`, so the
	// blob TX must carry exactly one blob.
	//
	// Wrap in BlobTxTimeout so a stuck WaitMined (e.g. dead EL websocket)
	// cannot hold the broadcast slot indefinitely and starve other jobs.
	// Matches the AckTxTimeout pattern above.
	var versionedHash common.Hash
	if ckpt.HasVersionedHash() {
		logger.Info("checkpoint hit, skipping stage 8a",
			"stage", "checkpoint",
			"reason", "blob_submitted",
			"versionedHash", ckpt.VersionedHash.Hex(),
		)
		versionedHash = ckpt.VersionedHash
	} else {
		stageStart = time.Now()
		logger.Info("stage 8a starting",
			"stage", "submit_blob",
			"ciphertextBytes", len(ciphertext),
			"timeout", h.cfg.BlobTxTimeout.String(),
		)
		submitCtx, submitCancel := context.WithTimeout(ctx, h.cfg.BlobTxTimeout)
		blobHashes, err := h.blobSubmitter.SubmitBlobTx(submitCtx, ciphertext)
		submitCancel()
		if err != nil {
			return fmt.Errorf("stage 8 (submit blob): %w", err)
		}
		if len(blobHashes) != 1 {
			return fmt.Errorf("stage 8 (submit blob): expected exactly 1 blob hash, got %d", len(blobHashes))
		}
		versionedHash = common.Hash(blobHashes[0])
		logger.Info("stage 8a complete",
			"stage", "submit_blob",
			"versionedHash", versionedHash.Hex(),
			"durationMs", time.Since(stageStart).Milliseconds(),
		)
		if h.checkpoints != nil {
			if err := h.checkpoints.SetVersionedHash(ctx, p.JobID, versionedHash); err != nil {
				logger.Warn("failed to persist versionedHash on checkpoint",
					"stage", "checkpoint",
					"error", err,
				)
			}
		}
	}

	// Stage 8b: Complete job on-chain.
	stageStart = time.Now()
	responseCiphertextHash := crypto.Keccak256Hash(ciphertext)
	logger.Info("stage 8b starting",
		"stage", "complete_job",
		"versionedHash", versionedHash.Hex(),
		"ciphertextHash", responseCiphertextHash.Hex(),
	)
	if err := h.completeJob(ctx, logger, p.JobID, versionedHash, responseCiphertextHash); err != nil {
		return fmt.Errorf("stage 8 (complete job): %w", err)
	}
	logger.Info("stage 8b complete",
		"stage", "complete_job",
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	// Tombstone the checkpoint after a successful stage 8b. A short TTL
	// lets any straggler retry observe "completed" before the record
	// disappears; outright Delete would race concurrent retries.
	if h.checkpoints != nil {
		if err := h.checkpoints.Tombstone(ctx, p.JobID); err != nil {
			logger.Warn("failed to tombstone checkpoint after completion",
				"stage", "checkpoint",
				"error", err,
			)
		}
	}

	return nil
}

// readCheckpoint returns the cached checkpoint for jobID, or a zero
// JobCheckpoint on nil handler / miss / error. Errors are logged and
// downgraded so a flaky Redis doesn't break job processing.
func (h *JobHandler) readCheckpoint(ctx context.Context, logger *slog.Logger, jobID uint64) JobCheckpoint {
	if h.checkpoints == nil {
		return JobCheckpoint{}
	}
	ckpt, err := h.checkpoints.Get(ctx, jobID)
	if err != nil {
		logger.Warn("failed to read checkpoint; treating as cache miss",
			"stage", "checkpoint",
			"error", err,
		)
		return JobCheckpoint{}
	}
	return ckpt
}

// runInferencePipeline runs stages 3-6: session key fetch, prompt
// decryption, Ollama inference (with optional conversation history),
// and response encryption. Returns the ciphertext that stages 7 and 8a
// will publish/submit. All stage-boundary log lines are preserved
// verbatim against the pre-checkpoint implementation.
func (h *JobHandler) runInferencePipeline(
	ctx context.Context,
	logger *slog.Logger,
	p JobPayload,
	blobData []byte,
) ([]byte, error) {
	// Stage 3: Get session key (cache miss → chain fetch → store)
	stageStart := time.Now()
	sessionKey, err := h.getOrDeriveSessionKey(ctx, logger, p.SessionID)
	if err != nil {
		return nil, fmt.Errorf("stage 3 (session key): %w", err)
	}
	logger.Info("stage 3 complete",
		"stage", "session_key",
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	// Stage 4: Decrypt prompt.
	//
	// On decryption failure, try refreshing the session key from chain once —
	// the session key may have been rotated via updateSessionKey.
	// GetSessionEncWorkerKey reads session storage directly, so a refresh
	// picks up the current key regardless of how it was set.
	stageStart = time.Now()
	prompt, err := pkgcrypto.Decrypt(sessionKey, blobData)
	if err != nil {
		logger.Warn("stage 4: initial decrypt failed, refreshing session key",
			"stage", "decrypt",
			"error", err,
		)
		refreshed, refreshErr := h.refreshSessionKey(ctx, logger, p.SessionID)
		if refreshErr != nil {
			return nil, fmt.Errorf("stage 4 (decrypt prompt): %w (refresh failed: %v)", err, refreshErr)
		}
		sessionKey = refreshed
		prompt, err = pkgcrypto.Decrypt(sessionKey, blobData)
		if err != nil {
			return nil, fmt.Errorf("stage 4 (decrypt prompt after key refresh): %w", err)
		}
	}
	logger.Info("stage 4 complete",
		"stage", "decrypt",
		"promptBytes", len(prompt),
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	// Stage 5: AI inference (with conversation history if prior jobs exist)
	modelName, err := h.resolveModelName(p.ModelID)
	if err != nil {
		return nil, fmt.Errorf("stage 5 (resolve model): %w", err)
	}

	var response string
	var history []ollama.ChatMessage
	if len(p.PriorJobIDs) > 0 {
		var hErr error
		history, hErr = h.buildConversationHistory(ctx, p.PriorJobIDs, sessionKey)
		if hErr != nil {
			logger.Warn("failed to build conversation history, falling back to single prompt",
				"stage", "inference",
				"error", hErr,
			)
			history = nil
		}
	}

	stageStart = time.Now()
	logger.Info("stage 5 starting",
		"stage", "inference",
		"model", modelName,
		"promptBytes", len(prompt),
		"historyTurns", len(history),
	)
	if len(history) > 0 {
		messages := append(history, ollama.ChatMessage{Role: "user", Content: string(prompt)})
		response, err = h.ollamaClient.Chat(ctx, modelName, messages)
		if err != nil {
			return nil, fmt.Errorf("stage 5 (chat inference): %w", err)
		}
	} else {
		response, err = h.ollamaClient.Generate(ctx, modelName, string(prompt))
		if err != nil {
			return nil, fmt.Errorf("stage 5 (inference): %w", err)
		}
	}
	logger.Info("stage 5 complete",
		"stage", "inference",
		"model", modelName,
		"responseBytes", len(response),
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	// Stage 6: Encrypt response
	stageStart = time.Now()
	ciphertext, err := pkgcrypto.Encrypt(sessionKey, []byte(response))
	if err != nil {
		return nil, fmt.Errorf("stage 6 (encrypt response): %w", err)
	}
	logger.Info("stage 6 complete",
		"stage", "encrypt",
		"ciphertextBytes", len(ciphertext),
		"durationMs", time.Since(stageStart).Milliseconds(),
	)

	return ciphertext, nil
}

func (h *JobHandler) ensureAcknowledged(ctx context.Context, logger *slog.Logger, jobID uint64) error {
	acknowledged, err := h.chainClient.HasJobAcknowledged(ctx, jobID)
	if err != nil {
		return fmt.Errorf("check acknowledged state: %w", err)
	}
	if acknowledged {
		logger.Info("stage 1: job already acknowledged on-chain, skipping ack tx",
			"stage", "ack",
			"path", "already_acked",
		)
		return nil
	}

	logger.Info("stage 1: sending ack tx",
		"stage", "ack",
		"path", "sending_ack",
		"timeout", h.cfg.AckTxTimeout.String(),
	)

	ackCtx, ackCancel := context.WithTimeout(ctx, h.cfg.AckTxTimeout)
	defer ackCancel()

	if err := h.chainClient.AcknowledgeJob(ackCtx, jobID); err != nil {
		acknowledged, checkErr := h.chainClient.HasJobAcknowledged(ctx, jobID)
		if checkErr != nil {
			return fmt.Errorf("acknowledge job: %w (recheck failed: %v)", err, checkErr)
		}
		if acknowledged {
			logger.Warn("stage 1: ack tx returned error but job is acknowledged on-chain",
				"stage", "ack",
				"error", err,
			)
			return nil
		}
		return fmt.Errorf("acknowledge job: %w", err)
	}

	return nil
}

func (h *JobHandler) completeJob(
	ctx context.Context,
	logger *slog.Logger,
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
			logger.Warn("stage 8b: completeJob tx returned error but job is completed on-chain",
				"stage", "complete_job",
				"error", err,
			)
			return nil
		}
		return fmt.Errorf("complete job: %w", err)
	}

	return nil
}

// getOrDeriveSessionKey tries the cache first, then derives from chain on miss.
func (h *JobHandler) getOrDeriveSessionKey(ctx context.Context, logger *slog.Logger, sessionID uint64) ([]byte, error) {
	key, err := h.keyStore.GetKey(sessionID)
	if err == nil {
		logger.Info("stage 3: session key cache hit",
			"stage", "session_key",
			"path", "cache_hit",
		)
		return key, nil
	}
	logger.Info("stage 3: session key cache miss, deriving from chain",
		"stage", "session_key",
		"path", "chain_derive",
	)
	return h.deriveAndStoreSessionKey(ctx, logger, sessionID)
}

// refreshSessionKey skips the local cache and derives the latest session key
// directly from chain events. Used on decryption failure to pick up a rotated
// session key.
func (h *JobHandler) refreshSessionKey(ctx context.Context, logger *slog.Logger, sessionID uint64) ([]byte, error) {
	logger.Info("stage 4: refreshing session key from chain (possible rotation)",
		"stage", "session_key",
		"path", "refresh",
	)
	return h.deriveAndStoreSessionKey(ctx, logger, sessionID)
}

// deriveAndStoreSessionKey fetches the latest encrypted worker key for the
// session from chain, decrypts it with the worker's ECDH private key, and
// stores the result in the local keystore. The on-chain query reads session
// storage directly via GetSession, so this always yields the current live
// key whether set at creation or after rotation.
func (h *JobHandler) deriveAndStoreSessionKey(ctx context.Context, logger *slog.Logger, sessionID uint64) ([]byte, error) {
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
		logger.Warn("failed to persist session key",
			"stage", "session_key",
			"error", err,
		)
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

// publishToRedis signs and publishes the response via the configured ResponsePublisher.
// Errors are logged but non-fatal — the on-chain blob is the authoritative response.
func (h *JobHandler) publishToRedis(
	ctx context.Context,
	logger *slog.Logger,
	jobID, sessionID uint64,
	correlationID string,
	ciphertext []byte,
) {
	if h.responsePublisher == nil {
		return
	}

	sig, err := signMismatchEvidence(h.cfg.ChainID, h.cfg.JobRegistryAddr, jobID, sessionID, ciphertext, h.signingKey)
	if err != nil {
		h.logger.Warn("failed to sign response", "jobID", jobID, "error", err)
		return
	}

	sigHex := "0x" + hex.EncodeToString(sig)
	h.responsePublisher.PublishResponse(ctx, jobID, sessionID, correlationID, sigHex, ciphertext)
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

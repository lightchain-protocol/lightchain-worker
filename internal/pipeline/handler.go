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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
	"github.com/lightchain/worker/internal/search"
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
	GenerateStream(ctx context.Context, model, prompt string, onDelta func(string)) (string, error)
	ChatStream(ctx context.Context, model string, messages []ollama.ChatMessage, onDelta func(string)) (string, error)
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
	// GetSessionEncWorkerKey retrieves the current encrypted worker key for a session
	// from JobRegistry session storage. Returns the latest key whether set at creation
	// or after rotation via updateSessionKey. Rejects sessions that are not active.
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
	AckTxTimeout        time.Duration
	BlobTxTimeout       time.Duration
	RedisPublishTimeout time.Duration
	ModelIDToName       map[string]string
	ChainID             *big.Int
	JobRegistryAddr     common.Address
	SearchMaxResults    int
	SearchTimeout       time.Duration
}

// ResponsePublisher publishes encrypted responses for real-time delivery.
// Implementations: RedisResponsePublisher (direct Redis PUBLISH) and
// gateway-based publisher (POST to worker-gateway).
type ResponsePublisher interface {
	PublishResponse(ctx context.Context, jobID, sessionID uint64, correlationID string, signature string, ciphertext []byte)
	PublishMetadata(ctx context.Context, jobID, sessionID uint64, correlationID string, payload []byte)
	PublishChunk(ctx context.Context, jobID, sessionID uint64, correlationID string, sequence uint32, payload []byte)
}

// ReleaseTracker records that a job has just been completed and is now
// awaiting on-chain release. Defined at the consumer (pipeline) per Go
// conventions; release.Tracker satisfies it. Optional — the handler runs
// fine when nil and the periodic reconciler will backfill missed writes.
type ReleaseTracker interface {
	MarkEligible(ctx context.Context, jobID uint64, completedAt int64) error
}

// JobHandler processes inference jobs received from Asynq or directly from
// the gateway via HandleJobPayload.
type JobHandler struct {
	chainClient       JobExecutionClient
	blobFetcher       BlobFetcher
	blobSubmitter     BlobSubmitter
	keyStore          SessionKeyGetter
	ollamaClient      InferenceClient
	searcher          search.Searcher // nil ⇒ web search disabled
	redisClient       *redis.Client
	responsePublisher ResponsePublisher
	signingKey        *ecdsa.PrivateKey
	ecdhKey           *ecdh.PrivateKey
	jobCounter        *atomic.Int32
	logger            *slog.Logger
	cfg               HandlerConfig
	modelIDToName     map[string]string
	// metrics is the per-process Prometheus surface. Always non-nil — service.New
	// constructs one and threads it through. Tests construct their own via
	// metrics.New(testModels) for isolation.
	metrics *metrics.Metrics
	// delivery labels every metric emission so the asynq vs gateway paths
	// are distinguishable in dashboards. Set once at construction.
	delivery string
	// checkpoints is optional. When nil, the handler runs without retry
	// caching. In direct (Asynq) mode the cache prevents re-running stages
	// 2-6 on retries; in gateway mode it has the same effect if the gateway
	// redelivers a jobID after worker disconnect.
	checkpoints *CheckpointStore
	// releaseTracker is optional. When set (via SetReleaseTracker), the
	// handler records a stage-8b success so the release scheduler can
	// settle the job after the dispute window. Failure to write is
	// non-fatal — the periodic reconciler picks up missed writes.
	releaseTracker ReleaseTracker
	// keyFetchGroup coalesces concurrent cache-miss derivations for the same
	// session so that N parallel jobs trigger exactly one chain RPC. Only
	// getOrDeriveSessionKey uses it; refreshSessionKey deliberately bypasses
	// this group because its purpose is to defeat any cache after a rotation.
	keyFetchGroup singleflight.Group
}

// SetReleaseTracker installs the release tracker after construction.
// Service wiring calls this once before Run; tests may leave it nil.
func (h *JobHandler) SetReleaseTracker(t ReleaseTracker) {
	h.releaseTracker = t
}

// NewJobHandler creates a handler wired with all dependencies. publisher
// may be nil — when nil and redisClient is non-nil, a RedisResponsePublisher
// is auto-built. checkpoints may be nil — when nil, the handler skips the
// retry-safety cache (stages 2-6 re-run on every retry).
//
// metricsCollector must be non-nil; service.New constructs it. delivery
// must be one of metrics.DeliveryAsynq or metrics.DeliveryGateway.
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
	publisher ResponsePublisher,
	checkpoints *CheckpointStore,
	metricsCollector *metrics.Metrics,
	delivery string,
	searcher search.Searcher,
) *JobHandler {
	modelIDToName := make(map[string]string, len(cfg.ModelIDToName))
	for k, v := range cfg.ModelIDToName {
		modelIDToName[normalizeModelLookupKey(k)] = v
	}

	if publisher == nil && redisClient != nil {
		publisher = &RedisResponsePublisher{
			client:         redisClient,
			logger:         logger,
			publishTimeout: cfg.RedisPublishTimeout,
			metrics:        metricsCollector,
		}
	}

	return &JobHandler{
		chainClient:       chainClient,
		blobFetcher:       blobFetcher,
		blobSubmitter:     blobSubmitter,
		keyStore:          keyStore,
		ollamaClient:      ollamaClient,
		searcher:          searcher,
		redisClient:       redisClient,
		responsePublisher: publisher,
		signingKey:        signingKey,
		ecdhKey:           ecdhKey,
		jobCounter:        jobCounter,
		logger:            logger,
		cfg:               cfg,
		modelIDToName:     modelIDToName,
		metrics:           metricsCollector,
		delivery:          delivery,
		checkpoints:       checkpoints,
	}
}

// modelLabel returns the bounded {model} label value for a JobPayload's
// ModelID. Two-step normalization: resolve to ollama tag first (so labels
// match what the worker actually invokes), then run through the metrics
// allowlist so unknown inputs collapse to ModelUnknown rather than blow
// up label cardinality.
func (h *JobHandler) modelLabel(modelID string) string {
	name, err := h.resolveModelName(modelID)
	if err != nil {
		return metrics.ModelUnknown
	}
	return h.metrics.NormalizeModel(name)
}

// RedisResponsePublisher publishes responses directly to Redis pub/sub.
// publishTimeout bounds each PUBLISH so a slow Redis cannot eat into stage 8's
// BlobTxTimeout budget (stage 7 is non-fatal but synchronous). When zero the
// publish runs with the caller-supplied context only.
type RedisResponsePublisher struct {
	client         *redis.Client
	logger         *slog.Logger
	publishTimeout time.Duration
	// metrics may be nil only in tests that exercise the publisher directly
	// without going through NewJobHandler. Production constructs always
	// pass a non-nil collector.
	metrics *metrics.Metrics
}

// PublishResponse builds the PubSubMessage and PUBLISHes it to the session's
// Redis channel. Errors are logged but non-fatal — the on-chain blob is the
// authoritative response. Failure counters are incremented when present so
// dashboards can track publish health independently of the on-chain path.
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
		if p.metrics != nil {
			p.metrics.RedisPublishFailures.Inc()
		}
		return
	}

	pubCtx := ctx
	if p.publishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}

	channel := fmt.Sprintf("session:%d:responses", sessionID)
	if err := p.client.Publish(pubCtx, channel, data).Err(); err != nil {
		p.logger.Warn("failed to publish response to Redis", "jobID", jobID, "channel", channel, "error", err)
		if p.metrics != nil {
			p.metrics.RedisPublishFailures.Inc()
		}
	}
}

// PublishMetadata fans out an encrypted metadata frame (e.g. web-search
// sources). Non-fatal — citations are best-effort UX, not the authoritative
// response. No signature: the relay only signature-checks complete frames.
func (p *RedisResponsePublisher) PublishMetadata(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	payload []byte,
) {
	msg := pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeMetadata,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      0,
		TotalChunks:   1,
		Payload:       payload,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		p.logger.Warn("failed to marshal metadata frame", "jobID", jobID, "error", err)
		return
	}
	pubCtx := ctx
	if p.publishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}
	channel := fmt.Sprintf("session:%d:responses", sessionID)
	if err := p.client.Publish(pubCtx, channel, data).Err(); err != nil {
		p.logger.Warn("failed to publish metadata frame", "jobID", jobID, "channel", channel, "error", err)
	}
}

// PublishChunk fans out an encrypted streaming chunk frame (best-effort UX).
// Non-fatal — chunk delivery failures are logged and skipped.
func (p *RedisResponsePublisher) PublishChunk(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	sequence uint32,
	payload []byte,
) {
	msg := pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeChunk,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      sequence,
		TotalChunks:   0,
		Payload:       payload,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		p.logger.Warn("failed to marshal chunk frame", "jobID", jobID, "seq", sequence, "error", err)
		return
	}
	pubCtx := ctx
	if p.publishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}
	channel := fmt.Sprintf("session:%d:responses", sessionID)
	if err := p.client.Publish(pubCtx, channel, data).Err(); err != nil {
		p.logger.Warn("failed to publish chunk frame", "jobID", jobID, "seq", sequence, "channel", channel, "error", err)
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
// Metrics: each stage observes worker_pipeline_stage_duration_seconds with
// {stage, model, cache, delivery, outcome} labels. End-to-end timing lands
// on worker_job_total_duration_seconds via the deferred observer below.
// The {cache} label on the job-total histogram is "hit" only if the
// checkpoint short-circuited stages 2-6 (the dominant inference cost).
//
// A child logger tagged with job/session/correlation IDs is threaded through
// every helper so each stage-boundary log line is automatically correlated
// in Cloud Logging. Every stage emits at minimum a completion log with a
// stable `stage` attribute and `durationMs` so slow/stuck stages are easy
// to locate when debugging production traces.
func (h *JobHandler) processJob(ctx context.Context, p JobPayload) (err error) {
	logger := h.logger.With(
		"jobID", p.JobID,
		"sessionID", p.SessionID,
		"correlationID", p.CorrelationID,
	)

	jobStart := time.Now()
	model := h.modelLabel(p.ModelID)
	delivery := h.delivery
	// cacheState reflects whether the dominant inference path was skipped.
	// Promoted to "hit" below if ckpt.HasCiphertext returns true.
	cacheState := metrics.CacheMiss

	defer func() {
		if r := recover(); r != nil {
			h.metrics.JobsTotal.WithLabelValues(
				metrics.OutcomePanic, metrics.ReasonPanic, model, delivery,
			).Inc()
			h.metrics.JobTotalDuration.WithLabelValues(
				cacheState, delivery, metrics.OutcomePanic,
			).Observe(time.Since(jobStart).Seconds())
			// Re-panic so asynq/gateway sees the failure and the stack
			// trace is preserved — silently swallowing would mask the bug.
			panic(r)
		}
		outcome := metrics.OutcomeFor(err)
		reason := metrics.ClassifyError(err)
		h.metrics.JobsTotal.WithLabelValues(outcome, reason, model, delivery).Inc()
		h.metrics.JobTotalDuration.WithLabelValues(
			cacheState, delivery, outcome,
		).Observe(time.Since(jobStart).Seconds())
	}()

	// Stage 1: ACK — acknowledge job on-chain
	rec := h.metrics.StartStage(metrics.StageAck, model, delivery)
	if ackErr := h.ensureAcknowledged(ctx, logger, p.JobID); ackErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheNone)
		err = fmt.Errorf("stage 1 (ack): %w", ackErr)
		return err
	}
	d := rec.End(metrics.OutcomeOK, metrics.CacheNone)
	logger.Info("stage 1 complete",
		"stage", "ack",
		"durationMs", d.Milliseconds(),
	)

	// Read checkpoint once after stage 1. The result determines whether
	// stages 2-6 (inference), 7 (redis publish), and 8a (blob submit) can
	// be skipped on a retry. Nil handler or Get error => treat as cache
	// miss and run the full pipeline — same as pre-checkpoint behavior.
	ckpt := h.readCheckpoint(ctx, logger, p.JobID)

	var ciphertext []byte

	if ckpt.HasCiphertext() {
		cacheState = metrics.CacheHit
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitInference).Inc()
		logger.Info("checkpoint hit, skipping stages 2-6",
			"stage", "checkpoint",
			"reason", "inference_cached",
			"ciphertextBytes", len(ckpt.Ciphertext),
		)
		ciphertext = ckpt.Ciphertext
	} else {
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventMiss).Inc()

		// Stage 2: Fetch the prompt blob from the consensus layer. Post-audit
		// each job carries a single bytes32 prompt blob hash.
		if p.PromptBlobHash == (common.Hash{}) {
			err = fmt.Errorf("stage 2 (fetch blob): prompt blob hash is zero")
			return err
		}
		rec = h.metrics.StartStage(metrics.StageFetchBlob, model, delivery)
		logger.Info("stage 2 starting",
			"stage", "fetch_blob",
			"promptBlobHash", p.PromptBlobHash.Hex(),
			"blockNumber", p.BlockNumber,
		)
		blobData, fetchErr := h.blobFetcher.FetchBlob(ctx, p.PromptBlobHash, p.BlockNumber)
		if fetchErr != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			err = fmt.Errorf("stage 2 (fetch blob %s): %w", p.PromptBlobHash.Hex(), fetchErr)
			return err
		}
		d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
		logger.Info("stage 2 complete",
			"stage", "fetch_blob",
			"blobBytes", len(blobData),
			"durationMs", d.Milliseconds(),
		)

		// Stages 3-6 produce ciphertext; they're wrapped in a helper so the
		// old inlined code keeps its exact stage-boundary logging but the
		// checkpoint branching stays readable.
		producedCiphertext, infErr := h.runInferencePipeline(ctx, logger, p, blobData, model, delivery)
		if infErr != nil {
			err = infErr
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
				h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventRaceLost).Inc()
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
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitDelivered).Inc()
		logger.Info("checkpoint hit, skipping stage 7",
			"stage", "checkpoint",
			"reason", "delivered",
		)
	} else {
		rec = h.metrics.StartStage(metrics.StageRedisPublish, model, delivery)
		h.publishToRedis(ctx, logger, p.JobID, p.SessionID, p.CorrelationID, ciphertext)
		d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
		logger.Info("stage 7 complete",
			"stage", "redis_publish",
			"durationMs", d.Milliseconds(),
		)
		if h.checkpoints != nil {
			if mErr := h.checkpoints.MarkDelivered(ctx, p.JobID); mErr != nil {
				logger.Warn("failed to mark delivered on checkpoint",
					"stage", "checkpoint",
					"error", mErr,
				)
			}
		}
	}

	// Stage 8a: Submit blob TX.
	versionedHash, blobErr := h.ensureBlobSubmitted(ctx, logger, p.JobID, ckpt, ciphertext, model, delivery)
	if blobErr != nil {
		err = blobErr
		return err
	}

	// Stage 8b: Complete job on-chain.
	rec = h.metrics.StartStage(metrics.StageCompleteJob, model, delivery)
	responseCiphertextHash := crypto.Keccak256Hash(ciphertext)
	logger.Info("stage 8b starting",
		"stage", "complete_job",
		"versionedHash", versionedHash.Hex(),
		"ciphertextHash", responseCiphertextHash.Hex(),
	)
	if cjErr := h.completeJob(ctx, logger, p.JobID, versionedHash, responseCiphertextHash); cjErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheNone)
		err = fmt.Errorf("stage 8 (complete job): %w", cjErr)
		return err
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheNone)
	logger.Info("stage 8b complete",
		"stage", "complete_job",
		"durationMs", d.Milliseconds(),
	)

	// Mark the job as eligible for the release scheduler. Best-effort:
	// completedAt here is wall-clock time. The reconciler later overwrites
	// it with the authoritative on-chain Job.completedAt, and the
	// scheduler always re-reads on-chain state via GetJobState before
	// releasing — so a slightly off local timestamp can only delay
	// release, never cause a wrongful one. This call MUST be non-fatal:
	// a failure cannot fail the job (which would trigger asynq retry of
	// an already-completed on-chain job). Reconciler backs us up.
	if h.releaseTracker != nil {
		if mErr := h.releaseTracker.MarkEligible(ctx, p.JobID, time.Now().Unix()); mErr != nil {
			logger.Warn("failed to mark job eligible for release; reconciler will backfill",
				"stage", "release_tracker",
				"error", mErr,
			)
		}
	}

	// Tombstone the checkpoint after a successful stage 8b. A short TTL
	// lets any straggler retry observe "completed" before the record
	// disappears; outright Delete would race concurrent retries.
	if h.checkpoints != nil {
		if tErr := h.checkpoints.Tombstone(ctx, p.JobID); tErr != nil {
			logger.Warn("failed to tombstone checkpoint after completion",
				"stage", "checkpoint",
				"error", tErr,
			)
		} else {
			h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventTombstoned).Inc()
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
	model, delivery string,
) ([]byte, error) {
	// Stage 3: Get session key (cache miss → chain fetch → store)
	rec := h.metrics.StartStage(metrics.StageSessionKey, model, delivery)
	sessionKey, err := h.getOrDeriveSessionKey(ctx, logger, p.SessionID)
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return nil, fmt.Errorf("stage 3 (session key): %w", err)
	}
	d := rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info("stage 3 complete",
		"stage", "session_key",
		"durationMs", d.Milliseconds(),
	)

	// Stage 4: Decrypt prompt.
	//
	// On decryption failure, try refreshing the session key from chain once —
	// the session key may have been rotated via updateSessionKey.
	// GetSessionEncWorkerKey reads session storage directly, so a refresh
	// picks up the current key regardless of how it was set.
	rec = h.metrics.StartStage(metrics.StageDecrypt, model, delivery)
	prompt, err := pkgcrypto.Decrypt(sessionKey, blobData)
	if err != nil {
		logger.Warn("stage 4: initial decrypt failed, refreshing session key",
			"stage", "decrypt",
			"error", err,
		)
		refreshed, refreshErr := h.refreshSessionKey(ctx, logger, p.SessionID)
		if refreshErr != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, fmt.Errorf("stage 4 (decrypt prompt): %w (refresh failed: %v)", err, refreshErr)
		}
		sessionKey = refreshed
		prompt, err = pkgcrypto.Decrypt(sessionKey, blobData)
		if err != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, fmt.Errorf("stage 4 (decrypt prompt after key refresh): %w", err)
		}
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info("stage 4 complete",
		"stage", "decrypt",
		"promptBytes", len(prompt),
		"durationMs", d.Milliseconds(),
	)

	// Stage 4.5: optional web-search augmentation (one-shot Tavily). Fail-open:
	// any search failure proceeds with the original prompt and emits no sources.
	// Sources are stashed here and published AFTER inference (see end of function)
	// so the UI renders "Sources" beneath the answer, not above it.
	promptText := string(prompt)
	var searchSources []search.Source
	if p.SearchEnabled && h.searcher != nil {
		searchCtx := ctx
		if h.cfg.SearchTimeout > 0 {
			var cancel context.CancelFunc
			searchCtx, cancel = context.WithTimeout(ctx, h.cfg.SearchTimeout)
			defer cancel()
		}
		maxResults := h.cfg.SearchMaxResults
		if maxResults <= 0 {
			maxResults = 5
		}
		sources, sErr := h.searcher.Search(searchCtx, promptText, maxResults)
		if sErr != nil {
			logger.Warn("stage 4.5: web search failed, proceeding without context",
				"stage", "search", "error", sErr)
		} else if len(sources) > 0 {
			promptText = buildSearchAugmentedPrompt(promptText, sources)
			searchSources = sources // stash; publish after inference
			logger.Info("stage 4.5 complete", "stage", "search", "sources", len(sources))
		}
	}

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

	rec = h.metrics.StartStage(metrics.StageInference, model, delivery)
	logger.Info("stage 5 starting",
		"stage", "inference",
		"model", modelName,
		"promptBytes", len(prompt),
		"historyTurns", len(history),
	)

	// Streaming chunk batching: flush when buffer reaches 24 bytes or at stream end.
	// seq is monotonically increasing from 1. Failures are non-fatal (best-effort UX).
	var seq uint32
	var chunkBuf strings.Builder
	flushChunk := func() {
		if chunkBuf.Len() == 0 {
			return
		}
		seq++
		if h.responsePublisher != nil {
			if enc, encErr := pkgcrypto.Encrypt(sessionKey, []byte(chunkBuf.String())); encErr == nil {
				h.responsePublisher.PublishChunk(ctx, p.JobID, p.SessionID, p.CorrelationID, seq, enc)
			} else {
				logger.Warn("chunk encrypt failed, skipping chunk", "seq", seq, "error", encErr)
			}
		}
		chunkBuf.Reset()
	}
	onDelta := func(d string) {
		chunkBuf.WriteString(d)
		if chunkBuf.Len() >= 24 {
			flushChunk()
		}
	}

	if len(history) > 0 {
		messages := append(history, ollama.ChatMessage{Role: "user", Content: promptText})
		response, err = h.ollamaClient.ChatStream(ctx, modelName, messages, onDelta)
		if err != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, fmt.Errorf("stage 5 (chat inference): %w", err)
		}
	} else {
		response, err = h.ollamaClient.GenerateStream(ctx, modelName, promptText, onDelta)
		if err != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, fmt.Errorf("stage 5 (inference): %w", err)
		}
	}
	flushChunk() // flush any remaining buffered delta
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info("stage 5 complete",
		"stage", "inference",
		"model", modelName,
		"responseBytes", len(response),
		"durationMs", d.Milliseconds(),
	)

	// Stage 6: Encrypt response
	rec = h.metrics.StartStage(metrics.StageEncrypt, model, delivery)
	ciphertext, err := pkgcrypto.Encrypt(sessionKey, []byte(response))
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return nil, fmt.Errorf("stage 6 (encrypt response): %w", err)
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info("stage 6 complete",
		"stage", "encrypt",
		"ciphertextBytes", len(ciphertext),
		"durationMs", d.Milliseconds(),
	)

	// Emit citations AFTER the answer is ready so the UI renders Sources
	// beneath the response (best-effort, non-fatal).
	if len(searchSources) > 0 && h.responsePublisher != nil {
		if metaPayload, encErr := pkgcrypto.Encrypt(sessionKey, sourcesMetadataJSON(searchSources)); encErr != nil {
			logger.Warn("post-inference: encrypt sources failed", "stage", "search", "error", encErr)
		} else {
			h.responsePublisher.PublishMetadata(ctx, p.JobID, p.SessionID, p.CorrelationID, metaPayload)
		}
	}

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

// ensureBlobSubmitted is the stage-8a equivalent of ensureAcknowledged: it
// returns the cached versioned hash from the checkpoint when stage 8a has
// already run on a prior attempt, or it submits the blob tx (with the
// per-stage BlobTxTimeout that prevents a stuck WaitMined from holding the
// broadcast slot indefinitely) and persists the resulting hash. Post-audit
// the contract's completeJob takes a single bytes32 responseBlobHash and
// enforces blobhash(0) == responseBlobHash, so the blob tx must carry
// exactly one blob — that invariant is checked here.
func (h *JobHandler) ensureBlobSubmitted(
	ctx context.Context,
	logger *slog.Logger,
	jobID uint64,
	ckpt JobCheckpoint,
	ciphertext []byte,
	model, delivery string,
) (common.Hash, error) {
	if ckpt.HasVersionedHash() {
		// Cache hit: short-circuit. Record an outcome=skipped sample so
		// dashboards can count "stage 8a skipped on retry" against total
		// invocations.
		rec := h.metrics.StartStage(metrics.StageSubmitBlob, model, delivery)
		rec.End(metrics.OutcomeSkipped, metrics.CacheHit)
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitBlob).Inc()
		logger.Info("checkpoint hit, skipping stage 8a",
			"stage", "checkpoint",
			"reason", "blob_submitted",
			"versionedHash", ckpt.VersionedHash.Hex(),
		)
		return ckpt.VersionedHash, nil
	}

	rec := h.metrics.StartStage(metrics.StageSubmitBlob, model, delivery)
	logger.Info("stage 8a starting",
		"stage", "submit_blob",
		"ciphertextBytes", len(ciphertext),
		"timeout", h.cfg.BlobTxTimeout.String(),
	)
	submitCtx, submitCancel := context.WithTimeout(ctx, h.cfg.BlobTxTimeout)
	blobHashes, err := h.blobSubmitter.SubmitBlobTx(submitCtx, ciphertext)
	submitCancel()
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return common.Hash{}, fmt.Errorf("stage 8 (submit blob): %w", err)
	}
	if len(blobHashes) != 1 {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return common.Hash{}, fmt.Errorf("stage 8 (submit blob): expected exactly 1 blob hash, got %d", len(blobHashes))
	}
	versionedHash := common.Hash(blobHashes[0])
	d := rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info("stage 8a complete",
		"stage", "submit_blob",
		"versionedHash", versionedHash.Hex(),
		"durationMs", d.Milliseconds(),
	)
	if h.checkpoints != nil {
		if err := h.checkpoints.SetVersionedHash(ctx, jobID, versionedHash); err != nil {
			logger.Warn("failed to persist versionedHash on checkpoint",
				"stage", "checkpoint",
				"error", err,
			)
		}
	}
	return versionedHash, nil
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
//
// Concurrent cache-miss callers for the same sessionID are coalesced via
// keyFetchGroup so that N parallel jobs trigger exactly one chain RPC instead
// of N. Followers wait for the leader's result; from their perspective the key
// was already in flight, so they are recorded as cache hits to keep the
// per-caller event count intact for dashboards.
func (h *JobHandler) getOrDeriveSessionKey(ctx context.Context, logger *slog.Logger, sessionID uint64) ([]byte, error) {
	if key, err := h.keyStore.GetKey(sessionID); err == nil {
		h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathCacheHit).Inc()
		logger.Info("stage 3: session key cache hit",
			"stage", "session_key",
			"path", "cache_hit",
		)
		return key, nil
	}

	leader := false
	v, err, _ := h.keyFetchGroup.Do(strconv.FormatUint(sessionID, 10), func() (any, error) {
		leader = true
		// Re-check the cache inside the flight: a sibling caller may have
		// just finished between our outer miss and entering Do.
		if k, err := h.keyStore.GetKey(sessionID); err == nil {
			h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathCacheHit).Inc()
			logger.Info("stage 3: session key cache hit (inside flight)",
				"stage", "session_key",
				"path", "cache_hit",
			)
			return k, nil
		}
		h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathChainDerive).Inc()
		logger.Info("stage 3: session key cache miss, deriving from chain",
			"stage", "session_key",
			"path", "chain_derive",
		)
		return h.deriveAndStoreSessionKey(ctx, logger, sessionID)
	})
	if err != nil {
		return nil, err
	}
	if !leader {
		// Follower: did not run the closure. Count as a cache hit so the
		// per-caller metric event is preserved.
		h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathCacheHit).Inc()
	}
	return v.([]byte), nil
}

// refreshSessionKey skips the local cache and derives the latest session key
// directly from chain. Used on decryption failure to pick up a rotated
// session key.
func (h *JobHandler) refreshSessionKey(ctx context.Context, logger *slog.Logger, sessionID uint64) ([]byte, error) {
	h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathRefresh).Inc()
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
		h.logger.Warn("failed to sign response",
			"stage", "redis_publish",
			"jobID", jobID,
			"error", err,
		)
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

// buildSearchAugmentedPrompt prepends a fixed-format context block built from
// web-search results to the user's prompt. The format is intentionally stable:
// dispute re-execution (v2) reproduces the prompt byte-for-byte from the
// captured sources, so DO NOT change this template without versioning it.
func buildSearchAugmentedPrompt(prompt string, sources []search.Source) string {
	if len(sources) == 0 {
		return prompt
	}
	var b strings.Builder
	b.WriteString("You have access to the following background context. Answer the question directly and naturally, as if from your own knowledge. ")
	b.WriteString("Do NOT mention this context, web searches, or \"search results\", and do NOT preface your answer by referring to them. Cite sources inline as [number] where relevant.\n\n")
	for _, s := range sources {
		fmt.Fprintf(&b, "[%d] %s\n%s\n%s\n\n", s.Position, s.Title, s.URL, s.Snippet)
	}
	b.WriteString("Question: ")
	b.WriteString(prompt)
	return b.String()
}

// sourcesMetadataJSON renders the citation payload in the exact shape the
// frontend's parseWebSearchSources expects.
func sourcesMetadataJSON(sources []search.Source) []byte {
	type wire struct {
		Position int    `json:"position"`
		Title    string `json:"title"`
		URL      string `json:"url"`
		Snippet  string `json:"snippet"`
	}
	out := struct {
		Type    string `json:"type"`
		Sources []wire `json:"sources"`
	}{Type: "webSearchSources"}
	for _, s := range sources {
		out.Sources = append(out.Sources, wire{s.Position, s.Title, s.URL, s.Snippet})
	}
	data, _ := json.Marshal(out)
	return data
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

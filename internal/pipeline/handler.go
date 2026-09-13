package pipeline

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/lightchain/pkg/searchaug"
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
}

// StreamingInferenceClient is the optional token-streaming extension of
// InferenceClient. *ollama.OllamaClient satisfies it. The handler probes
// for it at stage 5 and silently falls back to the batch methods when the
// configured client does not implement it, so an InferenceClient that only
// knows Generate/Chat keeps working unchanged.
//
// Both methods return the full accumulated response â€” byte-identical to the
// concatenation of every delta passed to OnToken â€” so the caller can stream
// deltas to the user and still encrypt one canonical full response for
// settlement. Reasoning deltas go to OnThinking and are deliberately absent
// from that return value; letting them in would change the hash committed by
// completeJob.
type StreamingInferenceClient interface {
	GenerateStream(ctx context.Context, model, prompt string, images []string, h ollama.StreamHandlers) (string, ollama.StreamStats, error)
	ChatStream(ctx context.Context, model string, messages []ollama.ChatMessage, h ollama.StreamHandlers) (string, ollama.StreamStats, error)
}

// overridingInferenceClient is the optional per-job override extension of
// InferenceClient, satisfied by *ollama.OllamaClient. When cfg.ModelOptions
// has an entry for the job's model, stage 5 runs on a WithOverrides copy
// instead of the shared client. A client without it logs a warning and runs
// unoverridden — production always wires *ollama.OllamaClient, so the
// fallback exists for tests and hypothetical alternative clients.
type overridingInferenceClient interface {
	WithOverrides(mutate func(*ollama.ClientOptions)) *ollama.OllamaClient
}

// JobExecutionClient submits job lifecycle transactions on-chain.
type JobExecutionClient interface {
	AcknowledgeJob(ctx context.Context, jobID uint64) error
	// CompleteJob submits a completeJob TX with a single bytes32 response blob hash.
	// The contract enforces `blobhash(0) == responseBlobHash`, so the caller must
	// have submitted exactly one blob in the same (blob-carrying) transaction.
	CompleteJob(ctx context.Context, jobID uint64, responseBlobHash, responseCiphertextHash [32]byte) error
	HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error)
	HasJobCompleted(ctx context.Context, jobID uint64) (bool, error)
	// GetSessionEncWorkerKey retrieves the current encrypted worker key for a session
	// from JobRegistry session storage. Returns the latest key whether set at creation
	// or after rotation via updateSessionKey. Rejects sessions that are not active.
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)
	// GetJobBlobInfo returns the single prompt and response blob hashes for a
	// completed job along with the blocks they were submitted in. Used to build
	// conversation history.
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash, responseHash common.Hash, submitBlock, completionBlock uint64, err error)
}

// AsyncAckClient is the optional non-blocking extension of
// JobExecutionClient. *chain.ChainClient satisfies it. Stage 1 probes for it
// and falls back to the blocking AcknowledgeJob when the configured client
// does not implement it, so a JobExecutionClient that only knows the
// original method keeps working unchanged.
//
// AcknowledgeJobAsync returns once the acknowledgement is on the wire,
// along with its tx hash and a join function that blocks until the tx is
// mined with receipt status 1. The join function must be called exactly
// once: the implementation may hold broadcast resources until it runs.
//
// The join is a plain func rather than an interface value so this package
// stays free of any dependency on the chain package.
type AsyncAckClient interface {
	AcknowledgeJobAsync(ctx context.Context, jobID uint64) (common.Hash, func(context.Context) error, error)
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

	// AckOverlapEnabled lets stage 1 return as soon as the acknowledge tx
	// is broadcast, so stages 2-6 run during the block it mines in rather
	// than after. It only takes effect when the chain client implements
	// AsyncAckClient. Stage 1b joins the confirmation before anything is
	// published or settled â€” see processJob. When off, stage 1 blocks on
	// the receipt exactly as it did before overlap existed.
	//
	// AckTxTimeout bounds broadcast and confirmation together, so the
	// overlap does not widen the window against the contract's
	// submit-time ack deadline.
	AckOverlapEnabled bool

	// StreamEnabled turns on incremental delivery at stage 5. It only
	// takes effect when the inference client implements
	// StreamingInferenceClient and the publisher reports SupportsChunks.
	// When off, the pipeline behaves exactly as it did before streaming
	// existed: one terminal frame at the end of generation.
	StreamEnabled bool
	// StreamReasoning forwards a reasoning model's chain of thought on its
	// own frame kind instead of discarding it. Off by default: a consumer
	// that cannot render reasoning would pay for frames it drops.
	StreamReasoning bool
	// StreamChunkTokens / StreamChunkInterval are the coalescing window.
	// A chunk frame goes out when either threshold is reached. Zero
	// values fall back to the tokenCoalescer defaults.
	StreamChunkTokens   int
	StreamChunkInterval time.Duration

	// ModelOptions maps an Ollama tag to per-model generation-option
	// overrides (config MODEL_OPTIONS; heat-tier enforcement). At stage 5 a
	// job for a listed model runs on a WithOverrides copy of the inference
	// client; models without an entry use the shared client unchanged, so
	// an empty map reproduces pre-override request bodies byte for byte.
	ModelOptions map[string]ollama.ClientOptions

	// DeadlineGuardEnabled turns on the on-chain deadline guard: the
	// pipeline reads job.deadline at pickup, before inference, and before
	// the stage-8a blob tx, and aborts with a no-retry error when the job
	// can no longer settle (completeJob reverts DeadlineExceeded past the
	// deadline). Only takes effect when the chain client implements
	// JobDeadlineClient. See pipeline/deadline.go.
	DeadlineGuardEnabled bool
	// SettleReserve is the time the guard keeps in hand after generation
	// ends for encrypt + publish + blob tx + completeJob. Zero falls back
	// to defaultSettleReserve.
	SettleReserve time.Duration
	// CompletionReserve is the minimum remaining window required to start
	// stage 8a. Zero falls back to defaultCompletionReserve.
	CompletionReserve time.Duration
	// MinInferenceBudget is the smallest generation window worth starting;
	// an acknowledged job with less than SettleReserve+MinInferenceBudget
	// left is aborted before inference. Zero falls back to
	// defaultMinInferenceBudget.
	MinInferenceBudget time.Duration

	// Voice I/O via local sidecars (whisper STT, Kokoro TTS). Both default
	// off and additionally require an installed VoiceEngine (see
	// SetVoiceEngine) and per-request opt-in fields in the prompt envelope
	// (v2). See pipeline/voice.go for the settlement posture.
	//
	// STTEnabled lets audio prompts be transcribed before inference.
	// TTSEnabled lets an opted-in response be rendered to speech and
	// streamed to the consumer as `audio` frames (PCM chunks bracketed by
	// JSON descriptors); the audio never enters the settlement ciphertext.
	// TTSVoice is the worker default when the envelope does not name a
	// voice. TTSMaxChars truncates the synthesized text (the settled text
	// answer is unaffected). TTSTimeout bounds one synthesis call; zero
	// falls back to 20 s. It also feeds the deadline budget check together
	// with a fixed stream margin.
	STTEnabled  bool
	TTSEnabled  bool
	TTSVoice    string
	// SpeechModelName is the model whose jobs settle audio rather than
	// text. Empty leaves the speech path inert.
	SpeechModelName string
	TTSMaxChars int
	TTSTimeout  time.Duration
	SearchMaxResults    int
	SearchTimeout       time.Duration
}

// ResponsePublisher publishes encrypted responses for real-time delivery.
// Implementations: RedisResponsePublisher (direct Redis PUBLISH) and
// gateway-based publisher (POST to worker-gateway).
//
// Frame contract, matching the relay's existing streaming envelope:
//
//   - PublishChunk emits type=chunk with Sequence 1..N and TotalChunks 0.
//     Zero means "not yet known" â€” the worker cannot know how many chunks
//     a generation will produce until it ends.
//   - PublishResponse emits the single terminal type=complete frame with
//     Sequence == TotalChunks == totalFrames, where totalFrames counts
//     every frame published for the response including this one. A
//     non-streamed response is therefore Sequence=1, TotalChunks=1.
//
// Only the terminal frame carries a signature. It is the EIP-191 signature
// over the FULL response ciphertext â€” the same bytes anchored in the blob
// and hashed into completeJob â€” so it stays verifiable against
// JobRegistry.disputeResponseMismatch. Chunk frames deliberately carry no
// signature: there is no on-chain commitment to an individual delta, and
// inventing a per-chunk scheme would produce evidence the contract cannot
// check.
//
// Chunk frames additionally carry a FrameKind naming which content channel
// the delta belongs to. Sequence stays global across kinds so the relay's gap
// detection and the terminal frame's totalFrames accounting keep working; the
// consumer demultiplexes on Kind. Only FrameKindText accumulates into the
// settlement ciphertext.
type ResponsePublisher interface {
	// PublishResponse publishes the terminal `complete` frame.
	PublishResponse(ctx context.Context, jobID, sessionID uint64, correlationID string, signature string, ciphertext []byte, totalFrames uint32)

	// PublishChunk publishes one in-flight `chunk` frame carrying a
	// single coalesced, independently AES-GCM-encrypted delta of the
	// given kind.
	PublishChunk(ctx context.Context, jobID, sessionID uint64, correlationID string, sequence uint32, kind pkgtypes.FrameKind, ciphertext []byte)

	// SupportsChunks reports whether PublishChunk actually reaches the
	// consumer as a distinct `chunk` frame. Publishers that cannot emit
	// one return false and the handler skips per-chunk encryption
	// entirely rather than doing work that gets dropped downstream.
	SupportsChunks() bool

	// PublishMetadata publishes search citations (best-effort).
	PublishMetadata(ctx context.Context, jobID, sessionID uint64, correlationID string, payload []byte)
}

// ReleaseTracker records that a job has just been completed and is now
// awaiting on-chain release. Defined at the consumer (pipeline) per Go
// conventions; release.Tracker satisfies it. Optional â€” the handler runs
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
	// metrics is the per-process Prometheus surface. Always non-nil â€” service.New
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
	// non-fatal â€” the periodic reconciler picks up missed writes.
	releaseTracker ReleaseTracker
	// voiceEngine is optional. When set (via SetVoiceEngine) and the
	// matching cfg gate is on, stage 5 can transcribe an audio prompt
	// (STT) and stage 6b can deliver a spoken rendering of the response
	// (TTS). Nil leaves every voice path inert.
	voiceEngine VoiceEngine
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
// may be nil â€” when nil and redisClient is non-nil, a RedisResponsePublisher
// is auto-built. checkpoints may be nil â€” when nil, the handler skips the
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

// inferenceClientFor returns the inference client stage 5 must use for
// modelName plus the effective per-request options that client will send.
//
// When cfg.ModelOptions has an entry for the model (heat-tier Max aliases),
// the job runs on a WithOverrides copy carrying only the listed knobs; every
// other model gets the shared client untouched, so without MODEL_OPTIONS the
// request bodies are byte-identical to before. The returned options feed the
// stage-5 audit log fields and the stats frame's appliedMaxTokens; they are
// zero when the client cannot report them (non-Ollama test doubles).
func (h *JobHandler) inferenceClientFor(logger *slog.Logger, modelName string) (InferenceClient, ollama.ClientOptions, bool) {
	report := func(c InferenceClient) ollama.ClientOptions {
		if oc, ok := c.(interface{ Options() ollama.ClientOptions }); ok {
			return oc.Options()
		}
		return ollama.ClientOptions{}
	}

	override, ok := h.cfg.ModelOptions[modelName]
	if !ok {
		return h.ollamaClient, report(h.ollamaClient), false
	}
	oc, ok := h.ollamaClient.(overridingInferenceClient)
	if !ok {
		logger.Warn("MODEL_OPTIONS entry configured but the inference client cannot apply overrides; running with process defaults",
			"stage", "inference",
			"model", modelName,
		)
		return h.ollamaClient, report(h.ollamaClient), false
	}
	client := oc.WithOverrides(func(o *ollama.ClientOptions) {
		if override.NumPredict != 0 {
			o.NumPredict = override.NumPredict
		}
		if override.NumCtx != 0 {
			o.NumCtx = override.NumCtx
		}
		if override.Temperature != nil {
			o.Temperature = override.Temperature
		}
	})
	return client, client.Options(), true
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

// PublishResponse builds the terminal `complete` PubSubMessage and PUBLISHes
// it to the session's Redis channel. Errors are logged but non-fatal â€” the
// on-chain blob is the authoritative response. Failure counters are
// incremented when present so dashboards can track publish health
// independently of the on-chain path.
//
// totalFrames is the number of frames published for this response including
// this one, so a non-streamed response passes 1 and reproduces the
// pre-streaming envelope's TotalChunks.
func (p *RedisResponsePublisher) PublishResponse(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	signature string,
	ciphertext []byte,
	totalFrames uint32,
) {
	if totalFrames == 0 {
		totalFrames = 1
	}
	p.publish(ctx, pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeComplete,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      totalFrames,
		TotalChunks:   totalFrames,
		Payload:       ciphertext,
		Signature:     signature,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	})
}

// PublishChunk PUBLISHes one in-flight `chunk` frame. TotalChunks stays 0
// because the chunk count is not known until generation ends, and the frame
// is unsigned â€” see the ResponsePublisher doc comment.
func (p *RedisResponsePublisher) PublishChunk(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	sequence uint32,
	kind pkgtypes.FrameKind,
	ciphertext []byte,
) {
	p.publish(ctx, pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeChunk,
		Kind:          kind.Normalize(),
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      sequence,
		TotalChunks:   0,
		Payload:       ciphertext,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	})
}

// SupportsChunks is always true: the relay subscribes to the same channel
// and forwards every frame immediately without batching or re-typing.
func (p *RedisResponsePublisher) SupportsChunks() bool { return true }

// publish marshals and PUBLISHes one frame, bounded by publishTimeout so a
// slow Redis cannot eat into the caller's remaining stage budget.
func (p *RedisResponsePublisher) publish(ctx context.Context, msg pkgtypes.PubSubMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		p.logger.Warn("failed to marshal response for Redis",
			"jobID", uint64(msg.JobID),
			"type", msg.Type,
			"error", err,
		)
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

	channel := fmt.Sprintf("session:%d:responses", uint64(msg.SessionID))
	if err := p.client.Publish(pubCtx, channel, data).Err(); err != nil {
		p.logger.Warn("failed to publish response to Redis",
			"jobID", uint64(msg.JobID),
			"type", msg.Type,
			"channel", channel,
			"error", err,
		)
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

// HandleJobPayload processes a job from a raw JobPayload (used in gateway mode
// where jobs come via HTTP polling instead of Asynq).
func (h *JobHandler) HandleJobPayload(ctx context.Context, payload JobPayload) error {
	h.jobCounter.Add(1)
	defer h.jobCounter.Add(-1)

	h.logger.Info(
		"processing job",
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
	// timeout. The worker does not control this ceiling â€” the dispatcher
	// passes it via ctx â€” so we log it and warn loudly if it looks too
	// tight for the pipeline's own stage timeouts.
	deadline, hasDeadline := ctx.Deadline()
	budget := time.Duration(0)
	if hasDeadline {
		budget = time.Until(deadline)
	}

	h.logger.Info(
		"processing job",
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
			h.logger.Warn(
				"task budget appears too small for the pipeline",
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
		h.logger.Error(
			"job failed",
			"jobID", payload.JobID,
			"attempt", retryCount+1,
			"maxRetry", maxRetry,
			"willRetry", retryCount < maxRetry,
			"error", err,
		)
		return err
	}

	h.logger.Info(
		"job completed",
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
	tier, _ := metrics.TierForModel(model)
	delivery := h.delivery
	// cacheState reflects whether the dominant inference path was skipped.
	// Promoted to "hit" below if ckpt.HasCiphertext returns true.
	cacheState := metrics.CacheMiss

	defer func() {
		if r := recover(); r != nil {
			h.metrics.JobsTotal.WithLabelValues(
				metrics.OutcomePanic, metrics.ReasonPanic, model, tier, delivery,
			).Inc()
			h.metrics.JobTotalDuration.WithLabelValues(
				cacheState, delivery, metrics.OutcomePanic,
			).Observe(time.Since(jobStart).Seconds())
			// Re-panic so asynq/gateway sees the failure and the stack
			// trace is preserved â€” silently swallowing would mask the bug.
			panic(r)
		}
		outcome := metrics.OutcomeFor(err)
		reason := metrics.ClassifyError(err)
		h.metrics.JobsTotal.WithLabelValues(outcome, reason, model, tier, delivery).Inc()
		h.metrics.JobTotalDuration.WithLabelValues(
			cacheState, delivery, outcome,
		).Observe(time.Since(jobStart).Seconds())
	}()

	// Stage 0: on-chain deadline guard. completeJob reverts DeadlineExceeded
	// once the job's deadline passes, so a job that is already expired - or
	// (retries/redeliveries) too close to expiry to finish inference plus
	// settlement - is abandoned here, before the ack tx burns gas. Nil when
	// disabled or when the chain client cannot report deadlines.
	guard := h.newDeadlineGuard(p.JobID)
	if guardErr := guard.checkPickup(ctx, logger); guardErr != nil {
		err = guardErr
		return err
	}

	// Stage 1: ACK â€” acknowledge job on-chain. With overlap enabled this
	// covers the broadcast only and returns a handle joined at stage 1b
	// below; the ack mines while stages 2-6 run.
	rec := h.metrics.StartStage(metrics.StageAck, model, delivery)
	ack, ackErr := h.ensureAcknowledged(ctx, logger, p.JobID)
	if ackErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheNone)
		err = fmt.Errorf("stage 1 (ack): %w", ackErr)
		return err
	}
	d := rec.End(metrics.OutcomeOK, metrics.CacheNone)
	logger.Info(
		"stage 1 complete",
		"stage", "ack",
		"overlapped", ack != nil,
		"durationMs", d.Milliseconds(),
	)

	// Read checkpoint once after stage 1. The result determines whether
	// stages 2-6 (inference), 7 (redis publish), and 8a (blob submit) can
	// be skipped on a retry. Nil handler or Get error => treat as cache
	// miss and run the full pipeline â€” same as pre-checkpoint behavior.
	ckpt := h.readCheckpoint(ctx, logger, p.JobID)

	var ciphertext []byte
	// chunkFrames counts the in-flight `chunk` frames stage 5 streamed.
	// It stays 0 on a checkpoint hit (inference did not re-run) and feeds
	// the terminal frame's Sequence/TotalChunks below.
	var chunkFrames uint32

	if ckpt.HasCiphertext() {
		cacheState = metrics.CacheHit
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitInference).Inc()
		logger.Info(
			"checkpoint hit, skipping stages 2-6",
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
		logger.Info(
			"stage 2 starting",
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
		logger.Info(
			"stage 2 complete",
			"stage", "fetch_blob",
			"blobBytes", len(blobData),
			"durationMs", d.Milliseconds(),
		)

		// Stages 3-6 produce ciphertext; they're wrapped in a helper so the
		// old inlined code keeps its exact stage-boundary logging but the
		// checkpoint branching stays readable.
		// ckpt.Delivered means a terminal frame already reached the
		// consumer on an earlier attempt, so re-streaming deltas now
		// would append text after the user already saw the final
		// answer. Suppress the deltas; the terminal frame is skipped
		// below by the same flag.
		producedCiphertext, streamed, infErr := h.runInferencePipeline(
			ctx, logger, p, blobData, model, tier, delivery, !ckpt.Delivered, guard, jobStart,
		)
		if infErr != nil {
			err = infErr
			return err
		}
		ciphertext = producedCiphertext
		chunkFrames = streamed

		// Persist canonical ciphertext. Under a concurrent retry on the
		// same jobID, SetCiphertextIfAbsent returns the earlier writer's
		// bytes â€” we overwrite our local with canonical so stage 7's
		// publish and stage 8b's hash match what the user already received.
		if h.checkpoints != nil {
			canonical, wasSet, cErr := h.checkpoints.SetCiphertextIfAbsent(ctx, p.JobID, ciphertext)
			switch {
			case cErr != nil:
				logger.Warn(
					"failed to persist ciphertext to checkpoint",
					"stage", "checkpoint",
					"error", cErr,
				)
			case !wasSet:
				h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventRaceLost).Inc()
				logger.Warn(
					"checkpoint race lost, using canonical ciphertext",
					"stage", "checkpoint",
					"localBytes", len(ciphertext),
					"canonicalBytes", len(canonical),
				)
				ciphertext = canonical
			default:
				logger.Debug(
					"ciphertext persisted to checkpoint",
					"stage", "checkpoint",
					"ciphertextBytes", len(ciphertext),
				)
			}
		}
	}

	// Stage 1b: join the overlapped acknowledgement. Everything past this
	// point either shows the user a final answer or moves the job toward
	// settlement, and neither may happen on a job whose acknowledgement is
	// not confirmed on-chain. No-op when stage 1 already blocked.
	//
	// Normally instant: the ack mines in about one block while stages 2-6
	// take far longer, so the confirmation has long since landed.
	if ackErr := h.awaitAcknowledged(ctx, logger, p.JobID, ack, model, delivery); ackErr != nil {
		err = ackErr
		return err
	}

	// Stage 7: Publish to Redis (non-fatal on error). Skipped on retry if
	// the checkpoint records delivered=true so relay subscribers don't see
	// a duplicate complete frame.
	if ckpt.Delivered {
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitDelivered).Inc()
		logger.Info(
			"checkpoint hit, skipping stage 7",
			"stage", "checkpoint",
			"reason", "delivered",
		)
	} else {
		rec = h.metrics.StartStage(metrics.StageRedisPublish, model, delivery)
		// completePayload and ciphertext intentionally differ for v2 search jobs:
		// the relay complete frame carries the PLAIN answer (consumer contract),
		// while ciphertext stays the {answer,searchContext} envelope used by the
		// blob + on-chain responseCiphertextHash (the disputer reads it). Do NOT
		// unify these — see relayCompleteCiphertext.
		completePayload := ciphertext
		if sk, skErr := h.getOrDeriveSessionKey(ctx, logger, p.SessionID); skErr == nil {
			completePayload = h.relayCompleteCiphertext(sk, ciphertext)
		} // on key error, fall back to ciphertext (non-fatal; matches existing publish best-effort posture)
		h.publishToRedis(ctx, logger, p.JobID, p.SessionID, p.CorrelationID, completePayload, chunkFrames+1)
		d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
		logger.Info(
			"stage 7 complete",
			"stage", "redis_publish",
			"chunkFrames", chunkFrames,
			"durationMs", d.Milliseconds(),
		)
		if h.checkpoints != nil {
			if mErr := h.checkpoints.MarkDelivered(ctx, p.JobID); mErr != nil {
				logger.Warn(
					"failed to mark delivered on checkpoint",
					"stage", "checkpoint",
					"error", mErr,
				)
			}
		}
	}

	// Stage 8a: Submit blob TX.
	versionedHash, blobErr := h.ensureBlobSubmitted(ctx, logger, p.JobID, ckpt, ciphertext, model, delivery, guard)
	if blobErr != nil {
		err = blobErr
		return err
	}

	// Stage 8b: Complete job on-chain.
	rec = h.metrics.StartStage(metrics.StageCompleteJob, model, delivery)
	responseCiphertextHash := crypto.Keccak256Hash(ciphertext)
	logger.Info(
		"stage 8b starting",
		"stage", "complete_job",
		"versionedHash", versionedHash.Hex(),
		"ciphertextHash", responseCiphertextHash.Hex(),
	)
	if cjErr := h.completeJob(ctx, logger, p.JobID, versionedHash, responseCiphertextHash, guard); cjErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheNone)
		err = fmt.Errorf("stage 8 (complete job): %w", cjErr)
		return err
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheNone)
	logger.Info(
		"stage 8b complete",
		"stage", "complete_job",
		"durationMs", d.Milliseconds(),
	)

	// Deadline headroom (DO-2 item 5): how much of the on-chain completion
	// window was left when the job settled. Negative samples are
	// late-but-accepted completions and are themselves an alarm. One extra
	// eth_call per completed job; the guard fails open on read errors.
	if guard != nil {
		if deadline, _, ok := guard.read(ctx, logger, "deadline_headroom"); ok {
			h.metrics.JobDeadlineHeadroom.WithLabelValues(tier, model).
				Observe(time.Until(deadline).Seconds())
		}
	}

	// Mark the job as eligible for the release scheduler. Best-effort:
	// completedAt here is wall-clock time. The reconciler later overwrites
	// it with the authoritative on-chain Job.completedAt, and the
	// scheduler always re-reads on-chain state via GetJobState before
	// releasing â€” so a slightly off local timestamp can only delay
	// release, never cause a wrongful one. This call MUST be non-fatal:
	// a failure cannot fail the job (which would trigger asynq retry of
	// an already-completed on-chain job). Reconciler backs us up.
	if h.releaseTracker != nil {
		if mErr := h.releaseTracker.MarkEligible(ctx, p.JobID, time.Now().Unix()); mErr != nil {
			logger.Warn(
				"failed to mark job eligible for release; reconciler will backfill",
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
			logger.Warn(
				"failed to tombstone checkpoint after completion",
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
		logger.Warn(
			"failed to read checkpoint; treating as cache miss",
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
// will publish/submit, plus the number of in-flight `chunk` frames stage 5
// streamed to the consumer. All stage-boundary log lines are preserved
// verbatim against the pre-checkpoint implementation.
//
// streamDeltas is the caller's permission to emit chunk frames; it is false
// on a retry whose checkpoint already records a delivered terminal frame.
//
// guard is the per-job on-chain deadline guard; nil disables the gate and
// the inference-window clamp. When active it can abort the pipeline before
// the history build with a no-retry ErrJobDoomed error.
func (h *JobHandler) runInferencePipeline(
	ctx context.Context,
	logger *slog.Logger,
	p JobPayload,
	blobData []byte,
	model, tier, delivery string,
	streamDeltas bool,
	guard *deadlineGuard,
	jobStart time.Time,
) ([]byte, uint32, error) {
	// Stage 3: Get session key (cache miss â†’ chain fetch â†’ store)
	rec := h.metrics.StartStage(metrics.StageSessionKey, model, delivery)
	sessionKey, err := h.getOrDeriveSessionKey(ctx, logger, p.SessionID)
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return nil, 0, fmt.Errorf("stage 3 (session key): %w", err)
	}
	d := rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info(
		"stage 3 complete",
		"stage", "session_key",
		"durationMs", d.Milliseconds(),
	)

	// Stage 4: Decrypt prompt.
	//
	// On decryption failure, try refreshing the session key from chain once â€”
	// the session key may have been rotated via updateSessionKey.
	// GetSessionEncWorkerKey reads session storage directly, so a refresh
	// picks up the current key regardless of how it was set.
	rec = h.metrics.StartStage(metrics.StageDecrypt, model, delivery)
	prompt, err := pkgcrypto.Decrypt(sessionKey, blobData)
	if err != nil {
		logger.Warn(
			"stage 4: initial decrypt failed, refreshing session key",
			"stage", "decrypt",
			"error", err,
		)
		refreshed, refreshErr := h.refreshSessionKey(ctx, logger, p.SessionID)
		if refreshErr != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, 0, fmt.Errorf("stage 4 (decrypt prompt): %w (refresh failed: %v)", err, refreshErr)
		}
		sessionKey = refreshed
		prompt, err = pkgcrypto.Decrypt(sessionKey, blobData)
		if err != nil {
			rec.End(metrics.OutcomeError, metrics.CacheMiss)
			return nil, 0, fmt.Errorf("stage 4 (decrypt prompt after key refresh): %w", err)
		}
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info(
		"stage 4 complete",
		"stage", "decrypt",
		"promptBytes", len(prompt),
		"durationMs", d.Milliseconds(),
	)

	// Stage 4.5: optional web-search augmentation (one-shot Tavily). Fail-open:
	// any search failure proceeds with the original prompt and emits no sources.
	// Sources are stashed here and published AFTER inference (see end of function)
	// so the UI renders "Sources" beneath the answer, not above it.
	promptText, searchEnabled, decErr := searchaug.DecodePrompt(prompt)
	if decErr != nil {
		return nil, 0, fmt.Errorf("stage 4 (decode prompt envelope): %w", decErr)
	}
	var searchSources []searchaug.Source
	if searchEnabled && h.searcher != nil {
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
			augSources := toSearchaugSources(sources)
			promptText = searchaug.BuildAugmentedPrompt(searchaug.CurrentTemplateVersion, promptText, augSources)
			searchSources = augSources // stash; publish after inference
			logger.Info("stage 4.5 complete", "stage", "search", "sources", len(sources))
		}
	}

	// Stage 5: AI inference (with conversation history if prior jobs exist)
	modelName, err := h.resolveModelName(p.ModelID)
	if err != nil {
		return nil, 0, fmt.Errorf("stage 5 (resolve model): %w", err)
	}

	// Per-model generation-option overrides (MODEL_OPTIONS). Without an
	// entry for this model, infClient IS the shared client and behavior is
	// unchanged; effectiveOpts reports what will actually be sent, for the
	// stage-5 audit fields and the stats frame.
	infClient, effectiveOpts, overrideApplied := h.inferenceClientFor(logger, modelName)

	// Decode the search-processed text, not the raw blob: stage 4.5 has
	// already unwrapped a search envelope (and augmented it when sources
	// came back), while a multimodal envelope passes through searchaug
	// untouched and is parsed here as before.
	envelope, err := decodePrompt([]byte(promptText))
	if err != nil {
		return nil, 0, fmt.Errorf("stage 5 (decode prompt): %w", err)
	}

	// Deadline gate: an acknowledged job whose remaining window cannot
	// cover generation plus settlement aborts here with a no-retry error,
	// before the model load begins. Otherwise the inference context is
	// clamped to deadline-minus-settle-reserve so an over-running
	// generation (cold load, slow decode) is cut off while stage 8 could
	// still theoretically land - and the stage-8a gate makes the final
	// call before any blob gas is burned.
	infCtx, infCancel, gateErr := guard.gateInference(ctx, logger)
	if gateErr != nil {
		return nil, 0, gateErr
	}
	defer infCancel()

	// Voice input: an audio prompt is transcribed by the STT sidecar and
	// the transcript merged into the envelope text. Runs under the clamped
	// inference context so a slow sidecar cannot push the job past its
	// settlement window. Failure classifications (no-retry on bad audio vs
	// retryable transport errors) live in maybeTranscribeAudio.
	if err := h.maybeTranscribeAudio(infCtx, logger, &envelope); err != nil {
		return nil, 0, err
	}

	var history []ollama.ChatMessage
	if len(p.PriorJobIDs) > 0 {
		histStart := time.Now()
		var hErr error
		history, hErr = h.buildConversationHistory(infCtx, p.PriorJobIDs, sessionKey)
		if hErr != nil {
			logger.Warn(
				"failed to build conversation history, falling back to single prompt",
				"stage", "inference",
				"error", hErr,
			)
			history = nil
		} else {
			logger.Info("conversation history built",
				"stage", "build_history",
				"priorJobs", len(p.PriorJobIDs),
				"historyTurns", len(history),
				"durationMs", time.Since(histStart).Milliseconds(),
			)
		}
	}

	streamer := h.newChunkStreamer(infCtx, logger, p, sessionKey, streamDeltas, infClient)

	// Queue wait (DO-2 item 2): job pickup to inference start. This includes
	// stages 1-4, so it upper-bounds true queue wait; on retries it measures
	// the re-run of those stages. Checkpoint hits skip this path entirely.
	h.metrics.JobQueueWait.WithLabelValues(tier, model).Observe(time.Since(jobStart).Seconds())

	rec = h.metrics.StartStage(metrics.StageInference, model, delivery)
	startAttrs := []any{
		"stage", "inference",
		"model", modelName,
		"promptBytes", envelope.bytes(),
		"promptImages", len(envelope.Images),
		"historyTurns", len(history),
		"streaming", streamer != nil,
		"reasoning", h.cfg.StreamReasoning,
	}
	if overrideApplied {
		// Audit trail: the exact generation knobs this job runs under. A
		// paid Max job must show its raised cap here; absence of these
		// fields on a -max model means enforcement silently did not apply.
		startAttrs = append(startAttrs,
			"numPredict", effectiveOpts.NumPredict,
			"numCtx", effectiveOpts.NumCtx,
		)
		if effectiveOpts.Temperature != nil {
			startAttrs = append(startAttrs, "temperature", *effectiveOpts.Temperature)
		}
	}
	logger.Info("stage 5 starting", startAttrs...)
	response, stats, err := h.runInference(infCtx, logger, infClient, modelName, envelope, history, streamer)
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		var wrapped error
		if len(history) > 0 {
			wrapped = fmt.Errorf("stage 5 (chat inference): %w", err)
		} else {
			wrapped = fmt.Errorf("stage 5 (inference): %w", err)
		}
		// No-retry classifications. An empty generation repeats
		// deterministically under identical parameters, and a job whose
		// on-chain deadline passed mid-generation (e.g. the clamped
		// inference context fired during a cold model load) can no longer
		// settle - both are terminal for this job regardless of attempt
		// count, and retrying only delays the keeper refund.
		if errors.Is(err, ollama.ErrEmptyGeneration) {
			return nil, streamer.frames(), noRetry(wrapped)
		}
		if guard.isDoomedNow(ctx, logger) {
			return nil, streamer.frames(), noRetry(fmt.Errorf("%w: %w", wrapped, ErrJobDoomed))
		}
		return nil, streamer.frames(), wrapped
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info(
		"stage 5 complete",
		"stage", "inference",
		"model", modelName,
		"responseBytes", len(response),
		"chunkFrames", streamer.frames(),
		"promptTokens", stats.PromptTokens,
		"evalTokens", stats.EvalTokens,
		"thinkingBytes", stats.ThinkingBytes,
		"tokensPerSecond", fmt.Sprintf("%.1f", stats.TokensPerSecond()),
		"durationMs", d.Milliseconds(),
	)

	// Hand the model's own measurements to the consumer alongside the
	// answer. Ollama reports these on its terminal object and the worker
	// used to drop them, so the UI had no way to show tokens or throughput.
	// appliedMaxTokens is the anti-fraud audit anchor: the token cap this
	// job actually ran under.
	streamer.recordStats(stats, effectiveOpts.NumPredict)

	// Heat-tier per-job metrics (DO-2 item 2): one block, all four. TTFT is
	// only observable on the streaming path (nil-safe: batch jobs skip it);
	// throughput and output tokens guard the 0-on-cached case. Warmth comes
	// from the model's own load report, never from latency.
	if ttft, ok := streamer.ttft(); ok {
		warm := metrics.WarmFalse
		if stats.LoadDuration <= 0 {
			warm = metrics.WarmTrue
		}
		h.metrics.JobTTFT.WithLabelValues(tier, model, warm, delivery).Observe(ttft.Seconds())
	}
	if tps := stats.TokensPerSecond(); tps > 0 {
		h.metrics.JobTokensPerSecond.WithLabelValues(tier, model).Observe(tps)
	}
	if stats.EvalTokens > 0 {
		h.metrics.JobOutputTokens.WithLabelValues(tier, model).Observe(float64(stats.EvalTokens))
	}

	// Stage 6: Encrypt response.
	//
	// This is the canonical full-response ciphertext: it is what stage 7's
	// terminal frame carries and signs, what stage 8a anchors in the blob,
	// and what stage 8b hashes into completeJob. Streaming does not touch
	// it â€” the per-chunk ciphertexts published above are an independent,
	// throwaway encryption of the same plaintext deltas and are never
	// concatenated, hashed, or committed anywhere.
	// Stage 6: Encode and encrypt response. For search jobs the response is
	// wrapped in a v2 JSON envelope that captures the search context so the
	// disputer can reproduce the exact augmented prompt on replay. For plain
	// (non-search) jobs EncodeResponse returns raw answer bytes — byte-identical
	// to the legacy format.
	rec = h.metrics.StartStage(metrics.StageEncrypt, model, delivery)
	respBytes, encErr := searchaug.EncodeResponse(response, searchSources)
	if encErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return nil, 0, fmt.Errorf("stage 6 (encode response): %w", encErr)
	}
	ciphertext, err := pkgcrypto.Encrypt(sessionKey, respBytes)
	if err != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return nil, streamer.frames(), fmt.Errorf("stage 6 (encrypt response): %w", err)
	}
	d = rec.End(metrics.OutcomeOK, metrics.CacheMiss)
	logger.Info(
		"stage 6 complete",
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

	// Voice output (stage 6b): when the consumer opted in, synthesize the
	// response text and stream the PCM to the consumer as `audio` frames
	// (raw chunks bracketed by JSON header/final descriptors). Best-effort:
	// any failure logs and skips, never fails the job. Runs before the
	// return so the audio frames are counted in streamer.frames() and the
	// terminal frame's TotalChunks stays honest. The settlement ciphertext
	// above is untouched: audio is delivered, not settled.
	h.maybeDeliverAudio(ctx, logger, streamer, response, envelope, guard)

	return ciphertext, streamer.frames(), nil
}

// runInference executes the stage-5 model call. When streamer is non-nil it
// uses the streaming API and feeds coalesced deltas to the consumer as they
// arrive; otherwise it makes the original batch call. Either way it returns
// the complete response text.
func (h *JobHandler) runInference(
	ctx context.Context,
	logger *slog.Logger,
	client InferenceClient,
	modelName string,
	prompt promptEnvelope,
	history []ollama.ChatMessage,
	streamer *chunkStreamer,
) (string, ollama.StreamStats, error) {
	// A speech job's answer IS the recording, so it never reaches a language
	// model: no Ollama call, no history, no streaming. Producing the audio
	// here rather than special-casing the caller means stages 6-8 encrypt,
	// blob and settle it byte for byte the way they settle a sentence.
	//
	// Distinct from the TTS_ENABLED path, which reads an ordinary answer
	// aloud over audio frames and settles nothing.
	if h.cfg.SpeechModelName != "" && modelName == h.cfg.SpeechModelName {
		return h.runSpeechJob(ctx, logger, prompt.Text)
	}

	if streamer == nil {
		var (
			text string
			err  error
		)
		if len(history) > 0 || len(prompt.Images) > 0 {
			text, err = client.Chat(ctx, modelName, chatMessages(history, prompt))
		} else {
			text, err = client.Generate(ctx, modelName, prompt.Text)
		}
		return text, ollama.StreamStats{}, err
	}

	handlers := ollama.StreamHandlers{
		OnToken: func(delta string) error {
			return streamer.Add(pkgtypes.FrameKindText, delta)
		},
	}
	// Reasoning is only forwarded when the consumer asked for it. A client
	// that cannot render a reasoning part would otherwise receive frames it
	// silently drops, having paid for the bandwidth.
	if h.cfg.StreamReasoning {
		handlers.OnThinking = func(delta string) error {
			return streamer.Add(pkgtypes.FrameKindReasoning, delta)
		}
	}

	var (
		response string
		stats    ollama.StreamStats
		err      error
	)
	if len(history) > 0 || len(prompt.Images) > 0 {
		response, stats, err = streamer.client.ChatStream(ctx, modelName, chatMessages(history, prompt), handlers)
	} else {
		response, stats, err = streamer.client.GenerateStream(ctx, modelName, prompt.Text, prompt.Images, handlers)
	}
	if err != nil {
		return "", stats, err
	}

	// Emit whatever is left in every coalescing window. publish never
	// returns an error, so this can only fail if the coalescer contract
	// changes; log rather than fail a generation that already succeeded.
	if fErr := streamer.FlushAll(); fErr != nil {
		logger.Warn("failed to flush trailing token chunk",
			"stage", "inference",
			"error", fErr,
		)
	}
	return response, stats, nil
}

// chatMessages appends the current prompt as the final user turn. Images ride
// on that turn: Ollama attaches them to the most recent user message.
func chatMessages(history []ollama.ChatMessage, prompt promptEnvelope) []ollama.ChatMessage {
	messages := make([]ollama.ChatMessage, 0, len(history)+1)
	messages = append(messages, history...)
	return append(messages, ollama.ChatMessage{
		Role:    "user",
		Content: prompt.Text,
		Images:  prompt.Images,
	})
}

// ackConfirmation is the handle stage 1 hands to stage 1b when the
// acknowledgement was broadcast but not yet mined. The confirmation runs on
// its own goroutine from the moment of broadcast, so it makes progress
// during stages 2-6 and the join is normally already satisfied.
//
// A nil *ackConfirmation means there is nothing to join â€” the job was
// already acknowledged, or stage 1 blocked on the receipt itself. All
// methods are nil-safe.
type ackConfirmation struct {
	txHash common.Hash
	// done is closed once err holds the final result. Closing establishes
	// the happens-before edge that lets the joiner read err unsynchronized.
	done chan struct{}
	err  error
}

// awaitConfirmed blocks until the confirmation resolves, ctx is done, or
// timeout elapses. The timeout is a backstop: the watcher goroutine is
// already bounded by the same budget, so a job can never park here waiting
// on a goroutine that has silently stopped making progress.
func (a *ackConfirmation) awaitConfirmed(ctx context.Context, timeout time.Duration) error {
	if a == nil {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-a.done:
		return a.err
	case <-timer.C:
		return fmt.Errorf("timed out after %s waiting for ack tx %s to confirm", timeout, a.txHash.Hex())
	case <-ctx.Done():
		return fmt.Errorf("job context ended before ack tx %s confirmed: %w", a.txHash.Hex(), ctx.Err())
	}
}

// ensureAcknowledged runs stage 1. It returns a non-nil *ackConfirmation
// only when an acknowledgement is in flight and stage 1b must join it;
// nil means the job is known-acknowledged and the pipeline may proceed
// unconditionally.
func (h *JobHandler) ensureAcknowledged(
	ctx context.Context,
	logger *slog.Logger,
	jobID uint64,
) (*ackConfirmation, error) {
	acknowledged, err := h.chainClient.HasJobAcknowledged(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("check acknowledged state: %w", err)
	}
	if acknowledged {
		logger.Info(
			"stage 1: job already acknowledged on-chain, skipping ack tx",
			"stage", "ack",
			"path", "already_acked",
		)
		return nil, nil
	}

	if async, ok := h.asyncAckClient(); ok {
		return h.broadcastAck(ctx, logger, async, jobID)
	}

	logger.Info(
		"stage 1: sending ack tx",
		"stage", "ack",
		"path", "sending_ack",
		"timeout", h.cfg.AckTxTimeout.String(),
	)

	ackCtx, ackCancel := context.WithTimeout(ctx, h.cfg.AckTxTimeout)
	defer ackCancel()

	if err := h.chainClient.AcknowledgeJob(ackCtx, jobID); err != nil {
		return nil, h.reconcileAckError(ctx, logger, jobID, err)
	}

	return nil, nil
}

// asyncAckClient reports whether stage 1 may overlap, i.e. the operator
// left ACK_OVERLAP_ENABLED on and the configured chain client can broadcast
// without blocking on the receipt.
func (h *JobHandler) asyncAckClient() (AsyncAckClient, bool) {
	if !h.cfg.AckOverlapEnabled {
		return nil, false
	}
	async, ok := h.chainClient.(AsyncAckClient)
	return async, ok
}

// broadcastAck puts the acknowledgement on the wire and starts watching for
// its receipt in the background, returning as soon as the broadcast lands
// so the caller can start fetching the blob and generating.
func (h *JobHandler) broadcastAck(
	ctx context.Context,
	logger *slog.Logger,
	async AsyncAckClient,
	jobID uint64,
) (*ackConfirmation, error) {
	logger.Info("stage 1: broadcasting ack tx, confirming in background",
		"stage", "ack",
		"path", "broadcast_ack",
		"timeout", h.cfg.AckTxTimeout.String(),
	)

	// One budget covers broadcast and confirmation together, matching what
	// the blocking path spends end to end. Overlapping must not stretch
	// the ack past the deadline the contract recorded at submit time.
	ackCtx, ackCancel := context.WithTimeout(ctx, h.cfg.AckTxTimeout)

	txHash, join, err := async.AcknowledgeJobAsync(ackCtx, jobID)
	if err != nil {
		ackCancel()
		return nil, h.reconcileAckError(ctx, logger, jobID, err)
	}

	confirmation := &ackConfirmation{txHash: txHash, done: make(chan struct{})}
	go func() {
		// Ordering matters: err is written, then done is closed, then the
		// budget is released. Deferred calls run last-registered-first.
		defer ackCancel()
		defer close(confirmation.done)
		confirmation.err = join(ackCtx)
	}()

	return confirmation, nil
}

// reconcileAckError converts a failed acknowledge attempt into either a
// clean nil (the tx actually landed) or a wrapped error. Shared by the
// blocking and broadcast paths.
func (h *JobHandler) reconcileAckError(
	ctx context.Context,
	logger *slog.Logger,
	jobID uint64,
	ackErr error,
) error {
	acknowledged, checkErr := h.chainClient.HasJobAcknowledged(ctx, jobID)
	if checkErr != nil {
		return fmt.Errorf("acknowledge job: %w (recheck failed: %v)", ackErr, checkErr)
	}
	if acknowledged {
		logger.Warn("stage 1: ack tx returned error but job is acknowledged on-chain",
			"stage", "ack",
			"error", ackErr,
		)
		return nil
	}
	return fmt.Errorf("acknowledge job: %w", ackErr)
}

// awaitAcknowledged runs stage 1b: the join that guarantees nothing is
// published or settled for a job whose acknowledgement is not confirmed
// on-chain with receipt status 1.
//
// Failure here is deliberately terminal. The job is abandoned before stage
// 7's terminal frame and before stages 8a/8b, so no settlement is attempted
// against an unacknowledged job. The error names the acknowledgement
// explicitly so it reads differently from an inference failure in logs and
// in the {reason} metric label.
func (h *JobHandler) awaitAcknowledged(
	ctx context.Context,
	logger *slog.Logger,
	jobID uint64,
	ack *ackConfirmation,
	model, delivery string,
) error {
	if ack == nil {
		return nil
	}

	rec := h.metrics.StartStage(metrics.StageAckConfirm, model, delivery)
	confirmErr := ack.awaitConfirmed(ctx, h.cfg.AckTxTimeout)
	if confirmErr == nil {
		d := rec.End(metrics.OutcomeOK, metrics.CacheNone)
		logger.Info("stage 1b complete",
			"stage", "ack_confirm",
			"ackTxHash", ack.txHash.Hex(),
			"durationMs", d.Milliseconds(),
		)
		return nil
	}

	// The watcher can lose its race without the acknowledgement having
	// failed â€” a receipt wait that expires moments before the tx lands, a
	// dropped RPC connection. Re-read chain state before condemning the
	// job: stages 2-6 have burned far more wall time than the confirmation
	// budget, so a late mine is visible by now. The JobAcknowledged event
	// only exists if the tx mined successfully, so this cannot mistake a
	// revert for a success.
	acknowledged, checkErr := h.chainClient.HasJobAcknowledged(ctx, jobID)
	if checkErr == nil && acknowledged {
		d := rec.End(metrics.OutcomeOK, metrics.CacheNone)
		logger.Warn("stage 1b: ack confirmation failed but job is acknowledged on-chain",
			"stage", "ack_confirm",
			"ackTxHash", ack.txHash.Hex(),
			"durationMs", d.Milliseconds(),
			"error", confirmErr,
		)
		return nil
	}

	rec.End(metrics.OutcomeError, metrics.CacheNone)
	logger.Error("stage 1b: acknowledgement not confirmed, abandoning job before settlement",
		"stage", "ack_confirm",
		"ackTxHash", ack.txHash.Hex(),
		"error", confirmErr,
		"recheckError", checkErr,
	)
	if checkErr != nil {
		return fmt.Errorf(
			"stage 1b (ack confirm): acknowledgement for job %d not confirmed on-chain, refusing to settle: %w (recheck failed: %v)",
			jobID, confirmErr, checkErr,
		)
	}
	return fmt.Errorf(
		"stage 1b (ack confirm): acknowledgement for job %d not confirmed on-chain, refusing to settle: %w",
		jobID, confirmErr,
	)
}

// ensureBlobSubmitted is the stage-8a equivalent of ensureAcknowledged: it
// returns the cached versioned hash from the checkpoint when stage 8a has
// already run on a prior attempt, or it submits the blob tx (with the
// per-stage BlobTxTimeout that prevents a stuck WaitMined from holding the
// broadcast slot indefinitely) and persists the resulting hash. Post-audit
// the contract's completeJob takes a single bytes32 responseBlobHash and
// enforces blobhash(0) == responseBlobHash, so the blob tx must carry
// exactly one blob — that invariant is checked here.
//
// guard gates the miss path: when the remaining deadline window cannot fit
// the blob tx plus completeJob, the job is doomed and the blob tx must not
// be burned. This is the load-bearing deadline gate for retries, where a
// checkpoint hit bypassed the stage-5 gate entirely.
func (h *JobHandler) ensureBlobSubmitted(
	ctx context.Context,
	logger *slog.Logger,
	jobID uint64,
	ckpt JobCheckpoint,
	ciphertext []byte,
	model, delivery string,
	guard *deadlineGuard,
) (common.Hash, error) {
	if ckpt.HasVersionedHash() {
		// Cache hit: short-circuit. Record an outcome=skipped sample so
		// dashboards can count "stage 8a skipped on retry" against total
		// invocations.
		rec := h.metrics.StartStage(metrics.StageSubmitBlob, model, delivery)
		rec.End(metrics.OutcomeSkipped, metrics.CacheHit)
		h.metrics.CheckpointEvents.WithLabelValues(metrics.CheckpointEventHitBlob).Inc()
		logger.Info(
			"checkpoint hit, skipping stage 8a",
			"stage", "checkpoint",
			"reason", "blob_submitted",
			"versionedHash", ckpt.VersionedHash.Hex(),
		)
		return ckpt.VersionedHash, nil
	}

	rec := h.metrics.StartStage(metrics.StageSubmitBlob, model, delivery)
	if gateErr := guard.gateBlobSubmit(ctx, logger); gateErr != nil {
		rec.End(metrics.OutcomeError, metrics.CacheMiss)
		return common.Hash{}, gateErr
	}
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
	logger.Info(
		"stage 8a complete",
		"stage", "submit_blob",
		"versionedHash", versionedHash.Hex(),
		"durationMs", d.Milliseconds(),
	)
	if h.checkpoints != nil {
		if err := h.checkpoints.SetVersionedHash(ctx, jobID, versionedHash); err != nil {
			logger.Warn(
				"failed to persist versionedHash on checkpoint",
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
	guard *deadlineGuard,
) error {
	// On a retry the job has usually already been completed by the earlier
	// attempt, so CompleteJob would fail gas estimation against the contract's
	// JobState.Acknowledged requirement. That failure is harmless on its own -
	// nothing is broadcast - but it used to reset the shared nonce counter
	// while sibling jobs held allocated nonces, stalling them behind a 30s
	// Asynq retry. Ask first on the path where the answer is usually yes.
	if retryCount, _ := asynq.GetRetryCount(ctx); retryCount > 0 {
		if completed, cErr := h.chainClient.HasJobCompleted(ctx, jobID); cErr == nil && completed {
			logger.Info("stage 8b: job already completed on-chain, skipping",
				"stage", "complete_job",
				"path", "retry_already_complete",
			)
			return nil
		}
	}

	if err := h.chainClient.CompleteJob(ctx, jobID, responseBlobHash, responseCiphertextHash); err != nil {
		completed, checkErr := h.chainClient.HasJobCompleted(ctx, jobID)
		if checkErr != nil {
			return fmt.Errorf("complete job: %w (recheck failed: %v)", err, checkErr)
		}
		if completed {
			logger.Warn(
				"stage 8b: completeJob tx returned error but job is completed on-chain",
				"stage", "complete_job",
				"error", err,
			)
			return nil
		}
		// Past the on-chain deadline the contract's DeadlineExceeded revert
		// is permanent: no retry can ever settle this job, so classify it
		// no-retry and let the keeper refund the consumer. Checking chain
		// state beats parsing the revert reason, which not all RPC nodes
		// surface faithfully.
		if guard.isDoomedNow(ctx, logger) {
			logger.Warn("stage 8b: completeJob failed and the on-chain deadline has passed; not retrying",
				"stage", "complete_job",
				"jobID", jobID,
				"error", err,
			)
			return noRetry(fmt.Errorf("complete job: %w: %w", err, ErrJobDoomed))
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
		logger.Info(
			"stage 3: session key cache hit",
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
			logger.Info(
				"stage 3: session key cache hit (inside flight)",
				"stage", "session_key",
				"path", "cache_hit",
			)
			return k, nil
		}
		h.metrics.SessionKeyEvents.WithLabelValues(metrics.SessionKeyPathChainDerive).Inc()
		logger.Info(
			"stage 3: session key cache miss, deriving from chain",
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
	logger.Info(
		"stage 4: refreshing session key from chain (possible rotation)",
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
		// Log but don't fail â€” key is in memory for this job
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

// relayCompleteCiphertext returns the ciphertext to deliver on the relay
// `complete` frame. For a v2 search envelope it re-encrypts just the plain
// answer (the v1.1 consumer contract — the envelope with searchContext stays in
// the blob for the disputer). For a legacy/plain ciphertext it returns it
// unchanged. Best-effort: on any decrypt/decode/encrypt error it falls back to
// the original ciphertext (never fails the job).
func (h *JobHandler) relayCompleteCiphertext(sessionKey, blobCiphertext []byte) []byte {
	plain, err := pkgcrypto.Decrypt(sessionKey, blobCiphertext)
	if err != nil {
		return blobCiphertext
	}
	env := searchaug.DecodeResponse(plain)
	if env.V != searchaug.ResponseEnvelopeVersion {
		return blobCiphertext // legacy/non-search: already the plain answer
	}
	ac, err := pkgcrypto.Encrypt(sessionKey, []byte(env.Answer))
	if err != nil {
		return blobCiphertext
	}
	return ac
}

// publishToRedis signs and publishes the terminal response frame via the
// configured ResponsePublisher. Errors are logged but non-fatal â€” the
// on-chain blob is the authoritative response.
//
// The signature is over the FULL response ciphertext, unchanged by
// streaming: it is the same EIP-191 evidence
// JobRegistry.disputeResponseMismatch verifies, over the same bytes stage
// 8a anchors and stage 8b hashes.
//
// totalFrames is chunkFrames+1 â€” every frame published for this response
// including this terminal one, so a non-streamed response passes 1.
func (h *JobHandler) publishToRedis(
	ctx context.Context,
	logger *slog.Logger,
	jobID, sessionID uint64,
	correlationID string,
	ciphertext []byte,
	totalFrames uint32,
) {
	if h.responsePublisher == nil {
		return
	}

	sig, err := signMismatchEvidence(h.cfg.ChainID, h.cfg.JobRegistryAddr, jobID, sessionID, ciphertext, h.signingKey)
	if err != nil {
		h.logger.Warn(
			"failed to sign response",
			"stage", "redis_publish",
			"jobID", jobID,
			"error", err,
		)
		return
	}

	sigHex := "0x" + hex.EncodeToString(sig)
	h.responsePublisher.PublishResponse(ctx, jobID, sessionID, correlationID, sigHex, ciphertext, totalFrames)
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
// jobId, sessionId, ciphertext)) â€” the inner hash that JobRegistry wraps with
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
// Additionally, blobs are decrypted with the current session's key â€” if a job
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
			// Prior prompts may be versioned envelopes. Decoding restores
			// the actual question as the user turn's text and re-attaches
			// its images, instead of dumping the raw JSON envelope -
			// including full base64 image payloads - into the context as
			// text, where it wastes prompt tokens against num_ctx and the
			// vision model never sees the image as an image.
			// A search envelope wraps the text the same way it does on the live
			// turn, so unwrap it before the multimodal decode.
			plain, _, dErr := searchaug.DecodePrompt(promptText)
			var env promptEnvelope
			if dErr == nil {
				env, dErr = decodePrompt([]byte(plain))
			}
			if dErr != nil {
				// Practically unreachable: stages 4.5 and 5 reject the same
				// malformed envelopes before a job can complete and land in
				// history. Stay conservative and keep the raw text rather
				// than drop the turn.
				messages = append(messages, ollama.ChatMessage{Role: "user", Content: string(promptText)})
			} else {
				messages = append(messages, ollama.ChatMessage{Role: "user", Content: env.Text, Images: env.Images})
			}
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
			messages = append(messages, ollama.ChatMessage{Role: "assistant", Content: searchaug.DecodeResponse(responseText).Answer})
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

// toSearchaugSources converts search.Source (the Tavily/search-package type)
// to searchaug.Source (the shared pkg type). Fields are identical; this
// converter avoids a dependency between the two packages.
func toSearchaugSources(sources []search.Source) []searchaug.Source {
	out := make([]searchaug.Source, len(sources))
	for i, s := range sources {
		out[i] = searchaug.Source{
			Position: s.Position,
			Title:    s.Title,
			URL:      s.URL,
			Snippet:  s.Snippet,
		}
	}
	return out
}

// sourcesMetadataJSON renders the citation payload in the exact shape the
// frontend's parseWebSearchSources expects. Accepts []searchaug.Source so it
// can be called after the search.Source → searchaug.Source conversion.
func sourcesMetadataJSON(sources []searchaug.Source) []byte {
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

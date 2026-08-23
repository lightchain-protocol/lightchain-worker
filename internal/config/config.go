// Package config loads and validates worker sidecar configuration from environment variables.
package config

import (
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/lightchain/worker/internal/metrics"
)

// Config holds all worker sidecar configuration parsed from environment variables.
type Config struct {
	// Identity — Ethereum signing key
	WorkerKeystorePath     string
	WorkerKeystorePassword string

	// Identity — ECDH encryption key (separate from signing key)
	EncryptionKeystorePath string

	// Chain
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address
	JobRegistryAddress    common.Address

	// Gas
	GasPriceMultiplierBps int
	MaxGasPrice           *big.Int

	// Models — comma-separated list
	SupportedModels []string

	// Redis
	RedisURL      string
	RedisPassword string

	// Heartbeat
	HeartbeatInterval time.Duration

	// Ollama
	OllamaURL     string
	OllamaTimeout time.Duration

	// OllamaStream selects stream=true inference. When true the worker
	// publishes incremental `chunk` frames as tokens arrive, collapsing
	// time-to-first-token from full generation time to ~1s. The terminal
	// `complete` frame and the whole on-chain settlement path are
	// unaffected either way — see pipeline.JobHandler.
	//
	// OllamaKeepAlive rides in the request body as `keep_alive`, so it
	// warms every Ollama instance the worker talks to (including
	// third-party hardware) with no operator action. "-1" pins the model
	// in memory; Ollama's default unloads after 5 idle minutes.
	//
	// OllamaNumPredict and OllamaNumCtx populate the request `options`
	// object. NumPredict bounds a runaway generation so it cannot blow
	// the whole OllamaTimeout budget. NumCtx of 0 means "unset" — leave
	// the model's own default in place.
	//
	// OllamaThink rides in the request body as `think`, enabling or
	// disabling the hidden reasoning channel on models that have one. It
	// defaults to off because the worker transmits only the answer:
	// thinking tokens are generated, charged to the consumer, and then
	// discarded. They also compete with the answer for OllamaNumPredict,
	// and a model that exhausts that budget mid-thought returns nothing
	// at all — which settles on chain as a zero-byte response.
	OllamaStream     bool
	OllamaKeepAlive  string
	OllamaNumPredict int
	OllamaNumCtx     int
	OllamaThink      bool

	// Token-stream coalescing. A chunk frame is published when either
	// StreamChunkTokens deltas or StreamChunkInterval have accumulated,
	// whichever comes first. Batching matters because the relay enforces
	// a per-consumer rate limit (~1000 messages/minute by default); at
	// ~10 tok/s the defaults below emit roughly 1-4 frames/second.
	StreamChunkTokens   int
	StreamChunkInterval time.Duration

	// StreamReasoning forwards a reasoning model's chain of thought to the
	// consumer on its own frame kind. Without it those tokens are generated,
	// paid for, and discarded: one measured job produced 962 tokens and
	// surfaced roughly 320, leaving the user on a spinner for the rest.
	StreamReasoning bool

	// Deadline guard (on-chain job.deadline awareness).
	//
	// completeJob reverts DeadlineExceeded once block.timestamp passes
	// job.deadline (ackTimeout before ack, completionTimeout = 120 s after).
	// The worker's own budgets (asynq task timeout, OllamaTimeout,
	// BlobTxTimeout) are not derived from that deadline, so without a guard
	// a cold model load can carry a job past the point where settlement is
	// possible - the job then still burns its stage-8a blob tx, fails
	// completeJob, and retries pointlessly until the keeper refunds the
	// consumer ~2h later.
	//
	// DeadlineGuardEnabled (DEADLINE_GUARD_ENABLED, default on) makes the
	// pipeline read job.deadline at pickup, before inference, and before
	// the blob tx, aborting with a no-retry error when settlement is
	// impossible. SettleReserve (DEADLINE_SETTLE_RESERVE, default 25s) is
	// the post-generation budget the guard protects: encrypt + publish +
	// blob tx + completeJob. CompletionReserve (DEADLINE_COMPLETION_RESERVE,
	// default 12s) is the minimum window required to start stage 8a.
	// MinInferenceBudget (DEADLINE_MIN_INFERENCE_BUDGET, default 10s) is
	// the smallest generation window worth starting.
	DeadlineGuardEnabled bool
	SettleReserve        time.Duration
	CompletionReserve    time.Duration
	MinInferenceBudget   time.Duration

	// Voice I/O via local sidecars (whisper STT, Kokoro TTS). Both default
	// off; each additionally requires the consumer to opt in per request
	// via the prompt envelope (v2) voice fields.
	//
	// STTEnabled (STT_ENABLED) lets audio prompts be transcribed by the
	// sidecar at STTSidecarURL before inference. The transcript is merged
	// into the prompt text; it is derived context, not settled content.
	//
	// TTSEnabled (TTS_ENABLED) lets an opted-in response be rendered to
	// speech and streamed to the consumer as `audio` frames (raw PCM s16le
	// 24 kHz chunks bracketed by JSON descriptors). The audio never enters
	// the settlement ciphertext. TTSVoice (default "af_heart") is the
	// worker default when the envelope does not name one. TTSMaxChars
	// truncates the synthesized text (0 = no limit; the settled text
	// answer is unaffected either way; the sidecar itself rejects text
	// over 4000 chars).
	//
	// Both URLs are required to be parseable http(s) URLs when the
	// matching feature is enabled. The timeouts bound one sidecar call
	// each and feed the deadline-guard budget check for voice work.
	STTEnabled    bool
	STTSidecarURL string
	STTTimeout    time.Duration
	TTSEnabled    bool
	TTSSidecarURL string
	TTSVoice      string
	TTSMaxChars   int
	TTSTimeout    time.Duration

	// Beacon API (CL node)
	BeaconAPIURL string

	// Job execution
	MaxConcurrentJobs   int
	AckTxTimeout        time.Duration
	BlobTxTimeout       time.Duration
	BlobFetchTimeout    time.Duration
	BlobFetchRetries    int
	SessionKeyFile      string
	ReceiptPollInterval time.Duration

	// AckOverlapEnabled lets stage 1 return as soon as the acknowledge
	// tx is broadcast, so stages 2-6 (blob fetch through inference) run
	// during the block the ack is mining in instead of after it. The
	// confirmation is joined before the response is published and before
	// settlement, so a job can still never complete on an unacknowledged
	// ack — see pipeline.JobHandler. When false, stage 1 blocks on
	// WaitMined exactly as it did before overlap existed.
	//
	// The confirmation stays bounded by AckTxTimeout, which must remain
	// under the contract's submit-time ack deadline
	// (block.timestamp + AIConfig.getAckTimeout()).
	AckOverlapEnabled bool

	// RedisPublishTimeout bounds stage 7 (redis_publish): a single
	// PUBLISH on the session response channel. Stage 7 is non-fatal but
	// runs synchronously inside processJob, so a slow Redis must not be
	// allowed to eat into stage 8's BlobTxTimeout budget.
	RedisPublishTimeout time.Duration

	// JobCheckpoint — retry-safety cache for stages 2-6.
	//
	// CheckpointTTL is how long a checkpoint record lives after the first
	// write. Chosen generously (2h) so asynq retries over long backoffs
	// still hit the cache; a shorter TTL is applied after on-chain
	// completion (see Tombstone).
	//
	// CheckpointMaxBytes caps the cached ciphertext size. Oversized
	// responses are refused and fall through to per-retry re-inference.
	// Default 256 KiB comfortably fits llama-scale responses with headroom.
	CheckpointTTL      time.Duration
	CheckpointMaxBytes int

	// Stuck-nonce recovery (Hazard B). When the worker's signing key has a
	// tx stuck in the mempool — e.g. BlobFeeCap underbid the current blob
	// base fee — every SendTransaction is rejected with "address already
	// reserved" and the ResetNonce+refetch loop cannot clear it. These
	// settings bound detection and automated replacement.
	//
	// StuckNonceThreshold — consecutive rejections at the same nonce before
	// declaring it stuck. Default 5 tolerates transient pool flakiness.
	//
	// StuckNonceMaxBumps — max replacement-tx attempts per stuck nonce
	// before returning a loud error. Default 3 caps the worst-case gas
	// cost of automated recovery.
	//
	// StuckNonceAutoReplace — operator kill-switch. When false the tracker
	// still detects and logs, but no replacement tx is submitted.
	StuckNonceThreshold   int
	StuckNonceMaxBumps    int
	StuckNonceAutoReplace bool

	// Shutdown
	ShutdownTimeout time.Duration

	// DrainTTLOverride is parsed from LIGHTCHAIN_DRAIN_TTL. When > 0 it
	// is used as the drain marker TTL, bypassing the on-chain dispute
	// window lookup. Both SIGTERM-driven and CLI-driven drain consult
	// this same field so the override is consistent across entrypoints.
	DrainTTLOverride time.Duration

	// DrainSlack is added to the on-chain dispute window when computing
	// the drain marker TTL. Parsed from LIGHTCHAIN_DRAIN_SLACK; defaults
	// to 2h when unset. Lowering it is the lever E2E suites use to keep
	// drain windows short without touching the chain dispute window.
	DrainSlack time.Duration

	// Gateway mode (optional — when set, worker uses HTTP gateway instead of direct Redis)
	WorkerGatewayURL string

	// Metrics — Prometheus /metrics HTTP endpoint.
	//
	// MetricsListenAddr defaults to 127.0.0.1:9101 (loopback-only, scraped
	// by the same-host Ops Agent). Set to empty to disable the endpoint.
	//
	// MetricsAllowPublic is a deliberate escape hatch: when false (default)
	// Validate rejects any non-loopback host to prevent accidentally
	// exposing /metrics on 0.0.0.0. Set true only when fronted by an
	// authenticated reverse proxy or scraped from outside the host.
	MetricsListenAddr  string
	MetricsAllowPublic bool

	// Release scheduler — settles completed jobs on-chain after the
	// dispute window. ReleaseEnabled gates the entire subsystem; when
	// false the pipeline still runs (no MarkEligible writes occur)
	// and the operator settles via worker-cli release.
	ReleaseEnabled               bool
	ReleaseStatePath             string
	ReleaseInterval              time.Duration
	ReleaseProbeInterval         time.Duration
	ReleaseBatchThreshold        int
	ReleaseMaxBatchSize          int
	ReleaseTxTimeout             time.Duration
	ReleaseStartBlock            uint64
	ReleaseChunkSize             uint64
	ReleaseConfirmations         uint64
	ReleaseReconcileInterval     time.Duration
	ReleaseBackoffBase           time.Duration
	ReleaseBackoffMax            time.Duration
	ReleasePausedCycleBackoff    time.Duration
	ReleaseStaleDisputeWarnAfter time.Duration
	ReleaseDisputeWindowOverride time.Duration
	ReleaseDisputeWindowCacheTTL time.Duration

	// Logging
	LogLevel  string
	LogFormat string
}

// Load reads all environment variables and returns a populated Config.
// It does not validate — call Validate() after Load() to check constraints.
func Load() (*Config, error) {
	cfg := &Config{
		WorkerKeystorePath:     os.Getenv("WORKER_KEYSTORE_PATH"),
		WorkerKeystorePassword: os.Getenv("WORKER_KEYSTORE_PASSWORD"),
		EncryptionKeystorePath: envOrDefault("ENCRYPTION_KEYSTORE_PATH", "data/worker-encryption.key"),
		RPCURL:                 envOrDefault("RPC_URL", "http://localhost:8545"),
		RedisURL:               envOrDefault("REDIS_URL", "redis://localhost:6379"),
		RedisPassword:          os.Getenv("REDIS_PASSWORD"),
		OllamaURL:              envOrDefault("OLLAMA_URL", "http://localhost:11434"),
		BeaconAPIURL:           envOrDefault("BEACON_API_URL", "http://localhost:3500"),
		SessionKeyFile:         envOrDefault("SESSION_KEY_FILE", "data/session-keys.enc"),
		WorkerGatewayURL:       os.Getenv("WORKER_GATEWAY_URL"),
		MetricsListenAddr:      envOrDefault("WORKER_METRICS_ADDR", "127.0.0.1:9101"),
		OllamaKeepAlive:        envOrDefault("OLLAMA_KEEP_ALIVE", "-1"),
		LogLevel:               envOrDefault("LOG_LEVEL", "info"),
		LogFormat:              envOrDefault("LOG_FORMAT", "json"),
	}

	var errs []string

	// ChainID — required int64
	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		errs = append(errs, "CHAIN_ID is required")
	} else {
		v := new(big.Int)
		if _, ok := v.SetString(chainIDStr, 10); !ok {
			errs = append(errs, fmt.Sprintf("CHAIN_ID: invalid integer %q", chainIDStr))
		} else if !v.IsInt64() {
			errs = append(errs, fmt.Sprintf("CHAIN_ID: value %q overflows int64", chainIDStr))
		} else {
			cfg.ChainID = v.Int64()
		}
	}

	// WorkerRegistryAddress — required
	registryStr := os.Getenv("WORKER_REGISTRY_ADDRESS")
	if registryStr == "" {
		errs = append(errs, "WORKER_REGISTRY_ADDRESS is required")
	} else if !common.IsHexAddress(registryStr) {
		errs = append(errs, fmt.Sprintf("WORKER_REGISTRY_ADDRESS: invalid hex address %q", registryStr))
	} else {
		cfg.WorkerRegistryAddress = common.HexToAddress(registryStr)
	}

	// AIConfigAddress — required
	aiConfigStr := os.Getenv("AI_CONFIG_ADDRESS")
	if aiConfigStr == "" {
		errs = append(errs, "AI_CONFIG_ADDRESS is required")
	} else if !common.IsHexAddress(aiConfigStr) {
		errs = append(errs, fmt.Sprintf("AI_CONFIG_ADDRESS: invalid hex address %q", aiConfigStr))
	} else {
		cfg.AIConfigAddress = common.HexToAddress(aiConfigStr)
	}

	// JobRegistryAddress — required
	jobRegStr := os.Getenv("JOB_REGISTRY_ADDRESS")
	if jobRegStr == "" {
		errs = append(errs, "JOB_REGISTRY_ADDRESS is required")
	} else if !common.IsHexAddress(jobRegStr) {
		errs = append(errs, fmt.Sprintf("JOB_REGISTRY_ADDRESS: invalid hex address %q", jobRegStr))
	} else {
		cfg.JobRegistryAddress = common.HexToAddress(jobRegStr)
	}

	// GasPriceMultiplierBps — default 11000 (1.1x)
	cfg.GasPriceMultiplierBps = parseInt("GAS_PRICE_MULTIPLIER_BPS", 11000, &errs)

	// MaxGasPrice — optional; default 100 gwei
	if maxGasStr := os.Getenv("MAX_GAS_PRICE"); maxGasStr != "" {
		maxGas := new(big.Int)
		if _, ok := maxGas.SetString(maxGasStr, 10); !ok {
			errs = append(errs, fmt.Sprintf("MAX_GAS_PRICE: invalid decimal wei value %q", maxGasStr))
		} else if maxGas.Sign() <= 0 {
			errs = append(errs, "MAX_GAS_PRICE: must be positive")
		} else {
			cfg.MaxGasPrice = maxGas
		}
	} else {
		cfg.MaxGasPrice = new(big.Int).Mul(big.NewInt(100), big.NewInt(1_000_000_000)) // 100 gwei
	}

	// SupportedModels — required, comma-separated
	modelsStr := os.Getenv("SUPPORTED_MODELS")
	if modelsStr == "" {
		errs = append(errs, "SUPPORTED_MODELS is required")
	} else {
		for _, m := range strings.Split(modelsStr, ",") {
			if trimmed := strings.TrimSpace(m); trimmed != "" {
				cfg.SupportedModels = append(cfg.SupportedModels, trimmed)
			}
		}
		if len(cfg.SupportedModels) == 0 {
			errs = append(errs, "SUPPORTED_MODELS: no valid model entries after parsing")
		}
	}

	// Durations
	cfg.HeartbeatInterval = parseDuration("HEARTBEAT_INTERVAL", "10s", &errs)
	cfg.ShutdownTimeout = parseDuration("SHUTDOWN_TIMEOUT", "30s", &errs)
	// Drain TTL override — empty means "consult AIConfig.getDisputeWindow()".
	// parseDuration treats "0s" as a valid zero, which is what we want
	// when the env var is unset. Any positive value bypasses the chain
	// lookup; negative values are rejected so a typo like "-1h" fails
	// fast instead of silently disabling the override (the > 0 short-
	// circuit in computeDrainTTL would treat it as "unset" otherwise).
	cfg.DrainTTLOverride = parseDuration("LIGHTCHAIN_DRAIN_TTL", "0s", &errs)
	if cfg.DrainTTLOverride < 0 {
		errs = append(errs, fmt.Sprintf("LIGHTCHAIN_DRAIN_TTL: must be >= 0, got %s", cfg.DrainTTLOverride))
	}
	// LIGHTCHAIN_DRAIN_SLACK extends the on-chain dispute window. Default
	// 2h matches the value previously hardcoded in service.go; tests
	// override it to 10s or so to keep drain windows tight without
	// touching the chain dispute window.
	cfg.DrainSlack = parseDuration("LIGHTCHAIN_DRAIN_SLACK", "2h", &errs)
	if cfg.DrainSlack < 0 {
		errs = append(errs, fmt.Sprintf("LIGHTCHAIN_DRAIN_SLACK: must be >= 0, got %s", cfg.DrainSlack))
	}
	cfg.OllamaTimeout = parseDuration("OLLAMA_TIMEOUT", "120s", &errs)
	cfg.AckTxTimeout = parseDuration("ACK_TX_TIMEOUT", "15s", &errs)
	// BlobTxTimeout bounds stage 8 (submit_blob): slot wait + SendTransaction
	// + WaitMined. Default 90s = ~15 blocks at 6s block time, generous buffer
	// over the ~12s typical to absorb network jitter and queued broadcasts.
	cfg.BlobTxTimeout = parseDuration("BLOB_TX_TIMEOUT", "90s", &errs)
	cfg.BlobFetchTimeout = parseDuration("BLOB_FETCH_TIMEOUT", "10s", &errs)
	cfg.ReceiptPollInterval = parseDuration("RECEIPT_POLL_INTERVAL", "2s", &errs)
	cfg.RedisPublishTimeout = parseDuration("REDIS_PUBLISH_TIMEOUT", "5s", &errs)

	// Job execution integers
	cfg.MaxConcurrentJobs = parseInt("MAX_CONCURRENT_JOBS", 2, &errs)
	cfg.BlobFetchRetries = parseInt("BLOB_FETCH_RETRIES", 3, &errs)

	cfg.AckOverlapEnabled = parseBool("ACK_OVERLAP_ENABLED", true, &errs)

	// Token streaming. OLLAMA_NUM_CTX defaults to 0 = "leave unset";
	// every other knob has a live default.
	cfg.OllamaStream = parseBool("OLLAMA_STREAM", true, &errs)
	cfg.OllamaNumPredict = parseInt("OLLAMA_NUM_PREDICT", 1024, &errs)
	cfg.OllamaNumCtx = parseInt("OLLAMA_NUM_CTX", 0, &errs)
	cfg.OllamaThink = parseBool("OLLAMA_THINK", false, &errs)
	cfg.StreamChunkTokens = parseInt("STREAM_CHUNK_TOKENS", 8, &errs)
	cfg.StreamChunkInterval = parseDuration("STREAM_CHUNK_INTERVAL", "250ms", &errs)
	cfg.StreamReasoning = parseBool("STREAM_REASONING", true, &errs)

	// On-chain deadline guard. Defaults match pipeline's fallback reserves.
	cfg.DeadlineGuardEnabled = parseBool("DEADLINE_GUARD_ENABLED", true, &errs)
	cfg.SettleReserve = parseDuration("DEADLINE_SETTLE_RESERVE", "25s", &errs)
	cfg.CompletionReserve = parseDuration("DEADLINE_COMPLETION_RESERVE", "12s", &errs)
	cfg.MinInferenceBudget = parseDuration("DEADLINE_MIN_INFERENCE_BUDGET", "10s", &errs)

	// Voice I/O (whisper STT / Kokoro TTS sidecars). Both default off; each
	// also requires per-request opt-in via the prompt envelope (v2). Default
	// URLs match provisioning/worker/voice-sidecars.md (loopback-only).
	cfg.STTEnabled = parseBool("STT_ENABLED", false, &errs)
	cfg.STTSidecarURL = envOrDefault("STT_SIDECAR_URL", "http://127.0.0.1:8100")
	cfg.STTTimeout = parseDuration("STT_TIMEOUT", "10s", &errs)
	cfg.TTSEnabled = parseBool("TTS_ENABLED", false, &errs)
	cfg.TTSSidecarURL = envOrDefault("TTS_SIDECAR_URL", "http://127.0.0.1:8101")
	cfg.TTSVoice = envOrDefault("TTS_VOICE", "af_heart")
	cfg.TTSMaxChars = parseInt("TTS_MAX_CHARS", 4000, &errs)
	cfg.TTSTimeout = parseDuration("TTS_TIMEOUT", "20s", &errs)

	// Stuck-nonce recovery config
	cfg.StuckNonceThreshold = parseInt("WORKER_STUCK_NONCE_THRESHOLD", 5, &errs)
	cfg.StuckNonceMaxBumps = parseInt("WORKER_STUCK_NONCE_MAX_BUMPS", 3, &errs)
	cfg.StuckNonceAutoReplace = parseBool("WORKER_STUCK_NONCE_AUTOREPLACE", true, &errs)
	cfg.MetricsAllowPublic = parseBool("WORKER_METRICS_ALLOW_PUBLIC", false, &errs)

	// Job checkpoint (retry-safety cache)
	cfg.CheckpointTTL = parseDuration("WORKER_CHECKPOINT_TTL", "2h", &errs)
	cfg.CheckpointMaxBytes = parseInt("WORKER_CHECKPOINT_MAX_BYTES", 262144, &errs)

	// Release scheduler. Defaults match release.DefaultConfig() — when
	// they drift, both must be updated.
	cfg.ReleaseEnabled = parseBool("RELEASE_ENABLED", true, &errs)
	cfg.ReleaseStatePath = envOrDefault("RELEASE_STATE_PATH", "./release_state.json")
	cfg.ReleaseInterval = parseDuration("RELEASE_INTERVAL", "8h", &errs)
	cfg.ReleaseProbeInterval = parseDuration("RELEASE_PROBE_INTERVAL", "5m", &errs)
	cfg.ReleaseBatchThreshold = parseInt("RELEASE_BATCH_THRESHOLD", 20, &errs)
	cfg.ReleaseMaxBatchSize = parseInt("RELEASE_MAX_BATCH_SIZE", 50, &errs)
	cfg.ReleaseTxTimeout = parseDuration("RELEASE_TX_TIMEOUT", "120s", &errs)
	cfg.ReleaseStartBlock = parseUint64("RELEASE_RECONCILE_START_BLOCK", 0, &errs)
	cfg.ReleaseChunkSize = parseUint64("RELEASE_RECONCILE_CHUNK_SIZE", 5000, &errs)
	cfg.ReleaseConfirmations = parseUint64("RELEASE_RECONCILE_CONFIRMATIONS", 5, &errs)
	cfg.ReleaseReconcileInterval = parseDuration("RELEASE_RECONCILE_INTERVAL", "1h", &errs)
	cfg.ReleaseBackoffBase = parseDuration("RELEASE_BACKOFF_BASE", "15m", &errs)
	cfg.ReleaseBackoffMax = parseDuration("RELEASE_BACKOFF_MAX", "24h", &errs)
	cfg.ReleasePausedCycleBackoff = parseDuration("RELEASE_PAUSED_CYCLE_BACKOFF", "1h", &errs)
	cfg.ReleaseStaleDisputeWarnAfter = parseDuration("RELEASE_STALE_DISPUTE_WARN_AFTER", "168h", &errs)
	cfg.ReleaseDisputeWindowOverride = parseDuration("RELEASE_DISPUTE_WINDOW_OVERRIDE", "0s", &errs)
	cfg.ReleaseDisputeWindowCacheTTL = parseDuration("RELEASE_DISPUTE_WINDOW_CACHE_TTL", "15m", &errs)

	if len(errs) > 0 {
		return nil, fmt.Errorf("config load errors:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

// Validate checks all constraints on a loaded Config and returns a slice of human-readable
// error strings — one per invalid field. The caller should log all of them then os.Exit(1).
// Validate is separate from Load so callers can validate after post-load adjustments.
func (c *Config) Validate() []string {
	var errs []string

	if c.WorkerKeystorePath == "" {
		errs = append(errs, "WORKER_KEYSTORE_PATH is required")
	}
	if c.WorkerKeystorePassword == "" {
		errs = append(errs, "WORKER_KEYSTORE_PASSWORD is required")
	}
	if c.ChainID <= 0 {
		errs = append(errs, "CHAIN_ID must be positive")
	}
	if c.WorkerRegistryAddress == (common.Address{}) {
		errs = append(errs, "WORKER_REGISTRY_ADDRESS must be a non-zero address")
	}
	if c.AIConfigAddress == (common.Address{}) {
		errs = append(errs, "AI_CONFIG_ADDRESS must be a non-zero address")
	}
	if c.JobRegistryAddress == (common.Address{}) {
		errs = append(errs, "JOB_REGISTRY_ADDRESS must be a non-zero address")
	}
	if len(c.SupportedModels) == 0 {
		errs = append(errs, "SUPPORTED_MODELS: at least one model is required")
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, "HEARTBEAT_INTERVAL must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, "SHUTDOWN_TIMEOUT must be positive")
	}
	if c.GasPriceMultiplierBps <= 0 {
		errs = append(errs, "GAS_PRICE_MULTIPLIER_BPS must be positive")
	}
	if c.MaxConcurrentJobs <= 0 {
		errs = append(errs, "MAX_CONCURRENT_JOBS must be positive")
	}
	if c.OllamaTimeout <= 0 {
		errs = append(errs, "OLLAMA_TIMEOUT must be positive")
	}
	if c.AckTxTimeout <= 0 {
		errs = append(errs, "ACK_TX_TIMEOUT must be positive")
	}
	if c.BlobTxTimeout <= 0 {
		errs = append(errs, "BLOB_TX_TIMEOUT must be positive")
	}
	if c.BlobFetchTimeout <= 0 {
		errs = append(errs, "BLOB_FETCH_TIMEOUT must be positive")
	}
	if c.BlobFetchRetries <= 0 {
		errs = append(errs, "BLOB_FETCH_RETRIES must be positive")
	}
	if c.ReceiptPollInterval <= 0 {
		errs = append(errs, "RECEIPT_POLL_INTERVAL must be positive")
	}
	if c.RedisPublishTimeout <= 0 {
		errs = append(errs, "REDIS_PUBLISH_TIMEOUT must be positive")
	}
	// num_predict accepts Ollama's sentinels -1 (unlimited) and -2 (fill
	// context); 0 would ask for an empty generation, which is never
	// intended and would silently produce blank responses.
	if c.OllamaNumPredict == 0 || c.OllamaNumPredict < -2 {
		errs = append(errs, fmt.Sprintf("OLLAMA_NUM_PREDICT must be positive, -1 (unlimited) or -2 (fill context), got %d", c.OllamaNumPredict))
	}
	if c.OllamaNumCtx < 0 {
		errs = append(errs, "OLLAMA_NUM_CTX must be >= 0 (0 leaves the model default)")
	}
	if c.StreamChunkTokens <= 0 {
		errs = append(errs, "STREAM_CHUNK_TOKENS must be positive")
	}
	if c.StreamChunkInterval <= 0 {
		errs = append(errs, "STREAM_CHUNK_INTERVAL must be positive")
	}
	if c.DeadlineGuardEnabled {
		if c.SettleReserve <= 0 {
			errs = append(errs, "DEADLINE_SETTLE_RESERVE must be positive")
		}
		if c.CompletionReserve <= 0 {
			errs = append(errs, "DEADLINE_COMPLETION_RESERVE must be positive")
		}
		if c.MinInferenceBudget <= 0 {
			errs = append(errs, "DEADLINE_MIN_INFERENCE_BUDGET must be positive")
		}
		if c.SettleReserve <= c.CompletionReserve {
			errs = append(errs, fmt.Sprintf("DEADLINE_SETTLE_RESERVE (%s) must be > DEADLINE_COMPLETION_RESERVE (%s) - the reserve covers the blob tx AND completeJob, the completion floor only the last leg",
				c.SettleReserve, c.CompletionReserve))
		}
	}
	if c.STTEnabled {
		if err := validateSidecarURL("STT_SIDECAR_URL", c.STTSidecarURL); err != nil {
			errs = append(errs, err.Error())
		}
		if c.STTTimeout <= 0 {
			errs = append(errs, "STT_TIMEOUT must be positive")
		}
	}
	if c.TTSEnabled {
		if err := validateSidecarURL("TTS_SIDECAR_URL", c.TTSSidecarURL); err != nil {
			errs = append(errs, err.Error())
		}
		if c.TTSTimeout <= 0 {
			errs = append(errs, "TTS_TIMEOUT must be positive")
		}
		if c.TTSMaxChars < 0 {
			errs = append(errs, "TTS_MAX_CHARS must be >= 0 (0 = no limit)")
		}
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT: must be \"json\" or \"text\", got %q", c.LogFormat))
	}
	if c.StuckNonceThreshold <= 0 {
		errs = append(errs, "WORKER_STUCK_NONCE_THRESHOLD must be positive")
	}
	if c.StuckNonceMaxBumps <= 0 {
		errs = append(errs, "WORKER_STUCK_NONCE_MAX_BUMPS must be positive")
	}
	if err := metrics.ValidateListenAddr(c.MetricsListenAddr, c.MetricsAllowPublic); err != nil {
		errs = append(errs, fmt.Sprintf("WORKER_METRICS_ADDR: %v", err))
	}

	// Release scheduler validation. The Tracker needs StatePath even when
	// the rest of the subsystem is disabled, so it is checked regardless.
	if c.ReleaseStatePath == "" {
		errs = append(errs, "RELEASE_STATE_PATH must not be empty")
	}
	if c.ReleaseEnabled {
		if c.ReleaseInterval <= 0 {
			errs = append(errs, "RELEASE_INTERVAL must be positive")
		}
		if c.ReleaseProbeInterval <= 0 {
			errs = append(errs, "RELEASE_PROBE_INTERVAL must be positive")
		}
		if c.ReleaseProbeInterval > c.ReleaseInterval {
			errs = append(errs, fmt.Sprintf("RELEASE_PROBE_INTERVAL (%s) must be ≤ RELEASE_INTERVAL (%s)",
				c.ReleaseProbeInterval, c.ReleaseInterval))
		}
		if c.ReleaseBatchThreshold < 1 {
			errs = append(errs, "RELEASE_BATCH_THRESHOLD must be ≥ 1")
		}
		if c.ReleaseMaxBatchSize < 1 {
			errs = append(errs, "RELEASE_MAX_BATCH_SIZE must be ≥ 1")
		}
		if c.ReleaseTxTimeout <= 0 {
			errs = append(errs, "RELEASE_TX_TIMEOUT must be positive")
		}
		if c.ReleaseChunkSize == 0 {
			errs = append(errs, "RELEASE_RECONCILE_CHUNK_SIZE must be ≥ 1")
		}
		if c.ReleaseReconcileInterval <= 0 {
			errs = append(errs, "RELEASE_RECONCILE_INTERVAL must be positive")
		}
		if c.ReleaseBackoffBase <= 0 {
			errs = append(errs, "RELEASE_BACKOFF_BASE must be positive")
		}
		if c.ReleaseBackoffMax < c.ReleaseBackoffBase {
			errs = append(errs, fmt.Sprintf("RELEASE_BACKOFF_MAX (%s) must be ≥ RELEASE_BACKOFF_BASE (%s)",
				c.ReleaseBackoffMax, c.ReleaseBackoffBase))
		}
		if c.ReleasePausedCycleBackoff <= 0 {
			errs = append(errs, "RELEASE_PAUSED_CYCLE_BACKOFF must be positive")
		}
		if c.ReleaseDisputeWindowCacheTTL < 0 {
			errs = append(errs, "RELEASE_DISPUTE_WINDOW_CACHE_TTL must be ≥ 0")
		}
	}

	return errs
}

// ParseModelID converts a string to the bytes32 model ID format used on-chain.
// Accepts:
//   - "0x{64 hex chars}" — decoded as-is into [32]byte
//   - any other string   — keccak256([]byte(s)), keeping first 32 bytes
func ParseModelID(s string) ([32]byte, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		hex := s[2:]
		if len(hex) != 64 {
			return [32]byte{}, fmt.Errorf("ParseModelID: hex model ID must be 64 hex chars (got %d): %q", len(hex), s)
		}
		b, err := hexDecode(hex)
		if err != nil {
			return [32]byte{}, fmt.Errorf("ParseModelID: invalid hex %q: %w", s, err)
		}
		var id [32]byte
		copy(id[:], b)
		return id, nil
	}

	// keccak256 of the UTF-8 name
	hash := crypto.Keccak256([]byte(s))
	var id [32]byte
	copy(id[:], hash)
	return id, nil
}

// hexDecode decodes a hex string (without 0x prefix) into bytes.
func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex string")
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi, ok1 := hexNibble(s[i])
		lo, ok2 := hexNibble(s[i+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("invalid hex character at position %d", i)
		}
		b[i/2] = hi<<4 | lo
	}
	return b, nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// validateSidecarURL requires a parseable http(s) URL with a host. Only
// consulted when the matching voice feature is enabled - a disabled
// sidecar's URL is never dialed, so it is not validated.
func validateSidecarURL(key, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%s: must be a parseable http(s) URL, got %q", key, raw)
	}
	return nil
}

func parseDuration(key, defaultVal string, errs *[]string) time.Duration {
	raw := envOrDefault(key, defaultVal)
	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: invalid duration %q: %v", key, raw, err))
		return 0
	}
	return d
}

// parseBool reads an env var and parses standard truthy/falsy strings.
// Accepts: true/false, 1/0, yes/no, on/off (case-insensitive). Empty
// defaults to defaultVal.
func parseBool(key string, defaultVal bool, errs *[]string) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultVal
	}
	switch strings.ToLower(raw) {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		*errs = append(*errs, fmt.Sprintf("%s: invalid boolean %q", key, raw))
		return defaultVal
	}
}

// parseUint64 reads an env var as a non-negative integer that fits in uint64.
// Returns defaultVal on empty/missing.
func parseUint64(key string, defaultVal uint64, errs *[]string) uint64 {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultVal
	}
	v := new(big.Int)
	if _, ok := v.SetString(raw, 10); !ok {
		*errs = append(*errs, fmt.Sprintf("%s: invalid integer %q", key, raw))
		return defaultVal
	}
	if v.Sign() < 0 {
		*errs = append(*errs, fmt.Sprintf("%s: must be non-negative", key))
		return defaultVal
	}
	if !v.IsUint64() {
		*errs = append(*errs, fmt.Sprintf("%s: value %q overflows uint64", key, raw))
		return defaultVal
	}
	return v.Uint64()
}

func parseInt(key string, defaultVal int, errs *[]string) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultVal
	}
	v := new(big.Int)
	if _, ok := v.SetString(raw, 10); !ok {
		*errs = append(*errs, fmt.Sprintf("%s: invalid integer %q", key, raw))
		return defaultVal
	}
	if !v.IsInt64() {
		*errs = append(*errs, fmt.Sprintf("%s: value %q overflows int64", key, raw))
		return defaultVal
	}
	i64 := v.Int64()
	if int64(int(i64)) != i64 {
		*errs = append(*errs, fmt.Sprintf("%s: value %q overflows int", key, raw))
		return defaultVal
	}
	return int(i64)
}

// Package metrics defines the Prometheus instrumentation surface for the
// worker sidecar. All collectors are registered on a Metrics-owned Registry
// rather than the global default — this keeps tests isolated (each test
// builds its own *Metrics) and prevents duplicate-registration panics
// across service.New invocations.
//
// Label cardinality is bounded at the package boundary: every label value
// must be one of the closed-enum constants below or pass through
// (*Metrics).NormalizeModel. New error categories require adding a Reason*
// constant and a ClassifyError case — there is no path for raw err.Error()
// strings to escape into label values.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Closed-enum constants for label values. All label-bound strings funnel
// through these so dashboards have a stable contract and label cardinality
// stays bounded.
const (
	// Outcome — captured per stage and per job.
	OutcomeOK      = "ok"
	OutcomeError   = "error"
	OutcomeSkipped = "skipped"
	OutcomePanic   = "panic"

	// Cache state — applies to stages whose work can be short-circuited by
	// a checkpoint hit, and to the JobTotalDuration histogram. Stages where
	// caching does not apply (ack, redis_publish, blob submit) emit "none".
	CacheHit  = "hit"
	CacheMiss = "miss"
	CacheNone = "none"

	// Delivery — set once per JobHandler at construction.
	DeliveryAsynq   = "asynq"
	DeliveryGateway = "gateway"

	// Stage names — must match the slog "stage" attribute used in
	// pipeline/handler.go, so log-based and metric-based diagnostics can
	// be cross-referenced by stage name.
	//
	// ack and ack_confirm split what used to be a single blocking stage 1.
	// With ACK_OVERLAP_ENABLED, ack covers only the broadcast and
	// ack_confirm covers the later join on the receipt; the two together
	// are comparable to the old ack timing. With overlap off, ack keeps
	// its original meaning and ack_confirm is never observed.
	StageAck          = "ack"
	StageAckConfirm   = "ack_confirm"
	StageFetchBlob    = "fetch_blob"
	StageSessionKey   = "session_key"
	StageDecrypt      = "decrypt"
	StageInference    = "inference"
	StageEncrypt      = "encrypt"
	StageRedisPublish = "redis_publish"
	StageSubmitBlob   = "submit_blob"
	StageCompleteJob  = "complete_job"

	// Reason — bounded set for JobsTotal{reason} on failure paths.
	// Empty string is reserved for the success path.
	ReasonDecryptFailed         = "decrypt_failed"
	ReasonOllamaUnreachable     = "ollama_unreachable"
	ReasonOllamaTimeout         = "ollama_timeout"
	ReasonBlobFetchFailed       = "blob_fetch_failed"
	ReasonBlobSubmitFailed      = "blob_submit_failed"
	ReasonRedisPublishFailed    = "redis_publish_failed"
	ReasonSessionKeyUnavailable = "session_key_unavailable"
	ReasonCheckpointResume      = "checkpoint_resume_failed"
	ReasonChainTxFailed         = "chain_tx_failed"
	ReasonPanic                 = "panic"
	ReasonCtxCanceled           = "ctx_canceled"
	ReasonUnknown               = "unknown"

	// CheckpointEvents{event} — must match log sites in pipeline/handler.go.
	CheckpointEventHitInference = "hit_inference"
	CheckpointEventHitDelivered = "hit_delivered"
	CheckpointEventHitBlob      = "hit_blob"
	CheckpointEventRaceLost     = "race_lost"
	CheckpointEventMiss         = "miss"
	CheckpointEventTombstoned   = "tombstoned"

	// SessionKeyEvents{path} — mirrors the slog "path" attribute on stage 3.
	SessionKeyPathCacheHit    = "cache_hit"
	SessionKeyPathChainDerive = "chain_derive"
	SessionKeyPathRefresh     = "refresh"

	// SubpoolClass — label values for worker_subpool_inflight.
	SubpoolClassBlob   = "blob"
	SubpoolClassLegacy = "legacy"

	// ModelUnknown is the fallback label value returned by NormalizeModel
	// for inputs not in the configured allowlist.
	ModelUnknown = "unknown"
)

// StageBuckets and JobBuckets are explicit boundaries (not exponential)
// because inference latency is bimodal — short responses cluster around
// 1-3s and long responses around 10-60s. Exponential factor=3 would give
// only 5 buckets in the entire 1-60s band, producing poor p95 estimates.
var (
	StageBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
	JobBuckets   = []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}
)

// Metrics owns a Prometheus registry and all worker collectors. Not a
// singleton: service.New constructs one for production; each test
// constructs its own.
type Metrics struct {
	Registry *prometheus.Registry

	// Histograms
	StageDuration    *prometheus.HistogramVec // labels: stage, model, cache, delivery, outcome
	JobTotalDuration *prometheus.HistogramVec // labels: cache, delivery, outcome (no model — cardinality)

	// Counters
	JobsTotal            *prometheus.CounterVec // labels: outcome, reason, model, delivery
	CheckpointEvents     *prometheus.CounterVec // labels: event
	SessionKeyEvents     *prometheus.CounterVec // labels: path
	RedisPublishFailures prometheus.Counter

	// Gauges (set directly)
	MaxJobs           prometheus.Gauge
	OllamaUp          prometheus.Gauge
	HeartbeatLastEmit prometheus.Gauge

	// Release subsystem.
	ReleasePending             prometheus.Gauge   // current count of pending jobs awaiting settlement
	ReleaseReleasedTotal       prometheus.Counter // jobs successfully released (cumulative)
	ReleaseFailedTotal         prometheus.Counter // per-job release failures (post-fallback)
	ReleaseDroppedTotal        *prometheus.CounterVec // labels: reason (terminal_state | foreign_worker | resolved_zero_escrow)
	ReleasePauseEventsTotal    prometheus.Counter // batch reverts classified as pause-class
	ReleaseLastSuccessTimestamp prometheus.Gauge  // unix seconds of last successful release tx
	ReleaseReconcileLastBlock  prometheus.Gauge   // last block number reconciled

	// modelAllowlist gates the {model} label. Captured at construction so
	// each *Metrics is isolated from any other (parallel tests, multiple
	// service instances in a process). Lowercased on insertion.
	modelAllowlist map[string]struct{}

	// bound tracks whether Bind has been called — duplicate Bind would
	// re-register GaugeFunc/CounterFunc collectors and panic on the
	// underlying registry.
	bound bool
}

// New constructs a Metrics with its own registry. modelAllowlist is the set
// of model identifiers (Ollama tags) that may appear as the {model} label;
// any other input maps to "unknown". Pass cfg.SupportedModels in production.
func New(modelAllowlist []string) *Metrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)

	allow := make(map[string]struct{}, len(modelAllowlist))
	for _, m := range modelAllowlist {
		s := strings.ToLower(strings.TrimSpace(m))
		if s != "" {
			allow[s] = struct{}{}
		}
	}

	return &Metrics{
		Registry:       reg,
		modelAllowlist: allow,

		StageDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "worker_pipeline_stage_duration_seconds",
			Help:    "Per-stage duration of the inference pipeline. cache=hit indicates the stage was short-circuited by a checkpoint cache.",
			Buckets: StageBuckets,
		}, []string{"stage", "model", "cache", "delivery", "outcome"}),

		JobTotalDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "worker_job_total_duration_seconds",
			Help:    "End-to-end job duration measured from processJob entry to return.",
			Buckets: JobBuckets,
		}, []string{"cache", "delivery", "outcome"}),

		JobsTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_jobs_total",
			Help: "Total jobs processed by terminal outcome. reason is empty on success.",
		}, []string{"outcome", "reason", "model", "delivery"}),

		CheckpointEvents: f.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_checkpoint_events_total",
			Help: "Checkpoint cache lifecycle events observed by the pipeline handler.",
		}, []string{"event"}),

		SessionKeyEvents: f.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_session_key_events_total",
			Help: "Session-key acquisition path observed at stage 3.",
		}, []string{"path"}),

		RedisPublishFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "worker_redis_publish_failures_total",
			Help: "Stage 7 PUBLISH failures (non-fatal — the on-chain blob is the authoritative response).",
		}),

		MaxJobs: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_max_jobs",
			Help: "Maximum concurrent jobs configured for this worker.",
		}),

		OllamaUp: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_ollama_up",
			Help: "1 when the Ollama health-check returned 200, 0 otherwise (including check-itself errors).",
		}),

		HeartbeatLastEmit: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_heartbeat_last_emit_timestamp_seconds",
			Help: "Unix timestamp of the last successful heartbeat write to Redis.",
		}),

		ReleasePending: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_release_pending",
			Help: "Current number of jobs awaiting on-chain release in the local store.",
		}),

		ReleaseReleasedTotal: f.NewCounter(prometheus.CounterOpts{
			Name: "worker_release_released_total",
			Help: "Cumulative count of jobs successfully released on-chain.",
		}),

		ReleaseFailedTotal: f.NewCounter(prometheus.CounterOpts{
			Name: "worker_release_failed_total",
			Help: "Per-job release failures after batch fallback. Pause-class cycle reverts are tracked separately by worker_release_pause_events_total.",
		}),

		ReleaseDroppedTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_release_dropped_total",
			Help: "Pending entries dropped without release. reason in {terminal_state, foreign_worker, resolved_zero_escrow}.",
		}, []string{"reason"}),

		ReleasePauseEventsTotal: f.NewCounter(prometheus.CounterOpts{
			Name: "worker_release_pause_events_total",
			Help: "Batch release reverts classified as pause-class (Pausable: paused / EnforcedPause).",
		}),

		ReleaseLastSuccessTimestamp: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_release_last_success_timestamp_seconds",
			Help: "Chain timestamp of the last successful release transaction.",
		}),

		ReleaseReconcileLastBlock: f.NewGauge(prometheus.GaugeOpts{
			Name: "worker_release_reconcile_last_block",
			Help: "Highest block number scanned by the release reconciler.",
		}),
	}
}

// Bind wires existing in-process atomic surfaces to GaugeFunc/CounterFunc
// collectors registered on m.Registry. Returns an error if Bind is called
// more than once on the same Metrics — duplicate registration would panic
// the underlying registry.
//
// Each accessor is a closure rather than a typed atomic so callers can
// adapt non-atomic surfaces (coordinator.Inflight takes a class argument;
// tracker.Size takes a lock).
func (m *Metrics) Bind(
	jobCounter *atomic.Int32,
	inflightBlob func() int,
	inflightLegacy func() int,
	orphanCount func() int64,
	stuckTracked func() int,
	stuckMaxHits func() int,
) error {
	if m.bound {
		return errors.New("metrics: Bind called more than once")
	}
	f := promauto.With(m.Registry)

	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_active_jobs",
		Help: "Current in-flight job count, read from the shared atomic counter.",
	}, func() float64 { return float64(jobCounter.Load()) })

	// worker_subpool_inflight is one metric with class={blob,legacy}; we
	// register two GaugeFuncs with the same Name but distinct ConstLabels
	// because GaugeFunc has no runtime label slots. The registry treats
	// them as distinct time series via their Desc fingerprint.
	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "worker_subpool_inflight",
		Help:        "Pending tx broadcasts in the subpool coordinator, by class.",
		ConstLabels: prometheus.Labels{"class": SubpoolClassBlob},
	}, func() float64 { return float64(inflightBlob()) })

	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "worker_subpool_inflight",
		Help:        "Pending tx broadcasts in the subpool coordinator, by class.",
		ConstLabels: prometheus.Labels{"class": SubpoolClassLegacy},
	}, func() float64 { return float64(inflightLegacy()) })

	f.NewCounterFunc(prometheus.CounterOpts{
		Name: "worker_subpool_orphans_total",
		Help: "Cumulative count of in-flight tokens evicted by the coordinator janitor.",
	}, func() float64 { return float64(orphanCount()) })

	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_stuck_nonce_tracked",
		Help: "Number of nonces currently tracked as stuck in the mempool.",
	}, func() float64 { return float64(stuckTracked()) })

	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "worker_stuck_nonce_max_hits",
		Help: "Largest consecutive 'address already reserved' rejection count across all tracked nonces.",
	}, func() float64 { return float64(stuckMaxHits()) })

	m.bound = true
	return nil
}

// NormalizeModel maps a model identifier (Ollama tag, hex bytes32, etc.)
// to a bounded label value. Unknown inputs return ModelUnknown so
// cardinality is always bounded by len(modelAllowlist)+1.
func (m *Metrics) NormalizeModel(modelID string) string {
	s := strings.ToLower(strings.TrimSpace(modelID))
	if s == "" {
		return ModelUnknown
	}
	if _, ok := m.modelAllowlist[s]; ok {
		return s
	}
	return ModelUnknown
}

// StageRecorder captures stage-invariant labels at start so call sites only
// pass the resolved outcome+cache at end. Returned by StartStage; not safe
// for concurrent End calls on the same recorder.
type StageRecorder struct {
	m        *Metrics
	stage    string
	model    string
	delivery string
	start    time.Time
}

// StartStage begins a stage observation. Call End once with the resolved
// outcome and cache state to record the histogram sample.
func (m *Metrics) StartStage(stage, model, delivery string) *StageRecorder {
	return &StageRecorder{
		m:        m,
		stage:    stage,
		model:    model,
		delivery: delivery,
		start:    time.Now(),
	}
}

// End observes a sample on StageDuration with the given outcome+cache labels
// and returns the elapsed duration so callers can include it in their slog
// "stage complete" line without re-measuring.
func (r *StageRecorder) End(outcome, cache string) time.Duration {
	d := time.Since(r.start)
	if r == nil || r.m == nil {
		return d
	}
	r.m.StageDuration.
		WithLabelValues(r.stage, r.model, cache, r.delivery, outcome).
		Observe(d.Seconds())
	return d
}

// Elapsed returns the time since StartStage. Useful for slog lines on the
// success path before End is called (rare).
func (r *StageRecorder) Elapsed() time.Duration {
	if r == nil {
		return 0
	}
	return time.Since(r.start)
}

// OutcomeFor maps a Go error to the closed Outcome enum used by stage and
// job-total histograms.
func OutcomeFor(err error) string {
	if err == nil {
		return OutcomeOK
	}
	return OutcomeError
}

// ClassifyError maps an error returned by processJob to the closed Reason
// enum used as the {reason} label on JobsTotal. Falls through to
// ReasonUnknown without logging — callers should already have logged the
// original error; the metric just classifies.
//
// Coupling: this matches the "stage N (...)" wrap convention in
// pipeline/handler.go. Tests pin the mapping so a change to that convention
// fails loudly rather than silently degrading dashboard reason cardinality.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ReasonCtxCanceled
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "stage 1"):
		return ReasonChainTxFailed
	case strings.Contains(s, "stage 2"):
		return ReasonBlobFetchFailed
	case strings.Contains(s, "stage 3"):
		return ReasonSessionKeyUnavailable
	case strings.Contains(s, "stage 4"):
		return ReasonDecryptFailed
	case strings.Contains(s, "stage 5"):
		if strings.Contains(s, "timeout") || strings.Contains(s, "deadline") {
			return ReasonOllamaTimeout
		}
		return ReasonOllamaUnreachable
	case strings.Contains(s, "stage 6"):
		// Encrypt is symmetric with decrypt; same key-handling path classifies
		// failures as decrypt_failed for dashboard consistency.
		return ReasonDecryptFailed
	case strings.Contains(s, "stage 7"):
		return ReasonRedisPublishFailed
	case strings.Contains(s, "stage 8"):
		// Stages 8a (submit_blob) and 8b (complete_job) — caller may
		// override by passing a more specific reason if it has one.
		if strings.Contains(s, "submit blob") {
			return ReasonBlobSubmitFailed
		}
		return ReasonChainTxFailed
	case strings.Contains(s, "checkpoint"):
		return ReasonCheckpointResume
	default:
		return ReasonUnknown
	}
}

// ValidateListenAddr returns nil if addr is empty (metrics server disabled)
// or bound to a loopback host; otherwise it returns an error unless
// allowPublic is true. This is the choke point that prevents accidentally
// exposing /metrics on 0.0.0.0.
func ValidateListenAddr(addr string, allowPublic bool) error {
	if addr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if allowPublic {
		return nil
	}
	switch host {
	case "127.0.0.1", "localhost", "::1":
		return nil
	case "", "0.0.0.0", "::":
		return errors.New("metrics: listen address must be loopback unless WORKER_METRICS_ALLOW_PUBLIC=true")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return errors.New("metrics: listen address must be loopback unless WORKER_METRICS_ALLOW_PUBLIC=true")
}

// Server returns an HTTP server bound to addr serving /metrics from
// m.Registry and a /healthz liveness endpoint. Caller owns the goroutine
// lifecycle and graceful shutdown via srv.Shutdown(ctx).
func (m *Metrics) Server(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

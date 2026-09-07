package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Test models — kept small and deterministic. Real cfg.SupportedModels are
// pulled from env at runtime; tests construct an explicit list per case so
// allowlist semantics are unambiguous.
var testModels = []string{"llama3.2:3b", "mistral:7b"}

func TestNew_RegistersExpectedMetrics(t *testing.T) {
	m := New(testModels)

	// Touch every label set so the time series materializes — Prometheus
	// only exposes a series after the first observation/inc.
	m.StageDuration.WithLabelValues(StageInference, "llama3.2:3b", TierStandard, CacheMiss, DeliveryAsynq, OutcomeOK).Observe(0.5)
	m.JobTotalDuration.WithLabelValues(CacheMiss, DeliveryAsynq, OutcomeOK).Observe(2.0)
	m.JobTTFT.WithLabelValues(TierMax, "llama3.2:3b", WarmTrue, DeliveryAsynq).Observe(0.8)
	m.JobTokensPerSecond.WithLabelValues(TierStandard, "llama3.2:3b").Observe(42.5)
	m.JobOutputTokens.WithLabelValues(TierStandard, "llama3.2:3b").Observe(512)
	m.JobQueueWait.WithLabelValues(TierStandard, "llama3.2:3b").Observe(1.2)
	m.JobDeadlineHeadroom.WithLabelValues(TierStandard, "llama3.2:3b").Observe(45)
	m.JobsTotal.WithLabelValues(OutcomeOK, "", "llama3.2:3b", TierStandard, DeliveryAsynq).Inc()
	m.CheckpointEvents.WithLabelValues(CheckpointEventMiss).Inc()
	m.SessionKeyEvents.WithLabelValues(SessionKeyPathCacheHit).Inc()
	m.RedisPublishFailures.Inc()
	m.MaxJobs.Set(2)
	m.OllamaUp.Set(1)
	m.HeartbeatLastEmit.SetToCurrentTime()

	got := gatherNames(t, m.Registry)
	want := []string{
		"worker_pipeline_stage_duration_seconds",
		"worker_job_total_duration_seconds",
		"worker_job_ttft_seconds",
		"worker_job_tokens_per_second",
		"worker_job_output_tokens",
		"worker_job_queue_wait_seconds",
		"worker_job_deadline_headroom_seconds",
		"worker_jobs_total",
		"worker_checkpoint_events_total",
		"worker_session_key_events_total",
		"worker_redis_publish_failures_total",
		"worker_max_jobs",
		"worker_ollama_up",
		"worker_heartbeat_last_emit_timestamp_seconds",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q not registered (registered: %v)", name, sortedKeys(got))
		}
	}
}

func TestNew_TwoInstancesAreIsolated(t *testing.T) {
	// The whole point of owning a registry: two New() calls in the same
	// process must not collide. If this panics with AlreadyRegisteredError,
	// the package has regressed to using the global default registry.
	m1 := New(testModels)
	m2 := New(testModels)

	m1.MaxJobs.Set(1)
	m2.MaxJobs.Set(2)

	if got := readGauge(t, m1.Registry, "worker_max_jobs"); got != 1 {
		t.Errorf("m1.MaxJobs = %v, want 1", got)
	}
	if got := readGauge(t, m2.Registry, "worker_max_jobs"); got != 2 {
		t.Errorf("m2.MaxJobs = %v, want 2", got)
	}
}

func TestStartStage_RecordsObservationAndReturnsElapsed(t *testing.T) {
	m := New(testModels)

	rec := m.StartStage(StageDecrypt, "llama3.2:3b", DeliveryAsynq)
	time.Sleep(5 * time.Millisecond)
	d := rec.End(OutcomeOK, CacheMiss)

	if d < 5*time.Millisecond {
		t.Errorf("End returned %v, expected >= 5ms", d)
	}

	// Confirm a sample landed on the right label set.
	mf := findMetricFamily(t, m.Registry, "worker_pipeline_stage_duration_seconds")
	if got := totalCount(mf); got != 1 {
		t.Errorf("histogram sample count = %d, want 1", got)
	}
	labels := firstSeriesLabels(mf)
	if labels["stage"] != StageDecrypt {
		t.Errorf("stage label = %q, want %q", labels["stage"], StageDecrypt)
	}
	if labels["tier"] != TierStandard {
		t.Errorf("tier label = %q, want %q", labels["tier"], TierStandard)
	}
	if labels["outcome"] != OutcomeOK {
		t.Errorf("outcome label = %q, want %q", labels["outcome"], OutcomeOK)
	}
	if labels["cache"] != CacheMiss {
		t.Errorf("cache label = %q, want %q", labels["cache"], CacheMiss)
	}
}

func TestStartStage_DerivesTierFromModelName(t *testing.T) {
	m := New(testModels)

	m.StartStage(StageInference, "agentworld-35b-max", DeliveryAsynq).End(OutcomeOK, CacheMiss)

	mf := findMetricFamily(t, m.Registry, "worker_pipeline_stage_duration_seconds")
	labels := firstSeriesLabels(mf)
	if labels["tier"] != TierMax {
		t.Errorf("tier label = %q, want %q", labels["tier"], TierMax)
	}
}

func TestTierForModel_SuffixRule(t *testing.T) {
	cases := []struct {
		in, wantTier, wantBase string
	}{
		{"agentworld-35b-max", TierMax, "agentworld-35b"},
		{"gpt-oss-20b-max", TierMax, "gpt-oss-20b"},
		{"AgentWorld-35B-MAX", TierMax, "agentworld-35b"}, // case-insensitive
		{"  gpt-oss-20b-max  ", TierMax, "gpt-oss-20b"},   // whitespace trimmed
		{"agentworld-35b", TierStandard, "agentworld-35b"},
		{"llama3.2:3b", TierStandard, "llama3.2:3b"},
		{"maximus", TierStandard, "maximus"}, // infix, not suffix
		{"-max", TierMax, ""},                // degenerate but deterministic
		{ModelUnknown, TierStandard, ModelUnknown},
		{"", TierStandard, ""},
	}
	for _, tc := range cases {
		tier, base := TierForModel(tc.in)
		if tier != tc.wantTier || base != tc.wantBase {
			t.Errorf("TierForModel(%q) = (%q, %q), want (%q, %q)",
				tc.in, tier, base, tc.wantTier, tc.wantBase)
		}
	}
}

func TestBind_GaugeFuncReadsLiveSurfaces(t *testing.T) {
	m := New(testModels)

	var jobCounter atomic.Int32
	var inflightBlob int
	var inflightLegacy int
	var orphans int64
	var stuck int
	var maxHits int

	err := m.Bind(
		&jobCounter,
		func() int { return inflightBlob },
		func() int { return inflightLegacy },
		func() int64 { return orphans },
		func() int { return stuck },
		func() int { return maxHits },
	)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	// Initial scrape — all zero.
	if got := readGauge(t, m.Registry, "worker_active_jobs"); got != 0 {
		t.Errorf("worker_active_jobs initial = %v, want 0", got)
	}

	// Mutate underlying state, scrape again — values follow.
	jobCounter.Store(3)
	inflightBlob = 2
	inflightLegacy = 1
	orphans = 7
	stuck = 4
	maxHits = 11

	if got := readGauge(t, m.Registry, "worker_active_jobs"); got != 3 {
		t.Errorf("worker_active_jobs = %v, want 3", got)
	}

	// worker_subpool_inflight has two series via ConstLabels {class}.
	mf := findMetricFamily(t, m.Registry, "worker_subpool_inflight")
	values := map[string]float64{}
	for _, s := range mf.GetMetric() {
		var class string
		for _, lp := range s.GetLabel() {
			if lp.GetName() == "class" {
				class = lp.GetValue()
			}
		}
		values[class] = s.GetGauge().GetValue()
	}
	if values[SubpoolClassBlob] != 2 {
		t.Errorf("subpool_inflight{class=blob} = %v, want 2", values[SubpoolClassBlob])
	}
	if values[SubpoolClassLegacy] != 1 {
		t.Errorf("subpool_inflight{class=legacy} = %v, want 1", values[SubpoolClassLegacy])
	}

	if got := readCounter(t, m.Registry, "worker_subpool_orphans_total"); got != 7 {
		t.Errorf("worker_subpool_orphans_total = %v, want 7", got)
	}
	if got := readGauge(t, m.Registry, "worker_stuck_nonce_tracked"); got != 4 {
		t.Errorf("worker_stuck_nonce_tracked = %v, want 4", got)
	}
	if got := readGauge(t, m.Registry, "worker_stuck_nonce_max_hits"); got != 11 {
		t.Errorf("worker_stuck_nonce_max_hits = %v, want 11", got)
	}
}

func TestBind_RejectsDoubleCall(t *testing.T) {
	m := New(testModels)

	noop := func() int { return 0 }
	noop64 := func() int64 { return 0 }
	var c atomic.Int32

	if err := m.Bind(&c, noop, noop, noop64, noop, noop); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := m.Bind(&c, noop, noop, noop64, noop, noop); err == nil {
		t.Fatal("second Bind: expected error, got nil")
	}
}

func TestNormalizeModel_AllowlistOnly(t *testing.T) {
	m := New([]string{"Llama3.2:3b", "mistral:7b"}) // mixed case to confirm normalization

	cases := []struct {
		in, want string
	}{
		{"llama3.2:3b", "llama3.2:3b"},   // exact lowercase match
		{"LLAMA3.2:3B", "llama3.2:3b"},   // uppercase normalized
		{"  mistral:7b  ", "mistral:7b"}, // whitespace trimmed
		{"qwen2.5:7b", ModelUnknown},     // not in allowlist
		{"", ModelUnknown},               // empty
		{"0xdeadbeef", ModelUnknown},     // hex bytes32 — must be in allowlist or unknown
	}
	for _, tc := range cases {
		if got := m.NormalizeModel(tc.in); got != tc.want {
			t.Errorf("NormalizeModel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestClassifyError_ClosedEnum(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, ""},
		{context.Canceled, ReasonCtxCanceled},
		{context.DeadlineExceeded, ReasonCtxCanceled},
		{fmt.Errorf("stage 1 (ack): broadcast failed"), ReasonChainTxFailed},
		{fmt.Errorf("stage 2 (fetch blob 0xabc): not found"), ReasonBlobFetchFailed},
		{fmt.Errorf("stage 3 (session key): chain read failed"), ReasonSessionKeyUnavailable},
		{fmt.Errorf("stage 4 (decrypt prompt): cipher mismatch"), ReasonDecryptFailed},
		{fmt.Errorf("stage 5 (inference): connection refused"), ReasonOllamaUnreachable},
		{fmt.Errorf("stage 5 (inference): context deadline exceeded"), ReasonOllamaTimeout},
		{fmt.Errorf("stage 5 (inference): client timeout"), ReasonOllamaTimeout},
		{fmt.Errorf("stage 6 (encrypt response): cipher error"), ReasonDecryptFailed},
		{fmt.Errorf("stage 7 redis publish broken"), ReasonRedisPublishFailed},
		{fmt.Errorf("stage 8 (submit blob): pool rejected"), ReasonBlobSubmitFailed},
		{fmt.Errorf("stage 8 (complete job): tx reverted"), ReasonChainTxFailed},
		{fmt.Errorf("checkpoint resume failed: redis down"), ReasonCheckpointResume},
		{errors.New("something completely unexpected"), ReasonUnknown},
	}
	for _, tc := range cases {
		var label string
		if tc.err == nil {
			label = "<nil>"
		} else {
			label = tc.err.Error()
		}
		if got := ClassifyError(tc.err); got != tc.want {
			t.Errorf("ClassifyError(%s) = %q, want %q", label, got, tc.want)
		}
	}
}

func TestOutcomeFor(t *testing.T) {
	if got := OutcomeFor(nil); got != OutcomeOK {
		t.Errorf("OutcomeFor(nil) = %q, want %q", got, OutcomeOK)
	}
	if got := OutcomeFor(errors.New("boom")); got != OutcomeError {
		t.Errorf("OutcomeFor(err) = %q, want %q", got, OutcomeError)
	}
}

func TestValidateListenAddr_LoopbackEnforced(t *testing.T) {
	cases := []struct {
		addr        string
		allowPublic bool
		wantErr     bool
	}{
		{"", false, false},               // disabled is fine
		{"127.0.0.1:9101", false, false}, // canonical
		{"localhost:9101", false, false}, // host-name loopback
		{"[::1]:9101", false, false},     // ipv6 loopback
		{"0.0.0.0:9101", false, true},    // explicit wildcard rejected
		{":9101", false, true},           // implicit wildcard rejected
		{"10.0.0.5:9101", false, true},   // private LAN rejected
		{"10.0.0.5:9101", true, false},   // escape hatch
		{"0.0.0.0:9101", true, false},    // escape hatch covers wildcard too
		{"not-an-address", false, true},  // garbage rejected by SplitHostPort
	}
	for _, tc := range cases {
		err := ValidateListenAddr(tc.addr, tc.allowPublic)
		if (err != nil) != tc.wantErr {
			t.Errorf("ValidateListenAddr(%q, allowPublic=%v) err=%v, wantErr=%v", tc.addr, tc.allowPublic, err, tc.wantErr)
		}
	}
}

func TestServer_ServesMetricsAndHealthz(t *testing.T) {
	m := New(testModels)
	m.MaxJobs.Set(7) // observable in /metrics output

	// Use httptest.NewServer for an ephemeral port + automatic shutdown,
	// so the test stays parallel-safe and doesn't depend on 9101 being free.
	srv := httptest.NewServer(m.Server("").Handler)
	defer srv.Close()

	// /metrics
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "worker_max_jobs 7") {
		t.Errorf("/metrics body missing worker_max_jobs 7; got first 500 bytes:\n%s", first500(body))
	}

	// /healthz
	resp2, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200", resp2.StatusCode)
	}
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

func gatherNames(t *testing.T, reg *prometheus.Registry) map[string]struct{} {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := map[string]struct{}{}
	for _, mf := range mfs {
		names[mf.GetName()] = struct{}{}
	}
	return names
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func findMetricFamily(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func readGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mf := findMetricFamily(t, reg, name)
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		t.Fatalf("metric %q has no samples", name)
	}
	return metrics[0].GetGauge().GetValue()
}

func readCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mf := findMetricFamily(t, reg, name)
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		t.Fatalf("metric %q has no samples", name)
	}
	return metrics[0].GetCounter().GetValue()
}

func totalCount(mf *dto.MetricFamily) uint64 {
	var n uint64
	for _, m := range mf.GetMetric() {
		if h := m.GetHistogram(); h != nil {
			n += h.GetSampleCount()
		}
	}
	return n
}

func firstSeriesLabels(mf *dto.MetricFamily) map[string]string {
	out := map[string]string{}
	if len(mf.GetMetric()) == 0 {
		return out
	}
	for _, lp := range mf.GetMetric()[0].GetLabel() {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

func first500(b []byte) string {
	if len(b) <= 500 {
		return string(b)
	}
	return string(b[:500])
}

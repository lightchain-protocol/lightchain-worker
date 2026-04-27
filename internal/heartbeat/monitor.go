// Package heartbeat publishes periodic worker liveness data to Redis.
// The dispatcher reads these keys to determine worker availability for job routing.
package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/metrics"
)

const (
	// OllamaStatusReady indicates the Ollama API responded with HTTP 200.
	OllamaStatusReady = "ready"
	// OllamaStatusUnreachable indicates the Ollama API is down or returned a non-200 status.
	OllamaStatusUnreachable = "unreachable"
)

// HeartbeatPayload is the JSON structure written to Redis on each heartbeat tick.
// Field names and types are an implicit contract with the dispatcher (see Concern C-2).
// Do not change field names without coordinating story 6-1.
type HeartbeatPayload struct {
	WorkerAddress string   `json:"workerAddress"` // EIP-55 checksummed hex, no 0x prefix
	Timestamp     int64    `json:"timestamp"`     // Unix milliseconds
	ActiveJobs    int      `json:"activeJobs"`    // always 0 in 3-1; updated in 3-2
	MaxJobs       int      `json:"maxJobs"`       // always 0 in 3-1; set from config in 3-2
	Models        []string `json:"models"`        // 0x-prefixed lowercase hex bytes32
	OllamaStatus  string   `json:"ollamaStatus"`  // "ready" | "unreachable"
	Uptime        int64    `json:"uptimeSeconds"` // seconds since service start
}

// MonitorConfig holds heartbeat-specific configuration.
type MonitorConfig struct {
	Interval  time.Duration
	OllamaURL string
}

// Monitor publishes periodic heartbeat payloads to Redis.
type Monitor struct {
	redisClient *redis.Client
	cfg         MonitorConfig
	workerAddr  string   // EIP-55 checksummed hex, no 0x prefix
	modelIDs    []string // 0x-prefixed lowercase hex bytes32
	startedAt   time.Time
	httpClient  *http.Client
	logger      *slog.Logger
	jobCounter  *atomic.Int32
	maxJobs     int
	// metrics is optional — when nil the monitor still writes to Redis but
	// does not update OllamaUp / HeartbeatLastEmit. Production constructs
	// always pass non-nil; some tests pass nil to keep them focused.
	metrics *metrics.Metrics

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewMonitor creates a Monitor. workerAddr must be the EIP-55 checksummed hex address
// without the 0x prefix (used directly as the Redis key suffix).
// modelIDs must be 0x-prefixed lowercase hex bytes32 strings for JSON serialization.
// jobCounter is shared with the pipeline handler; maxJobs comes from config.
// metricsCollector may be nil (tests); when non-nil, OllamaUp and
// HeartbeatLastEmit gauges are updated on each emit.
func NewMonitor(
	redisClient *redis.Client,
	cfg MonitorConfig,
	workerAddr string,
	modelIDs []string,
	jobCounter *atomic.Int32,
	maxJobs int,
	logger *slog.Logger,
	metricsCollector *metrics.Metrics,
) *Monitor {
	return &Monitor{
		redisClient: redisClient,
		cfg:         cfg,
		workerAddr:  workerAddr,
		modelIDs:    modelIDs,
		startedAt:   time.Now(),
		httpClient:  &http.Client{Timeout: 2 * time.Second},
		logger:      logger,
		jobCounter:  jobCounter,
		maxJobs:     maxJobs,
		metrics:     metricsCollector,
		done:        make(chan struct{}),
	}
}

// Start launches the heartbeat goroutine. It emits immediately then ticks every cfg.Interval.
// The goroutine exits when ctx is cancelled or Stop is called.
func (m *Monitor) Start(ctx context.Context) {
	if m.cfg.Interval <= 0 {
		m.logger.Error("heartbeat interval must be positive, not starting monitor",
			"interval", m.cfg.Interval)
		return
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		// Emit immediately on start
		if err := m.emit(ctx); err != nil {
			m.logger.Warn("initial heartbeat emit failed", "error", err)
		}

		ticker := time.NewTicker(m.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.emit(ctx); err != nil {
					m.logger.Warn("heartbeat emit failed", "error", err)
				}
			case <-m.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop signals the heartbeat goroutine to exit and waits for it to finish.
// Stop is idempotent — calling it multiple times is safe.
func (m *Monitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.done)
	})
	m.wg.Wait()
}

// EmitOnce performs a single heartbeat write to Redis. Used at startup to gate
// service readiness on a successful dispatcher-key publication.
func (m *Monitor) EmitOnce(ctx context.Context) error {
	return m.emit(ctx)
}

// emit writes heartbeat data to Redis as an HSET hash.
// The key format and field names are defined in pkg/types to ensure the dispatcher
// can read exactly what the worker writes (fixing Concern C-2).
// Redis errors are returned but are non-fatal — the caller logs and continues.
func (m *Monitor) emit(ctx context.Context) error {
	ollamaStatus := m.checkOllama(ctx)
	activeJobs := int(m.jobCounter.Load())

	modelsJSON, err := json.Marshal(m.modelIDs)
	if err != nil {
		return fmt.Errorf("marshal model IDs: %w", err)
	}

	ttl := 3 * m.cfg.Interval
	key := pkgtypes.HeartbeatRedisKey(m.workerAddr)

	pipe := m.redisClient.TxPipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		pkgtypes.HBFieldLastHeartbeat: time.Now().Unix(),
		pkgtypes.HBFieldActiveJobs:    activeJobs,
		pkgtypes.HBFieldMaxJobs:       m.maxJobs,
		pkgtypes.HBFieldLatencyMs:     0,
		pkgtypes.HBFieldGPUUtil:       strconv.FormatFloat(0, 'f', -1, 64),
		pkgtypes.HBFieldStatus:        pkgtypes.HeartbeatStatusActive,
		pkgtypes.HBFieldModels:        string(modelsJSON),
		pkgtypes.HBFieldOllamaStatus:  ollamaStatus,
		pkgtypes.HBFieldUptime:        int64(time.Since(m.startedAt).Seconds()),
	})
	pipe.PExpire(ctx, key, ttl)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("write heartbeat %s: %w", key, err)
	}

	// Mirror the heartbeat outcome into Prometheus gauges so dashboards can
	// display fleet-wide Ollama health and per-worker heartbeat freshness
	// without scraping the Redis HSET keys directly.
	if m.metrics != nil {
		if ollamaStatus == OllamaStatusReady {
			m.metrics.OllamaUp.Set(1)
		} else {
			m.metrics.OllamaUp.Set(0)
		}
		m.metrics.HeartbeatLastEmit.SetToCurrentTime()
	}
	return nil
}

// checkOllama performs a GET /api/tags to the Ollama endpoint with a 2s timeout.
// Returns "ready" on HTTP 200, "unreachable" on any error or non-200 response.
func (m *Monitor) checkOllama(ctx context.Context) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.OllamaURL+"/api/tags", nil)
	if err != nil {
		return OllamaStatusUnreachable
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return OllamaStatusUnreachable
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return OllamaStatusReady
	}
	return OllamaStatusUnreachable
}

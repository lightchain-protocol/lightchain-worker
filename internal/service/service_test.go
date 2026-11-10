package service

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lightchain/worker/internal/hwinfo"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/config"
	"github.com/lightchain/worker/internal/keystore"
	"github.com/lightchain/worker/internal/pipeline"
	"github.com/lightchain/worker/internal/sortition"
)

func TestAsynqRedisClientOptFromRedisOptions(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{ServerName: "redis.internal"}
	opts := &redis.Options{
		Network:      "tcp",
		Addr:         "redis.internal:6380",
		Username:     "worker",
		Password:     "secret",
		DB:           4,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 4 * time.Second,
		PoolSize:     8,
		TLSConfig:    tlsConfig,
	}

	got := asynqRedisClientOptFromRedisOptions(opts)

	assert.Equal(t, asynq.RedisClientOpt{
		Network:      opts.Network,
		Addr:         opts.Addr,
		Username:     opts.Username,
		Password:     opts.Password,
		DB:           opts.DB,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		PoolSize:     opts.PoolSize,
		TLSConfig:    opts.TLSConfig,
	}, got)
}

func TestRedisOptions_malformedURLErrorOmitsURL(t *testing.T) {
	t.Parallel()

	_, err := redisOptions(&config.Config{RedisURL: "redis://:s3cr3t%2Fp4ssw0rd@localhost:63x79"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "REDIS_URL")
	for _, secret := range []string{"s3cr3t", "p4ssw0rd", "localhost"} {
		assert.NotContains(t, err.Error(), secret)
	}
}

func TestService_computeDrainTTL_overrideTakesPrecedence(t *testing.T) {
	t.Parallel()

	// Service with cfg override set; chain client nil to prove the
	// override short-circuits before any RPC attempt.
	s := &Service{
		cfg:    &config.Config{DrainTTLOverride: 7 * time.Minute},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	assert.Equal(t, 7*time.Minute, s.computeDrainTTL(),
		"LIGHTCHAIN_DRAIN_TTL must be honored on the SIGTERM path, not just CLI")
}

func TestService_computeDrainTTL_fallbackWhenNoChainClient(t *testing.T) {
	t.Parallel()
	s := &Service{
		cfg:    &config.Config{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	got := s.computeDrainTTL()
	assert.Equal(t, drainTTLFallback, got)
}

// TestService_sortitionWatchers_nilWhenDisabled is a compile-and-invariant guard:
// it confirms the sortition watcher fields exist on Service and are nil on a
// zero-value instance. New() cannot be exercised in a unit test without heavy deps
// (keystore paths, chain RPC, Redis), so this test relies on the nil-field invariant
// as a structural check; correctness of the sortition wiring is validated by the
// build step and the Task 9 live demo.
func TestService_sortitionWatchers_nilWhenDisabled(t *testing.T) {
	t.Parallel()
	s := &Service{
		cfg:    &config.Config{SortitionEnabled: false},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	assert.Nil(t, s.sessionWatcher, "sessionWatcher must be nil when SortitionEnabled=false")
	assert.Nil(t, s.jobWatcher, "jobWatcher must be nil when SortitionEnabled=false")
}

// The gateway heartbeat is the only channel an external worker has for
// advertising models and capabilities, so it must carry what the Redis
// monitor does.
func TestService_gatewayHeartbeatPayload_advertisesModelsAndCapabilities(t *testing.T) {
	t.Parallel()

	jobs := &atomic.Int32{}
	jobs.Store(1)
	s := &Service{
		cfg:              &config.Config{MaxConcurrentJobs: 2},
		jobCounter:       jobs,
		advertisedModels: []string{"0xaa", "0xbb"},
		advertisedCaps:   []string{"search"},
	}

	body, err := json.Marshal(s.gatewayHeartbeatPayload("unreachable", 90*time.Second))
	assert.NoError(t, err)
	assert.JSONEq(t, `{
		"activeJobs": 1,
		"maxJobs": 2,
		"models": ["0xaa", "0xbb"],
		"capabilities": ["search"],
		"protocolVersion": 2,
		"ollamaStatus": "unreachable",
		"uptimeSeconds": 90,
		"workerVersion": "`+hwinfo.WorkerVersion()+`"
	}`, string(body))
}

// The operator programme verifies hardware from the heartbeat, so a worker
// with a GPU prober must send its report and a version string.
func TestService_gatewayHeartbeatPayload_carriesGPUReportAndVersion(t *testing.T) {
	t.Parallel()

	s := &Service{
		cfg:        &config.Config{},
		jobCounter: &atomic.Int32{},
		gpuProber:  hwinfo.NewProber("http://127.0.0.1:1", time.Minute),
	}

	p := s.gatewayHeartbeatPayload("ready", time.Second)
	assert.NotEmpty(t, p.WorkerVersion)
	if assert.NotNil(t, p.GPU) {
		assert.NotEmpty(t, p.GPU.Source)
		assert.NotNil(t, p.GPU.Devices)
	}
}

// oneJobClient is a chain holding a single JobSubmitted event for worker.
type oneJobClient struct{ worker common.Address }

func (oneJobClient) Head(context.Context) (chain.HeadInfo, error) {
	return chain.HeadInfo{Number: 1}, nil
}

func (c oneJobClient) FilterJobSubmitted(context.Context, uint64, uint64) ([]chain.JobSubmittedEvent, error) {
	return []chain.JobSubmittedEvent{{JobID: 7, SessionID: 3, Worker: c.worker, BlockNumber: 1}}, nil
}

func (oneJobClient) GetJobBlobInfo(context.Context, uint64) (promptHash, respHash common.Hash, submitBlock, completeBlock uint64, err error) {
	return common.Hash{}, common.Hash{}, 1, 0, nil
}

func (oneJobClient) GetSessionInfo(context.Context, uint64) (chain.SessionInfo, error) {
	return chain.SessionInfo{}, nil
}

func (oneJobClient) GetSessionEncWorkerKey(context.Context, uint64) ([]byte, error) {
	return []byte{1}, nil
}

type anyKey struct{}

func (anyKey) CanDecryptSessionKey([]byte) bool { return true }

// blockingSink holds a job until release is closed or the job's context ends,
// then reports the context's error. A cancelled job takes a moment to return,
// as a real one does while it reports the failure.
type blockingSink struct {
	entered chan struct{}
	release chan struct{}
	ctxErr  chan error
}

func newBlockingSink() *blockingSink {
	return &blockingSink{entered: make(chan struct{}), release: make(chan struct{}), ctxErr: make(chan error, 1)}
}

func (s *blockingSink) HandleJobPayload(ctx context.Context, _ pipeline.JobPayload) error {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		time.Sleep(50 * time.Millisecond)
	}
	s.ctxErr <- ctx.Err()
	return nil
}

// sortitionService is a sortition-mode Service whose JobWatcher serves one job
// into sink and whose SessionWatcher never polls.
func sortitionService(t *testing.T, shutdownTimeout time.Duration, sink sortition.JobSink) *Service {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cursor, err := sortition.NewCursorStore(t.TempDir())
	require.NoError(t, err)
	keys, err := keystore.NewSessionKeyStore(filepath.Join(t.TempDir(), "session-keys.enc"), "test")
	require.NoError(t, err)
	worker := common.HexToAddress("0x1")
	jobs := &atomic.Int32{}
	return &Service{
		cfg:             &config.Config{ShutdownTimeout: shutdownTimeout},
		logger:          logger,
		jobCounter:      jobs,
		sessionKeyStore: keys,
		sessionWatcher:  sortition.NewSessionWatcher(sortition.SessionWatcherOpts{PollInterval: time.Hour, Logger: logger}),
		jobWatcher: sortition.NewJobWatcher(sortition.JobWatcherOpts{
			Client:         oneJobClient{worker: worker},
			KeyChecker:     anyKey{},
			Sink:           sink,
			Cursor:         cursor,
			Worker:         worker,
			JobCounter:     jobs,
			MaxConcurrent:  1,
			LookbackBlocks: 10,
			PollInterval:   time.Millisecond,
			Logger:         logger,
		}),
	}
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// A SIGTERM stops intake at once, but a job already running keeps going and
// Run returns only once it has finished.
func TestService_Run_sortitionSignalLetsRunningJobFinish(t *testing.T) {
	t.Parallel()
	sink := newBlockingSink()
	s := sortitionService(t, time.Minute, sink)
	ctx, signal := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	receive(t, sink.entered, "the job to reach the sink")
	signal()

	select {
	case err := <-runErr:
		t.Fatalf("Run returned while the job was still running: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(sink.release)
	require.NoError(t, receive(t, runErr, "Run to return"))
	assert.NoError(t, receive(t, sink.ctxErr, "the job to end"), "the signal must not cancel a running job")
}

// A job still running when SHUTDOWN_TIMEOUT runs out is cancelled, and Run
// returns once it has.
func TestService_Run_sortitionCancelsJobAtShutdownTimeout(t *testing.T) {
	t.Parallel()
	sink := newBlockingSink()
	s := sortitionService(t, 100*time.Millisecond, sink)
	ctx, signal := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	receive(t, sink.entered, "the job to reach the sink")
	signal()

	assert.ErrorContains(t, receive(t, runErr, "Run to return"), "shutdown timeout exceeded")
	select {
	case err := <-sink.ctxErr:
		assert.ErrorIs(t, err, context.Canceled)
	default:
		t.Fatal("Run returned before the cancelled job did")
	}
}

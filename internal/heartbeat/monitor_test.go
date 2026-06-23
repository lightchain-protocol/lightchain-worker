package heartbeat

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgtypes "github.com/lightchain/pkg/types"
)

const (
	testWorkerAddr = "AbCdEf1234567890AbCdEf1234567890AbCdEf12" // no 0x prefix
	testInterval   = 50 * time.Millisecond
)

func newTestMonitor(t *testing.T, mr *miniredis.Miniredis, ollamaURL string) (*Monitor, *redis.Client) {
	t.Helper()
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	cfg := MonitorConfig{
		Interval:  testInterval,
		OllamaURL: ollamaURL,
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	counter := &atomic.Int32{}
	m := NewMonitor(redisClient, cfg, testWorkerAddr, []string{"0xmodel1"}, nil, counter, 0, logger, nil)
	return m, redisClient
}

func TestMonitor_emit_KeyExistsWithCorrectPayload(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	// Ollama returns 200
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, redisClient := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	err := m.emit(ctx)
	require.NoError(t, err)

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	vals, err := redisClient.HGetAll(ctx, key).Result()
	require.NoError(t, err, "key must exist after emit")
	require.NotEmpty(t, vals)

	assert.Equal(t, OllamaStatusReady, vals[pkgtypes.HBFieldOllamaStatus])
	assert.Equal(t, pkgtypes.HeartbeatStatusActive, vals[pkgtypes.HBFieldStatus])
	assert.JSONEq(t, `["0xmodel1"]`, vals[pkgtypes.HBFieldModels])
	assert.NotEmpty(t, vals[pkgtypes.HBFieldLastHeartbeat])
}

func TestMonitor_emit_OllamaUnreachable(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	// Closed server → connection refused
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ollamaSrv.Close() // immediately close so requests fail

	m, redisClient := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	err := m.emit(ctx)
	require.NoError(t, err)

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	vals, err := redisClient.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	assert.Equal(t, OllamaStatusUnreachable, vals[pkgtypes.HBFieldOllamaStatus])
}

func TestMonitor_emit_TTL(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, _ := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	require.NoError(t, m.emit(ctx))

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	ttl := mr.TTL(key)

	expectedTTL := 3 * testInterval
	// TTL should be between 2x and 3x interval (allowing for slight timing imprecision)
	assert.GreaterOrEqual(t, ttl, 2*testInterval, "TTL must be at least 2× interval")
	assert.LessOrEqual(t, ttl, expectedTTL, "TTL must be at most 3× interval")
}

func TestMonitor_Start_EmitsWithinInterval(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, redisClient := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	m.Start(ctx)
	defer m.Stop()

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	require.Eventually(t, func() bool {
		exists, _ := redisClient.Exists(ctx, key).Result()
		return exists > 0
	}, 2*time.Second, 10*time.Millisecond, "heartbeat key must exist after Start")
}

func TestMonitor_Stop_DoesNotHang(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, _ := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	m.Start(ctx)

	done := make(chan struct{})
	go func() {
		m.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() hung — goroutine did not exit")
	}
}

func TestMonitor_Stop_Idempotent(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, _ := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	m.Start(ctx)

	// Calling Stop twice must not panic
	m.Stop()
	m.Stop()
}

func TestMonitor_emit_RedisFailure_ReturnsError(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, _ := newTestMonitor(t, mr, ollamaSrv.URL)

	// Close miniredis to force a Redis connection failure
	mr.Close()

	ctx := t.Context()
	err := m.emit(ctx)
	require.Error(t, err, "emit must return error when Redis is unavailable")
}

func TestEmitOnce_Success(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, redisClient := newTestMonitor(t, mr, ollamaSrv.URL)

	ctx := t.Context()
	err := m.EmitOnce(ctx)
	require.NoError(t, err, "EmitOnce must succeed when Redis is reachable")

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	exists, err := redisClient.Exists(ctx, key).Result()
	require.NoError(t, err, "heartbeat key must exist after EmitOnce")
	assert.Equal(t, int64(1), exists)

	// TTL must be set (key should not be persistent)
	ttl := mr.TTL(key)
	assert.Greater(t, ttl, time.Duration(0), "key must have a TTL set")
}

func TestEmitOnce_RedisUnreachable(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	m, _ := newTestMonitor(t, mr, ollamaSrv.URL)

	// Stop Redis before the call to simulate an unreachable server
	mr.Close()

	ctx := t.Context()
	err := m.EmitOnce(ctx)
	require.Error(t, err, "EmitOnce must return error when Redis is stopped")
}

func TestEmit_WritesCapabilities(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	counter := &atomic.Int32{}
	m := NewMonitor(rdb, MonitorConfig{Interval: time.Second}, testWorkerAddr,
		[]string{"0xmodel"}, []string{"search"}, counter, 4, logger, nil)
	require.NoError(t, m.EmitOnce(t.Context()))

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	got, err := rdb.HGet(t.Context(), key, pkgtypes.HBFieldCapabilities).Result()
	require.NoError(t, err)
	assert.JSONEq(t, `["search"]`, got)
}

func TestMonitor_emit_DynamicJobCounts(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ollamaSrv.Close()

	counter := &atomic.Int32{}
	counter.Store(3) // Simulate 3 active jobs

	cfg := MonitorConfig{
		Interval:  testInterval,
		OllamaURL: ollamaSrv.URL,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	m := NewMonitor(redisClient, cfg, testWorkerAddr, []string{"0xmodel1"}, nil, counter, 5, logger, nil)

	ctx := t.Context()
	require.NoError(t, m.emit(ctx))

	key := pkgtypes.HeartbeatRedisKey(testWorkerAddr)
	vals, err := redisClient.HGetAll(ctx, key).Result()
	require.NoError(t, err)

	assert.Equal(t, "3", vals[pkgtypes.HBFieldActiveJobs])
	assert.Equal(t, "5", vals[pkgtypes.HBFieldMaxJobs])
}

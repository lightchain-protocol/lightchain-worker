package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgtypes "github.com/lightchain/pkg/types"
)

var testDrainWorker = common.HexToAddress("0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B")

func newDrainTestRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, mr
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type stubDisputeWindow struct {
	value time.Duration
	err   error
}

func (s stubDisputeWindow) GetDisputeWindow(_ context.Context) (time.Duration, error) {
	return s.value, s.err
}

// blockingDisputeWindow simulates a hung chain RPC: it blocks until the
// caller's context is canceled, then returns ctx.Err. Used to assert the
// drain TTL lookup is bounded by its own sub-context, not the caller's.
type blockingDisputeWindow struct{}

func (blockingDisputeWindow) GetDisputeWindow(ctx context.Context) (time.Duration, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

type stubGateway struct {
	drainCalls   int
	undrainCalls int
	drainErr     error
	undrainErr   error
}

func (g *stubGateway) SendDrain(_ context.Context) error {
	g.drainCalls++
	return g.drainErr
}

func (g *stubGateway) SendUndrain(_ context.Context) error {
	g.undrainCalls++
	return g.undrainErr
}

func TestDrain_directMode_writesMarker(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)
	out := &bytes.Buffer{}

	h := &DrainHandler{
		RedisClient: rdb,
		ChainClient: stubDisputeWindow{value: 24 * time.Hour},
		WorkerAddr:  testDrainWorker,
		Out:         out,
		Logger:      discardLogger(),
	}

	require.NoError(t, h.Drain(context.Background()))

	assert.True(t, mr.Exists(pkgtypes.DrainRedisKey(testDrainWorker.Hex())))
	ttl := mr.TTL(pkgtypes.DrainRedisKey(testDrainWorker.Hex()))
	assert.Greater(t, ttl, 24*time.Hour, "TTL should be disputeWindow + slack")
	assert.Contains(t, out.String(), "drained")
}

func TestDrain_directMode_usesFallbackWhenChainFails(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)

	h := &DrainHandler{
		RedisClient: rdb,
		ChainClient: stubDisputeWindow{err: errors.New("rpc down")},
		WorkerAddr:  testDrainWorker,
		Logger:      discardLogger(),
	}

	require.NoError(t, h.Drain(context.Background()))

	ttl := mr.TTL(pkgtypes.DrainRedisKey(testDrainWorker.Hex()))
	assert.InDelta(t, DrainTTLFallback.Seconds(), ttl.Seconds(), 5,
		"falls back to DrainTTLFallback when chain read fails")
}

func TestDrain_directMode_writeStillSucceedsWhenChainHangs(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)

	h := &DrainHandler{
		RedisClient: rdb,
		ChainClient: blockingDisputeWindow{},
		WorkerAddr:  testDrainWorker,
		Logger:      discardLogger(),
	}

	// Caller's deadline is well beyond the internal lookup timeout.
	// computeDrainTTL must bound its own RPC so SetDraining still has
	// time to run with the fallback TTL.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	require.NoError(t, h.Drain(ctx))
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 15*time.Second,
		"drain must not block on the chain RPC for the full caller deadline")
	ttl := mr.TTL(pkgtypes.DrainRedisKey(testDrainWorker.Hex()))
	assert.InDelta(t, DrainTTLFallback.Seconds(), ttl.Seconds(), 5,
		"falls back to DrainTTLFallback when the chain RPC times out")
}

func TestDrain_directMode_overrideTakesPrecedence(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)

	h := &DrainHandler{
		RedisClient:      rdb,
		ChainClient:      stubDisputeWindow{value: 100 * time.Hour},
		WorkerAddr:       testDrainWorker,
		DrainTTLOverride: 30 * time.Minute,
		Logger:           discardLogger(),
	}
	require.NoError(t, h.Drain(context.Background()))

	ttl := mr.TTL(pkgtypes.DrainRedisKey(testDrainWorker.Hex()))
	assert.LessOrEqual(t, ttl, 30*time.Minute)
	assert.Greater(t, ttl, 25*time.Minute)
}

func TestDrain_gatewayMode_callsSendDrain(t *testing.T) {
	t.Parallel()
	gw := &stubGateway{}

	h := &DrainHandler{
		Gateway:    gw,
		WorkerAddr: testDrainWorker,
		Logger:     discardLogger(),
	}
	require.NoError(t, h.Drain(context.Background()))

	assert.Equal(t, 1, gw.drainCalls)
}

func TestDrain_gatewayMode_warnsWhenDrainTTLOverrideSet(t *testing.T) {
	t.Parallel()
	gw := &stubGateway{}
	out := &bytes.Buffer{}

	h := &DrainHandler{
		Gateway:          gw,
		WorkerAddr:       testDrainWorker,
		DrainTTLOverride: 10 * time.Minute,
		Out:              out,
		Logger:           discardLogger(),
	}
	require.NoError(t, h.Drain(context.Background()))

	assert.Equal(t, 1, gw.drainCalls,
		"warning must not block the drain itself")
	assert.Contains(t, out.String(), "LIGHTCHAIN_DRAIN_TTL has no effect in gateway mode")
}

func TestDrain_gatewayMode_silentWhenNoOverride(t *testing.T) {
	t.Parallel()
	gw := &stubGateway{}
	out := &bytes.Buffer{}

	h := &DrainHandler{
		Gateway:    gw,
		WorkerAddr: testDrainWorker,
		Out:        out,
		Logger:     discardLogger(),
	}
	require.NoError(t, h.Drain(context.Background()))

	assert.NotContains(t, out.String(), "no effect in gateway mode",
		"warning must only fire when an override is actually set")
}

func TestDrain_failsWhenNoBackendConfigured(t *testing.T) {
	t.Parallel()
	h := &DrainHandler{WorkerAddr: testDrainWorker, Logger: discardLogger()}
	err := h.Drain(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Redis client")
}

func TestUndrain_directMode_skipConfirmDeletesMarker(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)
	require.NoError(t,
		pkgtypes.SetDraining(context.Background(), rdb, testDrainWorker.Hex(), time.Hour))

	h := &DrainHandler{
		RedisClient: rdb,
		WorkerAddr:  testDrainWorker,
		SkipConfirm: true,
		Logger:      discardLogger(),
	}
	require.NoError(t, h.Undrain(context.Background()))
	assert.False(t, mr.Exists(pkgtypes.DrainRedisKey(testDrainWorker.Hex())))
}

func TestUndrain_promptsAndAbortsOnNo(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)
	require.NoError(t,
		pkgtypes.SetDraining(context.Background(), rdb, testDrainWorker.Hex(), time.Hour))

	out := &bytes.Buffer{}
	h := &DrainHandler{
		RedisClient: rdb,
		ChainClient: stubDisputeWindow{value: 24 * time.Hour},
		WorkerAddr:  testDrainWorker,
		Stdin:       strings.NewReader("n\n"),
		Out:         out,
		Logger:      discardLogger(),
	}

	err := h.Undrain(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")

	// Marker must still be present — undrain was rejected.
	assert.True(t, mr.Exists(pkgtypes.DrainRedisKey(testDrainWorker.Hex())))
	assert.Contains(t, out.String(), "dispute window")
}

func TestUndrain_promptAcceptsYes(t *testing.T) {
	t.Parallel()
	rdb, mr := newDrainTestRedis(t)
	require.NoError(t,
		pkgtypes.SetDraining(context.Background(), rdb, testDrainWorker.Hex(), time.Hour))

	h := &DrainHandler{
		RedisClient: rdb,
		WorkerAddr:  testDrainWorker,
		Stdin:       strings.NewReader("y\n"),
		Out:         &bytes.Buffer{},
		Logger:      discardLogger(),
	}

	require.NoError(t, h.Undrain(context.Background()))
	assert.False(t, mr.Exists(pkgtypes.DrainRedisKey(testDrainWorker.Hex())))
}

func TestUndrain_failsWhenStdinNilAndNotSkipConfirm(t *testing.T) {
	t.Parallel()
	rdb, _ := newDrainTestRedis(t)

	h := &DrainHandler{
		RedisClient: rdb,
		WorkerAddr:  testDrainWorker,
		Out:         &bytes.Buffer{},
		Logger:      discardLogger(),
	}

	err := h.Undrain(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no stdin")
}

func TestUndrain_idempotentOnAbsentMarker(t *testing.T) {
	t.Parallel()
	rdb, _ := newDrainTestRedis(t)

	h := &DrainHandler{
		RedisClient: rdb,
		WorkerAddr:  testDrainWorker,
		SkipConfirm: true,
		Logger:      discardLogger(),
	}

	require.NoError(t, h.Undrain(context.Background()))
}

func TestUndrain_gatewayMode_callsSendUndrain(t *testing.T) {
	t.Parallel()
	gw := &stubGateway{}

	h := &DrainHandler{
		Gateway:     gw,
		WorkerAddr:  testDrainWorker,
		SkipConfirm: true,
		Logger:      discardLogger(),
	}
	require.NoError(t, h.Undrain(context.Background()))
	assert.Equal(t, 1, gw.undrainCalls)
}

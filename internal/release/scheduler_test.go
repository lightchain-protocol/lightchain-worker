package release

import (
	"context"
	"errors"
	"math/big"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

// schedStub implements schedulerChain with hand-controlled returns.
type schedStub struct {
	mu               sync.Mutex
	headFn           func() (chain.HeadInfo, error)
	disputeWindowFn  func() (time.Duration, error)
	getJobStateFn    func(jobID uint64) (chain.JobStateInfo, error)
	releaseJobFn     func(jobID uint64) error
	releaseJobsFn    func(ids []uint64) error
	releaseJobsCalls atomic.Int32
	releaseJobCalls  atomic.Int32
}

func (s *schedStub) Head(_ context.Context) (chain.HeadInfo, error) { return s.headFn() }
func (s *schedStub) GetDisputeWindow(_ context.Context) (time.Duration, error) {
	if s.disputeWindowFn == nil {
		return 24 * time.Hour, nil
	}
	return s.disputeWindowFn()
}
func (s *schedStub) GetJobState(_ context.Context, jobID uint64) (chain.JobStateInfo, error) {
	return s.getJobStateFn(jobID)
}
func (s *schedStub) ReleaseJob(_ context.Context, jobID uint64) error {
	s.releaseJobCalls.Add(1)
	if s.releaseJobFn == nil {
		return nil
	}
	return s.releaseJobFn(jobID)
}
func (s *schedStub) ReleaseJobs(_ context.Context, ids []uint64) error {
	s.releaseJobsCalls.Add(1)
	if s.releaseJobsFn == nil {
		return nil
	}
	return s.releaseJobsFn(ids)
}

func newSchedStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStore(filepath.Join(dir, "rs.json"), defaultIdentityForTest(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func defaultSchedConfig() Config {
	c := DefaultConfig()
	// Tighten for tests so backoff arithmetic is easy to inspect.
	c.Interval = 8 * time.Hour
	c.ProbeInterval = 1 * time.Second
	c.BatchThreshold = 2
	c.MaxBatchSize = 50
	c.TxTimeout = 5 * time.Second
	c.BackoffBase = 15 * time.Minute
	c.BackoffMax = 24 * time.Hour
	c.PausedCycleBackoff = time.Hour
	c.StaleDisputeWarnAfter = 7 * 24 * time.Hour
	c.DisputeWindowOverride = 24 * time.Hour
	c.DisputeWindowCacheTTL = 0
	return c
}

func mkInfo(state chain.JobState, worker common.Address, completedAt int64, fee int64) chain.JobStateInfo {
	return chain.JobStateInfo{
		State:       state,
		Worker:      worker,
		CompletedAt: completedAt,
		EscrowedFee: big.NewInt(fee),
	}
}

func TestComputeBackoff(t *testing.T) {
	t.Parallel()
	base := 15 * time.Minute
	max := 24 * time.Hour

	assert.Equal(t, base, computeBackoff(base, max, 1))
	assert.Equal(t, 2*base, computeBackoff(base, max, 2))
	assert.Equal(t, 4*base, computeBackoff(base, max, 3))
	assert.Equal(t, max, computeBackoff(base, max, 100), "saturates at max")
	assert.Equal(t, base, computeBackoff(base, max, 0), "zero attempt clamps to 1")
}

func TestIsPauseError(t *testing.T) {
	t.Parallel()
	assert.False(t, isPauseError(nil))
	assert.False(t, isPauseError(errors.New("nonce too low")))
	assert.True(t, isPauseError(errors.New("execution reverted: Pausable: paused")))
	assert.True(t, isPauseError(errors.New("EnforcedPause()")))
	assert.True(t, isPauseError(errors.New("ENFORCEDPAUSE custom error")), "case-insensitive")
}

func TestCountEligible_AppliesBackoffAndWindow(t *testing.T) {
	t.Parallel()
	chainNow := int64(100000)
	window := time.Hour // 3600s
	pending := []PendingJob{
		{JobID: 1, CompletedAt: chainNow - 7200},                              // window elapsed → eligible
		{JobID: 2, CompletedAt: chainNow - 1000},                              // window not elapsed → not eligible
		{JobID: 3, CompletedAt: chainNow - 7200, BackoffUntil: chainNow + 10}, // backed off → not eligible
		{JobID: 4, CompletedAt: chainNow - 7200, BackoffUntil: chainNow - 10}, // backoff expired → eligible
	}
	assert.Equal(t, 2, countEligible(pending, window, chainNow))
}

// TestScheduler_BatchSuccessPath exercises the happy path: time trigger
// fires, batch release succeeds, all eligible jobs are removed.
func TestScheduler_BatchSuccessPath(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	ctx := context.Background()

	chainNow := int64(1_000_000)
	// Two jobs, both Completed and past window.
	require.NoError(t, store.AddEligible(ctx, 10, chainNow-3*86400))
	require.NoError(t, store.AddEligible(ctx, 11, chainNow-3*86400))

	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			return mkInfo(chain.JobStateCompleted, worker, chainNow-2*86400, 100), nil
		},
	}

	cfg := defaultSchedConfig()
	s := NewScheduler(store, stub, worker, cfg, nil)
	require.NoError(t, s.RunOnce(ctx))

	assert.Equal(t, int32(1), stub.releaseJobsCalls.Load(), "one batch tx")
	assert.Equal(t, int32(0), stub.releaseJobCalls.Load(), "no per-job fallback")

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	assert.Empty(t, pending, "all released jobs removed from pending")

	ts, err := store.GetLastReleaseTs(ctx)
	require.NoError(t, err)
	assert.Equal(t, chainNow, ts)
}

// TestScheduler_PartitionEachState covers all 9 rows of the Revised Rules
// table using a single cycle with one job per state.
func TestScheduler_PartitionEachState(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	other := common.HexToAddress("0xaabbccddee00112233445566778899aabbccddee")
	ctx := context.Background()

	chainNow := int64(1_000_000)
	pastWindow := chainNow - 2*86400 // 2 days ago — past 24h dispute window
	withinWindow := chainNow - 60    // 60s ago — within window

	jobs := []struct {
		id          uint64
		completedAt int64
		state       chain.JobStateInfo
	}{
		// Completed past window, our worker → release.
		{1, pastWindow, mkInfo(chain.JobStateCompleted, worker, pastWindow, 100)},
		// Completed but window not elapsed → keep in pending, no release.
		{2, withinWindow, mkInfo(chain.JobStateCompleted, worker, withinWindow, 100)},
		// Completed but foreign worker → drop (corrupt local state).
		{3, pastWindow, mkInfo(chain.JobStateCompleted, other, pastWindow, 100)},
		// Resolved with positive escrow → release.
		{4, pastWindow, mkInfo(chain.JobStateResolved, worker, pastWindow, 50)},
		// Resolved with zero escrow (guilty path) → drop.
		{5, pastWindow, mkInfo(chain.JobStateResolved, worker, pastWindow, 0)},
		// Released → drop (already settled).
		{6, pastWindow, mkInfo(chain.JobStateReleased, worker, pastWindow, 0)},
		// TimedOut → drop (slashed).
		{7, pastWindow, mkInfo(chain.JobStateTimedOut, worker, pastWindow, 0)},
		// Disputed → keep in pending (will resolve eventually).
		{8, pastWindow, mkInfo(chain.JobStateDisputed, worker, pastWindow, 100)},
		// Acknowledged (state-machine impossibility) → keep + warn.
		{9, pastWindow, mkInfo(chain.JobStateAcknowledged, worker, pastWindow, 0)},
	}

	stateMap := make(map[uint64]chain.JobStateInfo, len(jobs))
	for _, j := range jobs {
		require.NoError(t, store.AddEligible(ctx, j.id, j.completedAt))
		stateMap[j.id] = j.state
	}

	var releasedBatch []uint64
	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(jobID uint64) (chain.JobStateInfo, error) { return stateMap[jobID], nil },
		releaseJobsFn: func(ids []uint64) error {
			releasedBatch = append([]uint64(nil), ids...)
			return nil
		},
	}

	cfg := defaultSchedConfig()
	s := NewScheduler(store, stub, worker, cfg, nil)
	require.NoError(t, s.RunOnce(ctx))

	// Only Completed-past-window (1) and Resolved-with-escrow (4) get released.
	assert.ElementsMatch(t, []uint64{1, 4}, releasedBatch)

	// After: 1 and 4 removed (released); 3, 5, 6, 7 dropped (terminal/foreign);
	//        2, 8, 9 kept (within-window / Disputed / impossible state).
	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	remainingIDs := make([]uint64, len(pending))
	for i, p := range pending {
		remainingIDs[i] = p.JobID
	}
	assert.ElementsMatch(t, []uint64{2, 8, 9}, remainingIDs)
}

// TestScheduler_BatchFallbackPerJob: batch reverts (non-pause); the
// per-job loop succeeds for some, fails for others.
func TestScheduler_BatchFallbackPerJob(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	ctx := context.Background()

	chainNow := int64(1_000_000)
	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, store.AddEligible(ctx, id, chainNow-2*86400))
	}

	// Per-job: id=1 succeeds; id=2 fails (will be backed off); id=3 succeeds.
	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			return mkInfo(chain.JobStateCompleted, worker, chainNow-2*86400, 100), nil
		},
		releaseJobsFn: func([]uint64) error { return errors.New("execution reverted: stale") },
		releaseJobFn: func(jobID uint64) error {
			if jobID == 2 {
				return errors.New("execution reverted: misc")
			}
			return nil
		},
	}

	cfg := defaultSchedConfig()
	s := NewScheduler(store, stub, worker, cfg, nil)
	require.NoError(t, s.RunOnce(ctx))

	assert.Equal(t, int32(1), stub.releaseJobsCalls.Load())
	assert.Equal(t, int32(3), stub.releaseJobCalls.Load(), "per-job retried for all 3")

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "id=2 remains; id=1 and 3 released")
	assert.Equal(t, uint64(2), pending[0].JobID)
	assert.Equal(t, 1, pending[0].FailCount, "fail_count incremented")
	assert.Greater(t, pending[0].BackoffUntil, chainNow, "backoff into future")
	expectedBackoff := chainNow + int64(cfg.BackoffBase/time.Second)
	assert.Equal(t, expectedBackoff, pending[0].BackoffUntil)
}

// TestScheduler_PauseClassificationSkipsPerJobBlame: contract is paused.
// Cycle should back off without per-job RecordFailure calls.
func TestScheduler_PauseClassificationSkipsPerJobBlame(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	ctx := context.Background()

	chainNow := int64(1_000_000)
	for _, id := range []uint64{1, 2} {
		require.NoError(t, store.AddEligible(ctx, id, chainNow-2*86400))
	}

	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			return mkInfo(chain.JobStateCompleted, worker, chainNow-2*86400, 100), nil
		},
		releaseJobsFn: func([]uint64) error {
			return errors.New("execution reverted: Pausable: paused")
		},
	}

	cfg := defaultSchedConfig()
	s := NewScheduler(store, stub, worker, cfg, nil)
	require.NoError(t, s.RunOnce(ctx))

	assert.Equal(t, int32(0), stub.releaseJobCalls.Load(), "per-job loop must NOT run on pause")

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	for _, p := range pending {
		assert.Equal(t, 0, p.FailCount, "per-job fail_count must NOT be incremented on pause")
		assert.Equal(t, int64(0), p.BackoffUntil, "per-job backoff must NOT be set on pause")
	}

	ts, err := store.GetLastReleaseTs(ctx)
	require.NoError(t, err)
	expected := chainNow + int64(cfg.PausedCycleBackoff/time.Second)
	assert.Equal(t, expected, ts, "cycle-level backoff applied to LastReleaseTs")
}

// TestScheduler_NoFireWhenBelowThresholdAndTime: neither trigger
// satisfied → no chain calls.
func TestScheduler_NoFireWhenBelowThresholdAndTime(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	ctx := context.Background()

	chainNow := int64(1_000_000)

	// One pending eligible job (below threshold of 2).
	require.NoError(t, store.AddEligible(ctx, 1, chainNow-2*86400))

	// LastReleaseTs is recent (interval not elapsed).
	require.NoError(t, store.SetLastReleaseTs(ctx, chainNow-60))

	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(uint64) (chain.JobStateInfo, error) {
			t.Fatal("GetJobState should not be called when neither trigger fires")
			return chain.JobStateInfo{}, nil
		},
	}

	cfg := defaultSchedConfig() // BatchThreshold=2, Interval=8h
	s := NewScheduler(store, stub, worker, cfg, nil)

	// Use the internal tick path (which is what the periodic loop uses).
	s.tick(ctx)

	assert.Equal(t, int32(0), stub.releaseJobsCalls.Load())
	assert.Equal(t, int32(0), stub.releaseJobCalls.Load())
}

// TestScheduler_TimeTriggerFiresWithSubThresholdCount: only one eligible
// (below threshold) but interval elapsed → should fire.
func TestScheduler_TimeTriggerFiresWithSubThresholdCount(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	ctx := context.Background()

	chainNow := int64(1_000_000)
	require.NoError(t, store.AddEligible(ctx, 7, chainNow-2*86400))
	// LastReleaseTs is far in the past so time trigger fires.
	require.NoError(t, store.SetLastReleaseTs(ctx, chainNow-9*3600))

	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: chainNow}, nil
		},
		getJobStateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			return mkInfo(chain.JobStateCompleted, worker, chainNow-2*86400, 100), nil
		},
	}

	cfg := defaultSchedConfig()
	s := NewScheduler(store, stub, worker, cfg, nil)
	s.tick(ctx)

	assert.Equal(t, int32(1), stub.releaseJobsCalls.Load(), "time trigger fired despite sub-threshold count")
}

// TestScheduler_StartStopLifecycle: clean shutdown of background loop.
func TestScheduler_StartStopLifecycle(t *testing.T) {
	t.Parallel()
	store := newSchedStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	var ticks atomic.Int32
	stub := &schedStub{
		headFn: func() (chain.HeadInfo, error) {
			ticks.Add(1)
			return chain.HeadInfo{Number: 100, Timestamp: 1}, nil
		},
		getJobStateFn: func(uint64) (chain.JobStateInfo, error) {
			return chain.JobStateInfo{}, nil
		},
	}

	cfg := defaultSchedConfig()
	cfg.ProbeInterval = 10 * time.Millisecond

	s := NewScheduler(store, stub, worker, cfg, nil)
	s.Start(context.Background())
	time.Sleep(45 * time.Millisecond) // ~4 probes
	s.Stop()
	s.Stop() // idempotent

	assert.GreaterOrEqual(t, ticks.Load(), int32(1))
}

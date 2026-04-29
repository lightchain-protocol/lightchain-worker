package release

import (
	"context"
	"errors"
	"math/big"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

// stubChain is a tiny in-process implementation of reconcilerChain. The
// chain package's MockSettlementClient also implements the full
// SettlementClient interface, but for reconciler tests we want a simpler
// surface that lets us tune per-call behavior with map lookups.
type stubChain struct {
	headFn   func() (chain.HeadInfo, error)
	filterFn func(worker common.Address, fromBlock, toBlock uint64) ([]chain.JobCompletedEvent, error)
	stateFn  func(jobID uint64) (chain.JobStateInfo, error)
	calls    atomic.Int32
}

func (s *stubChain) Head(_ context.Context) (chain.HeadInfo, error) { return s.headFn() }
func (s *stubChain) FilterJobCompleted(_ context.Context, w common.Address, lo, hi uint64) ([]chain.JobCompletedEvent, error) {
	s.calls.Add(1)
	return s.filterFn(w, lo, hi)
}
func (s *stubChain) GetJobState(_ context.Context, jobID uint64) (chain.JobStateInfo, error) {
	return s.stateFn(jobID)
}

func newReconcilerStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStore(filepath.Join(dir, "rs.json"), defaultIdentityForTest(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestReconciler_NoOpWhenStartAboveHead(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)

	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 100, Timestamp: 1000}, nil },
		filterFn: func(common.Address, uint64, uint64) ([]chain.JobCompletedEvent, error) {
			t.Fatal("FilterJobCompleted should not be called when caught up")
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 0

	// Set cursor at safeHead — start = cursor + 1 = 101 > safeHead = 100.
	require.NoError(t, store.SetReconcileBlock(context.Background(), 100))

	r := NewReconciler(store, ch, defaultIdentityForTest().WorkerAddress, cfg, nil)
	require.NoError(t, r.Run(context.Background()))
}

func TestReconciler_UnderflowGuardWhenHeadBelowConfirmations(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)

	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 3, Timestamp: 100}, nil },
		filterFn: func(common.Address, uint64, uint64) ([]chain.JobCompletedEvent, error) {
			t.Fatal("FilterJobCompleted should not be called when head < confirmations")
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 5 // > head (3)
	cfg.StartBlock = 0

	r := NewReconciler(store, ch, defaultIdentityForTest().WorkerAddress, cfg, nil)
	require.NoError(t, r.Run(context.Background()), "must no-op without uint64 underflow")
}

func TestReconciler_ResumesFromCursorPlusOne(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	require.NoError(t, store.SetReconcileBlock(context.Background(), 1000))

	var observedLo uint64
	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 1500, Timestamp: 1000}, nil },
		filterFn: func(_ common.Address, lo, _ uint64) ([]chain.JobCompletedEvent, error) {
			observedLo = lo
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 0
	cfg.ChunkSize = 5000

	r := NewReconciler(store, ch, worker, cfg, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.Equal(t, uint64(1001), observedLo, "resume must be cursor+1")
}

func TestReconciler_UsesStartBlockOnFirstRun(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	var observedLo uint64
	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 50000, Timestamp: 1000}, nil },
		filterFn: func(_ common.Address, lo, _ uint64) ([]chain.JobCompletedEvent, error) {
			observedLo = lo
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 42000
	cfg.ChunkSize = 100000 // big enough to fit in one chunk

	r := NewReconciler(store, ch, worker, cfg, nil)
	require.NoError(t, r.Run(context.Background()))
	assert.Equal(t, uint64(42000), observedLo, "first run must honor StartBlock")
}

func TestReconciler_ChunksLargeRanges(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	type call struct{ lo, hi uint64 }
	var calls []call
	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 12345, Timestamp: 1}, nil },
		filterFn: func(_ common.Address, lo, hi uint64) ([]chain.JobCompletedEvent, error) {
			calls = append(calls, call{lo, hi})
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.ChunkSize = 5000

	r := NewReconciler(store, ch, worker, cfg, nil)
	require.NoError(t, r.Run(context.Background()))

	// Expect chunks: [1..5000], [5001..10000], [10001..12345]
	require.Len(t, calls, 3)
	assert.Equal(t, call{1, 5000}, calls[0])
	assert.Equal(t, call{5001, 10000}, calls[1])
	assert.Equal(t, call{10001, 12345}, calls[2])

	cursor, err := store.GetReconcileBlock(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), cursor, "cursor must end at safeHead after a complete pass")
}

func TestReconciler_AddsCompletedAndResolvedEligibleSkipsTerminal(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	// Five events, one each across all interesting states.
	events := []chain.JobCompletedEvent{
		{JobID: 1, Worker: worker, BlockNumber: 10},
		{JobID: 2, Worker: worker, BlockNumber: 11},
		{JobID: 3, Worker: worker, BlockNumber: 12},
		{JobID: 4, Worker: worker, BlockNumber: 13},
		{JobID: 5, Worker: worker, BlockNumber: 14},
	}
	states := map[uint64]chain.JobStateInfo{
		1: {State: chain.JobStateCompleted, Worker: worker, CompletedAt: 1000, EscrowedFee: big.NewInt(100)},
		2: {State: chain.JobStateResolved, Worker: worker, CompletedAt: 1001, EscrowedFee: big.NewInt(200)},
		3: {State: chain.JobStateResolved, Worker: worker, CompletedAt: 1002, EscrowedFee: big.NewInt(0)}, // guilty: skip
		4: {State: chain.JobStateReleased, Worker: worker, CompletedAt: 1003, EscrowedFee: big.NewInt(0)}, // skip
		5: {State: chain.JobStateTimedOut, Worker: worker, CompletedAt: 1004, EscrowedFee: big.NewInt(0)}, // skip
	}

	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 100, Timestamp: 1}, nil },
		filterFn: func(common.Address, uint64, uint64) ([]chain.JobCompletedEvent, error) {
			return events, nil
		},
		stateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			info, ok := states[jobID]
			if !ok {
				return chain.JobStateInfo{}, errors.New("unknown")
			}
			return info, nil
		},
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.ChunkSize = 5000

	r := NewReconciler(store, ch, worker, cfg, nil)
	require.NoError(t, r.Run(context.Background()))

	pending, err := store.Pending(context.Background())
	require.NoError(t, err)
	require.Len(t, pending, 2, "only Completed (job 1) and Resolved-with-escrow (job 2) added")
	ids := []uint64{pending[0].JobID, pending[1].JobID}
	assert.Equal(t, []uint64{1, 2}, ids)

	// CompletedAt comes from the on-chain Job struct, not the event.
	for _, p := range pending {
		assert.Equal(t, states[p.JobID].CompletedAt, p.CompletedAt,
			"reconciler must use authoritative on-chain CompletedAt")
	}
}

func TestReconciler_FiltersOutForeignWorkerEvents(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress
	other := common.HexToAddress("0xdeadbeef00000000000000000000000000000000")

	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) { return chain.HeadInfo{Number: 100, Timestamp: 1}, nil },
		filterFn: func(common.Address, uint64, uint64) ([]chain.JobCompletedEvent, error) {
			// Belt-and-braces: even if FilterJobCompleted somehow returned a
			// foreign worker event, the reconciler must drop it.
			return []chain.JobCompletedEvent{{JobID: 99, Worker: other, BlockNumber: 50}}, nil
		},
		stateFn: func(jobID uint64) (chain.JobStateInfo, error) {
			return chain.JobStateInfo{
				State: chain.JobStateCompleted, Worker: other, CompletedAt: 1000,
				EscrowedFee: big.NewInt(100),
			}, nil
		},
	}

	cfg := DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1

	r := NewReconciler(store, ch, worker, cfg, nil)
	require.NoError(t, r.Run(context.Background()))

	pending, err := store.Pending(context.Background())
	require.NoError(t, err)
	assert.Empty(t, pending, "foreign-worker events must not enter our pending set")
}

func TestReconciler_StartPeriodicAndStopAreClean(t *testing.T) {
	t.Parallel()
	store := newReconcilerStore(t)
	worker := defaultIdentityForTest().WorkerAddress

	var fired atomic.Int32
	ch := &stubChain{
		headFn: func() (chain.HeadInfo, error) {
			fired.Add(1)
			return chain.HeadInfo{Number: 0, Timestamp: 0}, nil // no-op every tick
		},
		filterFn: func(common.Address, uint64, uint64) ([]chain.JobCompletedEvent, error) {
			return nil, nil
		},
		stateFn: func(uint64) (chain.JobStateInfo, error) { return chain.JobStateInfo{}, nil },
	}

	cfg := DefaultConfig()
	cfg.ReconcileInterval = 10 * time.Millisecond
	cfg.Confirmations = 0

	r := NewReconciler(store, ch, worker, cfg, nil)
	r.StartPeriodic(context.Background())
	time.Sleep(45 * time.Millisecond) // expect ~4 ticks
	r.StopPeriodic()
	r.StopPeriodic() // idempotent

	// At least one tick fired; we deliberately do not pin the upper bound
	// because timer scheduling is non-deterministic in CI.
	assert.GreaterOrEqual(t, fired.Load(), int32(1))
}

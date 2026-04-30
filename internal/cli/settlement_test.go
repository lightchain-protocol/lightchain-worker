package cli

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/release"
)

// mockSettlement is the smallest function-var mock that satisfies
// chain.SettlementClient. The full chain.MockSettlementClient lives in
// the chain package; we keep a local mini-version here so cli tests do
// not pull in chain test helpers transitively.
type mockSettlement struct {
	workerBalanceFn func(ctx context.Context, worker common.Address) (*big.Int, error)
	withdrawFn      func(ctx context.Context) error
	getJobStateFn   func(ctx context.Context, jobID uint64) (chain.JobStateInfo, error)
	disputeWindowFn func(ctx context.Context) (time.Duration, error)
	headFn          func(ctx context.Context) (chain.HeadInfo, error)
	releaseJobFn    func(ctx context.Context, jobID uint64) error
	releaseJobsFn   func(ctx context.Context, ids []uint64) error
	filterFn        func(ctx context.Context, w common.Address, lo, hi uint64) ([]chain.JobCompletedEvent, error)
}

func (m *mockSettlement) WorkerBalance(ctx context.Context, w common.Address) (*big.Int, error) {
	return m.workerBalanceFn(ctx, w)
}
func (m *mockSettlement) Withdraw(ctx context.Context) error { return m.withdrawFn(ctx) }
func (m *mockSettlement) GetJobState(ctx context.Context, id uint64) (chain.JobStateInfo, error) {
	return m.getJobStateFn(ctx, id)
}
func (m *mockSettlement) GetDisputeWindow(ctx context.Context) (time.Duration, error) {
	if m.disputeWindowFn == nil {
		return 24 * time.Hour, nil
	}
	return m.disputeWindowFn(ctx)
}
func (m *mockSettlement) Head(ctx context.Context) (chain.HeadInfo, error) { return m.headFn(ctx) }
func (m *mockSettlement) ReleaseJob(ctx context.Context, id uint64) error {
	return m.releaseJobFn(ctx, id)
}
func (m *mockSettlement) ReleaseJobs(ctx context.Context, ids []uint64) error {
	return m.releaseJobsFn(ctx, ids)
}
func (m *mockSettlement) FilterJobCompleted(ctx context.Context, w common.Address, lo, hi uint64) ([]chain.JobCompletedEvent, error) {
	return m.filterFn(ctx, w, lo, hi)
}

func TestBalance_PrintsAddressAndBalance(t *testing.T) {
	t.Parallel()
	settlement := &mockSettlement{
		workerBalanceFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(1_500_000_000_000_000_000), nil // 1.5 ETH
		},
	}
	var out bytes.Buffer
	h := &Handler{
		Settlement: settlement,
		WorkerAddr: testAddr,
		Out:        &out,
		Logger:     testLogger(),
	}
	require.NoError(t, h.Balance(context.Background()))

	got := out.String()
	assert.Contains(t, got, testAddr.Hex())
	assert.Contains(t, got, "1500000000000000000 wei")
	assert.Contains(t, got, "1.500000000 ETH")
}

func TestBalance_NilSettlementErrors(t *testing.T) {
	t.Parallel()
	h := &Handler{WorkerAddr: testAddr, Out: &bytes.Buffer{}, Logger: testLogger()}
	err := h.Balance(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Settlement")
}

func TestWithdraw_NoBalanceShortCircuits(t *testing.T) {
	t.Parallel()
	called := false
	settlement := &mockSettlement{
		workerBalanceFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(0), nil
		},
		withdrawFn: func(_ context.Context) error {
			called = true
			return nil
		},
	}
	var out bytes.Buffer
	h := &Handler{Settlement: settlement, WorkerAddr: testAddr, Out: &out, Logger: testLogger()}
	require.NoError(t, h.Withdraw(context.Background()))

	assert.False(t, called, "Withdraw must not be invoked when balance is zero")
	assert.Contains(t, out.String(), "no balance to withdraw")
}

func TestWithdraw_HappyPathPrintsPreReadAmount(t *testing.T) {
	t.Parallel()

	balances := []int64{500_000_000_000_000_000, 0} // pre, post (post = withdrawn)
	calls := 0
	settlement := &mockSettlement{
		workerBalanceFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			defer func() { calls++ }()
			if calls < len(balances) {
				return big.NewInt(balances[calls]), nil
			}
			return big.NewInt(0), nil
		},
		withdrawFn: func(_ context.Context) error { return nil },
	}
	var out bytes.Buffer
	h := &Handler{Settlement: settlement, WorkerAddr: testAddr, Out: &out, Logger: testLogger()}
	require.NoError(t, h.Withdraw(context.Background()))

	got := out.String()
	assert.Contains(t, got, "withdraw submitted")
	assert.Contains(t, got, "500000000000000000 wei")
	assert.Contains(t, got, "0.500000000 ETH")
	assert.Contains(t, got, "post-balance: 0 wei")
}

func TestWithdraw_TxFailureSurfaces(t *testing.T) {
	t.Parallel()
	settlement := &mockSettlement{
		workerBalanceFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(100), nil
		},
		withdrawFn: func(_ context.Context) error { return errors.New("nonce too low") },
	}
	h := &Handler{Settlement: settlement, WorkerAddr: testAddr, Out: &bytes.Buffer{}, Logger: testLogger()}
	err := h.Withdraw(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonce too low")
}

func TestRelease_RunsReconcilerThenCycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := release.NewFileStore(filepath.Join(dir, "rs.json"), release.StoreIdentity{
		ChainID:       1337,
		JobRegistry:   common.HexToAddress("0xaaaa000000000000000000000000000000000001"),
		WorkerAddress: testAddr,
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	var (
		filterCalled  bool
		releaseCalled bool
	)
	settlement := &mockSettlement{
		filterFn: func(_ context.Context, _ common.Address, _, _ uint64) ([]chain.JobCompletedEvent, error) {
			filterCalled = true
			return nil, nil // nothing to add — empty event log
		},
		headFn: func(_ context.Context) (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: 1_000_000}, nil
		},
		getJobStateFn: func(_ context.Context, _ uint64) (chain.JobStateInfo, error) {
			return chain.JobStateInfo{}, errors.New("never called for empty pending")
		},
		releaseJobsFn: func(_ context.Context, _ []uint64) error {
			releaseCalled = true
			return nil
		},
	}

	cfg := release.DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.DisputeWindowOverride = time.Hour

	var out bytes.Buffer
	h := &Handler{
		Settlement:    settlement,
		WorkerAddr:    testAddr,
		ReleaseStore:  store,
		ReleaseConfig: cfg,
		Out:           &out,
		Logger:        testLogger(),
	}
	require.NoError(t, h.Release(context.Background(), false))

	assert.True(t, filterCalled, "reconciler must invoke FilterJobCompleted")
	assert.False(t, releaseCalled, "no eligible jobs → no ReleaseJobs call")

	got := out.String()
	assert.Contains(t, got, "reconciliation complete")
	assert.Contains(t, got, "release cycle complete")
	assert.Contains(t, got, "pending after cycle: 0")
}

// W6: contract pause must surface as a non-zero exit (returned error)
// AND a clear "paused" message on stdout. Previously the CLI printed
// "release cycle complete" and returned nil even when the cycle was
// silently backed off because the contract was paused.
func TestRelease_PauseSurfacesAsErrorWithBackoffMessage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := release.NewFileStore(filepath.Join(dir, "rs.json"), release.StoreIdentity{
		ChainID:       1337,
		JobRegistry:   common.HexToAddress("0xaaaa000000000000000000000000000000000001"),
		WorkerAddress: testAddr,
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	// Seed a pending job so the cycle has something to attempt.
	require.NoError(t, store.AddEligible(context.Background(), 99, 1_000_000-2*86400))

	settlement := &mockSettlement{
		filterFn: func(_ context.Context, _ common.Address, _, _ uint64) ([]chain.JobCompletedEvent, error) {
			return nil, nil
		},
		headFn: func(_ context.Context) (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: 1_000_000}, nil
		},
		getJobStateFn: func(_ context.Context, jobID uint64) (chain.JobStateInfo, error) {
			return chain.JobStateInfo{
				State:       chain.JobStateCompleted,
				Worker:      testAddr,
				CompletedAt: 1_000_000 - 2*86400,
				EscrowedFee: big.NewInt(100),
			}, nil
		},
		releaseJobsFn: func(_ context.Context, _ []uint64) error {
			return errors.New("execution reverted: Pausable: paused")
		},
	}

	cfg := release.DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.DisputeWindowOverride = time.Hour
	cfg.PausedCycleBackoff = 30 * time.Minute

	var out bytes.Buffer
	h := &Handler{
		Settlement:    settlement,
		WorkerAddr:    testAddr,
		ReleaseStore:  store,
		ReleaseConfig: cfg,
		Out:           &out,
		Logger:        testLogger(),
	}

	err = h.Release(context.Background(), false)
	require.Error(t, err, "pause must produce a non-zero exit so scripted callers detect the silent failure")
	assert.Contains(t, err.Error(), "paused")

	got := out.String()
	assert.Contains(t, got, "release cycle paused")
	assert.NotContains(t, got, "release cycle complete",
		"the misleading 'release cycle complete' message must NOT appear on pause")
}

// W6: per-job failures must produce non-zero exit so a scripted
// caller (cron, smoke test) can detect partial failure.
func TestRelease_PerJobFailuresSurfaceAsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := release.NewFileStore(filepath.Join(dir, "rs.json"), release.StoreIdentity{
		ChainID:       1337,
		JobRegistry:   common.HexToAddress("0xaaaa000000000000000000000000000000000001"),
		WorkerAddress: testAddr,
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	for _, id := range []uint64{1, 2} {
		require.NoError(t, store.AddEligible(context.Background(), id, 1_000_000-2*86400))
	}

	settlement := &mockSettlement{
		filterFn: func(_ context.Context, _ common.Address, _, _ uint64) ([]chain.JobCompletedEvent, error) {
			return nil, nil
		},
		headFn: func(_ context.Context) (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: 1_000_000}, nil
		},
		getJobStateFn: func(_ context.Context, _ uint64) (chain.JobStateInfo, error) {
			return chain.JobStateInfo{
				State:       chain.JobStateCompleted,
				Worker:      testAddr,
				CompletedAt: 1_000_000 - 2*86400,
				EscrowedFee: big.NewInt(100),
			}, nil
		},
		releaseJobsFn: func(_ context.Context, _ []uint64) error {
			return errors.New("execution reverted: stale") // non-pause; triggers fallback
		},
		releaseJobFn: func(_ context.Context, _ uint64) error {
			return errors.New("execution reverted: misc") // every per-job fails
		},
	}

	cfg := release.DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.DisputeWindowOverride = time.Hour

	var out bytes.Buffer
	h := &Handler{
		Settlement:    settlement,
		WorkerAddr:    testAddr,
		ReleaseStore:  store,
		ReleaseConfig: cfg,
		Out:           &out,
		Logger:        testLogger(),
	}

	err = h.Release(context.Background(), false)
	require.Error(t, err, "per-job failures must produce a non-zero exit")
	assert.Contains(t, err.Error(), "failed")

	got := out.String()
	assert.Contains(t, got, "release cycle completed with failures")
}

func TestRelease_ReconcileOnlySkipsCycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := release.NewFileStore(filepath.Join(dir, "rs.json"), release.StoreIdentity{
		ChainID:       1337,
		JobRegistry:   common.HexToAddress("0xaaaa000000000000000000000000000000000001"),
		WorkerAddress: testAddr,
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	var releaseJobsCalled bool
	settlement := &mockSettlement{
		filterFn: func(_ context.Context, _ common.Address, _, _ uint64) ([]chain.JobCompletedEvent, error) {
			return nil, nil
		},
		headFn: func(_ context.Context) (chain.HeadInfo, error) {
			return chain.HeadInfo{Number: 100, Timestamp: 1_000_000}, nil
		},
		releaseJobsFn: func(_ context.Context, _ []uint64) error {
			releaseJobsCalled = true
			return nil
		},
	}

	cfg := release.DefaultConfig()
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.DisputeWindowOverride = time.Hour

	var out bytes.Buffer
	h := &Handler{
		Settlement:    settlement,
		WorkerAddr:    testAddr,
		ReleaseStore:  store,
		ReleaseConfig: cfg,
		Out:           &out,
		Logger:        testLogger(),
	}
	require.NoError(t, h.Release(context.Background(), true)) // reconcileOnly=true

	assert.False(t, releaseJobsCalled, "reconcile-only must not run a release cycle")

	got := out.String()
	assert.Contains(t, got, "reconciliation complete")
	assert.False(t, strings.Contains(got, "release cycle complete"),
		"reconcile-only path must not print release-cycle output")
}

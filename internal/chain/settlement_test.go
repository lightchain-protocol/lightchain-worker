package chain

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

// MockSettlementClient is a hand-rolled mock following the same function-var
// pattern as MockRegistrationClient / MockJobExecutionClient. The release
// scheduler and reconciler use it for unit testing.
type MockSettlementClient struct {
	ReleaseJobFn          func(ctx context.Context, jobID uint64) error
	ReleaseJobsFn         func(ctx context.Context, jobIDs []uint64) error
	GetJobStateFn         func(ctx context.Context, jobID uint64) (JobStateInfo, error)
	GetDisputeWindowFn    func(ctx context.Context) (time.Duration, error)
	WorkerBalanceFn       func(ctx context.Context, worker common.Address) (*big.Int, error)
	WithdrawFn            func(ctx context.Context) error
	HeadFn                func(ctx context.Context) (HeadInfo, error)
	FilterJobCompletedFn  func(ctx context.Context, worker common.Address, fromBlock, toBlock uint64) ([]JobCompletedEvent, error)
}

func (m *MockSettlementClient) ReleaseJob(ctx context.Context, jobID uint64) error {
	return m.ReleaseJobFn(ctx, jobID)
}

func (m *MockSettlementClient) ReleaseJobs(ctx context.Context, jobIDs []uint64) error {
	return m.ReleaseJobsFn(ctx, jobIDs)
}

func (m *MockSettlementClient) GetJobState(ctx context.Context, jobID uint64) (JobStateInfo, error) {
	return m.GetJobStateFn(ctx, jobID)
}

func (m *MockSettlementClient) GetDisputeWindow(ctx context.Context) (time.Duration, error) {
	return m.GetDisputeWindowFn(ctx)
}

func (m *MockSettlementClient) WorkerBalance(ctx context.Context, worker common.Address) (*big.Int, error) {
	return m.WorkerBalanceFn(ctx, worker)
}

func (m *MockSettlementClient) Withdraw(ctx context.Context) error {
	return m.WithdrawFn(ctx)
}

func (m *MockSettlementClient) Head(ctx context.Context) (HeadInfo, error) {
	return m.HeadFn(ctx)
}

func (m *MockSettlementClient) FilterJobCompleted(
	ctx context.Context,
	worker common.Address,
	fromBlock, toBlock uint64,
) ([]JobCompletedEvent, error) {
	return m.FilterJobCompletedFn(ctx, worker, fromBlock, toBlock)
}

func TestMockSettlementClient_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ SettlementClient = (*MockSettlementClient)(nil)
}

func TestJobState_String(t *testing.T) {
	t.Parallel()
	cases := map[JobState]string{
		JobStateSubmitted:    "Submitted",
		JobStateAcknowledged: "Acknowledged",
		JobStateCompleted:    "Completed",
		JobStateTimedOut:     "TimedOut",
		JobStateDisputed:     "Disputed",
		JobStateResolved:     "Resolved",
		JobStateReleased:     "Released",
		JobState(99):         "Unknown",
	}
	for state, want := range cases {
		assert.Equal(t, want, state.String(), "state=%d", state)
	}
}

func TestJobState_EnumValuesMatchSolidity(t *testing.T) {
	t.Parallel()
	// These constants are part of the on-chain ABI surface — reordering or
	// re-numbering them silently breaks settlement decisions. Pin the values
	// explicitly so accidental edits trip the test.
	assert.Equal(t, JobState(0), JobStateSubmitted)
	assert.Equal(t, JobState(1), JobStateAcknowledged)
	assert.Equal(t, JobState(2), JobStateCompleted)
	assert.Equal(t, JobState(3), JobStateTimedOut)
	assert.Equal(t, JobState(4), JobStateDisputed)
	assert.Equal(t, JobState(5), JobStateResolved)
	assert.Equal(t, JobState(6), JobStateReleased)
}

func TestSetDisputeWindowCacheTTL_InvalidatesCache(t *testing.T) {
	t.Parallel()

	// We can't construct a fully-wired ChainClient without dialing a chain,
	// but we can manipulate the cache fields directly to verify the
	// invalidation behavior. This is the tightest unit-level check possible
	// without an integration harness; full happy-path coverage of
	// GetDisputeWindow lives in the workertest integration suite.
	c := &ChainClient{
		disputeWindowValue:   42 * time.Second,
		disputeWindowExpires: time.Now().Add(1 * time.Hour),
	}

	// Pre-condition: cache populated and not expired.
	assert.False(t, c.disputeWindowExpires.IsZero())

	c.SetDisputeWindowCacheTTL(5 * time.Minute)

	// Post-condition: TTL recorded, expiry zeroed so next GetDisputeWindow
	// fetches fresh.
	assert.Equal(t, 5*time.Minute, c.disputeWindowCacheTTL)
	assert.True(t, c.disputeWindowExpires.IsZero(), "expires should be zeroed after TTL change")
}

func TestSetDisputeWindowCacheTTL_ZeroFallsBackToDefault(t *testing.T) {
	t.Parallel()

	c := &ChainClient{}
	c.SetDisputeWindowCacheTTL(0)

	// Stored TTL is the zero passed in; the runtime fallback in
	// GetDisputeWindow uses defaultDisputeWindowCacheTTL when the field is
	// non-positive.
	assert.Equal(t, time.Duration(0), c.disputeWindowCacheTTL)
	assert.Greater(t, defaultDisputeWindowCacheTTL, time.Duration(0),
		"default TTL must be positive so the fallback path produces a real expiry")
}

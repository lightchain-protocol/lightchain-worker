package chain

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time assertions: ChainClient must satisfy both interfaces.
var (
	_ RegistrationClient = (*ChainClient)(nil)
	_ JobExecutionClient = (*ChainClient)(nil)
)

// MockRegistrationClient is a hand-rolled mock for RegistrationClient.
// All fields are function vars so tests can inject return values without a mock library.
type MockRegistrationClient struct {
	IsWorkerRegisteredFn     func(ctx context.Context, worker common.Address) (bool, error)
	RegisterWorkerFn         func(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error
	AddSupportedModelFn      func(ctx context.Context, modelID [32]byte) error
	DeregisterWorkerFn       func(ctx context.Context) error
	GetMinWorkerStakeFn      func(ctx context.Context) (*big.Int, error)
	GetWorkerEncryptionKeyFn func(ctx context.Context, worker common.Address) ([]byte, error)
}

func (m *MockRegistrationClient) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	return m.IsWorkerRegisteredFn(ctx, worker)
}

func (m *MockRegistrationClient) RegisterWorker(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error {
	return m.RegisterWorkerFn(ctx, encryptionPubKey, stake)
}

func (m *MockRegistrationClient) AddSupportedModel(ctx context.Context, modelID [32]byte) error {
	return m.AddSupportedModelFn(ctx, modelID)
}

func (m *MockRegistrationClient) DeregisterWorker(ctx context.Context) error {
	return m.DeregisterWorkerFn(ctx)
}

func (m *MockRegistrationClient) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	return m.GetMinWorkerStakeFn(ctx)
}

func (m *MockRegistrationClient) GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error) {
	return m.GetWorkerEncryptionKeyFn(ctx, worker)
}

// MockJobExecutionClient is a hand-rolled mock for JobExecutionClient.
type MockJobExecutionClient struct {
	AcknowledgeJobFn         func(ctx context.Context, jobID uint64) error
	CompleteJobFn            func(ctx context.Context, jobID uint64, responseBlobHash [32]byte, responseCiphertextHash [32]byte) error
	HasJobAcknowledgedFn     func(ctx context.Context, jobID uint64) (bool, error)
	HasJobCompletedFn        func(ctx context.Context, jobID uint64) (bool, error)
	GetSessionEncWorkerKeyFn func(ctx context.Context, sessionID uint64) ([]byte, error)
}

func (m *MockJobExecutionClient) AcknowledgeJob(ctx context.Context, jobID uint64) error {
	return m.AcknowledgeJobFn(ctx, jobID)
}

func (m *MockJobExecutionClient) CompleteJob(
	ctx context.Context,
	jobID uint64,
	responseBlobHash [32]byte,
	responseCiphertextHash [32]byte,
) error {
	return m.CompleteJobFn(ctx, jobID, responseBlobHash, responseCiphertextHash)
}

func (m *MockJobExecutionClient) HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error) {
	return m.HasJobAcknowledgedFn(ctx, jobID)
}

func (m *MockJobExecutionClient) HasJobCompleted(ctx context.Context, jobID uint64) (bool, error) {
	return m.HasJobCompletedFn(ctx, jobID)
}

func (m *MockJobExecutionClient) GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	return m.GetSessionEncWorkerKeyFn(ctx, sessionID)
}

func (m *MockJobExecutionClient) GetJobBlobInfo(_ context.Context, _ uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	return common.Hash{}, common.Hash{}, 0, 0, nil
}

// TestMockImplementsInterface verifies mocks satisfy their interfaces.
func TestMockImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ RegistrationClient = (*MockRegistrationClient)(nil)
	var _ JobExecutionClient = (*MockJobExecutionClient)(nil)
}

func TestCheckReceipt(t *testing.T) {
	t.Parallel()

	txHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	tests := []struct {
		name        string
		receipt     *types.Receipt
		wantErr     bool
		errContains string
	}{
		{
			name:        "nil receipt returns error",
			receipt:     nil,
			wantErr:     true,
			errContains: "WaitMined returned nil receipt",
		},
		{
			name:        "status 0 (reverted) returns error",
			receipt:     &types.Receipt{Status: types.ReceiptStatusFailed, TxHash: txHash},
			wantErr:     true,
			errContains: "transaction reverted",
		},
		{
			name:    "status 1 (success) returns nil",
			receipt: &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: txHash},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkReceipt(tt.receipt, "TestTx")
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSubmitPreparedJobTx_ResetNonceOnBuildFailure(t *testing.T) {
	t.Parallel()

	client := newTestChainClient(t)

	err := client.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(*bind.TransactOpts) (*types.Transaction, error) {
		return nil, assert.AnError
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AcknowledgeJob transaction")
	assert.False(t, client.nonceMgr.initialized, "pre-send failure must reset the nonce manager")
	assert.Equal(t, 0, client.jobTxBackend.(*mockJobTxBackend).sendCalls)
}

func TestSubmitPreparedTx_DoesNotResetNonceOnAmbiguousSendFailure(t *testing.T) {
	t.Parallel()

	client := newTestChainClient(t)
	client.jobTxBackend.(*mockJobTxBackend).sendErr = assert.AnError

	err := client.submitPreparedTx(context.Background(), "CompleteJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send CompleteJob tx")
	assert.True(t, client.nonceMgr.initialized, "send failure is ambiguous and must not reset the nonce manager")
	assert.Equal(t, uint64(6), client.nonceMgr.pendingNonce)
	assert.Equal(t, 1, client.jobTxBackend.(*mockJobTxBackend).sendCalls)
}

func TestSubmitPreparedTx_ResetsNonceOnPreBroadcastSendFailure(t *testing.T) {
	t.Parallel()

	client := newTestChainClient(t)
	client.jobTxBackend.(*mockJobTxBackend).sendErr = mockRPCError{code: rpcCodeInsufficientFunds}

	err := client.submitPreparedTx(context.Background(), "CompleteJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send CompleteJob tx")
	assert.False(t, client.nonceMgr.initialized, "definite pre-broadcast failure must reset the nonce manager")
	assert.Equal(t, 1, client.jobTxBackend.(*mockJobTxBackend).sendCalls)
}

// TestSubmitPreparedTx_RecordsStuckNonceHitWithoutReplacement asserts that
// a non-blob tx hitting "address already reserved" records a hit into the
// shared tracker but does NOT itself attempt replacement — the blob pool
// only accepts blob replacements, so the non-blob path defers recovery to
// the next blob broadcast's decision.
func TestSubmitPreparedTx_RecordsStuckNonceHitWithoutReplacement(t *testing.T) {
	t.Parallel()

	tracker := NewStuckNonceTracker()
	client := newTestChainClient(t)
	client.stuckTracker = tracker
	client.jobTxBackend.(*mockJobTxBackend).sendErr = errors.New("eth_sendRawTransaction: address already reserved")

	for i := 0; i < 3; i++ {
		err := client.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "address already reserved")
	}

	assert.Equal(t, 3, tracker.MaxConsecutiveHits(),
		"three consecutive reservation rejections must accumulate in the shared tracker")
	// The legacy path doesn't bump for any nonce, so checking BumpAttemptsUsedFor
	// for the broadcast nonce (0 — mock backend uses opts.Nonce starting at 0)
	// must report 0. Reading by nonce is required since the global reader was
	// removed to fix per-nonce budget leakage (PR #20 follow-up).
	assert.Equal(t, 0, tracker.BumpAttemptsUsedFor(0),
		"non-blob path must not attempt any replacement bumps")
}

// TestSubmitPreparedTx_BlocksOnBlobInFlight asserts that cross-class
// serialization works: a blob Enter held in the coordinator (simulating a
// concurrent BlobTxSubmitter broadcast) prevents submitPreparedTx — which
// enters ClassLegacy — from reaching SendTransaction. This is the geth
// cross-subpool exclusion invariant (blobpool.go:336).
func TestSubmitPreparedTx_BlocksOnBlobInFlight(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator
	client.jobTxBackend.(*mockJobTxBackend).sendErr = assert.AnError

	// External holder simulates a concurrent BlobTxSubmitter in its
	// SendTransaction..WaitMined window. Same coordinator pointer => the
	// Legacy Enter inside submitPreparedTx must block until release.
	blobTok, err := coordinator.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- client.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
		"submitPreparedTx must not reach SendTransaction while a blob is in flight")

	blobTok.Done()

	select {
	case err := <-submitDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "send AcknowledgeJob tx")
		assert.Equal(t, 1, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
			"SendTransaction must fire exactly once after blob drains")
	case <-time.After(2 * time.Second):
		t.Fatal("submitPreparedTx did not proceed within 2s of blob drain — coordinator is not shared correctly")
	}
}

// TestSubmitPreparedTx_LegacyParallelism asserts that two concurrent legacy
// broadcasts DO NOT block each other — they share the class, and geth's
// legacypool handles nonce ordering internally. This is the within-class
// throughput win over the old single-slot BroadcastSerializer.
func TestSubmitPreparedTx_LegacyParallelism(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()

	// Two independent test clients share ONE coordinator (same signing
	// key in production). Each client has its own backend so sendCalls
	// is attributed separately.
	c1 := newTestChainClient(t)
	c1.coordinator = coordinator
	c2 := newTestChainClient(t)
	c2.coordinator = coordinator

	// Make SendTransaction block on a channel so we can observe both
	// entering their critical section simultaneously.
	release := make(chan struct{})
	blockingSend := func() error {
		<-release
		return assert.AnError // fail cheaply so WaitMined isn't called
	}
	c1.jobTxBackend.(*mockJobTxBackend).sendFn = blockingSend
	c2.jobTxBackend.(*mockJobTxBackend).sendFn = blockingSend

	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() {
		done1 <- c1.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
	}()
	go func() {
		done2 <- c2.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
	}()

	// Both must reach SendTransaction before we release either — proof
	// that the coordinator did NOT serialize them.
	require.Eventually(t, func() bool {
		return coordinator.Inflight(ClassLegacy) == 2
	}, 500*time.Millisecond, 10*time.Millisecond,
		"both legacy broadcasts must be in flight simultaneously")

	close(release)
	<-done1
	<-done2
}

// TestSubmitPreparedTx_CtxCancelDuringEnter asserts that a submitPreparedTx
// call blocked in coordinator.Enter exits promptly on ctx cancel without
// reaching SendTransaction.
func TestSubmitPreparedTx_CtxCancelDuringEnter(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator

	// External blob holder keeps the legacy lane blocked until test end.
	blobTok, err := coordinator.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	defer blobTok.Done()

	ctx, cancel := context.WithCancel(context.Background())
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- client.submitPreparedTx(ctx, "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancelAt := time.Now()
	cancel()

	select {
	case err := <-submitDone:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(cancelAt), 500*time.Millisecond,
			"submitPreparedTx must exit within 500ms of ctx cancel")
		assert.Equal(t, 0, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
			"SendTransaction must not fire when ctx is cancelled during Enter")
		assert.False(t, client.nonceMgr.initialized,
			"cancelled Enter must not reserve a nonce")
	case <-time.After(2 * time.Second):
		t.Fatal("submitPreparedTx did not return within 2s of ctx cancel")
	}
}

type staticNonceFetcher struct {
	nonce uint64
	calls int
}

func (f *staticNonceFetcher) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	f.calls++
	return f.nonce, nil
}

type mockJobTxBackend struct {
	gasPrice  *big.Int
	sendErr   error
	sendCalls int
	// sendFn, if set, is invoked BEFORE sendErr is returned. Used in
	// concurrency tests to synchronize multiple goroutines at the
	// SendTransaction boundary.
	sendFn func() error
	mu     sync.Mutex
}

func (m *mockJobTxBackend) SuggestGasPrice(context.Context) (*big.Int, error) {
	return m.gasPrice, nil
}

func (m *mockJobTxBackend) SendTransaction(context.Context, *types.Transaction) error {
	m.mu.Lock()
	m.sendCalls++
	fn := m.sendFn
	err := m.sendErr
	m.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return err
}

func newTestChainClient(t *testing.T) *ChainClient {
	t.Helper()

	signingKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	backend := &mockJobTxBackend{gasPrice: big.NewInt(1)}
	fetcher := &staticNonceFetcher{nonce: 5}
	workerAddr := crypto.PubkeyToAddress(signingKey.PublicKey)

	return &ChainClient{
		jobTxBackend:   backend,
		signingKey:     signingKey,
		workerAddr:     workerAddr,
		chainID:        big.NewInt(1),
		gasPriceMulBps: 10000,
		nonceMgr:       NewNonceManager(fetcher, workerAddr),
	}
}

type mockRPCError struct {
	code int
	msg  string
}

func (e mockRPCError) Error() string {
	if e.msg == "" {
		return "rpc error"
	}
	return e.msg
}

func (e mockRPCError) ErrorCode() int {
	return e.code
}

// TestChainClient_ClaimSession_NoBindingErrors verifies that a ChainClient
// without a SessionManager binding returns an error rather than panicking.
func TestChainClient_ClaimSession_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	err := c.ClaimSession(context.Background(), 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session manager binding not configured")
}

// TestChainClient_EligibleNow_NoBindingErrors verifies that a ChainClient
// without a SessionManager binding returns an error rather than panicking.
func TestChainClient_EligibleNow_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.EligibleNow(context.Background(), 1, common.Address{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session manager binding not configured")
}

// TestChainClient_FilterSessionRequested_RangeGuard verifies that a range
// where toBlock < fromBlock is rejected immediately.
func TestChainClient_FilterSessionRequested_RangeGuard(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.FilterSessionRequested(context.Background(), 10, 5) // to < from
	require.Error(t, err)
	require.Contains(t, err.Error(), "toBlock")
}

// TestChainClient_FilterSessionRequested_NoBindingErrors verifies that a
// ChainClient without a SessionManager binding returns an error.
func TestChainClient_FilterSessionRequested_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.FilterSessionRequested(context.Background(), 1, 10) // valid range
	require.Error(t, err)
	require.Contains(t, err.Error(), "session manager binding not configured")
}

// TestChainClient_FilterJobSubmitted_RangeGuard verifies that a range
// where toBlock < fromBlock is rejected immediately.
func TestChainClient_FilterJobSubmitted_RangeGuard(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.FilterJobSubmitted(context.Background(), 10, 5) // to < from
	require.Error(t, err)
	require.Contains(t, err.Error(), "toBlock")
}

// TestChainClient_FilterJobSubmitted_NoBindingErrors verifies that a
// ChainClient without a JobRegistry binding returns an error.
func TestChainClient_FilterJobSubmitted_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.FilterJobSubmitted(context.Background(), 1, 10) // valid range
	require.Error(t, err)
	require.Contains(t, err.Error(), "jobRegistry not configured")
}

// TestChainClient_GetPriorSessionJobIDs_RangeGuard verifies that a range
// where toBlock < fromBlock is rejected immediately.
func TestChainClient_GetPriorSessionJobIDs_RangeGuard(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.GetPriorSessionJobIDs(context.Background(), 10, 5, 10, 5) // to < from
	require.Error(t, err)
	require.Contains(t, err.Error(), "toBlock")
}

// TestChainClient_GetPriorSessionJobIDs_NoBindingErrors verifies that a
// ChainClient without a JobRegistry binding returns an error rather than
// panicking.
func TestChainClient_GetPriorSessionJobIDs_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.GetPriorSessionJobIDs(context.Background(), 10, 5, 1, 10) // valid range
	require.Error(t, err)
	require.Contains(t, err.Error(), "jobRegistry not configured")
}

// TestChainClient_GetSessionInfo_NoBindingErrors verifies that a ChainClient
// without a JobRegistry binding returns an error rather than panicking.
func TestChainClient_GetSessionInfo_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.GetSessionInfo(context.Background(), 1)
	require.Error(t, err)
}

// TestChainClient_GetRequestInfo_NoBindingErrors verifies that a ChainClient
// without a SessionManager binding returns an error rather than panicking.
func TestChainClient_GetRequestInfo_NoBindingErrors(t *testing.T) {
	t.Parallel()
	c := &ChainClient{}
	_, err := c.GetRequestInfo(context.Background(), 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session manager binding not configured")
}

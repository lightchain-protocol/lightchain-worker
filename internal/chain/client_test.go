package chain

import (
	"context"
	"errors"
	"math/big"
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
var _ RegistrationClient = (*ChainClient)(nil)
var _ JobExecutionClient = (*ChainClient)(nil)

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

	assert.Equal(t, 3, tracker.ConsecutiveHits(),
		"three consecutive reservation rejections must accumulate in the shared tracker")
	assert.Equal(t, 0, tracker.BumpAttemptsUsed(),
		"non-blob path must not attempt any replacement bumps")
}

// TestSubmitPreparedTx_BlocksOnExternallyHeldSlot asserts that the shared
// BroadcastSerializer actually serializes across submitters: a goroutine
// holding the serializer (simulating a concurrent BlobTxSubmitter in-flight)
// prevents submitPreparedTx from reaching SendTransaction. This is the
// Hazard A invariant — go-ethereum's txpool reservation is sender-wide, so
// one shared serializer must cover blob AND non-blob paths.
func TestSubmitPreparedTx_BlocksOnExternallyHeldSlot(t *testing.T) {
	t.Parallel()

	serializer := NewBroadcastSerializer()
	client := newTestChainClient(t)
	client.serializer = serializer
	// Fail SendTransaction cheaply so submitPreparedTx returns without
	// needing a real ethClient for WaitMined.
	client.jobTxBackend.(*mockJobTxBackend).sendErr = assert.AnError

	// External holder simulates a concurrent BlobTxSubmitter broadcast in
	// its SendTransaction..WaitMined window. Same serializer pointer => any
	// concurrent submitPreparedTx must block.
	require.NoError(t, serializer.Acquire(context.Background()))

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- client.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
			return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
		})
	}()

	// Give the goroutine time to arrive at the serializer.Acquire select,
	// then verify it's blocked before SendTransaction.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
		"submitPreparedTx must not reach SendTransaction while external holder owns the shared serializer")

	// Release the external holder; submitPreparedTx should now proceed,
	// fail its SendTransaction (due to injected sendErr), and return.
	serializer.Release()

	select {
	case err := <-submitDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "send AcknowledgeJob tx")
		assert.Equal(t, 1, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
			"SendTransaction must fire exactly once after external holder releases")
	case <-time.After(2 * time.Second):
		t.Fatal("submitPreparedTx did not proceed within 2s of external release — shared slot is not actually shared")
	}
}

// TestSubmitPreparedTx_CtxCancelDuringSlotWait asserts that a submitPreparedTx
// call blocked on the shared serializer exits promptly on ctx cancel without
// reaching SendTransaction. Regression guard for asynq task cancellation
// propagating through the non-blob broadcast path.
func TestSubmitPreparedTx_CtxCancelDuringSlotWait(t *testing.T) {
	t.Parallel()

	serializer := NewBroadcastSerializer()
	client := newTestChainClient(t)
	client.serializer = serializer

	// External holder keeps the slot until the test ends.
	require.NoError(t, serializer.Acquire(context.Background()))
	defer serializer.Release()

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
			"SendTransaction must not fire when ctx is cancelled during slot wait")
		assert.False(t, client.nonceMgr.initialized,
			"cancelled slot wait must not reserve a nonce")
	case <-time.After(2 * time.Second):
		t.Fatal("submitPreparedTx did not return within 2s of ctx cancel — serializer.Acquire is not ctx-aware")
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
}

func (m *mockJobTxBackend) SuggestGasPrice(context.Context) (*big.Int, error) {
	return m.gasPrice, nil
}

func (m *mockJobTxBackend) SendTransaction(context.Context, *types.Transaction) error {
	m.sendCalls++
	return m.sendErr
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

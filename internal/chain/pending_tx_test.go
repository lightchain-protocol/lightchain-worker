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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubReceiptBackend stands in for the EL when exercising the
// post-broadcast half of a transaction. receipt/err are returned on every
// poll; when gate is non-nil, TransactionReceipt blocks on it first so a
// test can hold a transaction "pending" for as long as it likes.
type stubReceiptBackend struct {
	mu      sync.Mutex
	calls   int
	gate    chan struct{}
	receipt *types.Receipt
	err     error
}

func (s *stubReceiptBackend) TransactionReceipt(ctx context.Context, _ common.Hash) (*types.Receipt, error) {
	s.mu.Lock()
	s.calls++
	gate := s.gate
	s.mu.Unlock()

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.receipt, s.err
}

func (s *stubReceiptBackend) receiptCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *stubReceiptBackend) CodeAt(context.Context, common.Address, *big.Int) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func minedReceipt() *types.Receipt {
	return &types.Receipt{
		Status:      types.ReceiptStatusSuccessful,
		TxHash:      common.HexToHash("0xabc"),
		BlockNumber: big.NewInt(1),
	}
}

func buildLegacyTx(opts *bind.TransactOpts) (*types.Transaction, error) {
	return types.NewTx(&types.LegacyTx{Nonce: opts.Nonce.Uint64()}), nil
}

// TestBroadcastPreparedTx_ReturnsBeforeMining is the property the ack
// overlap depends on: the call returns once the tx is on the wire, without
// polling for a receipt, so the caller can get on with other work.
func TestBroadcastPreparedTx_ReturnsBeforeMining(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator

	// Gate never opens: any receipt poll would block forever, so a
	// returning broadcast proves no poll was attempted.
	backend := &stubReceiptBackend{gate: make(chan struct{}), receipt: minedReceipt()}
	client.receiptBackend = backend

	pending, err := client.broadcastPreparedTx(context.Background(), "AcknowledgeJob", nil, buildLegacyTx)
	require.NoError(t, err)
	require.NotNil(t, pending)

	assert.Equal(t, 1, client.jobTxBackend.(*mockJobTxBackend).sendCalls,
		"broadcast must send the tx")
	assert.Equal(t, 0, backend.receiptCalls(),
		"broadcast must not wait for a receipt")
	assert.NotEqual(t, common.Hash{}, pending.Hash(),
		"the tx hash must be available before the tx mines")

	// The broadcast slot stays held until the join runs — geth reserves
	// the sender across subpools while the tx is in the pool.
	assert.Equal(t, 1, coordinator.Inflight(ClassLegacy),
		"the broadcast slot must be held until Wait runs")
}

// TestPendingTx_WaitReleasesSlotOnSuccess covers the handoff: the slot the
// broadcast acquired is released by Wait, not by the broadcast.
func TestPendingTx_WaitReleasesSlotOnSuccess(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator
	client.receiptBackend = &stubReceiptBackend{receipt: minedReceipt()}

	pending, err := client.broadcastPreparedTx(context.Background(), "AcknowledgeJob", nil, buildLegacyTx)
	require.NoError(t, err)

	require.NoError(t, pending.Wait(context.Background()))
	assert.Equal(t, 0, coordinator.Inflight(ClassLegacy),
		"a mined tx must release the broadcast slot")
}

// TestPendingTx_WaitReleasesSlotOnFailure is the important half: a slot
// stranded by a failed wait would block every blob broadcast for this
// signing key until the coordinator's janitor reaps it.
func TestPendingTx_WaitReleasesSlotOnFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backend     *stubReceiptBackend
		errContains string
	}{
		{
			name:        "receipt wait times out",
			backend:     &stubReceiptBackend{gate: make(chan struct{})},
			errContains: "wait for AcknowledgeJob tx",
		},
		{
			name:        "tx reverted",
			backend:     &stubReceiptBackend{receipt: &types.Receipt{Status: types.ReceiptStatusFailed, BlockNumber: big.NewInt(1)}},
			errContains: "AcknowledgeJob transaction reverted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			coordinator := NewSubpoolCoordinator(nil, 0)
			defer coordinator.Close()
			client := newTestChainClient(t)
			client.coordinator = coordinator
			client.receiptBackend = tt.backend

			pending, err := client.broadcastPreparedTx(context.Background(), "AcknowledgeJob", nil, buildLegacyTx)
			require.NoError(t, err)

			waitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = pending.Wait(waitCtx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Equal(t, 0, coordinator.Inflight(ClassLegacy),
				"a failed wait must still release the broadcast slot")
		})
	}
}

// TestPendingTx_WaitIsIdempotent guards the documented contract that a
// second join returns the first result rather than re-polling the chain or
// double-releasing the coordinator slot.
func TestPendingTx_WaitIsIdempotent(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator
	backend := &stubReceiptBackend{receipt: minedReceipt()}
	client.receiptBackend = backend

	pending, err := client.broadcastPreparedTx(context.Background(), "AcknowledgeJob", nil, buildLegacyTx)
	require.NoError(t, err)

	require.NoError(t, pending.Wait(context.Background()))
	require.NoError(t, pending.Wait(context.Background()))
	assert.Equal(t, 1, backend.receiptCalls(), "the second Wait must not re-poll")
	assert.Equal(t, 0, coordinator.Inflight(ClassLegacy))
}

// TestBroadcastPreparedTx_ReleasesSlotOnPreSendFailure guards the ownership
// transfer: until a PendingTx exists there is nobody to release the slot,
// so every failure path before SendTransaction must release it itself.
func TestBroadcastPreparedTx_ReleasesSlotOnPreSendFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*ChainClient)
		build func(*bind.TransactOpts) (*types.Transaction, error)
	}{
		{
			name:  "build fails",
			setup: func(*ChainClient) {},
			build: func(*bind.TransactOpts) (*types.Transaction, error) { return nil, assert.AnError },
		},
		{
			name: "send fails",
			setup: func(c *ChainClient) {
				c.jobTxBackend.(*mockJobTxBackend).sendErr = assert.AnError
			},
			build: buildLegacyTx,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			coordinator := NewSubpoolCoordinator(nil, 0)
			defer coordinator.Close()
			client := newTestChainClient(t)
			client.coordinator = coordinator
			tt.setup(client)

			pending, err := client.broadcastPreparedTx(context.Background(), "AcknowledgeJob", nil, tt.build)
			require.Error(t, err)
			assert.Nil(t, pending)
			assert.Equal(t, 0, coordinator.Inflight(ClassLegacy),
				"a pre-send failure must not strand the broadcast slot")
		})
	}
}

// TestSubmitPreparedTx_StillBlocksUntilMined pins the unchanged contract of
// the blocking path after it was refactored on top of broadcast+Wait.
func TestSubmitPreparedTx_StillBlocksUntilMined(t *testing.T) {
	t.Parallel()

	coordinator := NewSubpoolCoordinator(nil, 0)
	defer coordinator.Close()
	client := newTestChainClient(t)
	client.coordinator = coordinator

	gate := make(chan struct{})
	client.receiptBackend = &stubReceiptBackend{gate: gate, receipt: minedReceipt()}

	done := make(chan error, 1)
	go func() {
		done <- client.submitPreparedTx(context.Background(), "AcknowledgeJob", nil, buildLegacyTx)
	}()

	select {
	case <-done:
		t.Fatal("submitPreparedTx returned before the tx mined")
	case <-time.After(150 * time.Millisecond):
	}

	close(gate)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("submitPreparedTx did not return after the receipt landed")
	}
	assert.Equal(t, 0, coordinator.Inflight(ClassLegacy))
}

package blob

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobTxSubmitter_ResetNonceOnSignFailure(t *testing.T) {
	t.Parallel()

	nonceMgr := &mockNonceManager{next: 7}
	submitter := &BlobTxSubmitter{
		txBackend:  mockBlobTxBackend{gasTipCap: big.NewInt(1)},
		signingKey: testSigningKey(t),
		signTx: func(*types.Transaction, types.Signer, *ecdsa.PrivateKey) (*types.Transaction, error) {
			return nil, assert.AnError
		},
		chainID:     big.NewInt(1),
		nonceMgr:    nonceMgr,
		maxGasPrice: big.NewInt(2),
	}

	_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sign blob tx")
	assert.Equal(t, 1, nonceMgr.resetCalls)
}

func TestBlobTxSubmitter_ResetNonceOnPreBroadcastSendFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		sendErr         error
		wantResetCalls  int
		wantErrContains string
	}{
		{
			name:            "insufficient funds rpc code resets nonce",
			sendErr:         mockRPCError{code: -38014},
			wantResetCalls:  1,
			wantErrContains: "send blob tx",
		},
		{
			name:            "plain reservation error resets nonce",
			sendErr:         errors.New("address already reserved"),
			wantResetCalls:  1,
			wantErrContains: "address already reserved",
		},
		{
			name:            "wrapped reservation error resets nonce",
			sendErr:         fmt.Errorf("eth_sendRawTransaction: %w", errors.New("address already reserved")),
			wantResetCalls:  1,
			wantErrContains: "address already reserved",
		},
		{
			name:            "unrelated rpc code does not reset nonce",
			sendErr:         mockRPCError{code: -32000},
			wantResetCalls:  0,
			wantErrContains: "send blob tx",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nonceMgr := &mockNonceManager{next: 7}
			submitter := &BlobTxSubmitter{
				txBackend:   mockBlobTxBackend{gasTipCap: big.NewInt(1), sendErr: tc.sendErr},
				signingKey:  testSigningKey(t),
				chainID:     big.NewInt(1),
				nonceMgr:    nonceMgr,
				maxGasPrice: big.NewInt(2),
			}

			_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrContains)
			assert.Equal(t, tc.wantResetCalls, nonceMgr.resetCalls)
		})
	}
}

// TestBlobTxSubmitter_SerializesBroadcast verifies that concurrent
// SubmitBlobTx invocations do not overlap in the SendTransaction..WaitMined
// window. Without the broadcastMu, go-ethereum's txpool would reject the
// second concurrent blob tx with "address already reserved".
func TestBlobTxSubmitter_SerializesBroadcast(t *testing.T) {
	t.Parallel()

	var inFlight, maxInFlight atomic.Int32
	backend := mockBlobTxBackend{
		gasTipCap:   big.NewInt(1),
		inFlight:    &inFlight,
		maxInFlight: &maxInFlight,
		sendDelay:   50 * time.Millisecond,
	}

	waitMined := func(ctx context.Context, _ bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error) {
		// Hold the critical section for a window long enough to expose any
		// concurrent broadcast if the mutex is broken.
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: tx.Hash()}, nil
	}

	submitter := &BlobTxSubmitter{
		txBackend:   backend,
		signingKey:  testSigningKey(t),
		waitMined:   waitMined,
		chainID:     big.NewInt(1),
		nonceMgr:    &mockNonceManager{next: 7},
		maxGasPrice: big.NewInt(2),
	}

	const goroutines = 2
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := submitter.SubmitBlobTx(context.Background(), []byte(fmt.Sprintf("ciphertext-%d", idx)))
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d", i)
	}
	assert.Equal(t, int32(1), maxInFlight.Load(), "expected broadcast to be serialized, max in-flight was %d", maxInFlight.Load())
}

// Compile-time assertion
var _ BlobSubmitter = (*BlobTxSubmitter)(nil)

type mockNonceManager struct {
	next       uint64
	resetCalls int
}

func (m *mockNonceManager) NextNonce(context.Context) (uint64, error) {
	return m.next, nil
}

func (m *mockNonceManager) ResetNonce() {
	m.resetCalls++
}

type mockBlobTxBackend struct {
	gasTipCap *big.Int
	sendErr   error

	// Optional instrumentation for concurrency assertions. When inFlight and
	// maxInFlight are set, SendTransaction increments inFlight on entry,
	// bumps maxInFlight via CAS to the observed peak, optionally sleeps to
	// widen the observation window, and decrements on exit.
	inFlight    *atomic.Int32
	maxInFlight *atomic.Int32
	sendDelay   time.Duration
}

func (m mockBlobTxBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return m.gasTipCap, nil
}

func (m mockBlobTxBackend) SendTransaction(ctx context.Context, _ *types.Transaction) error {
	if m.inFlight != nil {
		cur := m.inFlight.Add(1)
		defer m.inFlight.Add(-1)
		for {
			peak := m.maxInFlight.Load()
			if cur <= peak || m.maxInFlight.CompareAndSwap(peak, cur) {
				break
			}
		}
	}
	if m.sendDelay > 0 {
		select {
		case <-time.After(m.sendDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.sendErr
}

func testSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return key
}

type mockRPCError struct {
	code int
}

func (e mockRPCError) Error() string {
	return "rpc error"
}

func (e mockRPCError) ErrorCode() int {
	return e.code
}

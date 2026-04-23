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

	workerchain "github.com/lightchain/worker/internal/chain"
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

// TestBlobTxSubmitter_CtxCancelDuringSlotWait asserts that a goroutine
// waiting for the broadcast slot aborts promptly when its context is
// cancelled, and never reaches SendTransaction. Without ctx-awareness on
// the slot acquire, the queued goroutine would be stuck until the prior
// holder's WaitMined returns.
//
// Synchronization relies on the signTx hook firing after KZG/signing but
// before the slot acquire select, giving the test a deterministic moment
// at which G2 is about to block on the slot.
func TestBlobTxSubmitter_CtxCancelDuringSlotWait(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	backend := mockBlobTxBackend{
		gasTipCap: big.NewInt(1),
		sendCalls: &sendCalls,
	}

	g1InSlot := make(chan struct{})
	g1Release := make(chan struct{})
	waitMined := func(ctx context.Context, _ bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error) {
		close(g1InSlot)
		select {
		case <-g1Release:
			return &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: tx.Hash()}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// signTx hook: fires g2Signed on the 2nd call (G2's), giving us a
	// deterministic signal that G2 has finished KZG+signing and is about
	// to enter the slot acquire select.
	var signCount atomic.Int32
	g2Signed := make(chan struct{})
	signTxStub := func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error) {
		result, err := types.SignTx(tx, signer, key)
		if signCount.Add(1) == 2 {
			close(g2Signed)
		}
		return result, err
	}

	submitter := &BlobTxSubmitter{
		txBackend:   backend,
		signingKey:  testSigningKey(t),
		signTx:      signTxStub,
		waitMined:   waitMined,
		chainID:     big.NewInt(1),
		nonceMgr:    &mockNonceManager{next: 7},
		maxGasPrice: big.NewInt(2),
	}

	// G1 enters the slot and blocks in waitMined.
	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("g1-ciphertext"))
		assert.NoError(t, err, "g1 should succeed after release")
	}()

	// Wait until G1 is holding the slot (generous timeout accounts for ~9s KZG).
	select {
	case <-g1InSlot:
	case <-time.After(30 * time.Second):
		close(g1Release)
		<-g1Done
		t.Fatal("g1 did not enter waitMined within 30s")
	}

	// G2 attempts acquire with a cancellable ctx.
	ctx, cancel := context.WithCancel(context.Background())
	g2ErrCh := make(chan error, 1)
	go func() {
		_, err := submitter.SubmitBlobTx(ctx, []byte("g2-ciphertext"))
		g2ErrCh <- err
	}()

	// Wait until G2 has signed — it's now at the slot acquire select,
	// blocked because G1 still holds the slot.
	select {
	case <-g2Signed:
	case <-time.After(30 * time.Second):
		close(g1Release)
		<-g1Done
		t.Fatal("g2 did not finish signing within 30s")
	}

	// Now cancel. With the ctx-aware slot, G2 exits promptly. Without
	// the fix, G2 would block on Lock() indefinitely since G1 still holds.
	cancelAt := time.Now()
	cancel()

	select {
	case err := <-g2ErrCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(cancelAt), 500*time.Millisecond,
			"g2 should exit within 500ms of cancel — slot must be ctx-aware")
	case <-time.After(5 * time.Second):
		close(g1Release)
		<-g1Done
		t.Fatal("g2 did not return within 5s of cancel — slot acquire is not ctx-aware")
	}

	assert.Equal(t, int32(1), sendCalls.Load(),
		"g2 must not reach SendTransaction while blocked on ctx-cancelled slot wait")

	// Release G1 to let the test complete cleanly.
	close(g1Release)
	<-g1Done
}

// TestBlobTxSubmitter_CtxCancelDuringWaitMined asserts that when the ctx
// deadline expires while the holder is inside WaitMined, the holder returns
// AND the broadcast slot is released so a subsequent caller is not starved.
// Regression guard for a hanging EL connection blocking all following
// broadcasts forever.
func TestBlobTxSubmitter_CtxCancelDuringWaitMined(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	backend := mockBlobTxBackend{
		gasTipCap: big.NewInt(1),
		sendCalls: &sendCalls,
	}

	// waitMined stub that hangs until ctx.Done fires, simulating a dead EL.
	waitMined := func(ctx context.Context, _ bind.DeployBackend, _ *types.Transaction) (*types.Receipt, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	submitter := &BlobTxSubmitter{
		txBackend:   backend,
		signingKey:  testSigningKey(t),
		waitMined:   waitMined,
		chainID:     big.NewInt(1),
		nonceMgr:    &mockNonceManager{next: 7},
		maxGasPrice: big.NewInt(2),
	}

	// First call: reaches waitMined (pays ~9s KZG overhead), then hangs
	// until ctx deadline fires. We give it 15s so it has time for KZG + a
	// few seconds inside waitMined.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel1()
	_, err := submitter.SubmitBlobTx(ctx1, []byte("ciphertext-1"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait for blob tx")
	assert.Equal(t, int32(1), sendCalls.Load(), "first call should have broadcast")

	// Second call: proves the slot was released. It ALSO pays ~9s KZG,
	// reaches SendTransaction (sendCalls becomes 2), enters waitMined,
	// and hangs until its own ctx deadline. The key assertion is that
	// SendTransaction was reached at all — without slot release, the
	// second call would block forever at slot acquire.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	_, err = submitter.SubmitBlobTx(ctx2, []byte("ciphertext-2"))
	require.Error(t, err)
	assert.Equal(t, int32(2), sendCalls.Load(),
		"second call must reach SendTransaction — slot was released after first call returned")
}

// TestBlobTxSubmitter_ResetsNonceOnWaitMinedFailure asserts that when
// WaitMined returns an error after a successful broadcast, the local nonce
// counter is reset so the next NextNonce() refetches from chain. Without
// this reset, the NonceManager accumulates persistent gaps from chain
// whenever a blob tx is stuck in the mempool past the ctx deadline — the
// cascading-gap failure reproduced in the 2026-04-23 testnet incident.
func TestBlobTxSubmitter_ResetsNonceOnWaitMinedFailure(t *testing.T) {
	t.Parallel()

	nonceMgr := &mockNonceManager{next: 50}
	waitMined := func(ctx context.Context, _ bind.DeployBackend, _ *types.Transaction) (*types.Receipt, error) {
		return nil, errors.New("simulated waitMined failure")
	}

	submitter := &BlobTxSubmitter{
		txBackend:   mockBlobTxBackend{gasTipCap: big.NewInt(1)},
		signingKey:  testSigningKey(t),
		waitMined:   waitMined,
		chainID:     big.NewInt(1),
		nonceMgr:    nonceMgr,
		maxGasPrice: big.NewInt(2),
	}

	_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait for blob tx")
	assert.Equal(t, 1, nonceMgr.resetCalls,
		"ResetNonce must fire on WaitMined failure so next broadcast refetches chain pending")
}

// TestBlobTxSubmitter_DetectsStuckNonce_BelowThreshold asserts that
// repeated "address already reserved" rejections at the same nonce bump
// the shared tracker's hit count, but replacement is NOT triggered while
// below the configured threshold. Commit H is detection-only; Commit I
// wires the replacement.
func TestBlobTxSubmitter_DetectsStuckNonce_BelowThreshold(t *testing.T) {
	t.Parallel()

	tracker := workerchain.NewStuckNonceTracker()
	submitter := &BlobTxSubmitter{
		txBackend: mockBlobTxBackend{
			gasTipCap: big.NewInt(1),
			sendErr:   errors.New("eth_sendRawTransaction: address already reserved"),
		},
		signingKey:   testSigningKey(t),
		chainID:      big.NewInt(1),
		nonceMgr:     &mockNonceManager{next: 152},
		maxGasPrice:  big.NewInt(2),
		stuckTracker: tracker,
		stuckCfg:     StuckNonceConfig{Threshold: 5, MaxBumps: 3, AutoReplace: true},
	}

	// Four consecutive rejections — below threshold of 5.
	for i := 0; i < 4; i++ {
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "address already reserved")
	}

	assert.Equal(t, 4, tracker.ConsecutiveHits(),
		"4 consecutive rejections at same nonce must produce 4 tracker hits")
	n, ok := tracker.LastNonce()
	assert.True(t, ok)
	assert.Equal(t, uint64(152), n)
	assert.Equal(t, 0, tracker.BumpAttemptsUsed(),
		"no bump attempts must fire below threshold")
}

// TestBlobTxSubmitter_TriggersReplacementAtThreshold asserts that once
// consecutive "address already reserved" rejections reach the configured
// threshold, a bumped-fee replacement blob tx is broadcast at the SAME
// nonce. This is the Hazard B recovery path — without it the stuck
// nonce loop would exhaust asynq's MaxRetry with no fix applied.
func TestBlobTxSubmitter_TriggersReplacementAtThreshold(t *testing.T) {
	t.Parallel()

	var captured []*types.Transaction
	var mu sync.Mutex
	backend := &capturingBlobTxBackend{
		gasTipCap: big.NewInt(100),
		sendErr:   errors.New("eth_sendRawTransaction: address already reserved"),
		onSend: func(tx *types.Transaction) {
			mu.Lock()
			defer mu.Unlock()
			captured = append(captured, tx)
		},
	}
	tracker := workerchain.NewStuckNonceTracker()
	submitter := &BlobTxSubmitter{
		txBackend:    backend,
		signingKey:   testSigningKey(t),
		chainID:      big.NewInt(1),
		nonceMgr:     &mockNonceManager{next: 152},
		maxGasPrice:  big.NewInt(1_000_000_000), // 1 gwei baseline
		stuckTracker: tracker,
		stuckCfg:     StuckNonceConfig{Threshold: 3, MaxBumps: 3, AutoReplace: true},
	}

	// 3 consecutive rejections — hit threshold on the 3rd. Each
	// SubmitBlobTx call broadcasts once (the original); on the 3rd
	// call the threshold triggers an ADDITIONAL replacement broadcast.
	for i := 0; i < 3; i++ {
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
		require.Error(t, err)
	}

	mu.Lock()
	defer mu.Unlock()
	// 3 originals + 1 replacement = 4 total broadcasts.
	require.Len(t, captured, 4, "expected 3 original broadcasts + 1 replacement on threshold hit")

	// The replacement is the 4th capture. Verify it targets the same
	// nonce with ALL three fee fields bumped by 2× (first bump).
	replacement := captured[3]
	assert.Equal(t, uint64(152), replacement.Nonce(),
		"replacement must target the stuck nonce, not a fresh one")
	assert.Equal(t, types.BlobTxType, int(replacement.Type()),
		"replacement must be a blob tx — go-ethereum blobpool refuses non-blob replacements")
	// GasTipCap bumped from 100 (suggested) to 200 (2×).
	assert.Equal(t, "200", replacement.GasTipCap().String())
	// GasFeeCap bumped from 1 gwei to 2 gwei.
	assert.Equal(t, "2000000000", replacement.GasFeeCap().String())
	// BlobFeeCap bumped from 1 gwei baseline to 2 gwei.
	assert.Equal(t, "2000000000", replacement.BlobGasFeeCap().String())

	assert.Equal(t, 1, tracker.BumpAttemptsUsed(),
		"exactly one bump attempt must be recorded after first threshold trigger")
}

// TestBlobTxSubmitter_StopsReplacingAfterMaxBumps asserts that once
// MaxBumps replacements have been attempted, further threshold hits log
// loudly but do NOT broadcast additional replacements. Protects against
// runaway fee bumps draining the worker's balance when the pool is
// genuinely unable to accept the tx at any price (e.g. EL RPC broken).
func TestBlobTxSubmitter_StopsReplacingAfterMaxBumps(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	backend := &capturingBlobTxBackend{
		gasTipCap: big.NewInt(100),
		sendErr:   errors.New("eth_sendRawTransaction: address already reserved"),
		onSend:    func(*types.Transaction) { sendCalls.Add(1) },
	}
	tracker := workerchain.NewStuckNonceTracker()
	submitter := &BlobTxSubmitter{
		txBackend:    backend,
		signingKey:   testSigningKey(t),
		chainID:      big.NewInt(1),
		nonceMgr:     &mockNonceManager{next: 152},
		maxGasPrice:  big.NewInt(1_000_000_000),
		stuckTracker: tracker,
		stuckCfg:     StuckNonceConfig{Threshold: 2, MaxBumps: 2, AutoReplace: true},
	}

	// 4 submissions — threshold=2, so after submission 2, 3, 4 each
	// hits the threshold. MaxBumps=2 caps replacements at 2.
	for i := 0; i < 4; i++ {
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
		require.Error(t, err)
	}

	// Breakdown: 4 originals + 2 replacements = 6 total broadcasts.
	// The 3rd and 4th threshold hits do NOT produce new replacements
	// because BumpAttemptsUsed has reached MaxBumps.
	assert.Equal(t, int32(6), sendCalls.Load(),
		"expected exactly MaxBumps replacements (2), not one per threshold hit")
	assert.Equal(t, 2, tracker.BumpAttemptsUsed())
}

// TestBlobTxSubmitter_ReplacementHonorsAutoReplaceDisabled asserts the
// operator kill-switch: when AutoReplace is false, the tracker still
// records hits and the ERROR log fires, but no replacement tx is broadcast.
func TestBlobTxSubmitter_ReplacementHonorsAutoReplaceDisabled(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	backend := &capturingBlobTxBackend{
		gasTipCap: big.NewInt(100),
		sendErr:   errors.New("eth_sendRawTransaction: address already reserved"),
		onSend:    func(*types.Transaction) { sendCalls.Add(1) },
	}
	tracker := workerchain.NewStuckNonceTracker()
	submitter := &BlobTxSubmitter{
		txBackend:    backend,
		signingKey:   testSigningKey(t),
		chainID:      big.NewInt(1),
		nonceMgr:     &mockNonceManager{next: 152},
		maxGasPrice:  big.NewInt(1_000_000_000),
		stuckTracker: tracker,
		stuckCfg:     StuckNonceConfig{Threshold: 2, MaxBumps: 3, AutoReplace: false},
	}

	// 3 submissions, each rejected, threshold hit on the 2nd and 3rd.
	for i := 0; i < 3; i++ {
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
		require.Error(t, err)
	}

	// Only the 3 originals — no replacement broadcasts.
	assert.Equal(t, int32(3), sendCalls.Load(),
		"AutoReplace=false must suppress all replacement broadcasts")
	assert.Equal(t, 0, tracker.BumpAttemptsUsed(),
		"AutoReplace=false must not call IncrementBumpAttempts either")
	assert.Equal(t, 3, tracker.ConsecutiveHits(),
		"detection still happens — the tracker counts regardless of auto-replace")
}

// TestBlobTxSubmitter_ClearsOnSuccessfulMine asserts that a successful
// blob tx mining resets the stuck tracker so a subsequent rejection at a
// different nonce starts from clean state.
func TestBlobTxSubmitter_ClearsOnSuccessfulMine(t *testing.T) {
	t.Parallel()

	tracker := workerchain.NewStuckNonceTracker()
	// Preload tracker with a stale "stuck" state that predates this tx.
	tracker.Record(151)
	tracker.Record(151)
	tracker.Record(151)
	require.Equal(t, 3, tracker.ConsecutiveHits())

	submitter := &BlobTxSubmitter{
		txBackend:  mockBlobTxBackend{gasTipCap: big.NewInt(1)},
		signingKey: testSigningKey(t),
		waitMined: func(ctx context.Context, _ bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error) {
			return &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: tx.Hash()}, nil
		},
		chainID:      big.NewInt(1),
		nonceMgr:     &mockNonceManager{next: 152},
		maxGasPrice:  big.NewInt(2),
		stuckTracker: tracker,
		stuckCfg:     StuckNonceConfig{Threshold: 5, MaxBumps: 3, AutoReplace: true},
	}

	_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
	require.NoError(t, err)

	assert.Equal(t, 0, tracker.ConsecutiveHits(),
		"successful mine must clear the tracker")
	_, ok := tracker.LastNonce()
	assert.False(t, ok,
		"successful mine must clear the tracked nonce observation")
}

// TestBlobTxSubmitter_BlocksOnExternallyHeldSlot mirrors the chain-package
// test on the blob side: when the same BroadcastSerializer is held by an
// external goroutine (simulating a concurrent ChainClient.submitPreparedTx
// broadcast), SubmitBlobTx must not reach SendTransaction until released.
// Together with the chain-side test, this proves the Hazard A invariant
// end-to-end across both submitter implementations.
func TestBlobTxSubmitter_BlocksOnExternallyHeldSlot(t *testing.T) {
	t.Parallel()

	var sendCalls atomic.Int32
	serializer := workerchain.NewBroadcastSerializer()
	submitter := &BlobTxSubmitter{
		txBackend:   mockBlobTxBackend{gasTipCap: big.NewInt(1), sendCalls: &sendCalls},
		signingKey:  testSigningKey(t),
		waitMined: func(ctx context.Context, _ bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error) {
			return &types.Receipt{Status: types.ReceiptStatusSuccessful, TxHash: tx.Hash()}, nil
		},
		chainID:     big.NewInt(1),
		nonceMgr:    &mockNonceManager{next: 7},
		maxGasPrice: big.NewInt(2),
		serializer:  serializer,
	}

	// External holder simulates a concurrent ChainClient broadcast in
	// its SendTransaction..WaitMined window on the SAME serializer.
	require.NoError(t, serializer.Acquire(context.Background()))

	submitDone := make(chan error, 1)
	go func() {
		_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
		submitDone <- err
	}()

	// Wait past the ~9s KZG overhead so we know the submitter has
	// reached the slot acquire select, then verify it's blocked.
	time.Sleep(11 * time.Second)
	assert.Equal(t, int32(0), sendCalls.Load(),
		"SubmitBlobTx must not reach SendTransaction while external holder owns the shared serializer")

	// Release; SubmitBlobTx should now proceed to a successful broadcast.
	serializer.Release()

	select {
	case err := <-submitDone:
		require.NoError(t, err)
		assert.Equal(t, int32(1), sendCalls.Load(),
			"SendTransaction must fire exactly once after external holder releases")
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitBlobTx did not proceed within 5s of external release — shared slot is not actually shared")
	}
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
	// widen the observation window, and decrements on exit. sendCalls, if
	// set, counts total SendTransaction invocations — used by cancellation
	// tests to assert that a cancelled goroutine never reached broadcast.
	inFlight    *atomic.Int32
	maxInFlight *atomic.Int32
	sendDelay   time.Duration
	sendCalls   *atomic.Int32
}

func (m mockBlobTxBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return m.gasTipCap, nil
}

func (m mockBlobTxBackend) SendTransaction(ctx context.Context, _ *types.Transaction) error {
	if m.sendCalls != nil {
		m.sendCalls.Add(1)
	}
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

// capturingBlobTxBackend is a test backend that invokes onSend on every
// SendTransaction call and optionally returns a fixed error. Used by
// replacement tests to capture each broadcast tx and inspect bumped fees.
type capturingBlobTxBackend struct {
	gasTipCap *big.Int
	sendErr   error
	onSend    func(tx *types.Transaction)
}

func (m *capturingBlobTxBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return m.gasTipCap, nil
}

func (m *capturingBlobTxBackend) SendTransaction(_ context.Context, tx *types.Transaction) error {
	if m.onSend != nil {
		m.onSend(tx)
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

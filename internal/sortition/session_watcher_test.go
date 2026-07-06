package sortition

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

// testLogger returns a *slog.Logger that writes to t.Log via a custom handler.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// errAlreadyClaimed returns a revert-shaped error for the AlreadyClaimed case.
func errAlreadyClaimed() error {
	return errors.New("execution reverted: AlreadyClaimed")
}

// farFuture is a far-future unix expiry used in tests to indicate "never expires".
const farFuture = ^uint64(0)

// mockClaimClient is the test double for ClaimClient. Head returns chain.HeadInfo.
//
// FilterSessionRequested filters by block number so that range-aware tests
// (e.g. cross-pass eligibility tests) only see events within [fromBlock, toBlock].
//
// GetRequestInfo looks up requestInfos by reqID; when the key is absent it
// defaults to {Status: 0 (Open), Expiry: farFuture} so existing tests that do
// not configure requestInfos explicitly continue to work.
//
// eligible is a mutable map; tests may flip entries between RunOnce calls to
// simulate the decaying sortition threshold.
type mockClaimClient struct {
	head         chain.HeadInfo
	requested    []chain.SessionRequestedEvent
	eligible     map[uint64]bool
	requestInfos map[uint64]chain.RequestInfo
	claimed      []uint64
	claimErr     error
}

func (m *mockClaimClient) Head(_ context.Context) (chain.HeadInfo, error) {
	return m.head, nil
}

func (m *mockClaimClient) FilterSessionRequested(_ context.Context, from, to uint64) ([]chain.SessionRequestedEvent, error) {
	var result []chain.SessionRequestedEvent
	for _, ev := range m.requested {
		if ev.BlockNumber >= from && ev.BlockNumber <= to {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (m *mockClaimClient) GetRequestInfo(_ context.Context, reqID uint64) (chain.RequestInfo, error) {
	if ri, ok := m.requestInfos[reqID]; ok {
		return ri, nil
	}
	// Default: Open, far-future expiry — so tests that don't configure
	// requestInfos explicitly see an immediately-actionable request.
	return chain.RequestInfo{Status: 0, Expiry: farFuture}, nil
}

func (m *mockClaimClient) EligibleNow(_ context.Context, reqID uint64, _ common.Address) (bool, error) {
	return m.eligible[reqID], nil
}

func (m *mockClaimClient) ClaimSession(_ context.Context, reqID uint64) error {
	if m.claimErr != nil {
		return m.claimErr
	}
	m.claimed = append(m.claimed, reqID)
	return nil
}

// newSW builds a SessionWatcher backed by a temp CursorStore, with cursor at 0.
// Returns both the watcher and its CursorStore for test assertions.
func newSW(t *testing.T, mc *mockClaimClient, counter *atomic.Int32) (*SessionWatcher, *CursorStore) {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	return NewSessionWatcher(SessionWatcherOpts{
		Client:        mc,
		Cursor:        cs,
		Worker:        common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:    counter,
		MaxConcurrent: 4,
		ChunkSize:     5000,
		Confirmations: 0,
		Logger:        testLogger(t),
	}), cs
}

func TestSessionWatcher_ClaimsOnlyEligible(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head: chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{
			{ReqID: 1, BlockNumber: 10},
			{ReqID: 2, BlockNumber: 11},
			{ReqID: 3, BlockNumber: 12},
		},
		eligible: map[uint64]bool{1: true, 2: false, 3: true},
	}
	sw, _ := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.ElementsMatch(t, []uint64{1, 3}, mc.claimed, "claims only eligible requests")
}

func TestSessionWatcher_RespectsCapacity(t *testing.T) {
	var counter atomic.Int32
	counter.Store(4) // at cap (== MaxConcurrent)
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:  map[uint64]bool{1: true},
	}
	sw, _ := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "no claims while at capacity")
}

func TestSessionWatcher_AdvancesCursor(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 50},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:  map[uint64]bool{1: true},
	}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	sw := NewSessionWatcher(SessionWatcherOpts{
		Client:        mc,
		Cursor:        cs,
		Worker:        common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:    &counter,
		MaxConcurrent: 4,
		ChunkSize:     5000,
		Confirmations: 0,
		Logger:        testLogger(t),
	})

	require.NoError(t, sw.RunOnce(context.Background()))

	got, err := cs.Get(cursorSessionRequested)
	require.NoError(t, err)
	require.Equal(t, uint64(50), got, "cursor should advance to safeHead (50)")
}

func TestSessionWatcher_AlreadyClaimedIsNotFatal(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:  map[uint64]bool{1: true},
		claimErr:  errAlreadyClaimed(),
	}
	sw, cs := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()), "a losing claim does not fail the pass")
	// Assert cursor advanced despite claim failure
	got, _ := cs.Get(cursorSessionRequested)
	require.Equal(t, uint64(100), got, "cursor should advance to safeHead even after claim error")
}

func TestSessionWatcher_NoOpWhenSafeHeadAtOrBelowCursor(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 10},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 5}},
		eligible:  map[uint64]bool{1: true},
	}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	// Seed the cursor ahead of safeHead.
	require.NoError(t, cs.Set(cursorSessionRequested, 15))

	sw := NewSessionWatcher(SessionWatcherOpts{
		Client:        mc,
		Cursor:        cs,
		Worker:        common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:    &counter,
		MaxConcurrent: 4,
		ChunkSize:     5000,
		Confirmations: 0,
		Logger:        testLogger(t),
	})

	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "no claims when safeHead <= cursor")

	// Cursor must remain at 15; RunOnce must not regress it.
	got, err := cs.Get(cursorSessionRequested)
	require.NoError(t, err)
	require.Equal(t, uint64(15), got, "cursor must not regress")
}

// TestSessionWatcher_ReevaluatesUntilEligible is the primary bug repro.
//
// The decaying sortition threshold means a worker is ineligible at first sight
// of a SessionRequested event but becomes eligible later as the claim window
// widens. The watcher must hold the request in its pending set and re-evaluate
// it on subsequent passes — not drop it when first seen as ineligible.
func TestSessionWatcher_ReevaluatesUntilEligible(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 50}},
		eligible:  map[uint64]bool{1: false},
	}
	sw, _ := newSW(t, mc, &counter)

	// Pass 1: ineligible at discovery time — must NOT claim, but must track.
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "no claim on first pass when ineligible")
	require.Contains(t, sw.pending, uint64(1), "reqId 1 must be tracked in pending after first pass")

	// Become eligible and advance head so there is a new (empty) discovery range.
	mc.eligible[1] = true
	mc.head = chain.HeadInfo{Number: 110}

	// Pass 2: cursor is at 100; discovery range is [101..110] which contains no
	// events (event was at block 50). The pending set must carry reqId 1 forward
	// and the evaluation phase must claim it now that eligibility has changed.
	require.NoError(t, sw.RunOnce(context.Background()))
	require.ElementsMatch(t, []uint64{1}, mc.claimed, "must claim on second pass once eligible")
}

// TestSessionWatcher_DropsClaimedRequest verifies that a request whose on-chain
// status is already Claimed (1) is pruned from the pending set without being
// claimed again, even if the worker would be eligible for it.
func TestSessionWatcher_DropsClaimedRequest(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 50}},
		eligible:  map[uint64]bool{1: true},
		requestInfos: map[uint64]chain.RequestInfo{
			1: {Status: 1, Expiry: farFuture}, // Claimed by another worker
		},
	}
	sw, _ := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "no claim when status is already Claimed")
	require.NotContains(t, sw.pending, uint64(1), "claimed request must be pruned from pending")
}

// TestSessionWatcher_DropsExpiredRequest verifies that a request whose expiry
// timestamp is in the past is pruned from the pending set without a claim attempt.
func TestSessionWatcher_DropsExpiredRequest(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 50}},
		eligible:  map[uint64]bool{1: true},
		requestInfos: map[uint64]chain.RequestInfo{
			1: {Status: 0, Expiry: 1}, // Open but expiry at unix epoch+1 (far past)
		},
	}
	sw, _ := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "no claim for expired request")
	require.NotContains(t, sw.pending, uint64(1), "expired request must be pruned from pending")
}

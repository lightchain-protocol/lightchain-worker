package sortition

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
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
	head           chain.HeadInfo
	requested      []chain.SessionRequestedEvent
	eligible       map[uint64]bool
	requestInfos   map[uint64]chain.RequestInfo
	claimed        []uint64
	claimErr       error
	requiredCaps   map[uint64]*big.Int // absent key → 0 (unconstrained)
	capsErr        error               // forces GetRequiredCapabilities to fail
	capsReads      int                 // counts GetRequiredCapabilities calls
	filterRanges   [][2]uint64         // every [from, to] FilterSessionRequested was asked for
	filterErr      error               // returned for ranges starting at or below filterErrBelow
	filterErrBelow uint64
}

func (m *mockClaimClient) GetRequiredCapabilities(_ context.Context, reqID uint64) (*big.Int, error) {
	m.capsReads++
	if m.capsErr != nil {
		return nil, m.capsErr
	}
	if c, ok := m.requiredCaps[reqID]; ok {
		return c, nil
	}
	return big.NewInt(0), nil
}

func (m *mockClaimClient) Head(_ context.Context) (chain.HeadInfo, error) {
	return m.head, nil
}

func (m *mockClaimClient) FilterSessionRequested(_ context.Context, from, to uint64) ([]chain.SessionRequestedEvent, error) {
	m.filterRanges = append(m.filterRanges, [2]uint64{from, to})
	if m.filterErr != nil && from <= m.filterErrBelow {
		return nil, m.filterErr
	}
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

// ──────────────────────────────────────────────
// Capability-aware claiming
// ──────────────────────────────────────────────

// newSWWithCaps mirrors newSW but sets the worker's own capability mask.
func newSWWithCaps(t *testing.T, mc *mockClaimClient, counter *atomic.Int32, ownCaps *big.Int) *SessionWatcher {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	return NewSessionWatcher(SessionWatcherOpts{
		Client:          mc,
		Cursor:          cs,
		Worker:          common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:      counter,
		MaxConcurrent:   4,
		ChunkSize:       5000,
		Confirmations:   0,
		Logger:          testLogger(t),
		OwnCapabilities: ownCaps,
	})
}

func TestSessionWatcher_SkipsRequestRequiringMissingCapability(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:         chain.HeadInfo{Number: 100},
		requested:    []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:     map[uint64]bool{1: true},
		requiredCaps: map[uint64]*big.Int{1: big.NewInt(1)},
	}
	sw := newSWWithCaps(t, mc, &counter, big.NewInt(0)) // worker has no capabilities
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "must not claim a request whose capabilities it lacks")

	// The request is pruned from pending — a second pass does not re-evaluate it.
	reads := mc.capsReads
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed)
	require.Equal(t, reads, mc.capsReads, "pruned request must not be re-fetched")
}

func TestSessionWatcher_ClaimsCoveredCapabilityRequest(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:         chain.HeadInfo{Number: 100},
		requested:    []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:     map[uint64]bool{1: true},
		requiredCaps: map[uint64]*big.Int{1: big.NewInt(1)},
	}
	sw := newSWWithCaps(t, mc, &counter, big.NewInt(1)) // worker declares the bit
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed, "covered request must be claimed")
}

func TestSessionWatcher_CapabilityReadFailure_FailsOpen(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:  map[uint64]bool{1: true},
		capsErr:   errors.New("rpc down"),
	}
	sw := newSWWithCaps(t, mc, &counter, big.NewInt(0))
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed,
		"capability read failure must fail open — the on-chain claim check is the guarantee")
}

func TestSessionWatcher_UnconstrainedRequest_ClaimedWithoutCapabilities(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:  map[uint64]bool{1: true},
	}
	sw := newSWWithCaps(t, mc, &counter, big.NewInt(0))
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed, "mask-0 requests remain claimable by any worker")
}

func TestSessionWatcher_CapabilityMaskCachedAcrossPasses(t *testing.T) {
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:         chain.HeadInfo{Number: 100},
		requested:    []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 10}},
		eligible:     map[uint64]bool{1: false}, // not yet sortition-eligible — stays pending
		requiredCaps: map[uint64]*big.Int{1: big.NewInt(1)},
	}
	sw := newSWWithCaps(t, mc, &counter, big.NewInt(1))
	require.NoError(t, sw.RunOnce(context.Background()))
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, 1, mc.capsReads, "immutable mask is fetched once and cached in pending")

	// When the window widens, the cached mask is used and the claim lands.
	mc.eligible[1] = true
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed)
	require.Equal(t, 1, mc.capsReads)
}

// ──────────────────────────────────────────────
// Restart survival (pending set is memory-only, cursor is not)
// ──────────────────────────────────────────────

// newSWWithCursor mirrors newSW but uses the caller's CursorStore, so two
// watchers can share one on-disk cursor the way a restarted process would, and
// sets the look-back the restarted watcher applies behind that cursor.
func newSWWithCursor(t *testing.T, mc *mockClaimClient, counter *atomic.Int32, cs *CursorStore, lookback uint64) *SessionWatcher {
	t.Helper()
	return NewSessionWatcher(SessionWatcherOpts{
		Client:         mc,
		Cursor:         cs,
		Worker:         common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:     counter,
		MaxConcurrent:  4,
		ChunkSize:      5000,
		Confirmations:  0,
		Logger:         testLogger(t),
		LookbackBlocks: lookback,
	})
}

// restartedWatcher runs one pass with a first watcher (ineligible, so the
// request at block 50 stays pending), then builds a second watcher on the same
// cursor directory — the restarted process — with the given look-back.
func restartedWatcher(t *testing.T, lookback uint64) (*mockClaimClient, *SessionWatcher, *CursorStore) {
	t.Helper()
	var counter atomic.Int32
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 50}},
		eligible:  map[uint64]bool{1: false},
	}
	dir := t.TempDir()
	cs1, err := NewCursorStore(dir)
	require.NoError(t, err)
	first := newSWWithCursor(t, mc, &counter, cs1, lookback)
	require.NoError(t, first.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "ineligible at discovery: no claim")
	require.Contains(t, first.pending, uint64(1))

	cs2, err := NewCursorStore(dir) // same cursor file, already at 100
	require.NoError(t, err)
	mc.eligible[1] = true
	mc.head = chain.HeadInfo{Number: 110}
	return mc, newSWWithCursor(t, mc, &counter, cs2, lookback), cs2
}

// TestSessionWatcher_ReseedsPendingAfterRestart: a request discovered while the
// worker was still ineligible must survive a process restart. The cursor is
// persisted but the pending set is not, so a fresh watcher on the same cursor
// store must look behind the cursor once and pick the still-Open request up.
func TestSessionWatcher_ReseedsPendingAfterRestart(t *testing.T) {
	mc, second, cs := restartedWatcher(t, 60) // looks back over [41..100], which covers block 50
	require.NoError(t, second.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed, "restarted watcher must still compete for the request")
	require.Equal(t, [][2]uint64{{1, 100}, {41, 100}, {101, 110}}, mc.filterRanges,
		"first watcher's discovery, then the restarted one's look-back and its discovery")

	got, err := cs.Get(cursorSessionRequested)
	require.NoError(t, err)
	require.Equal(t, uint64(110), got, "looking back must not regress the cursor")
}

// The look-back is a bound, not a full rescan: a request older than it is not
// reseeded (it is expired or claimed by then in practice), and zero disables it.
func TestSessionWatcher_LookbackIsBounded(t *testing.T) {
	mc, second, _ := restartedWatcher(t, 10) // [91..100] does not reach block 50
	require.NoError(t, second.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "request behind the look-back window is not reseeded")

	mc, second, _ = restartedWatcher(t, 0)
	require.NoError(t, second.RunOnce(context.Background()))
	require.Empty(t, mc.claimed, "zero look-back keeps the old cursor-only behaviour")
}

// The look-back runs on the first pass only; later passes scan forward from the
// cursor as before.
func TestSessionWatcher_LookbackRunsOnce(t *testing.T) {
	mc, second, cs := restartedWatcher(t, 60)
	require.NoError(t, second.RunOnce(context.Background()))
	scans := len(mc.filterRanges)

	// Head unchanged → safeHead == cursor → no discovery range; any filter call
	// now would be a repeated look-back.
	require.NoError(t, second.RunOnce(context.Background()))
	require.Equal(t, scans, len(mc.filterRanges), "no second look-back behind the cursor")
	got, err := cs.Get(cursorSessionRequested)
	require.NoError(t, err)
	require.Equal(t, uint64(110), got, "a pass with no discovery leaves the cursor alone")
}

// A failing look-back must not wedge the pass: discovery and evaluation still
// run (the pre-look-back behaviour) and the look-back is retried next pass.
func TestSessionWatcher_LookbackFailureDoesNotWedgeThePass(t *testing.T) {
	mc, second, cs := restartedWatcher(t, 70)
	mc.filterErr, mc.filterErrBelow = errors.New("history pruned"), 100 // look-back ranges fail, [101..] is fine
	require.NoError(t, second.RunOnce(context.Background()), "the pass degrades instead of failing")
	got, err := cs.Get(cursorSessionRequested)
	require.NoError(t, err)
	require.Equal(t, uint64(110), got, "discovery still ran")
	require.Empty(t, mc.claimed)

	mc.filterErr = nil
	require.NoError(t, second.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed, "look-back retried once the filter recovers: [41..110] covers block 50")
}

// A look-back that keeps failing is abandoned after reseedMaxAttempts, so a
// pruned-history RPC does not cost a filter call and a warning on every pass forever.
func TestSessionWatcher_LookbackGivesUpAfterMaxAttempts(t *testing.T) {
	mc, second, _ := restartedWatcher(t, 60)
	mc.filterRanges = nil
	mc.filterErr, mc.filterErrBelow = errors.New("history pruned"), 100
	for i := 0; i < reseedMaxAttempts+2; i++ {
		require.NoError(t, second.RunOnce(context.Background()))
	}
	attempts := 0
	for _, r := range mc.filterRanges {
		if r[0] <= 100 { // look-back ranges start at or below the old cursor; discovery starts at 101
			attempts++
		}
	}
	require.Equal(t, reseedMaxAttempts, attempts, "look-back attempted exactly reseedMaxAttempts times")
}

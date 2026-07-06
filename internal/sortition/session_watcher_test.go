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

// mockClaimClient is the test double for ClaimClient. Head returns chain.HeadInfo.
type mockClaimClient struct {
	head      chain.HeadInfo
	requested []chain.SessionRequestedEvent
	eligible  map[uint64]bool
	claimed   []uint64
	claimErr  error
}

func (m *mockClaimClient) Head(_ context.Context) (chain.HeadInfo, error) {
	return m.head, nil
}

func (m *mockClaimClient) FilterSessionRequested(_ context.Context, _, _ uint64) ([]chain.SessionRequestedEvent, error) {
	return m.requested, nil
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
func newSW(t *testing.T, mc *mockClaimClient, counter *atomic.Int32) *SessionWatcher {
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
	})
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
	sw := newSW(t, mc, &counter)
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
	sw := newSW(t, mc, &counter)
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
	sw := newSW(t, mc, &counter)
	require.NoError(t, sw.RunOnce(context.Background()), "a losing claim does not fail the pass")
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

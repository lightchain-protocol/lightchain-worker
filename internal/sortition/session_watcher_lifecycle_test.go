package sortition

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

var (
	me    = common.HexToAddress("0x0000000000000000000000000000000000000001")
	other = common.HexToAddress("0x0000000000000000000000000000000000000002")
)

// lifecycleClient is a mockClaimClient on a SessionManager with chosen workers
// and claim slots.
type lifecycleClient struct {
	*mockClaimClient
	chosenBy    map[uint64]common.Address
	chosenReads int
	declined    []uint64
	released    []uint64
	declineErr  error
	// fullOnChain is what AtCapacity answers: in-flight jobs plus unadopted
	// claims at the chain's limit, whatever the local job counter says.
	fullOnChain   bool
	capacityReads int
	releaseErr    error
	releaseTries  int
}

func (m *lifecycleClient) AtCapacity(_ context.Context, _ common.Address) (bool, error) {
	m.capacityReads++
	return m.fullOnChain, nil
}

func (m *lifecycleClient) GetChosenWorker(_ context.Context, reqID uint64) (common.Address, error) {
	m.chosenReads++
	return m.chosenBy[reqID], nil
}

func (m *lifecycleClient) DeclineSession(_ context.Context, reqID uint64) error {
	if m.declineErr != nil {
		return m.declineErr
	}
	m.declined = append(m.declined, reqID)
	return nil
}

func (m *lifecycleClient) ReleaseClaim(_ context.Context, reqID uint64) error {
	m.releaseTries++
	if m.releaseErr != nil {
		return m.releaseErr
	}
	m.released = append(m.released, reqID)
	// The claim is closed on chain.
	m.requestInfos[reqID] = chain.RequestInfo{Status: 3, Expiry: farFuture, Worker: me}
	return nil
}

func newLifecycleClient(ids ...uint64) *lifecycleClient {
	reqs := make([]chain.SessionRequestedEvent, 0, len(ids))
	eligible := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		reqs = append(reqs, chain.SessionRequestedEvent{ReqID: id, BlockNumber: 50})
		eligible[id] = true
	}
	return &lifecycleClient{
		mockClaimClient: &mockClaimClient{
			head: chain.HeadInfo{Number: 100}, requested: reqs, eligible: eligible,
			requestInfos: map[uint64]chain.RequestInfo{},
		},
		chosenBy: map[uint64]common.Address{},
	}
}

func newLifecycleSW(t *testing.T, mc ClaimClient, counter *atomic.Int32, adoptTimeout uint64, decline bool) *SessionWatcher {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, cs.Set(cursorSessionRequested, 0)) // scan from the first block
	return NewSessionWatcher(SessionWatcherOpts{
		Client:             mc,
		Cursor:             cs,
		Worker:             me,
		JobCounter:         counter,
		MaxConcurrent:      2,
		Logger:             testLogger(t),
		AdoptTimeoutBlocks: adoptTimeout,
		DeclineWhenFull:    decline,
	})
}

// At capacity, a request that named this worker is declined (the requester is
// re-routed at once); a plain request and one naming another worker stay
// pending for when a slot frees up.
func TestSessionWatcher_AtCapacity_DeclinesOnlyRequestsThatNamedIt(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	counter.Store(2) // full
	mc := newLifecycleClient(1, 2, 3)
	mc.chosenBy[1] = me
	mc.chosenBy[2] = other
	sw := newLifecycleSW(t, mc, &counter, 30, true)

	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.declined)
	require.Empty(t, mc.claimed)
	require.NotContains(t, sw.pending, uint64(1))
	require.Contains(t, sw.pending, uint64(2))
	require.Contains(t, sw.pending, uint64(3))

	// The chosen worker is read once per request, not every pass.
	reads := mc.chosenReads
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, reads, mc.chosenReads)

	// A slot frees: the pending requests are claimed again as before.
	counter.Store(0)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.ElementsMatch(t, []uint64{2, 3}, mc.claimed)
}

func TestSessionWatcher_AtCapacity_DeclineOff_KeepsWaiting(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	counter.Store(2)
	mc := newLifecycleClient(1)
	mc.chosenBy[1] = me
	sw := newLifecycleSW(t, mc, &counter, 30, false)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.declined)
	require.Contains(t, sw.pending, uint64(1))
}

// A failed decline keeps the request pending, so the next pass tries again.
func TestSessionWatcher_DeclineFailure_Retried(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	counter.Store(2)
	mc := newLifecycleClient(1)
	mc.chosenBy[1] = me
	mc.declineErr = errors.New("rpc down")
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Contains(t, sw.pending, uint64(1))
	mc.declineErr = nil
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.declined)
}

// A claim the requester never adopts is released after AdoptTimeoutBlocks,
// freeing its on-chain slot; an adopted claim is just forgotten.
func TestSessionWatcher_ReleasesClaimsNeverAdopted(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1, 2)
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.ElementsMatch(t, []uint64{1, 2}, mc.claimed)
	require.Len(t, sw.claims, 2)

	// Both claimed by us; request 2 gets adopted (Ready).
	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}
	mc.requestInfos[2] = chain.RequestInfo{Status: 2, Expiry: farFuture, Worker: me}

	// Before the timeout nothing is released.
	mc.head = chain.HeadInfo{Number: 129}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.released)

	mc.head = chain.HeadInfo{Number: 130}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.released)
	require.Empty(t, sw.claims, "released and adopted claims are both forgotten")

	// Nothing is released twice.
	mc.head = chain.HeadInfo{Number: 200}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.released)
}

func TestSessionWatcher_AdoptTimeoutZero_NeverReleases(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1)
	sw := newLifecycleSW(t, mc, &counter, 0, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}
	mc.head = chain.HeadInfo{Number: 10_000}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.released)
}

// lifecycleTermsClient also reads request terms.
type lifecycleTermsClient struct {
	*lifecycleClient
	terms map[uint64]chain.RequestTerms
	quote *big.Int
	fee   *big.Int
}

func (m *lifecycleTermsClient) GetRequestTerms(_ context.Context, reqID uint64) (chain.RequestTerms, error) {
	return m.terms[reqID], nil
}

func (m *lifecycleTermsClient) QuoteWorkerFee(_ context.Context, worker common.Address, _ [32]byte) (*big.Int, error) {
	if worker == (common.Address{}) {
		return m.fee, nil
	}
	return m.quote, nil
}

// A request reserved for another worker with no fallback is dropped at once
// (never claimable by us); one with a fallback stays pending.
func TestSessionWatcher_SkipsRequestsReservedForAnotherWorker(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	lc := newLifecycleClient(1, 2)
	lc.eligible[1], lc.eligible[2] = false, false
	mc := &lifecycleTermsClient{
		lifecycleClient: lc,
		terms: map[uint64]chain.RequestTerms{
			1: {MaxFee: big.NewInt(0), Worker: other, FallbackBlocks: 0},
			2: {MaxFee: big.NewInt(0), Worker: other, FallbackBlocks: 5},
		},
		quote: big.NewInt(500), fee: big.NewInt(1000),
	}
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.NotContains(t, sw.pending, uint64(1))
	require.Contains(t, sw.pending, uint64(2))
	require.Empty(t, lc.declined, "requests that named another worker are never declined by us")
}

// A request that named this worker but can never be served by it (its cap is
// below our price) is declined, so the requester does not wait.
func TestSessionWatcher_DeclinesNamedRequestItCanNeverServe(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	lc := newLifecycleClient(1)
	lc.eligible[1] = false
	lc.chosenBy[1] = me
	mc := &lifecycleTermsClient{
		lifecycleClient: lc,
		terms:           map[uint64]chain.RequestTerms{1: {MaxFee: big.NewInt(600), Worker: me, FallbackBlocks: 5}},
		quote:           big.NewInt(700), fee: big.NewInt(1000),
	}
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, lc.declined)
	require.NotContains(t, sw.pending, uint64(1))
}

// Without the lifecycle surface (an older SessionManager) the watcher behaves
// exactly as before: no reads of chosen workers, no declines, no releases.
func TestSessionWatcher_WithoutLifecycleClient_Unchanged(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	counter.Store(2)
	mc := &mockClaimClient{
		head:      chain.HeadInfo{Number: 100},
		requested: []chain.SessionRequestedEvent{{ReqID: 1, BlockNumber: 50}},
		eligible:  map[uint64]bool{1: true},
	}
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.claimed)
	require.Contains(t, sw.pending, uint64(1))
	counter.Store(0)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed)
}

// Full on chain (unadopted claims hold slots) but not locally: a request that
// named this worker is still declined, and a plain one waits as before. The
// chain's capacity is read at most once per pass.
func TestSessionWatcher_FullOnChain_DeclinesRequestsThatNamedIt(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32 // locally free
	mc := newLifecycleClient(1, 2, 3)
	mc.eligible[1], mc.eligible[2], mc.eligible[3] = false, false, false // eligibleNow is false while full
	mc.chosenBy[1], mc.chosenBy[2] = me, me
	mc.fullOnChain = true
	sw := newLifecycleSW(t, mc, &counter, 30, true)

	require.NoError(t, sw.RunOnce(context.Background()))
	require.ElementsMatch(t, []uint64{1, 2}, mc.declined)
	require.Contains(t, sw.pending, uint64(3))
	require.Equal(t, 1, mc.capacityReads)
}

// A claim whose call failed (a receipt wait that timed out) may still have
// landed: it is tracked like any other, and released if it did land.
func TestSessionWatcher_ReleasesClaimWhoseCallFailed(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1)
	mc.claimErr = errors.New("receipt wait timed out")
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))

	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}
	mc.head = chain.HeadInfo{Number: 130}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.released)
}

// A restarted worker finds the claims it made before the restart among the
// requests it looks back over, and releases them once they time out.
func TestSessionWatcher_ReleasesClaimsMadeBeforeARestart(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1, 2)
	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}    // ours, not adopted
	mc.requestInfos[2] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: other} // someone else's
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Empty(t, mc.released, "the timeout runs from when the claim is found")

	mc.head = chain.HeadInfo{Number: 130}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.released)
}

// A release that keeps failing (a SessionManager without releaseClaim, or a
// claim another party freed) is given up after a few passes instead of being
// retried forever.
func TestSessionWatcher_GivesUpAReleaseThatKeepsFailing(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1)
	mc.releaseErr = errors.New("execution reverted")
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	require.NoError(t, sw.RunOnce(context.Background()))
	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}

	for head := uint64(130); head < 140; head++ {
		mc.head = chain.HeadInfo{Number: head}
		require.NoError(t, sw.RunOnce(context.Background()))
	}
	require.Equal(t, releaseMaxAttempts, mc.releaseTries)
	require.Empty(t, sw.claims)
}

func TestSessionWatcher_KeepsRetryingAReleaseThatFailsTransiently(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{
		"rpc down":        errors.New("suggest gas price: dial tcp: connection refused"),
		"contract paused": fmt.Errorf("ReleaseClaim transaction reverted (status 0, tx 0x01): %w", chain.ErrContractPaused),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var counter atomic.Int32
			mc := newLifecycleClient(1)
			mc.releaseErr = failure
			sw := newLifecycleSW(t, mc, &counter, 30, true)
			require.NoError(t, sw.RunOnce(context.Background()))
			mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}

			for head := uint64(130); head < 140; head++ {
				mc.head = chain.HeadInfo{Number: head}
				require.NoError(t, sw.RunOnce(context.Background()))
			}
			require.Greater(t, mc.releaseTries, releaseMaxAttempts)
			require.Contains(t, sw.claims, uint64(1))
		})
	}
}

// A claim made before a restart is found and released even when its request
// is older than MaxRequestAgeBlocks: the age check only stops new claims.
func TestSessionWatcher_ReleasesOldClaimsMadeBeforeARestart(t *testing.T) {
	t.Parallel()
	var counter atomic.Int32
	mc := newLifecycleClient(1)
	mc.requested[0].RequestBlock = 50
	mc.head = chain.HeadInfo{Number: 1000}
	mc.requestInfos[1] = chain.RequestInfo{Status: 1, Expiry: farFuture, Worker: me}
	sw := newLifecycleSW(t, mc, &counter, 30, true)
	sw.maxAge = 150
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Contains(t, sw.claims, uint64(1))

	mc.head = chain.HeadInfo{Number: 1030}
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.released)
}

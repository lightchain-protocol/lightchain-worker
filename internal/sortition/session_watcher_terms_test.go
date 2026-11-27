package sortition

import (
	"bytes"
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

// termsClaimClient is a mockClaimClient that also reads request terms.
type termsClaimClient struct {
	*mockClaimClient
	terms      map[uint64]chain.RequestTerms
	termsErr   error
	termsReads int
	quote      *big.Int // this worker's quote
	fee        *big.Int // the model's fee (the zero address's quote)
}

func (m *termsClaimClient) GetRequestTerms(_ context.Context, reqID uint64) (chain.RequestTerms, error) {
	m.termsReads++
	if m.termsErr != nil {
		return chain.RequestTerms{}, m.termsErr
	}
	return m.terms[reqID], nil
}

func (m *termsClaimClient) QuoteWorkerFee(_ context.Context, worker common.Address, _ [32]byte) (*big.Int, error) {
	if worker == (common.Address{}) {
		return m.fee, nil
	}
	return m.quote, nil
}

func newTermsSW(t *testing.T, mc *termsClaimClient, ownCaps int64) (*SessionWatcher, *bytes.Buffer) {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, cs.Set(cursorSessionRequested, 0)) // scan from the first block
	var logs bytes.Buffer
	var counter atomic.Int32
	return NewSessionWatcher(SessionWatcherOpts{
		Client:          mc,
		Cursor:          cs,
		Worker:          common.HexToAddress("0x0000000000000000000000000000000000000001"),
		JobCounter:      &counter,
		MaxConcurrent:   4,
		Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
		OwnCapabilities: big.NewInt(ownCaps),
	}), &logs
}

func pricedClient(terms map[uint64]chain.RequestTerms, quote, fee int64) *termsClaimClient {
	reqs := make([]chain.SessionRequestedEvent, 0, len(terms))
	eligible := make(map[uint64]bool, len(terms))
	for id := range terms {
		reqs = append(reqs, chain.SessionRequestedEvent{ReqID: id, BlockNumber: 50})
		eligible[id] = false // eligibleNow is false for every reason alike
	}
	return &termsClaimClient{
		mockClaimClient: &mockClaimClient{head: chain.HeadInfo{Number: 100}, requested: reqs, eligible: eligible},
		terms:           terms,
		quote:           big.NewInt(quote),
		fee:             big.NewInt(fee),
	}
}

func TestSessionWatcher_SaysWhyItNeverClaims(t *testing.T) {
	t.Parallel()
	const apiInference = 4
	mc := pricedClient(map[uint64]chain.RequestTerms{
		1: {MaxFee: big.NewInt(600)},                        // cap below the 700 quote
		2: {MaxFee: big.NewInt(0)},                          // no cap: the 1000 fee, above the quote
		3: {Excluded: big.NewInt(apiInference)},             // excludes a declared capability
		4: {MaxFee: big.NewInt(700)},                        // cap equal to the quote
		5: {Excluded: big.NewInt(1), MaxFee: big.NewInt(0)}, // excludes a capability not declared
	}, 700, 1000)
	sw, logs := newTermsSW(t, mc, 2|apiInference)

	require.NoError(t, sw.RunOnce(context.Background()))

	out := logs.String()
	require.Contains(t, out, `msg="skipping session request: its fee cap 600 wei is below this worker's price 700 wei" reqId=1`)
	require.Contains(t, out, `msg="skipping session request: it excludes capabilities this worker declares (mask 4)" reqId=3`)
	require.NotContains(t, out, "reqId=2")
	require.NotContains(t, out, "reqId=4")
	require.NotContains(t, out, "reqId=5")
	require.NotContains(t, sw.pending, uint64(1), "a request it can never claim is dropped")
	require.NotContains(t, sw.pending, uint64(3))
	for _, id := range []uint64{2, 4, 5} {
		require.Contains(t, sw.pending, id, "request %d waits for the draw", id)
	}

	// The terms are read once per request, not on every pass.
	reads := mc.termsReads
	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, reads, mc.termsReads)
}

func TestSessionWatcher_TermsReadFailureFailsOpen(t *testing.T) {
	t.Parallel()
	mc := pricedClient(map[uint64]chain.RequestTerms{1: {}}, 700, 1000)
	mc.termsErr = errors.New("execution reverted") // a SessionManager without terms
	mc.eligible[1] = true
	sw, _ := newTermsSW(t, mc, 0)

	require.NoError(t, sw.RunOnce(context.Background()))
	require.Equal(t, []uint64{1}, mc.claimed, "eligibleNow and the claim still decide")
	require.Equal(t, 1, mc.termsReads)
}

package chain

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockNonceFetcher is a test double for NonceFetcher.
type mockNonceFetcher struct {
	nonce     uint64
	callCount int
	err       error
}

func (m *mockNonceFetcher) PendingNonceAt(_ context.Context, _ common.Address) (uint64, error) {
	m.callCount++
	if m.err != nil {
		return 0, m.err
	}
	return m.nonce, nil
}

var testAddr = common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

func TestNonceManager_FirstCallFetchesFromChain(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 42}
	nm := NewNonceManager(fetcher, testAddr)

	nonce, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(42), nonce)
	assert.Equal(t, 1, fetcher.callCount, "should fetch from chain on first call")
}

func TestNonceManager_SecondCallIncrements(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 10}
	nm := NewNonceManager(fetcher, testAddr)

	n1, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(10), n1)

	n2, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(11), n2)

	n3, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(12), n3)

	assert.Equal(t, 1, fetcher.callCount, "should fetch from chain only once")
}

func TestNonceManager_ResetForcesFetch(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 5}
	nm := NewNonceManager(fetcher, testAddr)

	n1, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(5), n1)

	// Simulate chain advancing — reset and update mock
	// The lease has to be handed back first: a reset is deliberately deferred
	// while any nonce is still outstanding.
	nm.ReleaseNonce(n1, true)
	fetcher.nonce = 20
	nm.ResetNonce()

	n2, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(20), n2)
	assert.Equal(t, 2, fetcher.callCount, "should fetch from chain again after reset")
}

// TestNonceManager_ResetDeferredWhileLeaseOutstanding covers the production
// failure this lease accounting exists for: a reset issued while a sibling
// goroutine holds an allocated-but-unsent nonce used to re-seed the counter
// from PendingNonceAt, which cannot see that nonce. The counter landed behind
// the holder and every subsequent send failed with "nonce too low".
func TestNonceManager_ResetDeferredWhileLeaseOutstanding(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 60}
	nm := NewNonceManager(fetcher, testAddr)

	held, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(60), held)

	// A peer fails to build its tx and asks for a reset while 60 is in flight.
	nm.ResetNonce()

	// The next allocation must continue the sequence, not re-seed behind it.
	n2, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(61), n2, "reset must not re-seed underneath a live lease")
	assert.Equal(t, 1, fetcher.callCount, "no refetch while a lease is outstanding")

	// Once every lease drains, the deferred reset applies.
	nm.ReleaseNonce(held, true)
	nm.ReleaseNonce(n2, true)
	fetcher.nonce = 62
	n3, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(62), n3, "deferred reset applies once leases drain")
	assert.Equal(t, 2, fetcher.callCount)
}

// TestNonceManager_UnusedTopNonceIsReturned keeps the sequence contiguous when
// a build fails: the nonce never reached the network, so it is handed back
// rather than left as a hole the chain would stall behind.
func TestNonceManager_UnusedTopNonceIsReturned(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 7}
	nm := NewNonceManager(fetcher, testAddr)

	n1, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	nm.ReleaseNonce(n1, false)

	n2, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, n1, n2, "unused top nonce should be reissued")
	assert.Equal(t, 1, fetcher.callCount, "no chain refetch needed to reuse it")
}

func TestNonceManager_FetchError(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{err: fmt.Errorf("RPC unavailable")}
	nm := NewNonceManager(fetcher, testAddr)

	_, err := nm.NextNonce(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RPC unavailable")
}

func TestNonceManager_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	fetcher := &mockNonceFetcher{nonce: 100}
	nm := NewNonceManager(fetcher, testAddr)

	const goroutines = 50
	results := make([]uint64, goroutines)
	errs := make([]error, goroutines)
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			nonce, err := nm.NextNonce(context.Background())
			errs[idx] = err
			results[idx] = nonce
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
	}

	// All nonces must be unique and in range [100, 150)
	seen := make(map[uint64]bool, goroutines)
	for _, n := range results {
		assert.False(t, seen[n], "duplicate nonce %d", n)
		seen[n] = true
		assert.GreaterOrEqual(t, n, uint64(100))
		assert.Less(t, n, uint64(100+goroutines))
	}
}

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
	fetcher.nonce = 20
	nm.ResetNonce()

	n2, err := nm.NextNonce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(20), n2)
	assert.Equal(t, 2, fetcher.callCount, "should fetch from chain again after reset")
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

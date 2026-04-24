package chain

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStuckNonceTracker_SameNonceIncrements(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	assert.Equal(t, 1, tr.Record(152))
	assert.Equal(t, 2, tr.Record(152))
	assert.Equal(t, 3, tr.Record(152))
	assert.Equal(t, 3, tr.ConsecutiveHits())
	assert.Equal(t, 3, tr.HitsAt(152))

	n, ok := tr.LastNonce()
	assert.True(t, ok)
	assert.Equal(t, uint64(152), n)
}

func TestStuckNonceTracker_DifferentNoncesAccumulateIndependently(t *testing.T) {
	t.Parallel()

	// Critical regression: under parallel broadcasts, Record(K) and
	// Record(K+1) MUST NOT reset each other's counters. The old single-
	// slot design did exactly that, causing the bump threshold to never
	// trigger under concurrency.
	tr := NewStuckNonceTracker()
	tr.Record(152)
	tr.Record(152)
	tr.Record(152)

	tr.Record(153)
	tr.Record(153)

	assert.Equal(t, 3, tr.HitsAt(152),
		"records at 153 must not clobber hits at 152")
	assert.Equal(t, 2, tr.HitsAt(153))
	assert.Equal(t, 3, tr.ConsecutiveHits(),
		"ConsecutiveHits returns the max across tracked nonces")
}

func TestStuckNonceTracker_BumpCounterResetsOnNonceChange(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	tr.IncrementBumpAttemptsFor(152)
	tr.IncrementBumpAttemptsFor(152)
	assert.Equal(t, 2, tr.BumpAttemptsUsed())

	// Bumping at a new nonce resets — replacement at 152 makes no sense
	// if we're now bumping 153.
	tr.IncrementBumpAttemptsFor(153)
	assert.Equal(t, 1, tr.BumpAttemptsUsed())
}

func TestStuckNonceTracker_LegacyIncrementBumpAttempts(t *testing.T) {
	t.Parallel()

	// The legacy nonce-less IncrementBumpAttempts is preserved for the
	// existing caller (blob_tx.go:521). It increments without resetting.
	tr := NewStuckNonceTracker()
	tr.IncrementBumpAttempts()
	tr.IncrementBumpAttempts()
	tr.IncrementBumpAttempts()
	assert.Equal(t, 3, tr.BumpAttemptsUsed())
}

func TestStuckNonceTracker_Clear(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	tr.Record(152)
	tr.Record(152)
	tr.Record(153)
	tr.IncrementBumpAttempts()

	tr.Clear()

	assert.Equal(t, 0, tr.ConsecutiveHits())
	assert.Equal(t, 0, tr.BumpAttemptsUsed())
	assert.Equal(t, 0, tr.Size())
	_, ok := tr.LastNonce()
	assert.False(t, ok, "LastNonce must report hasObserved=false after Clear")
}

func TestStuckNonceTracker_StaleEntriesEvicted(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	tr.entryTTL = 50 * time.Millisecond

	// Inject a frozen clock to avoid flaky timing.
	clock := time.Now()
	tr.now = func() time.Time { return clock }

	tr.Record(152)
	tr.Record(153)
	assert.Equal(t, 2, tr.Size())

	// Advance clock beyond TTL; next Record triggers reap.
	clock = clock.Add(1 * time.Second)
	tr.Record(154)
	assert.Equal(t, 1, tr.Size(), "stale entries must be reaped on access")
	assert.Equal(t, 0, tr.HitsAt(152))
	assert.Equal(t, 0, tr.HitsAt(153))
	assert.Equal(t, 1, tr.HitsAt(154))
}

func TestStuckNonceTracker_ConcurrentInterleavedNonces(t *testing.T) {
	t.Parallel()

	// Race-detector exercise: many goroutines hammering DIFFERENT nonces
	// simultaneously must all count their hits correctly. This is the
	// scenario the old single-slot design broke.
	tr := NewStuckNonceTracker()
	const goroutinesPerNonce = 8
	const hitsPerGoroutine = 50
	const nonces = 4

	var wg sync.WaitGroup
	for n := uint64(0); n < nonces; n++ {
		for g := 0; g < goroutinesPerNonce; g++ {
			wg.Add(1)
			nonce := n
			go func() {
				defer wg.Done()
				for j := 0; j < hitsPerGoroutine; j++ {
					tr.Record(nonce)
				}
			}()
		}
	}
	wg.Wait()

	for n := uint64(0); n < nonces; n++ {
		assert.Equal(t, goroutinesPerNonce*hitsPerGoroutine, tr.HitsAt(n),
			"hits at nonce %d must equal total parallel records", n)
	}
}

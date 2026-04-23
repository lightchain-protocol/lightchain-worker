package chain

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStuckNonceTracker_SameNonceIncrements(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	assert.Equal(t, 1, tr.Record(152))
	assert.Equal(t, 2, tr.Record(152))
	assert.Equal(t, 3, tr.Record(152))
	assert.Equal(t, 3, tr.ConsecutiveHits())

	n, ok := tr.LastNonce()
	assert.True(t, ok)
	assert.Equal(t, uint64(152), n)
}

func TestStuckNonceTracker_DifferentNonceResets(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	tr.Record(152)
	tr.Record(152)
	tr.Record(152)
	tr.IncrementBumpAttempts()
	tr.IncrementBumpAttempts()
	assert.Equal(t, 2, tr.BumpAttemptsUsed())

	// A different nonce appearing means the previous stuck candidate is
	// no longer a concern (the counter has moved). Hit count and bump
	// counter must both reset — bumping replacement at nonce 152 makes
	// no sense once we're trying 153.
	assert.Equal(t, 1, tr.Record(153))
	assert.Equal(t, 0, tr.BumpAttemptsUsed())
	n, _ := tr.LastNonce()
	assert.Equal(t, uint64(153), n)
}

func TestStuckNonceTracker_Clear(t *testing.T) {
	t.Parallel()

	tr := NewStuckNonceTracker()
	tr.Record(152)
	tr.Record(152)
	tr.IncrementBumpAttempts()

	tr.Clear()

	assert.Equal(t, 0, tr.ConsecutiveHits())
	assert.Equal(t, 0, tr.BumpAttemptsUsed())
	_, ok := tr.LastNonce()
	assert.False(t, ok, "LastNonce must report hasObserved=false after Clear")
}

func TestStuckNonceTracker_BumpAttemptsIndependentOfHits(t *testing.T) {
	t.Parallel()

	// Bump attempts and consecutive hits are related but independent: a
	// caller may hit threshold, increment bumps, then record more hits
	// at the same nonce (replacement didn't evict). Hits continue
	// counting, bumps track replacement attempts separately.
	tr := NewStuckNonceTracker()
	tr.Record(152)
	tr.Record(152)
	tr.Record(152)
	tr.Record(152)
	tr.Record(152) // threshold hit at 5
	tr.IncrementBumpAttempts()
	assert.Equal(t, 5, tr.ConsecutiveHits())
	assert.Equal(t, 1, tr.BumpAttemptsUsed())

	tr.Record(152)
	assert.Equal(t, 6, tr.ConsecutiveHits(),
		"hits keep counting even after a bump is attempted")
	assert.Equal(t, 1, tr.BumpAttemptsUsed(),
		"bumps are not incremented by Record — only by IncrementBumpAttempts")
}

func TestStuckNonceTracker_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	// Race-detector exercise: many goroutines hammering the same tracker
	// at the same nonce must not trip -race.
	tr := NewStuckNonceTracker()
	const goroutines = 16
	const hitsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < hitsPerGoroutine; j++ {
				tr.Record(77)
				_ = tr.ConsecutiveHits()
				_, _ = tr.LastNonce()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, goroutines*hitsPerGoroutine, tr.ConsecutiveHits(),
		"every Record at the same nonce must increment the counter exactly once")
}

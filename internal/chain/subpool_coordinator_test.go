package chain

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubpoolCoordinator_WithinClassParallel asserts that two concurrent
// Enter calls of the SAME class both succeed immediately (no serialization).
// This is the core throughput win over the old BroadcastSerializer.
func TestSubpoolCoordinator_WithinClassParallel(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	var inFlight, maxInFlight atomic.Int32
	var wg sync.WaitGroup
	const goroutines = 4
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			tok, err := c.Enter(context.Background(), ClassBlob)
			require.NoError(t, err)
			defer tok.Done()

			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				peak := maxInFlight.Load()
				if cur <= peak || maxInFlight.CompareAndSwap(peak, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(goroutines), maxInFlight.Load(),
		"within-class Enters must run in parallel (no serialization)")
}

// TestSubpoolCoordinator_CrossClassBlocks asserts that a Legacy Enter blocks
// while a Blob entry is in flight, and unblocks immediately after the Blob
// token's Done releases the class.
func TestSubpoolCoordinator_CrossClassBlocks(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	blobTok, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)

	legacyDone := make(chan error, 1)
	var legacyTok *BroadcastToken
	go func() {
		tok, err := c.Enter(context.Background(), ClassLegacy)
		legacyTok = tok
		legacyDone <- err
	}()

	// Legacy Enter must NOT complete while blob is in flight.
	select {
	case err := <-legacyDone:
		t.Fatalf("legacy Enter returned while blob was in flight (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	blobTok.Done()

	// Now legacy must unblock quickly.
	select {
	case err := <-legacyDone:
		require.NoError(t, err)
		require.NotNil(t, legacyTok)
		legacyTok.Done()
	case <-time.After(500 * time.Millisecond):
		t.Fatal("legacy Enter did not unblock within 500ms of blob Done")
	}
}

// TestSubpoolCoordinator_CtxCancelWhileBlocked asserts that a blocked Enter
// returns ctx.Err() promptly when the caller's context is cancelled, and
// does NOT end up holding a slot.
func TestSubpoolCoordinator_CtxCancelWhileBlocked(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	blobTok, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	defer blobTok.Done()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		tok, err := c.Enter(ctx, ClassLegacy)
		if tok != nil {
			// Must not have acquired — test failure scenario; clean up anyway.
			tok.Done()
		}
		result <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancelAt := time.Now()
	cancel()

	select {
	case err := <-result:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(cancelAt), 500*time.Millisecond,
			"Enter must return within 500ms of ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("Enter did not return within 2s of ctx cancel — not ctx-aware")
	}

	assert.Equal(t, 0, c.Inflight(ClassLegacy),
		"cancelled Enter must not leave an in-flight entry behind")
}

// TestSubpoolCoordinator_DoneIdempotent asserts that calling Done twice on the
// same token doesn't double-release — which would corrupt the in-flight count
// and wake waiters prematurely on a future Done from a different token.
func TestSubpoolCoordinator_DoneIdempotent(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	t1, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	t2, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)

	t1.Done()
	t1.Done() // second call must be a no-op
	assert.Equal(t, 1, c.Inflight(ClassBlob), "Done must be idempotent")

	t2.Done()
	assert.Equal(t, 0, c.Inflight(ClassBlob))
}

// TestSubpoolCoordinator_Registered records a hash but doesn't affect the
// release path. Regression guard that we don't accidentally release on
// Registered.
func TestSubpoolCoordinator_Registered(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	tok, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)

	tok.Registered(common.HexToHash("0xabc"))
	assert.Equal(t, 1, c.Inflight(ClassBlob),
		"Registered must not release the slot")

	tok.Done()
	assert.Equal(t, 0, c.Inflight(ClassBlob))
}

// TestSubpoolCoordinator_Seed populates in-flight with synthetic entries
// classified as ClassBlob, so the first Legacy Enter blocks until they are
// released (or evicted by the janitor).
func TestSubpoolCoordinator_Seed(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	c.Seed(5, 2) // gap=3
	assert.Equal(t, 3, c.Inflight(ClassBlob), "Seed must add gap-sized synthetic entries")

	// Legacy Enter must block.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Enter(ctx, ClassLegacy)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestSubpoolCoordinator_SeedNoOpWhenNoGap is the happy-startup case — if
// the local pending and latest nonces agree, Seed must be a no-op.
func TestSubpoolCoordinator_SeedNoOpWhenNoGap(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	c.Seed(10, 10)
	c.Seed(10, 11) // pending <= latest — should also no-op
	assert.Equal(t, 0, c.Inflight(ClassBlob))
}

// TestSubpoolCoordinator_JanitorEvictsStale drives reapStale directly with a
// crafted "now" so we don't depend on the real ticker interval.
func TestSubpoolCoordinator_JanitorEvictsStale(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 100*time.Millisecond)
	defer c.Close()

	tok, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	_ = tok // intentionally forget Done — simulating an orphan

	// Pretend now is 5 minutes in the future; the token's 100ms lifetime
	// has long expired.
	c.reapStale(time.Now().Add(5 * time.Minute))

	assert.Equal(t, 0, c.Inflight(ClassBlob),
		"stale entry must be evicted")
	assert.Equal(t, int64(1), c.OrphanCount(),
		"eviction must bump orphan metric")
}

// TestSubpoolCoordinator_JanitorWakesWaiters asserts that when the janitor
// drains a class to empty, waiters on the other class unblock.
func TestSubpoolCoordinator_JanitorWakesWaiters(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 50*time.Millisecond)
	defer c.Close()

	orphanTok, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	_ = orphanTok // never Done

	waiterDone := make(chan error, 1)
	go func() {
		tok, err := c.Enter(context.Background(), ClassLegacy)
		if tok != nil {
			tok.Done()
		}
		waiterDone <- err
	}()

	// Trigger the reap manually; simulate janitor firing after cutoff.
	time.Sleep(10 * time.Millisecond)
	c.reapStale(time.Now().Add(1 * time.Minute))

	select {
	case err := <-waiterDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waiter did not unblock after janitor evicted orphan")
	}
}

// TestSubpoolCoordinator_AlternatingClassesSerialize is the end-to-end flow
// mirror: blob → legacy → blob, each holding the coordinator exclusively
// against the other class but never blocking same-class peers.
func TestSubpoolCoordinator_AlternatingClassesSerialize(t *testing.T) {
	t.Parallel()

	c := NewSubpoolCoordinator(nil, 0)
	defer c.Close()

	b1, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	b2, err := c.Enter(context.Background(), ClassBlob)
	require.NoError(t, err)
	assert.Equal(t, 2, c.Inflight(ClassBlob))

	// Legacy must wait for BOTH blobs to Done.
	legacyResult := make(chan error, 1)
	go func() {
		tok, err := c.Enter(context.Background(), ClassLegacy)
		if tok != nil {
			tok.Done()
		}
		legacyResult <- err
	}()

	b1.Done()
	select {
	case err := <-legacyResult:
		t.Fatalf("legacy unblocked after only ONE of two blobs drained (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}
	b2.Done()

	select {
	case err := <-legacyResult:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("legacy did not unblock after both blobs drained")
	}
}

package chain

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBroadcastSerializer_Serializes asserts that two concurrent Acquire
// calls never hold the slot simultaneously. Without serialization, both
// goroutines would enter the critical section and maxInFlight would be 2.
func TestBroadcastSerializer_Serializes(t *testing.T) {
	t.Parallel()

	serializer := NewBroadcastSerializer()

	var inFlight, maxInFlight atomic.Int32
	var wg sync.WaitGroup
	const goroutines = 2
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			err := serializer.Acquire(context.Background())
			require.NoError(t, err)
			defer serializer.Release()

			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				peak := maxInFlight.Load()
				if cur <= peak || maxInFlight.CompareAndSwap(peak, cur) {
					break
				}
			}
			// Hold the slot long enough to expose any serialization break.
			time.Sleep(20 * time.Millisecond)
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(1), maxInFlight.Load(),
		"max concurrent holders must be 1 — serializer is not actually serializing")
}

// TestBroadcastSerializer_CtxCancelDuringAcquire asserts that a goroutine
// blocked on Acquire exits promptly when its ctx is cancelled, and does
// NOT end up holding the slot. Without ctx-awareness the second goroutine
// would block forever behind a stalled holder.
func TestBroadcastSerializer_CtxCancelDuringAcquire(t *testing.T) {
	t.Parallel()

	serializer := NewBroadcastSerializer()

	// G1 takes the slot and holds it.
	require.NoError(t, serializer.Acquire(context.Background()))
	defer serializer.Release()

	// G2 tries to acquire with a cancellable ctx.
	ctx, cancel := context.WithCancel(context.Background())
	g2Err := make(chan error, 1)
	go func() {
		g2Err <- serializer.Acquire(ctx)
	}()

	// Give G2 time to enter the select and block.
	time.Sleep(30 * time.Millisecond)
	cancelAt := time.Now()
	cancel()

	select {
	case err := <-g2Err:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(cancelAt), 500*time.Millisecond,
			"Acquire must return within 500ms of ctx cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("G2 did not return within 2s of ctx cancel — Acquire is not ctx-aware")
	}
}

// TestBroadcastSerializer_CanReacquireAfterRelease is a smoke test that
// Release actually frees the slot for the next caller.
func TestBroadcastSerializer_CanReacquireAfterRelease(t *testing.T) {
	t.Parallel()

	serializer := NewBroadcastSerializer()
	for i := 0; i < 3; i++ {
		require.NoError(t, serializer.Acquire(context.Background()))
		serializer.Release()
	}
}

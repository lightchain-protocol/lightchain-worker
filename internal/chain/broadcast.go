package chain

import "context"

// BroadcastSerializer is a ctx-aware 1-slot semaphore serializing every
// SendTransaction..WaitMined window from a single signing key. It exists
// because go-ethereum's txpool reservation is sender-wide: while ANY tx
// (blob or non-blob) from sender S is pending in the pool, any concurrent
// broadcast from S — regardless of nonce or type — is rejected with
// ErrAlreadyReserved ("address already reserved"). Serializing blob-vs-blob
// alone (as PR #16 did) leaves blob-vs-ACK and blob-vs-CompleteJob exposed
// whenever two asynq worker goroutines run concurrently.
//
// One signing key => one BroadcastSerializer. The service layer owns the
// single instance and injects the same pointer into both BlobTxSubmitter
// and ChainClient. If a second signing key is ever introduced (e.g.
// multi-wallet sharding), each key gets its own serializer.
//
// The underlying channel is a buffered chan of size 1 used as a binary
// semaphore rather than sync.Mutex so Acquire can respect ctx.Done() —
// essential for a cancelled asynq task to drop out of a queue behind a
// slow WaitMined instead of waiting indefinitely.
type BroadcastSerializer struct {
	slot chan struct{}
}

// NewBroadcastSerializer returns a serializer with an empty (available) slot.
func NewBroadcastSerializer() *BroadcastSerializer {
	return &BroadcastSerializer{slot: make(chan struct{}, 1)}
}

// Acquire blocks until the slot is free or ctx is done. On success the
// caller MUST call Release exactly once (defer is idiomatic). On ctx
// cancellation the slot is NOT held and Release must not be called.
func (b *BroadcastSerializer) Acquire(ctx context.Context) error {
	select {
	case b.slot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees the slot. Must be called exactly once per successful Acquire.
func (b *BroadcastSerializer) Release() {
	<-b.slot
}

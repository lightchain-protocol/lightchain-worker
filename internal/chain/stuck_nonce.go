package chain

import "sync"

// StuckNonceTracker counts consecutive ErrAlreadyReserved rejections at a
// specific nonce, to detect a tx stuck in the mempool that the ResetNonce
// + refetch loop cannot clear on its own.
//
// Failure mode (testnet, 2026-04-23): a blob tx at nonce K is broadcast
// but never mines (e.g. BlobFeeCap underbid the current blob base fee).
// Every subsequent SendTransaction gets rejected with "address already
// reserved"; ResetNonce refetches chain pending = K, retry tries K again,
// loop. asynq exhausts its MaxRetry budget without a single successful
// broadcast. The tracker's role is to detect this state deterministically
// so callers can take corrective action (replacement tx with bumped fees,
// or loud failure for operator intervention).
//
// Shared across BlobTxSubmitter and ChainClient via injection at the
// service layer. One signing key => one tracker. Safe for concurrent
// access under its internal mutex.
type StuckNonceTracker struct {
	mu              sync.Mutex
	lastNonce       uint64
	hasObserved     bool
	consecutiveHits int
	bumpAttempts    int
}

// NewStuckNonceTracker returns an empty tracker.
func NewStuckNonceTracker() *StuckNonceTracker {
	return &StuckNonceTracker{}
}

// Record notes a reservation rejection at nonce n. If n matches the last
// recorded nonce, the consecutive-hit counter increments; otherwise the
// counter resets to 1 (a new stuck candidate). Returns the current
// consecutive-hit count at n.
func (t *StuckNonceTracker) Record(n uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.hasObserved && t.lastNonce == n {
		t.consecutiveHits++
	} else {
		t.lastNonce = n
		t.hasObserved = true
		t.consecutiveHits = 1
		// A new stuck candidate means replacement attempts for the
		// previous nonce no longer apply — reset the bump counter.
		t.bumpAttempts = 0
	}
	return t.consecutiveHits
}

// ConsecutiveHits returns the current consecutive-hit count without
// mutating state. Returns 0 if no rejection has ever been recorded.
func (t *StuckNonceTracker) ConsecutiveHits() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.consecutiveHits
}

// LastNonce returns the last nonce at which a rejection was recorded, and
// whether any rejection has ever been recorded. Callers use this to
// identify WHICH nonce is stuck so they can build a replacement tx at that
// exact nonce.
func (t *StuckNonceTracker) LastNonce() (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastNonce, t.hasObserved
}

// BumpAttemptsUsed reports how many replacement-tx attempts have been made
// for the current stuck nonce. Callers compare against a configured max
// before attempting another bump.
func (t *StuckNonceTracker) BumpAttemptsUsed() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bumpAttempts
}

// IncrementBumpAttempts is called by the replacement-tx submitter each
// time it broadcasts a bumped-fee replacement.
func (t *StuckNonceTracker) IncrementBumpAttempts() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bumpAttempts++
}

// Clear resets the tracker. Called after a successful normal broadcast
// (the stuck nonce is no longer stuck) or when the caller chooses to
// abandon tracking (e.g. NonceManager refetched a nonce greater than the
// tracked one, implying the stuck tx mined or was evicted).
func (t *StuckNonceTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastNonce = 0
	t.hasObserved = false
	t.consecutiveHits = 0
	t.bumpAttempts = 0
}

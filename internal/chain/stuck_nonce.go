package chain

import (
	"sync"
	"time"
)

// StuckNonceTracker counts consecutive ErrAlreadyReserved rejections at a
// specific nonce, so callers can detect a tx that's wedged in the mempool
// (pool not draining via normal nonce incrementing) and trigger a
// bumped-fee replacement.
//
// Failure mode (testnet, 2026-04-23): a blob tx at nonce K is broadcast
// but never mines (e.g. BlobFeeCap underbid the current blob base fee).
// Every subsequent SendTransaction gets rejected with "address already
// reserved"; the loop never reaches the bump threshold because each retry
// re-records the nonce from scratch.
//
// Parallel-broadcast requirement: with the SubpoolCoordinator allowing
// concurrent within-class broadcasts, two goroutines can fail at different
// nonces (K and K+1) back-to-back. The previous single-slot design keyed
// on a single `lastNonce` field, so interleaved `Record(K)` and
// `Record(K+1)` would each reset the other's hit count — the threshold
// was never reached. This implementation keeps a per-nonce hit counter so
// each wedged nonce is tracked independently.
//
// Entries older than `entryTTL` are evicted on each access so the map
// can't grow unboundedly under a misbehaving EL.
//
// Shared across BlobTxSubmitter and ChainClient via injection at the
// service layer. One signing key => one tracker. Safe for concurrent access
// under its internal mutex.
type StuckNonceTracker struct {
	mu      sync.Mutex
	entries map[uint64]*nonceState
	// bumpAttempts is tracked globally (not per-nonce) because the
	// replacement-broadcast decision is made by the blob-tx path, which
	// only submits one replacement at a time regardless of which nonce is
	// wedged. Resetting the bump counter on a NEW nonce-candidate is
	// important: bumping at nonce K makes no sense if K has mined and we're
	// now seeing rejections at K+1.
	bumpAttempts int
	// lastBumpNonce tracks which nonce the current bumpAttempts counter
	// belongs to, so IncrementBumpAttemptsFor / BumpAttemptsUsedFor can
	// detect when the counter should reset.
	lastBumpNonce    uint64
	hasLastBumpNonce bool

	entryTTL time.Duration
	now      func() time.Time // injectable for tests
}

type nonceState struct {
	hits     int
	lastSeen time.Time
}

// defaultEntryTTL expires per-nonce entries that haven't been touched in a
// while. Long enough that a genuine slow drain (blob fee pricing catching
// up over several blocks) doesn't age out mid-recovery; short enough that
// a misbehaving EL doesn't accumulate unbounded entries.
const defaultEntryTTL = 10 * time.Minute

// NewStuckNonceTracker returns an empty tracker with the default TTL.
func NewStuckNonceTracker() *StuckNonceTracker {
	return &StuckNonceTracker{
		entries:  make(map[uint64]*nonceState),
		entryTTL: defaultEntryTTL,
		now:      time.Now,
	}
}

// Record notes a reservation rejection at nonce n and returns the running
// hit count at n. Unlike the old single-slot design, other nonces'
// counters are unaffected — interleaved records at K and K+1 from
// concurrent goroutines accumulate independently.
func (t *StuckNonceTracker) Record(n uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.reapStaleLocked()

	entry, ok := t.entries[n]
	if !ok {
		entry = &nonceState{}
		t.entries[n] = entry
	}
	entry.hits++
	entry.lastSeen = t.now()
	return entry.hits
}

// MaxConsecutiveHits returns the largest hit count across all currently
// tracked nonces — i.e. "how stuck is the most-stuck nonce right now".
// Returns 0 when nothing has been recorded. Callers that need the count
// for a specific nonce should use HitsAt(n) instead.
func (t *StuckNonceTracker) MaxConsecutiveHits() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	var maxHits int
	for _, e := range t.entries {
		if e.hits > maxHits {
			maxHits = e.hits
		}
	}
	return maxHits
}

// HitsAt returns the hit count for a specific nonce. Zero if unknown.
// Used by callers (BlobTxSubmitter) that need to reason about a particular
// nonce they just broadcasted.
func (t *StuckNonceTracker) HitsAt(n uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[n]; ok {
		return e.hits
	}
	return 0
}

// LastNonce returns the nonce most recently recorded, and whether any
// rejection has been recorded at all. Under parallel broadcasts "most
// recently" is determined by the latest lastSeen timestamp.
func (t *StuckNonceTracker) LastNonce() (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var (
		latest   time.Time
		lastN    uint64
		observed bool
	)
	for n, e := range t.entries {
		if !observed || e.lastSeen.After(latest) {
			latest = e.lastSeen
			lastN = n
			observed = true
		}
	}
	return lastN, observed
}

// BumpAttemptsUsedFor reports how many replacement-tx attempts have been
// made for the given nonce. Returns 0 when the tracker's last-bumped
// nonce differs from n — i.e. each new stuck nonce starts with a fresh
// budget, mirroring IncrementBumpAttemptsFor's reset semantics.
//
// Use this paired with IncrementBumpAttemptsFor; the global
// BumpAttemptsUsed reader was removed because it carried state across
// nonce changes and could exhaust the per-nonce budget incorrectly when
// successive nonces hit the stuck path.
func (t *StuckNonceTracker) BumpAttemptsUsedFor(n uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasLastBumpNonce || t.lastBumpNonce != n {
		return 0
	}
	return t.bumpAttempts
}

// IncrementBumpAttemptsFor records a replacement-tx attempt for the given
// nonce. If the last bump was for a DIFFERENT nonce, the counter resets
// before incrementing — each new stuck nonce gets a fresh budget.
func (t *StuckNonceTracker) IncrementBumpAttemptsFor(n uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasLastBumpNonce || t.lastBumpNonce != n {
		t.bumpAttempts = 0
		t.lastBumpNonce = n
		t.hasLastBumpNonce = true
	}
	t.bumpAttempts++
}

// Clear resets the tracker fully. Called after a successful normal
// broadcast (stuck condition resolved) or when the caller abandons
// tracking (e.g. chain refetch returned a nonce greater than all tracked).
func (t *StuckNonceTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for n := range t.entries {
		delete(t.entries, n)
	}
	t.bumpAttempts = 0
	t.hasLastBumpNonce = false
	t.lastBumpNonce = 0
}

// Size returns the number of nonces currently tracked. Exposed for
// metrics (`stuck_nonce_tracked` gauge).
func (t *StuckNonceTracker) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

// reapStaleLocked is called with the mutex held. Removes entries whose
// lastSeen is older than entryTTL. Cheap amortization over Record calls.
func (t *StuckNonceTracker) reapStaleLocked() {
	cutoff := t.now().Add(-t.entryTTL)
	for n, e := range t.entries {
		if e.lastSeen.Before(cutoff) {
			delete(t.entries, n)
		}
	}
}

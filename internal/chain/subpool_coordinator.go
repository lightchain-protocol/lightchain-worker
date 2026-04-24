package chain

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// TxClass identifies which geth subpool a broadcast targets. go-ethereum's
// Reserver grants exclusivity PER SUBPOOL per sender (el/core/txpool/blobpool/
// blobpool.go:336 — "ensure exclusivity across subpools"), not per-tx. Within
// a subpool, multiple pending txs from the same sender are allowed and
// ordered by nonce (blobpool.go:354: map[address][]blobTxMeta). So the only
// serialization we need is across classes; within a class the pool handles
// ordering.
type TxClass int

const (
	// ClassBlob is a type-3 EIP-4844 blob tx. Lives in geth's blobpool.
	ClassBlob TxClass = iota
	// ClassLegacy is any non-blob tx (legacy, dynamic-fee, etc.). Lives in
	// geth's legacypool. For this worker that's ACK and CompleteJob.
	ClassLegacy
)

func (c TxClass) String() string {
	switch c {
	case ClassBlob:
		return "blob"
	case ClassLegacy:
		return "legacy"
	default:
		return "unknown"
	}
}

// other returns the opposite class. Used by Enter to decide which in-flight
// set to wait on.
func (c TxClass) other() TxClass {
	if c == ClassBlob {
		return ClassLegacy
	}
	return ClassBlob
}

// defaultMaxTokenLifetime is the janitor eviction threshold for in-flight
// tokens. Chosen as the sum of the two largest per-tx timeouts (BlobTxTimeout
// + AckTxTimeout, plus slack) so a legitimate slow broadcast is never evicted
// but a leaked token cannot wedge the coordinator forever.
const defaultMaxTokenLifetime = 3 * time.Minute

// defaultJanitorInterval is how often the janitor goroutine scans for stale
// tokens. 30s gives sub-minute recovery without CPU noise.
const defaultJanitorInterval = 30 * time.Second

// inflightEntry is what's tracked per live broadcast. `hash` is zero until
// Registered is called; it stays zero for tokens abandoned before
// SendTransaction succeeds.
type inflightEntry struct {
	id        uint64
	class     TxClass
	hash      common.Hash
	enteredAt time.Time
}

// SubpoolCoordinator serializes cross-class broadcasts from one signing key
// and allows unbounded within-class concurrency. It replaces the per-slot
// BroadcastSerializer that over-serialized all tx types against each other.
//
// Correctness invariant: at any time, if inflight[ClassBlob] is non-empty
// then inflight[ClassLegacy] is empty (and vice versa). Enter enforces this
// by blocking the newcomer until the other class fully drains; Done wakes
// waiters when its class transitions to empty.
//
// Shared across BlobTxSubmitter and ChainClient via injection at the service
// layer. One signing key => one coordinator. Safe for concurrent use.
//
// Ctx-aware wait uses a snapshot-and-select idiom over a notify channel
// because sync.Cond.Wait cannot respect ctx.Done. See Enter for details.
type SubpoolCoordinator struct {
	logger           *slog.Logger
	maxTokenLifetime time.Duration
	janitorInterval  time.Duration

	mu       sync.Mutex
	inflight map[TxClass]map[uint64]*inflightEntry
	notify   chan struct{} // closed+recreated when a class transitions to empty
	nextID   uint64        // monotonic token IDs; unique under mu

	orphanCount atomic.Int64 // exposed via OrphanCount for metrics

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewSubpoolCoordinator returns a coordinator with an empty in-flight set
// and a janitor goroutine running at the default interval. Callers must
// call Close() on shutdown to stop the janitor.
//
// Logger may be nil; when nil, only atomic metrics are updated (no log
// output). maxTokenLifetime may be zero to use the default.
func NewSubpoolCoordinator(logger *slog.Logger, maxTokenLifetime time.Duration) *SubpoolCoordinator {
	if maxTokenLifetime <= 0 {
		maxTokenLifetime = defaultMaxTokenLifetime
	}

	c := &SubpoolCoordinator{
		logger:           logger,
		maxTokenLifetime: maxTokenLifetime,
		janitorInterval:  defaultJanitorInterval,
		inflight: map[TxClass]map[uint64]*inflightEntry{
			ClassBlob:   make(map[uint64]*inflightEntry),
			ClassLegacy: make(map[uint64]*inflightEntry),
		},
		notify: make(chan struct{}),
		stopCh: make(chan struct{}),
	}

	go c.janitor()

	return c
}

// Close stops the janitor goroutine. Idempotent.
func (c *SubpoolCoordinator) Close() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

// BroadcastToken is returned by Enter. The caller MUST invoke exactly one
// of Done / Abandon to release the coordinator's in-flight slot. Registered
// is an optional call between Enter and Done to record the tx hash for
// observability and debugging.
type BroadcastToken struct {
	coord   *SubpoolCoordinator
	entryID uint64
	class   TxClass
	settled atomic.Bool
}

// Enter blocks until the OTHER class has no pending broadcasts, or ctx is
// done. On success, a placeholder is added to inflight[class] and a token
// is returned. The caller must pair Enter with exactly one Done or Abandon.
//
// Same-class concurrent Enter calls do NOT block each other — geth's pool
// serializes them by nonce internally, and NonceManager ensures strictly
// increasing nonces.
func (c *SubpoolCoordinator) Enter(ctx context.Context, class TxClass) (*BroadcastToken, error) {
	for {
		c.mu.Lock()
		if len(c.inflight[class.other()]) == 0 {
			c.nextID++
			id := c.nextID
			entry := &inflightEntry{
				id:        id,
				class:     class,
				enteredAt: time.Now(),
			}
			c.inflight[class][id] = entry
			c.mu.Unlock()
			if c.logger != nil {
				c.logger.Debug("broadcast slot acquired",
					"class", class.String(),
					"inflightSameClass", c.Inflight(class),
				)
			}
			return &BroadcastToken{
				coord:   c,
				entryID: id,
				class:   class,
			}, nil
		}
		// Other class has pending broadcasts. Snapshot the notify channel
		// BEFORE releasing the mutex so we don't race a Done() that closes
		// the channel between our unlock and our select — if it closes we
		// observe it via the zero-value receive.
		ch := c.notify
		c.mu.Unlock()

		select {
		case <-ch:
			// Class may have transitioned; loop and re-check under lock.
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Registered records the tx hash on the token's in-flight entry. Called
// AFTER SendTransaction returns success. Safe to skip (Done still works);
// only used for observability on janitor evictions.
func (t *BroadcastToken) Registered(hash common.Hash) {
	if t == nil || t.settled.Load() {
		return
	}
	t.coord.mu.Lock()
	defer t.coord.mu.Unlock()
	if entry, ok := t.coord.inflight[t.class][t.entryID]; ok {
		entry.hash = hash
	}
}

// Done removes the token's entry from in-flight. Idempotent against the
// token itself via the `settled` guard. If this was the last entry in the
// class, waiters on the other class are notified.
//
// Call after WaitMined returns — success or error. A WaitMined failure
// does NOT keep the token in flight because the broadcast reservation
// is bound to the tx being in the pool; once it mines or errors, the pool
// entry is gone and the reservation released.
func (t *BroadcastToken) Done() {
	if t == nil {
		return
	}
	if !t.settled.CompareAndSwap(false, true) {
		return
	}
	t.coord.release(t.class, t.entryID)
}

// Abandon is the symmetric counterpart to Done for the pre-broadcast
// failure path (nonce read failed, sign failed, ctx cancelled before Send).
// The entry is removed and waiters notified — identical to Done — but the
// intent is documented separately so callers are precise about which
// lifecycle path ran.
func (t *BroadcastToken) Abandon() {
	t.Done()
}

// release removes the entry and, if the class is now empty, closes the
// current notify channel (waking all waiters) and installs a fresh one for
// future arrivals.
func (c *SubpoolCoordinator) release(class TxClass, entryID uint64) {
	c.mu.Lock()
	delete(c.inflight[class], entryID)
	if len(c.inflight[class]) == 0 {
		close(c.notify)
		c.notify = make(chan struct{})
	}
	c.mu.Unlock()

	if c.logger != nil {
		c.logger.Debug("broadcast slot released",
			"class", class.String(),
			"inflightSameClass", c.Inflight(class),
			"inflightOtherClass", c.Inflight(class.other()),
		)
	}
}

// Inflight returns the current number of in-flight broadcasts for a class.
// Used by tests and metrics; not on the hot path.
func (c *SubpoolCoordinator) Inflight(class TxClass) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight[class])
}

// OrphanCount returns the cumulative number of in-flight entries evicted
// by the janitor (caller forgot Done/Abandon, or process wedged between
// Send and the deferred release). Exposed for metrics.
func (c *SubpoolCoordinator) OrphanCount() int64 {
	return c.orphanCount.Load()
}

// Seed pre-populates the coordinator with synthetic in-flight tokens for
// any pending txs the previous process left behind in geth's pools. Called
// at service startup once pendingNonce > latestNonce is observed. The
// classification is conservative: we don't know which subpool each stuck
// tx targets, so we flag them all as ClassBlob — that makes the first
// Legacy Enter block until the drain completes, which is correct either
// way (a blob pending locks out legacy, and a legacy pending locks out
// blob, but we can only over-serialize in this direction since we can't
// query geth's pool contents from here).
//
// The seeded entries are synthetic: no hash, enteredAt=now, and the
// janitor may evict them if they linger beyond maxTokenLifetime. That's
// the desired behavior: if the pool doesn't drain within the budget,
// operator intervention is needed regardless.
func (c *SubpoolCoordinator) Seed(pendingNonce, latestNonce uint64) {
	if pendingNonce <= latestNonce {
		return
	}
	gap := pendingNonce - latestNonce
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint64(0); i < gap; i++ {
		c.nextID++
		id := c.nextID
		c.inflight[ClassBlob][id] = &inflightEntry{
			id:        id,
			class:     ClassBlob,
			enteredAt: time.Now(),
		}
	}
	if c.logger != nil {
		c.logger.Warn("coordinator seeded with pending txs from previous process",
			"gap", gap,
			"pendingNonce", pendingNonce,
			"latestNonce", latestNonce,
			"hint", "process restart? tokens will age out via janitor if pool drains",
		)
	}
}

// janitor evicts in-flight entries older than maxTokenLifetime. An orphan
// is ALWAYS a bug (caller forgot Done/Abandon, or a panic skipped defer),
// so we log at ERROR and bump a metric. Silently wedging is worse than
// paging loudly.
func (c *SubpoolCoordinator) janitor() {
	t := time.NewTicker(c.janitorInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case now := <-t.C:
			c.reapStale(now)
		}
	}
}

// reapStale is the janitor pass — split from janitor() so tests can invoke
// it deterministically without waiting on the ticker.
func (c *SubpoolCoordinator) reapStale(now time.Time) {
	cutoff := now.Add(-c.maxTokenLifetime)
	c.mu.Lock()
	var evicted []inflightEntry
	for _, class := range []TxClass{ClassBlob, ClassLegacy} {
		for id, entry := range c.inflight[class] {
			if entry.enteredAt.Before(cutoff) {
				evicted = append(evicted, *entry)
				delete(c.inflight[class], id)
			}
		}
	}
	classEmptied := make(map[TxClass]bool)
	for _, e := range evicted {
		if len(c.inflight[e.class]) == 0 {
			classEmptied[e.class] = true
		}
	}
	needsNotify := len(classEmptied) > 0
	if needsNotify {
		close(c.notify)
		c.notify = make(chan struct{})
	}
	c.mu.Unlock()

	for _, e := range evicted {
		c.orphanCount.Add(1)
		if c.logger != nil {
			c.logger.Error("coordinator evicted stale in-flight entry (orphan)",
				"class", e.class.String(),
				"entryID", e.id,
				"hash", e.hash.Hex(),
				"ageMs", now.Sub(e.enteredAt).Milliseconds(),
				"anomaly", true,
				"hint", "caller forgot Done/Abandon — investigate stack traces",
			)
		}
	}
}

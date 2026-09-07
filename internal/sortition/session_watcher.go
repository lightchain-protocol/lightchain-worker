package sortition

import (
	"context"
	"log/slog"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

const cursorSessionRequested = "session_requested"

// ClaimClient is the narrow chain interface the SessionWatcher needs.
// Head returns chain.HeadInfo (not a bare uint64) so the watcher can use
// head.Number with the same confirmations-guard pattern as the release Reconciler.
type ClaimClient interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	FilterSessionRequested(ctx context.Context, fromBlock, toBlock uint64) ([]chain.SessionRequestedEvent, error)
	GetRequestInfo(ctx context.Context, reqID uint64) (chain.RequestInfo, error)
	// GetRequiredCapabilities reads a request's required-capability mask (zero = unconstrained)..
	GetRequiredCapabilities(ctx context.Context, reqID uint64) (*big.Int, error)
	EligibleNow(ctx context.Context, reqID uint64, worker common.Address) (bool, error)
	ClaimSession(ctx context.Context, reqID uint64) error
}

// SessionWatcherOpts carries all configuration for a SessionWatcher.
type SessionWatcherOpts struct {
	Client        ClaimClient
	Cursor        *CursorStore
	Worker        common.Address
	JobCounter    *atomic.Int32
	MaxConcurrent int
	ChunkSize     uint64
	Confirmations uint64
	PollInterval  time.Duration
	Logger        *slog.Logger
	// OwnCapabilities is this worker's declared on-chain capability mask.
	// Requests requiring bits outside it are skipped instead of burning a doomed
	// claim tx. Nil is treated as zero (no capabilities).
	OwnCapabilities *big.Int
	// LookbackBlocks is how far behind the persisted cursor the first pass
	// re-scans for SessionRequested events. The pending set lives only in memory,
	// so without this a restart forgets every request discovered before the
	// cursor — including ones still waiting out the sortition threshold. Size it
	// to the longest a request can stay Open (consumer-api default expiry is
	// 1 h). Zero disables the look-back.
	LookbackBlocks uint64
}

// SessionWatcher polls the chain for SessionRequested events and claims
// sessions this worker is eligible for (subject to local capacity).
//
// Discovered requests are held in an in-memory pending set and re-evaluated
// every pass so that the decaying sortition threshold is respected: a worker
// becomes eligible only after the claim window widens over blocks, which may
// happen on a later pass than discovery.
type SessionWatcher struct {
	c        ClaimClient
	cursor   *CursorStore
	worker   common.Address
	counter  *atomic.Int32
	maxJobs  int
	chunk    uint64
	confs    uint64
	interval time.Duration
	log      *slog.Logger
	ownCaps  *big.Int
	lookback uint64
	// seeded is set once the first pass has re-scanned LookbackBlocks behind the
	// persisted cursor (see RunOnce, phase 0).
	seeded bool
	// pending maps reqID → its required-capability mask, fetched lazily on first
	// evaluation (nil = not yet fetched). The mask is immutable per request, so a
	// successful fetch is cached for every later pass.
	pending map[uint64]*big.Int
}

// NewSessionWatcher constructs a SessionWatcher from the given options.
// ChunkSize defaults to 5000 when zero. PollInterval defaults to 30s when zero.
func NewSessionWatcher(o SessionWatcherOpts) *SessionWatcher {
	chunk := o.ChunkSize
	if chunk == 0 {
		chunk = 5000
	}
	interval := o.PollInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	ownCaps := o.OwnCapabilities
	if ownCaps == nil {
		ownCaps = big.NewInt(0)
	}
	return &SessionWatcher{
		c:        o.Client,
		cursor:   o.Cursor,
		worker:   o.Worker,
		counter:  o.JobCounter,
		maxJobs:  o.MaxConcurrent,
		chunk:    chunk,
		confs:    o.Confirmations,
		interval: interval,
		log:      o.Logger,
		ownCaps:  ownCaps,
		lookback: o.LookbackBlocks,
		pending:  make(map[uint64]*big.Int),
	}
}

// Start runs a ticker loop calling RunOnce on each tick until ctx is cancelled.
func (w *SessionWatcher) Start(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.RunOnce(ctx); err != nil {
				w.log.Warn("session watcher pass failed", "error", err)
			}
		}
	}
}

// RunOnce performs one poll pass in three phases.
//
// Phase 0 — Reseed (first pass only): re-scan LookbackBlocks behind the
// persisted cursor and add whatever SessionRequested events are there to the
// pending set, without moving the cursor. The evaluation phase then prunes the
// ones that are no longer Open. This is what lets a restarted worker keep
// competing for requests it had discovered but was not yet eligible for.
//
// Phase 1 — Discovery: scan cursor+1..safeHead in chunks, add discovered
// SessionRequested events to the in-memory pending set, and advance the cursor
// per chunk (resumable on error). If safeHead <= cursor, discovery is skipped
// but the evaluation phase still runs.
//
// Phase 2 — Evaluation: iterate the pending set. For each request, fetch its
// on-chain status (GetRequestInfo); drop it if it is no longer Open or has
// expired. If this worker is now eligible (EligibleNow), claim it. Requests
// that are not yet eligible are kept in the set for re-evaluation next pass —
// this is the fix for the decaying sortition threshold: a worker that was
// ineligible at discovery time will be re-checked here on every subsequent
// pass until the claim window widens enough.
//
// A claim error (e.g. AlreadyClaimed revert, racing loser) is logged at debug
// and is not fatal — the request is removed optimistically (if our tx lost the
// race, the status will be non-Open on the next pass anyway).
func (w *SessionWatcher) RunOnce(ctx context.Context) error {
	head, err := w.c.Head(ctx)
	if err != nil {
		return err
	}

	// Underflow guard: head.Number < confs wraps on uint64 subtraction.
	// Mirror the reconciler pattern exactly (reconciler.go:97-102).
	var safeHead uint64
	if head.Number > w.confs {
		safeHead = head.Number - w.confs
	}

	cursor, err := w.cursor.Get(cursorSessionRequested)
	if err != nil {
		return err
	}

	// Phase 0: Reseed — once per process, look behind the cursor.
	if !w.seeded {
		if err := w.reseed(ctx, cursor); err != nil {
			return err // seeded stays false: retried next pass
		}
		w.seeded = true
	}

	// Phase 1: Discovery — scan new blocks, populate pending.
	if safeHead > cursor {
		for lo := cursor + 1; lo <= safeHead; lo += w.chunk {
			hi := lo + w.chunk - 1
			if hi > safeHead {
				hi = safeHead
			}

			reqs, err := w.c.FilterSessionRequested(ctx, lo, hi)
			if err != nil {
				return err
			}

			for _, r := range reqs {
				w.pending[r.ReqID] = nil // capability mask fetched lazily at evaluation
			}

			if err := w.cursor.Set(cursorSessionRequested, hi); err != nil {
				return err
			}
		}
	}

	// Phase 2: Evaluation — re-check all pending requests regardless of
	// whether discovery ran. This is the core fix: a request discovered in a
	// prior pass when the worker was ineligible is re-evaluated here every
	// pass until the sortition claim window widens.
	for reqID := range w.pending {
		ri, err := w.c.GetRequestInfo(ctx, reqID)
		if err != nil {
			w.log.Warn("getRequestInfo failed", "reqId", reqID, "error", err)
			continue // transient; keep in pending
		}

		if ri.Status != 0 { // not Open (Claimed=1, Ready=2, Expired=3)
			delete(w.pending, reqID)
			continue
		}

		if uint64(time.Now().Unix()) > ri.Expiry {
			delete(w.pending, reqID)
			continue
		}

		// Skip requests whose required capabilities this worker does not
		// cover — the claim would revert MissingCapabilities anyway. Fail-open on
		// read errors (the on-chain check is the guarantee, this is gas politeness).
		caps := w.pending[reqID]
		if caps == nil {
			fetched, err := w.c.GetRequiredCapabilities(ctx, reqID)
			if err != nil {
				w.log.Warn("required-capabilities read failed; proceeding unconstrained",
					"reqId", reqID, "error", err)
				fetched = big.NewInt(0) // fail-open; not cached so the next pass retries
			} else {
				w.pending[reqID] = fetched
			}
			caps = fetched
		}
		if caps.Sign() != 0 && new(big.Int).And(caps, w.ownCaps).Cmp(caps) != 0 {
			w.log.Debug("skipping request requiring capabilities this worker lacks",
				"reqId", reqID, "requiredMask", caps.String())
			delete(w.pending, reqID)
			continue
		}

		if int(w.counter.Load()) >= w.maxJobs {
			// At capacity — stop evaluating; all pending requests are
			// preserved and will be re-tried next pass.
			break
		}

		ok, err := w.c.EligibleNow(ctx, reqID, w.worker)
		if err != nil {
			w.log.Warn("eligibleNow check failed", "reqId", reqID, "error", err)
			continue // keep in pending
		}

		if ok {
			if err := w.c.ClaimSession(ctx, reqID); err != nil {
				// Losing a claim race (AlreadyClaimed) or a transient tx error
				// is not fatal to the pass — log at debug and move on.
				w.log.Debug("claim not landed", "reqId", reqID, "error", err)
			} else {
				w.log.Info("claimed session request", "reqId", reqID)
			}
			// Remove optimistically: if our tx lost the race the request's
			// status will be non-Open on the next pass and it would be pruned
			// from pending then anyway.
			delete(w.pending, reqID)
		}
		// else: not yet eligible — keep in pending for re-evaluation next pass.
	}

	return nil
}

// reseed adds every SessionRequested event in the last w.lookback blocks up to
// and including cursor to the pending set. It never touches the cursor; the
// evaluation phase drops anything that is no longer Open or has expired.
func (w *SessionWatcher) reseed(ctx context.Context, cursor uint64) error {
	if w.lookback == 0 || cursor == 0 {
		return nil
	}
	lo := uint64(1)
	if cursor > w.lookback {
		lo = cursor - w.lookback + 1
	}
	found := 0
	for from := lo; from <= cursor; from += w.chunk {
		to := from + w.chunk - 1
		if to > cursor {
			to = cursor
		}
		reqs, err := w.c.FilterSessionRequested(ctx, from, to)
		if err != nil {
			return err
		}
		for _, r := range reqs {
			w.pending[r.ReqID] = nil // capability mask fetched lazily at evaluation
		}
		found += len(reqs)
	}
	w.log.Info("reseeded pending session requests from behind the cursor",
		"fromBlock", lo, "toBlock", cursor, "requests", found)
	return nil
}

package sortition

import (
	"context"
	"log/slog"
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
	pending  map[uint64]struct{}
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
		pending:  make(map[uint64]struct{}),
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

// RunOnce performs one poll pass in two phases.
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
				w.pending[r.ReqID] = struct{}{}
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

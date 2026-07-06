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

// RunOnce performs one poll pass: read cursor → safeHead → for each chunk in
// [cursor+1..safeHead], filter SessionRequested events and claim eligible ones
// under capacity. Cursor is advanced to each chunk's hi before moving on.
//
// A claim error (e.g. AlreadyClaimed revert, racing loser) is logged at debug
// and is not fatal — the pass continues and RunOnce returns nil.
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

	if safeHead <= cursor {
		return nil
	}

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
			if int(w.counter.Load()) >= w.maxJobs {
				w.log.Debug("at capacity, skipping claim", "reqId", r.ReqID)
				continue
			}

			ok, err := w.c.EligibleNow(ctx, r.ReqID, w.worker)
			if err != nil {
				w.log.Warn("eligibleNow check failed", "reqId", r.ReqID, "error", err)
				continue
			}
			if !ok {
				continue
			}

			if err := w.c.ClaimSession(ctx, r.ReqID); err != nil {
				// Losing a claim race (AlreadyClaimed) or a transient tx error
				// is not fatal to the pass — log at debug and move on.
				w.log.Debug("claim not landed", "reqId", r.ReqID, "error", err)
				continue
			}
			w.log.Info("claimed session request", "reqId", r.ReqID)
		}

		if err := w.cursor.Set(cursorSessionRequested, hi); err != nil {
			return err
		}
	}

	return nil
}

package release

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// reconcilerChain is the narrow chain interface the Reconciler needs. Defined
// at the consumer per Go conventions; chain.SettlementClient satisfies it.
type reconcilerChain interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	FilterJobCompleted(ctx context.Context, worker common.Address, fromBlock, toBlock uint64) ([]chain.JobCompletedEvent, error)
	GetJobState(ctx context.Context, jobID uint64) (chain.JobStateInfo, error)
}

// Reconciler scans the on-chain JobCompleted event log for events emitted by
// the local worker, populates the Store with any pending entries it does not
// yet know about, and advances the on-disk cursor.
//
// Reconciliation runs at three points:
//  1. Once at Service.New (best-effort; errors logged but non-fatal).
//  2. Periodically on cfg.ReconcileInterval (StartPeriodic / StopPeriodic).
//  3. On demand from the worker-cli `release` subcommand.
//
// The reconciler does NOT settle anything on-chain — it only updates local
// state. The scheduler picks up newly-added pending entries on its next tick.
type Reconciler struct {
	store      Store
	chain      reconcilerChain
	workerAddr common.Address
	cfg        Config
	logger     *slog.Logger

	// Periodic-mode lifecycle.
	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewReconciler constructs a Reconciler. None of the dependencies may be nil
// except logger (defaults to slog.Default()).
func NewReconciler(store Store, settlement reconcilerChain, workerAddr common.Address, cfg Config, logger *slog.Logger) *Reconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reconciler{
		store:      store,
		chain:      settlement,
		workerAddr: workerAddr,
		cfg:        cfg,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Run executes one reconciliation pass. Safe to call concurrently with
// StartPeriodic — both share the Store's flock.
func (r *Reconciler) Run(ctx context.Context) error {
	startCursor, err := r.store.GetReconcileBlock(ctx)
	if err != nil {
		return fmt.Errorf("read reconcile cursor: %w", err)
	}

	// Resume from cursor + 1. Use cfg.StartBlock when there is no cursor
	// AND a non-zero StartBlock is configured (operator-specified
	// deployment block). Otherwise start from block 1 (block 0 is genesis
	// and never has events).
	start := startCursor + 1
	if startCursor == 0 && r.cfg.StartBlock > 0 {
		start = r.cfg.StartBlock
	}

	head, err := r.chain.Head(ctx)
	if err != nil {
		return fmt.Errorf("read chain head: %w", err)
	}

	// Underflow guard. uint64 subtraction wraps when head < confirmations.
	// This happens on a freshly-launched local Anvil if the operator
	// somehow leaves Confirmations > 0 — better to no-op than to scan an
	// invalid range.
	if head.Number < r.cfg.Confirmations {
		r.logger.Debug("reconciler: chain head below confirmation depth; nothing to scan",
			"head", head.Number, "confirmations", r.cfg.Confirmations)
		return nil
	}
	safeHead := head.Number - r.cfg.Confirmations

	if start > safeHead {
		// Already caught up to the safe head.
		return nil
	}

	chunkSize := r.cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = 5000
	}

	added := 0
	scannedChunks := 0
	for lo := start; lo <= safeHead; {
		hi := lo + chunkSize - 1
		if hi > safeHead {
			hi = safeHead
		}

		events, fErr := r.chain.FilterJobCompleted(ctx, r.workerAddr, lo, hi)
		if fErr != nil {
			return fmt.Errorf("FilterJobCompleted [%d..%d]: %w", lo, hi, fErr)
		}

		for _, ev := range events {
			info, gErr := r.chain.GetJobState(ctx, ev.JobID)
			if gErr != nil {
				r.logger.Warn("reconciler: GetJobState failed; will retry on next pass",
					"job_id", ev.JobID, "err", gErr)
				continue
			}
			if info.Worker != r.workerAddr {
				// Belt-and-braces: filter is already worker-scoped, but
				// reaffirm before writing in case bindings change.
				continue
			}
			switch info.State {
			case chain.JobStateReleased, chain.JobStateTimedOut:
				// Already settled or slashed; nothing to do.
				continue
			case chain.JobStateResolved:
				if info.EscrowedFee == nil || info.EscrowedFee.Sign() == 0 {
					// Resolved-guilty path zeroed the escrow; releasing
					// would be a no-op that costs gas. Skip.
					continue
				}
			}
			if aErr := r.store.AddEligible(ctx, ev.JobID, info.CompletedAt); aErr != nil {
				return fmt.Errorf("AddEligible job %d: %w", ev.JobID, aErr)
			}
			added++
		}

		if sErr := r.store.SetReconcileBlock(ctx, hi); sErr != nil {
			return fmt.Errorf("SetReconcileBlock %d: %w", hi, sErr)
		}
		scannedChunks++

		if hi == safeHead {
			break
		}
		lo = hi + 1
	}

	r.logger.Info("reconciler: pass complete",
		"start", start, "safe_head", safeHead,
		"chunks", scannedChunks, "added", added)
	return nil
}

// StartPeriodic launches a background goroutine that calls Run on
// cfg.ReconcileInterval. Stop with StopPeriodic.
func (r *Reconciler) StartPeriodic(ctx context.Context) {
	if r.cfg.ReconcileInterval <= 0 {
		r.logger.Warn("reconciler: ReconcileInterval not positive; periodic mode disabled",
			"interval", r.cfg.ReconcileInterval)
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.cfg.ReconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := r.Run(ctx); err != nil {
					r.logger.Warn("reconciler: periodic pass failed",
						"err", err)
				}
			case <-r.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopPeriodic cancels the background goroutine and waits for it to exit.
// Idempotent.
func (r *Reconciler) StopPeriodic() {
	r.stopOnce.Do(func() {
		close(r.done)
	})
	r.wg.Wait()
}

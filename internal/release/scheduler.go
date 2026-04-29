package release

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// schedulerChain is the narrow chain interface the Scheduler needs. Defined
// at the consumer; chain.SettlementClient satisfies it.
type schedulerChain interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	GetDisputeWindow(ctx context.Context) (time.Duration, error)
	GetJobState(ctx context.Context, jobID uint64) (chain.JobStateInfo, error)
	ReleaseJob(ctx context.Context, jobID uint64) error
	ReleaseJobs(ctx context.Context, jobIDs []uint64) error
}

// Scheduler runs a background loop that periodically settles eligible jobs.
// It triggers on either a count threshold or an elapsed-time interval
// (whichever fires first), and falls back from batched to per-job releases
// when the batch tx reverts so a single stale job cannot jam the loop.
type Scheduler struct {
	store      Store
	chain      schedulerChain
	workerAddr common.Address
	cfg        Config
	logger     *slog.Logger
	metrics    Metrics

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewScheduler constructs a Scheduler. logger may be nil.
func NewScheduler(store Store, settlement schedulerChain, workerAddr common.Address, cfg Config, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{
		store:      store,
		chain:      settlement,
		workerAddr: workerAddr,
		cfg:        cfg,
		logger:     logger,
		metrics:    noopMetrics{},
		done:       make(chan struct{}),
	}
}

// SetMetrics installs the observability sink. Optional; defaults to a
// silent no-op. Must be called before Start.
func (s *Scheduler) SetMetrics(m Metrics) {
	s.metrics = callMetrics(m)
}

// Start launches the background goroutine. Returns immediately.
func (s *Scheduler) Start(ctx context.Context) {
	if s.cfg.ProbeInterval <= 0 {
		s.logger.Warn("scheduler: ProbeInterval not positive; not starting", "probe", s.cfg.ProbeInterval)
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.cfg.ProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.tick(ctx)
			case <-s.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop signals the background goroutine and waits for it to exit. Idempotent.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
	})
	s.wg.Wait()
}

// tick is the per-probe evaluation. Reads chain.Head once and decides
// whether to fire a release cycle.
func (s *Scheduler) tick(ctx context.Context) {
	cycleCtx, cancel := context.WithTimeout(ctx, s.cfg.TxTimeout)
	defer cancel()

	head, err := s.chain.Head(cycleCtx)
	if err != nil {
		s.logger.Warn("scheduler: read chain head failed", "err", err)
		return
	}

	window, err := s.disputeWindow(cycleCtx)
	if err != nil {
		s.logger.Warn("scheduler: read dispute window failed", "err", err)
		return
	}

	pending, err := s.store.Pending(cycleCtx)
	if err != nil {
		s.logger.Warn("scheduler: read pending failed", "err", err)
		return
	}
	s.metrics.SetPending(len(pending))

	lastReleaseTs, err := s.store.GetLastReleaseTs(cycleCtx)
	if err != nil {
		s.logger.Warn("scheduler: read last release ts failed", "err", err)
		return
	}

	eligibleNow := countEligible(pending, window, head.Timestamp)
	dueByTime := head.Timestamp-lastReleaseTs >= int64(s.cfg.Interval/time.Second)

	if eligibleNow < s.cfg.BatchThreshold && !dueByTime {
		return
	}

	s.logger.Debug("scheduler: firing release cycle",
		"eligible_now", eligibleNow,
		"pending_total", len(pending),
		"due_by_time", dueByTime,
		"head_block", head.Number,
		"head_timestamp", head.Timestamp,
	)

	s.runReleaseCycle(cycleCtx, head, window, pending)
}

// runReleaseCycle is the actual settlement work. Exposed for the CLI's
// `release` subcommand; the periodic loop calls it via tick.
//
// pending is the snapshot taken under shared lock by tick; passing it in
// avoids a second lock acquisition inside the cycle.
func (s *Scheduler) runReleaseCycle(ctx context.Context, head chain.HeadInfo, window time.Duration, pending []PendingJob) {
	candidates := s.collectCandidates(pending, window, head.Timestamp)
	if len(candidates) == 0 {
		// Still record the attempt so the time trigger does not fire
		// hot-loop on the next tick.
		s.recordAttempt(ctx, head.Timestamp)
		return
	}

	releaseBatch, dropIDs, staleDisputed := s.partition(ctx, candidates, window, head.Timestamp)

	for _, id := range staleDisputed {
		s.logger.Warn("scheduler: job has been Disputed for an unusually long time",
			"job_id", id, "stale_after", s.cfg.StaleDisputeWarnAfter)
	}

	if len(dropIDs) > 0 {
		if err := s.store.Remove(ctx, dropIDs); err != nil {
			s.logger.Warn("scheduler: remove terminal-state jobs failed",
				"count", len(dropIDs), "err", err)
		}
	}

	if len(releaseBatch) == 0 {
		s.recordAttempt(ctx, head.Timestamp)
		return
	}

	err := s.chain.ReleaseJobs(ctx, releaseBatch)
	if err == nil {
		if rErr := s.store.Remove(ctx, releaseBatch); rErr != nil {
			s.logger.Warn("scheduler: post-release Remove failed",
				"count", len(releaseBatch), "err", rErr)
		}
		s.metrics.IncReleased(len(releaseBatch))
		s.metrics.SetLastSuccessTimestamp(head.Timestamp)
		s.logger.Info("scheduler: batch release succeeded",
			"released", len(releaseBatch), "head_block", head.Number)
		s.recordAttempt(ctx, head.Timestamp)
		return
	}

	// Batch revert. Classify the error.
	if isPauseError(err) {
		s.metrics.IncPauseEvent()
		s.logger.Warn("scheduler: contract paused; backing off without per-job blame",
			"err", err, "back_off", s.cfg.PausedCycleBackoff)
		s.recordAttemptWithBackoff(ctx, head.Timestamp, s.cfg.PausedCycleBackoff)
		return
	}

	s.logger.Warn("scheduler: batch release reverted; falling back to per-job",
		"batch_size", len(releaseBatch), "err", err)
	s.perJobFallback(ctx, releaseBatch, window, head)
	s.recordAttempt(ctx, head.Timestamp)
}

// RunOnce executes a single release cycle synchronously. Used by the CLI
// `release` subcommand. Reads chain head + pending snapshot itself.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	cycleCtx, cancel := context.WithTimeout(ctx, s.cfg.TxTimeout)
	defer cancel()

	head, err := s.chain.Head(cycleCtx)
	if err != nil {
		return fmt.Errorf("read chain head: %w", err)
	}
	window, err := s.disputeWindow(cycleCtx)
	if err != nil {
		return fmt.Errorf("read dispute window: %w", err)
	}
	pending, err := s.store.Pending(cycleCtx)
	if err != nil {
		return fmt.Errorf("read pending: %w", err)
	}
	s.runReleaseCycle(cycleCtx, head, window, pending)
	return nil
}

// disputeWindow returns the contract's dispute window or the configured
// override. The override is intended for dev/test where AIConfig is not
// deployed.
func (s *Scheduler) disputeWindow(ctx context.Context) (time.Duration, error) {
	if s.cfg.DisputeWindowOverride > 0 {
		return s.cfg.DisputeWindowOverride, nil
	}
	return s.chain.GetDisputeWindow(ctx)
}

// collectCandidates filters pending jobs by chain-time eligibility and
// per-job backoff, sorts (already sorted by Store.Pending) and caps at
// MaxBatchSize.
func (s *Scheduler) collectCandidates(pending []PendingJob, window time.Duration, chainNow int64) []PendingJob {
	out := pending[:0:0]
	windowSec := int64(window / time.Second)
	for _, p := range pending {
		if p.BackoffUntil > chainNow {
			continue
		}
		if p.CompletedAt+windowSec > chainNow {
			continue
		}
		out = append(out, p)
		if len(out) >= s.cfg.MaxBatchSize {
			break
		}
	}
	return out
}

// partition resolves each candidate against on-chain state and bins it.
// Returns:
//
//	releaseBatch  — jobs to include in ReleaseJobs (and the per-job fallback)
//	dropIDs       — terminal-state jobs to delete from the pending set
//	staleDisputed — Disputed jobs older than StaleDisputeWarnAfter (warned but kept)
func (s *Scheduler) partition(ctx context.Context, candidates []PendingJob, window time.Duration, chainNow int64) ([]uint64, []uint64, []uint64) {
	var releaseBatch, dropIDs, staleDisputed []uint64
	windowSec := int64(window / time.Second)

	for _, p := range candidates {
		info, err := s.chain.GetJobState(ctx, p.JobID)
		if err != nil {
			s.logger.Warn("scheduler: GetJobState failed; deferring",
				"job_id", p.JobID, "err", err)
			continue
		}
		if info.Worker != s.workerAddr {
			s.logger.Warn("scheduler: pending job has foreign worker; dropping",
				"job_id", p.JobID, "stored_worker", info.Worker.Hex())
			dropIDs = append(dropIDs, p.JobID)
			s.metrics.IncDropped(DropReasonForeignWorker)
			continue
		}
		switch info.State {
		case chain.JobStateCompleted:
			if info.CompletedAt+windowSec <= chainNow {
				releaseBatch = append(releaseBatch, p.JobID)
			}
			// else: dispute window not elapsed by chain reckoning; leave
			// in pending without log noise.
		case chain.JobStateResolved:
			if info.EscrowedFee != nil && info.EscrowedFee.Sign() > 0 {
				releaseBatch = append(releaseBatch, p.JobID)
			} else {
				s.logger.Info("scheduler: dropping Resolved job with zero escrow (guilty path)",
					"job_id", p.JobID)
				dropIDs = append(dropIDs, p.JobID)
				s.metrics.IncDropped(DropReasonResolvedZeroFee)
			}
		case chain.JobStateReleased:
			dropIDs = append(dropIDs, p.JobID)
			s.metrics.IncDropped(DropReasonTerminalState)
		case chain.JobStateTimedOut:
			s.logger.Info("scheduler: dropping TimedOut job (slashed; no payout)",
				"job_id", p.JobID)
			dropIDs = append(dropIDs, p.JobID)
			s.metrics.IncDropped(DropReasonTerminalState)
		case chain.JobStateDisputed:
			if s.cfg.StaleDisputeWarnAfter > 0 {
				ageSec := chainNow - p.CompletedAt
				if time.Duration(ageSec)*time.Second > s.cfg.StaleDisputeWarnAfter {
					staleDisputed = append(staleDisputed, p.JobID)
				}
			}
			// Keep in pending; the dispute will resolve eventually.
		default:
			// Submitted / Acknowledged should be impossible — the pipeline
			// only marks eligible after CompleteJob succeeds.
			s.logger.Warn("scheduler: pending job in unexpected state",
				"job_id", p.JobID, "state", info.State.String())
		}
	}
	return releaseBatch, dropIDs, staleDisputed
}

// perJobFallback retries each job individually after a batch revert. State
// is re-read so a job that became ineligible between the batch and now is
// handled correctly.
func (s *Scheduler) perJobFallback(ctx context.Context, batch []uint64, window time.Duration, head chain.HeadInfo) {
	windowSec := int64(window / time.Second)
	released := 0
	failed := 0
	dropped := 0
	for _, id := range batch {
		info, err := s.chain.GetJobState(ctx, id)
		if err != nil {
			s.logger.Warn("scheduler: per-job GetJobState failed",
				"job_id", id, "err", err)
			s.recordPerJobFailure(ctx, id, head.Timestamp)
			failed++
			continue
		}
		eligible := false
		switch info.State {
		case chain.JobStateCompleted:
			eligible = info.CompletedAt+windowSec <= head.Timestamp
		case chain.JobStateResolved:
			eligible = info.EscrowedFee != nil && info.EscrowedFee.Sign() > 0
		case chain.JobStateReleased, chain.JobStateTimedOut:
			// Already terminal — drop and continue.
			if rErr := s.store.Remove(ctx, []uint64{id}); rErr != nil {
				s.logger.Warn("scheduler: per-job drop failed", "job_id", id, "err", rErr)
			} else {
				dropped++
			}
			continue
		}
		if !eligible {
			// Defer; do not increment fail_count (state-change is not
			// the job's "fault" from the scheduler's perspective).
			continue
		}
		if rErr := s.chain.ReleaseJob(ctx, id); rErr != nil {
			if isPauseError(rErr) {
				// Pause hit mid-fallback. Stop the per-job loop entirely;
				// resuming would just keep failing.
				s.metrics.IncPauseEvent()
				s.logger.Warn("scheduler: pause detected mid-fallback; stopping",
					"job_id", id, "err", rErr)
				return
			}
			s.logger.Warn("scheduler: per-job release failed",
				"job_id", id, "err", rErr)
			s.recordPerJobFailure(ctx, id, head.Timestamp)
			s.metrics.IncFailed(1)
			failed++
			continue
		}
		if rErr := s.store.Remove(ctx, []uint64{id}); rErr != nil {
			s.logger.Warn("scheduler: per-job remove failed", "job_id", id, "err", rErr)
		}
		s.metrics.IncReleased(1)
		s.metrics.SetLastSuccessTimestamp(head.Timestamp)
		released++
	}
	s.logger.Info("scheduler: per-job fallback complete",
		"released", released, "failed", failed, "dropped", dropped)
}

func (s *Scheduler) recordPerJobFailure(ctx context.Context, jobID uint64, chainNow int64) {
	// We need the current fail_count to compute backoff; read pending and
	// look it up. Cheap (in-memory, file-locked).
	pending, err := s.store.Pending(ctx)
	if err != nil {
		s.logger.Warn("scheduler: read pending for backoff failed",
			"job_id", jobID, "err", err)
		return
	}
	failCount := 0
	for _, p := range pending {
		if p.JobID == jobID {
			failCount = p.FailCount
			break
		}
	}
	backoff := computeBackoff(s.cfg.BackoffBase, s.cfg.BackoffMax, failCount+1)
	until := chainNow + int64(backoff/time.Second)
	if err := s.store.RecordFailure(ctx, jobID, until); err != nil {
		s.logger.Warn("scheduler: RecordFailure failed",
			"job_id", jobID, "err", err)
	}
}

func (s *Scheduler) recordAttempt(ctx context.Context, chainNow int64) {
	if err := s.store.SetLastReleaseTs(ctx, chainNow); err != nil {
		s.logger.Warn("scheduler: SetLastReleaseTs failed", "err", err)
	}
}

// recordAttemptWithBackoff sets LastReleaseTs into the future so the time
// trigger does not fire for `back` after `chainNow`. Used for cycle-level
// backoffs (e.g. paused contract).
func (s *Scheduler) recordAttemptWithBackoff(ctx context.Context, chainNow int64, back time.Duration) {
	until := chainNow + int64(back/time.Second)
	if err := s.store.SetLastReleaseTs(ctx, until); err != nil {
		s.logger.Warn("scheduler: SetLastReleaseTs (with backoff) failed",
			"backoff", back, "err", err)
	}
}

// countEligible returns the number of pending jobs whose dispute window
// has elapsed by chain time and whose backoff has expired.
func countEligible(pending []PendingJob, window time.Duration, chainNow int64) int {
	windowSec := int64(window / time.Second)
	n := 0
	for _, p := range pending {
		if p.BackoffUntil > chainNow {
			continue
		}
		if p.CompletedAt+windowSec > chainNow {
			continue
		}
		n++
	}
	return n
}

// computeBackoff returns base * 2^(attempt-1), capped at maxBackoff.
// attempt is 1-indexed (the first failure yields `base`, not `base/2`).
func computeBackoff(base, maxBackoff time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Guard against overflow: shift up to attempt-1, but cap the shift.
	const maxShift = 30 // 2^30 * 15min = ~30,000 years; anything more is meaningless
	shift := attempt - 1
	if shift > maxShift {
		shift = maxShift
	}
	d := base << shift
	if d > maxBackoff || d <= 0 { // overflow check
		return maxBackoff
	}
	return d
}

// isPauseError pattern-matches the contract's revert reason for "paused".
// OpenZeppelin Pausable emits "Pausable: paused"; the v5 version uses the
// custom error EnforcedPause(). go-ethereum surfaces both as text in the
// returned error. The match is case-insensitive and substring-based to
// tolerate wrapper layers.
func isPauseError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "pausable: paused") ||
		strings.Contains(msg, "enforcedpause")
}

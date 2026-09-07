package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
)

// ErrJobDoomed marks the point where the job's on-chain deadline makes
// settlement impossible: completeJob reverts DeadlineExceeded once
// block.timestamp > job.deadline (JobRegistry.sol), so every further stage
// — above all the stage-8a blob transaction — is guaranteed waste. The
// pipeline aborts with this error wrapped in asynq.SkipRetry: the job's
// terminal state is the keeper's settleStuckJob refund, and retries cannot
// change that.
var ErrJobDoomed = errors.New("job cannot settle before its on-chain deadline")

// noRetry marks err so asynq archives the task instead of retrying it.
// errors.Is(err, asynq.SkipRetry) survives the "stage N (...)" wrapping the
// caller applies. In gateway mode the gateway has no skip-retry signal, so
// the job is retried there regardless — acceptable because every no-retry
// path in this file aborts before doing expensive work, making the wasted
// retries cheap.
func noRetry(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", err, asynq.SkipRetry)
}

// JobDeadlineClient is the optional chain-client extension the deadline
// guard probes for, mirroring the AsyncAckClient pattern: a JobExecutionClient
// without it runs with the guard disabled, exactly as before the guard
// existed. Defined with plain types so this package stays free of any
// dependency on the chain package.
//
// The contract keeps a single job.deadline field whose meaning flips at
// acknowledgement: submittedAt + ackTimeout beforehand, ackTimestamp +
// completionTimeout after. Callers must therefore pair the deadline with
// the acknowledged flag.
type JobDeadlineClient interface {
	GetJobDeadline(ctx context.Context, jobID uint64) (deadline time.Time, acknowledged bool, err error)
}

// Default reserves used when HandlerConfig carries zero values (tests, CLI
// constructions). Keep in sync with the config package defaults.
//
// SettleReserve covers everything after generation ends: stage 6 encrypt,
// stage 7 publish, stage 8a blob tx (typical ~12 s at 2 s blocks) and stage
// 8b completeJob (1-2 blocks), plus margin for RPC jitter. CompletionReserve
// is the smaller floor required to start stage 8a: the blob tx plus
// completeJob must both land before the deadline.
const (
	defaultSettleReserve      = 25 * time.Second
	defaultCompletionReserve  = 12 * time.Second
	defaultMinInferenceBudget = 10 * time.Second
)

// deadlineGuard tracks one job's on-chain deadline and decides whether
// remaining work can still settle. One instance per processJob invocation;
// it re-reads the deadline at each gate because acknowledgeJob rewrites it
// (a retry sees the completion deadline, a fresh job the ack deadline).
//
// The guard fails open: a read error (RPC flake) disables that gate rather
// than risking a healthy job. Every abort path is wrapped in noRetry.
type deadlineGuard struct {
	client             JobDeadlineClient
	jobID              uint64
	settleReserve      time.Duration
	completionReserve  time.Duration
	minInferenceBudget time.Duration
}

// newDeadlineGuard returns the per-job guard, or nil when the guard is
// disabled by config or the chain client cannot report deadlines.
func (h *JobHandler) newDeadlineGuard(jobID uint64) *deadlineGuard {
	if !h.cfg.DeadlineGuardEnabled {
		return nil
	}
	client, ok := h.chainClient.(JobDeadlineClient)
	if !ok {
		return nil
	}
	settle := h.cfg.SettleReserve
	if settle <= 0 {
		settle = defaultSettleReserve
	}
	completion := h.cfg.CompletionReserve
	if completion <= 0 {
		completion = defaultCompletionReserve
	}
	minInference := h.cfg.MinInferenceBudget
	if minInference <= 0 {
		minInference = defaultMinInferenceBudget
	}
	return &deadlineGuard{
		client:             client,
		jobID:              jobID,
		settleReserve:      settle,
		completionReserve:  completion,
		minInferenceBudget: minInference,
	}
}

// read fetches the current on-chain deadline. ok=false means the read
// failed and the caller must proceed without the guard.
func (g *deadlineGuard) read(ctx context.Context, logger *slog.Logger, stage string) (deadline time.Time, acknowledged bool, ok bool) {
	deadline, acknowledged, err := g.client.GetJobDeadline(ctx, g.jobID)
	if err != nil {
		logger.Warn("deadline guard: read failed, proceeding without guard",
			"stage", stage,
			"jobID", g.jobID,
			"error", err,
		)
		return time.Time{}, false, false
	}
	return deadline, acknowledged, true
}

// checkPickup runs at stage 0, before the ack tx is broadcast. It aborts
// only on hard expiry — pre-ack the deadline bounds the acknowledgement
// itself, and a passed ack deadline means the ack would revert, so sending
// it would burn gas for nothing. For already-acknowledged jobs (retries,
// gateway redeliveries) it also aborts when the remaining completion window
// cannot possibly cover inference plus settlement.
func (g *deadlineGuard) checkPickup(ctx context.Context, logger *slog.Logger) error {
	if g == nil {
		return nil
	}
	deadline, acknowledged, ok := g.read(ctx, logger, "deadline_pickup")
	if !ok {
		return nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		logger.Warn("deadline guard: job deadline already passed at pickup; refusing to ack",
			"stage", "deadline_pickup",
			"jobID", g.jobID,
			"acknowledged", acknowledged,
			"deadline", deadline.Unix(),
		)
		return noRetry(fmt.Errorf("stage 1 (ack): %w: on-chain deadline %s passed %s ago",
			ErrJobDoomed, deadline.UTC().Format(time.RFC3339), (-remaining).Round(time.Second)))
	}
	if acknowledged && remaining <= g.settleReserve+g.minInferenceBudget {
		logger.Warn("deadline guard: acknowledged job has no viable settlement window at pickup",
			"stage", "deadline_pickup",
			"jobID", g.jobID,
			"remainingMs", remaining.Milliseconds(),
		)
		return noRetry(fmt.Errorf("stage 1 (ack): %w: %s remain of the completion deadline, need > %s",
			ErrJobDoomed, remaining.Round(time.Second), (g.settleReserve + g.minInferenceBudget).Round(time.Second)))
	}
	return nil
}

// gateInference runs before stage 5's expensive work (history build, model
// load, generation). It returns the context inference must run under: when
// the job is acknowledged, that context is clamped to deadline minus the
// settle reserve so a generation that cannot be followed by a timely blob
// tx is cut off instead of overshooting into a guaranteed DeadlineExceeded.
//
// When the job is not yet acknowledged on-chain (ack-overlap: the ack is
// still mining), the gate is skipped entirely — the completion deadline
// will be rewritten to ack+completionTimeout when the ack lands, so any
// remaining-time math against the stale ack deadline would abort healthy
// jobs. The pre-8a gate re-checks with the post-ack deadline.
func (g *deadlineGuard) gateInference(
	ctx context.Context,
	logger *slog.Logger,
) (context.Context, context.CancelFunc, error) {
	if g == nil {
		return ctx, func() {}, nil
	}
	deadline, acknowledged, ok := g.read(ctx, logger, "deadline_inference")
	if !ok || !acknowledged {
		return ctx, func() {}, nil
	}
	remaining := time.Until(deadline)
	need := g.settleReserve + g.minInferenceBudget
	if remaining <= need {
		logger.Warn("deadline guard: aborting before inference; window too small to settle",
			"stage", "deadline_inference",
			"jobID", g.jobID,
			"remainingMs", remaining.Milliseconds(),
			"requiredMs", need.Milliseconds(),
		)
		return ctx, func() {}, noRetry(fmt.Errorf("stage 5 (inference): %w: %s remain of the completion deadline, need > %s",
			ErrJobDoomed, remaining.Round(time.Second), need.Round(time.Second)))
	}
	infCtx, cancel := context.WithDeadline(ctx, deadline.Add(-g.settleReserve))
	logger.Info("deadline guard: inference window clamped",
		"stage", "deadline_inference",
		"jobID", g.jobID,
		"remainingMs", remaining.Milliseconds(),
		"inferenceBudgetMs", (remaining - g.settleReserve).Milliseconds(),
	)
	return infCtx, cancel, nil
}

// gateBlobSubmit runs before stage 8a submits the blob transaction — the
// most expensive irreversible step. A job that reaches this point with less
// than the completion reserve left cannot land both the blob tx and
// completeJob before the deadline, so the blob tx must not be burned. This
// gate is the load-bearing one for retries: a checkpoint hit skips the
// stage-5 gate entirely.
func (g *deadlineGuard) gateBlobSubmit(ctx context.Context, logger *slog.Logger) error {
	if g == nil {
		return nil
	}
	deadline, _, ok := g.read(ctx, logger, "deadline_blob_submit")
	if !ok {
		return nil
	}
	remaining := time.Until(deadline)
	if remaining <= g.completionReserve {
		logger.Warn("deadline guard: aborting before blob submit; settlement cannot land in time",
			"stage", "deadline_blob_submit",
			"jobID", g.jobID,
			"remainingMs", remaining.Milliseconds(),
			"reserveMs", g.completionReserve.Milliseconds(),
		)
		return noRetry(fmt.Errorf("stage 8 (submit blob): %w: %s remain of the completion deadline, need > %s",
			ErrJobDoomed, remaining.Round(time.Second), g.completionReserve.Round(time.Second)))
	}
	return nil
}

// isDoomedNow reports whether the on-chain deadline has already passed.
// Used to reclassify a failed completeJob: once the deadline is gone the
// revert is permanent (DeadlineExceeded), so retrying is pure waste.
func (g *deadlineGuard) isDoomedNow(ctx context.Context, logger *slog.Logger) bool {
	if g == nil {
		return false
	}
	deadline, _, ok := g.read(ctx, logger, "deadline_classify")
	return ok && time.Now().After(deadline)
}

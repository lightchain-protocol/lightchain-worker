package release

import (
	"context"
	"log/slog"
)

// Tracker is the pipeline-facing wrapper for marking jobs eligible for
// release. The pipeline package consumes it via a 1-method interface
// (defined at the consumer per the project's accept-interfaces convention)
// so that test doubles do not need to implement the full Store surface.
//
// Failures are surfaced to the caller; the pipeline is expected to log
// and continue. Missed writes are recovered by the periodic reconciler.
type Tracker struct {
	store  Store
	logger *slog.Logger
}

// NewTracker wraps a Store. logger may be nil (defaults to slog.Default()).
func NewTracker(store Store, logger *slog.Logger) *Tracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Tracker{store: store, logger: logger}
}

// MarkEligible records a freshly-completed job. completedAt is best-effort
// (typically time.Now().Unix() from the pipeline post-CompleteJob); the
// scheduler always re-reads the authoritative on-chain Job.completedAt
// before deciding to release, so a slightly off local timestamp can only
// delay release — it cannot cause a wrongful one.
func (t *Tracker) MarkEligible(ctx context.Context, jobID uint64, completedAt int64) error {
	return t.store.AddEligible(ctx, jobID, completedAt)
}

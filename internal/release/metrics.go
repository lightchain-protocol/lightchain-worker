package release

// Metrics is the optional observability surface the Scheduler and
// Reconciler emit to. Defined at the consumer (release) so that
// internal/metrics never imports release. Service wiring provides a
// thin adapter that bridges these methods to the Prometheus
// collectors in internal/metrics.
//
// Implementations MUST be safe for concurrent use; both the scheduler
// goroutine and the reconciler goroutine call into Metrics.
//
// All methods are no-ops when Metrics is nil — callers do not have to
// nil-check at every site (helpers in this package check once).
type Metrics interface {
	SetPending(n int)
	IncReleased(n int)
	IncFailed(n int)
	IncDropped(reason string)
	IncPauseEvent()
	SetLastSuccessTimestamp(ts int64)
	SetReconcileLastBlock(block uint64)
}

// Drop reasons used by the scheduler. Kept as constants so dashboards
// can pin exact label values.
const (
	DropReasonTerminalState   = "terminal_state"
	DropReasonForeignWorker   = "foreign_worker"
	DropReasonResolvedZeroFee = "resolved_zero_escrow"
)

// noopMetrics is the silent default. callMetrics() returns this when
// the caller passes nil so dispatch sites do not have to nil-check.
type noopMetrics struct{}

func (noopMetrics) SetPending(int)                {}
func (noopMetrics) IncReleased(int)               {}
func (noopMetrics) IncFailed(int)                 {}
func (noopMetrics) IncDropped(string)             {}
func (noopMetrics) IncPauseEvent()                {}
func (noopMetrics) SetLastSuccessTimestamp(int64) {}
func (noopMetrics) SetReconcileLastBlock(uint64)  {}

// callMetrics returns m or a no-op shim when m is nil.
func callMetrics(m Metrics) Metrics {
	if m == nil {
		return noopMetrics{}
	}
	return m
}

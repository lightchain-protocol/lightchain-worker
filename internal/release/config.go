package release

import (
	"fmt"
	"time"
)

// Config bundles every knob the release package consumes. Service wiring
// populates this from the global worker config (env vars). Defaults match
// the plan's documented values; Validate enforces the cross-field
// invariants (e.g. ProbeInterval ≤ Interval).
type Config struct {
	// Enabled gates the entire subsystem. When false, the pipeline still
	// writes to the Tracker but no scheduler or reconciler goroutine runs.
	Enabled bool

	// StatePath is the on-disk JSON file for the release Store.
	StatePath string

	// --- Scheduler ---

	// Interval is the time-trigger period: a release cycle fires after this
	// long since the last attempt regardless of pending count.
	Interval time.Duration

	// ProbeInterval is the inner tick at which the scheduler evaluates
	// whether to fire (threshold or time). Should be ≤ Interval.
	ProbeInterval time.Duration

	// BatchThreshold fires a release cycle when the count of eligible
	// pending jobs reaches this value (early trigger).
	BatchThreshold int

	// MaxBatchSize caps the number of jobs included in one ReleaseJobs tx.
	// The contract iterates without a built-in cap; large batches risk
	// gas-limit reverts.
	MaxBatchSize int

	// TxTimeout bounds each release-cycle transaction (broadcast +
	// WaitMined) so the scheduler goroutine cannot block indefinitely.
	TxTimeout time.Duration

	// --- Reconciler ---

	// StartBlock is the first block to scan when no on-disk cursor exists.
	// Operators set this to (or near) the JobRegistry deployment block.
	StartBlock uint64

	// ChunkSize bounds the [from..to] range passed to FilterJobCompleted
	// per call to keep RPC responses small.
	ChunkSize uint64

	// Confirmations is the number of blocks held back from `head` when
	// computing the safe upper bound of a reconciliation scan. Guards
	// against reorgs. Set to 0 on Anvil/devnet.
	Confirmations uint64

	// ReconcileInterval is the periodic background reconciliation period.
	ReconcileInterval time.Duration

	// --- Backoff ---

	// BackoffBase is the initial per-job backoff after a fallback ReleaseJob
	// failure. Subsequent failures double up to BackoffMax.
	BackoffBase time.Duration

	// BackoffMax caps per-job exponential backoff.
	BackoffMax time.Duration

	// PausedCycleBackoff is the cycle-level back-off when the contract
	// reports paused. Per-job fail_count is NOT incremented in this case
	// (it's the contract's fault, not the jobs).
	PausedCycleBackoff time.Duration

	// --- Misc ---

	// StaleDisputeWarnAfter logs a warning when a job has been in the
	// Disputed state for longer than this. Pure observability.
	StaleDisputeWarnAfter time.Duration

	// DisputeWindowOverride forces a dispute window value instead of
	// reading from AIConfig. Zero means "read from AIConfig" (the
	// production path). Non-zero is intended only for dev/test where the
	// AIConfig contract is not deployed.
	DisputeWindowOverride time.Duration

	// DisputeWindowCacheTTL controls how long the SettlementClient
	// memoizes AIConfig.getDisputeWindow().
	DisputeWindowCacheTTL time.Duration
}

// DefaultConfig returns the baseline values documented in the plan.
// Service wiring should override individual fields from env vars.
func DefaultConfig() Config {
	return Config{
		Enabled:               true,
		StatePath:             "./release_state.json",
		Interval:              8 * time.Hour,
		ProbeInterval:         5 * time.Minute,
		BatchThreshold:        20,
		MaxBatchSize:          50,
		TxTimeout:             120 * time.Second,
		StartBlock:            0,
		ChunkSize:             5000,
		Confirmations:         5,
		ReconcileInterval:     time.Hour,
		BackoffBase:           15 * time.Minute,
		BackoffMax:            24 * time.Hour,
		PausedCycleBackoff:    time.Hour,
		StaleDisputeWarnAfter: 168 * time.Hour, // 7 days
		DisputeWindowOverride: 0,
		DisputeWindowCacheTTL: 15 * time.Minute,
	}
}

// Validate returns a slice of error strings describing config violations.
// Returns nil when all checks pass.
func (c Config) Validate() []string {
	var errs []string
	if !c.Enabled {
		// Disabled config skips all field checks except StatePath, which the
		// pipeline still writes through Tracker → Store.
		if c.StatePath == "" {
			errs = append(errs, "StatePath must not be empty")
		}
		return errs
	}
	if c.StatePath == "" {
		errs = append(errs, "StatePath must not be empty")
	}
	if c.Interval <= 0 {
		errs = append(errs, "Interval must be positive")
	}
	if c.ProbeInterval <= 0 {
		errs = append(errs, "ProbeInterval must be positive")
	}
	if c.ProbeInterval > c.Interval {
		errs = append(errs, fmt.Sprintf("ProbeInterval (%s) must be ≤ Interval (%s)", c.ProbeInterval, c.Interval))
	}
	if c.BatchThreshold < 1 {
		errs = append(errs, "BatchThreshold must be ≥ 1")
	}
	if c.MaxBatchSize < 1 {
		errs = append(errs, "MaxBatchSize must be ≥ 1")
	}
	if c.TxTimeout <= 0 {
		errs = append(errs, "TxTimeout must be positive")
	}
	if c.ChunkSize == 0 {
		errs = append(errs, "ChunkSize must be ≥ 1")
	}
	if c.ReconcileInterval <= 0 {
		errs = append(errs, "ReconcileInterval must be positive")
	}
	if c.BackoffBase <= 0 {
		errs = append(errs, "BackoffBase must be positive")
	}
	if c.BackoffMax < c.BackoffBase {
		errs = append(errs, fmt.Sprintf("BackoffMax (%s) must be ≥ BackoffBase (%s)", c.BackoffMax, c.BackoffBase))
	}
	if c.PausedCycleBackoff <= 0 {
		errs = append(errs, "PausedCycleBackoff must be positive")
	}
	if c.DisputeWindowCacheTTL < 0 {
		errs = append(errs, "DisputeWindowCacheTTL must be ≥ 0")
	}
	return errs
}

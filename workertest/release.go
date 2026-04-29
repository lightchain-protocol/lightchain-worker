package workertest

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	workerchain "github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/release"
)

// ReleasePendingJob mirrors release.PendingJob so cross-module tests do not
// need to import the internal release package directly.
type ReleasePendingJob struct {
	JobID        uint64
	CompletedAt  int64
	FailCount    int
	BackoffUntil int64
}

// ReleaseTesterConfig is the public override surface for ReleaseTester. We
// do NOT embed release.Config because it lives in an internal package —
// external callers cannot name the type, which would make a public field
// of that type unusable.
//
// Any field left at its zero value is replaced by the wrapper's defaults
// (matching release.DefaultConfig() except StatePath, which defaults to
// filepath.Join(t.TempDir(), "release_state.json") when empty, and
// Confirmations / StartBlock / MaxBatchSize, which use Anvil-friendly
// defaults documented per field).
type ReleaseTesterConfig struct {
	StatePath             string        // empty → t.TempDir()/release_state.json
	Confirmations         uint64        // zero → 0 (Anvil default; production should set ≥ 5)
	StartBlock            uint64        // zero → 1
	ChunkSize             uint64        // zero → release.DefaultConfig().ChunkSize
	MaxBatchSize          int           // zero → 100
	BatchThreshold        int           // zero → release.DefaultConfig().BatchThreshold
	Interval              time.Duration // zero → release.DefaultConfig().Interval
	ProbeInterval         time.Duration // zero → release.DefaultConfig().ProbeInterval
	TxTimeout             time.Duration // zero → release.DefaultConfig().TxTimeout
	DisputeWindowOverride time.Duration // zero → 0 (read from AIConfig — production path)
	DisputeWindowCacheTTL time.Duration // zero → release.DefaultConfig().DisputeWindowCacheTTL
}

// ReleaseTesterOptions configures a ReleaseTester. SigningKey identifies
// the worker on-chain; the wrapper derives the worker address from it.
type ReleaseTesterOptions struct {
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address
	JobRegistryAddress    common.Address
	SigningKey            *ecdsa.PrivateKey
	GasPriceMultiplierBps int

	// Config is the public override surface; all fields optional.
	Config ReleaseTesterConfig
}

// ReleaseTester is the test-facing wrapper around the worker's release
// subsystem (Store + Scheduler + Reconciler) wired against a real chain.
// Lives in workertest so it can legally import internal/release and
// internal/chain — the orchestrator's e2e tests cannot import those
// packages directly per Go's internal-package visibility rule.
type ReleaseTester struct {
	workerAddr  common.Address
	chainClient *workerchain.ChainClient
	store       release.Store
	cfg         release.Config
	statePath   string
	identity    release.StoreIdentity
}

// NewReleaseTester constructs a ReleaseTester. Validates required options,
// dials the chain, opens the local state store with the given identity,
// and applies any non-zero overrides from opts.Config.
//
// The chain client and store are closed via t.Cleanup; callers should not
// call Close themselves unless they want deterministic teardown ordering.
func NewReleaseTester(t testing.TB, opts ReleaseTesterOptions) *ReleaseTester {
	t.Helper()

	if opts.RPCURL == "" {
		t.Fatal("workertest: RPC URL must not be empty")
	}
	if opts.ChainID <= 0 {
		t.Fatal("workertest: ChainID must be positive")
	}
	if opts.SigningKey == nil {
		t.Fatal("workertest: signing key must not be nil")
	}
	if opts.JobRegistryAddress == (common.Address{}) {
		t.Fatal("workertest: JobRegistryAddress must be set")
	}
	if opts.WorkerRegistryAddress == (common.Address{}) {
		t.Fatal("workertest: WorkerRegistryAddress must be set")
	}
	if opts.AIConfigAddress == (common.Address{}) {
		t.Fatal("workertest: AIConfigAddress must be set")
	}
	if opts.GasPriceMultiplierBps <= 0 {
		opts.GasPriceMultiplierBps = 11000
	}

	workerAddr := crypto.PubkeyToAddress(opts.SigningKey.PublicKey)

	chainClient, err := workerchain.NewChainClient(
		opts.RPCURL,
		opts.ChainID,
		opts.WorkerRegistryAddress,
		opts.AIConfigAddress,
		opts.JobRegistryAddress,
		opts.SigningKey,
		opts.GasPriceMultiplierBps,
		workerchain.NewSubpoolCoordinator(nil, 0),
		workerchain.NewStuckNonceTracker(),
	)
	if err != nil {
		t.Fatalf("workertest: create chain client: %v", err)
	}

	cfg := buildReleaseConfigForTester(t, opts.Config)
	chainClient.SetDisputeWindowCacheTTL(cfg.DisputeWindowCacheTTL)

	identity := release.StoreIdentity{
		ChainID:       uint64(opts.ChainID),
		JobRegistry:   opts.JobRegistryAddress,
		WorkerAddress: workerAddr,
	}

	store, err := release.NewFileStore(cfg.StatePath, identity, nil)
	if err != nil {
		chainClient.Close()
		t.Fatalf("workertest: open release store: %v", err)
	}

	tester := &ReleaseTester{
		workerAddr:  workerAddr,
		chainClient: chainClient,
		store:       store,
		cfg:         cfg,
		statePath:   cfg.StatePath,
		identity:    identity,
	}
	t.Cleanup(func() { _ = tester.Close() })
	return tester
}

// buildReleaseConfigForTester overlays the public ReleaseTesterConfig
// overrides on top of release.DefaultConfig(), then applies wrapper-specific
// defaults (StatePath under t.TempDir(), Anvil-friendly Confirmations=0,
// StartBlock=1, MaxBatchSize=100). Unexported because callers should
// configure via ReleaseTesterConfig, not by reaching into release.Config.
func buildReleaseConfigForTester(t testing.TB, override ReleaseTesterConfig) release.Config {
	t.Helper()
	cfg := release.DefaultConfig()
	cfg.Enabled = true
	cfg.Confirmations = 0
	cfg.StartBlock = 1
	cfg.MaxBatchSize = 100
	if override.StatePath == "" {
		cfg.StatePath = filepath.Join(t.TempDir(), "release_state.json")
	} else {
		cfg.StatePath = override.StatePath
	}
	if override.Confirmations != 0 {
		cfg.Confirmations = override.Confirmations
	}
	if override.StartBlock != 0 {
		cfg.StartBlock = override.StartBlock
	}
	if override.ChunkSize != 0 {
		cfg.ChunkSize = override.ChunkSize
	}
	if override.MaxBatchSize != 0 {
		cfg.MaxBatchSize = override.MaxBatchSize
	}
	if override.BatchThreshold != 0 {
		cfg.BatchThreshold = override.BatchThreshold
	}
	if override.Interval != 0 {
		cfg.Interval = override.Interval
	}
	if override.ProbeInterval != 0 {
		cfg.ProbeInterval = override.ProbeInterval
	}
	if override.TxTimeout != 0 {
		cfg.TxTimeout = override.TxTimeout
	}
	if override.DisputeWindowOverride != 0 {
		cfg.DisputeWindowOverride = override.DisputeWindowOverride
	}
	if override.DisputeWindowCacheTTL != 0 {
		cfg.DisputeWindowCacheTTL = override.DisputeWindowCacheTTL
	}
	return cfg
}

// WorkerAddress returns the address derived from the signing key.
func (r *ReleaseTester) WorkerAddress() common.Address {
	return r.workerAddr
}

// Reconcile runs one reconciliation pass. Constructs a fresh Reconciler per
// call (no goroutines started; cheap).
func (r *ReleaseTester) Reconcile(ctx context.Context) error {
	rec := release.NewReconciler(r.store, r.chainClient, r.workerAddr, r.cfg, nil)
	return rec.Run(ctx)
}

// RunCycle runs one release cycle synchronously. Bypasses trigger evaluation
// (Scheduler.RunOnce semantics) so back-to-back calls always proceed.
func (r *ReleaseTester) RunCycle(ctx context.Context) error {
	sched := release.NewScheduler(r.store, r.chainClient, r.workerAddr, r.cfg, nil)
	return sched.RunOnce(ctx)
}

// Pending returns a snapshot of the local pending set, sorted by JobID.
func (r *ReleaseTester) Pending(ctx context.Context) ([]ReleasePendingJob, error) {
	internal, err := r.store.Pending(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ReleasePendingJob, len(internal))
	for i, p := range internal {
		out[i] = ReleasePendingJob{
			JobID:        p.JobID,
			CompletedAt:  p.CompletedAt,
			FailCount:    p.FailCount,
			BackoffUntil: p.BackoffUntil,
		}
	}
	return out, nil
}

// ReconcileBlock returns the highest block number reconciled so far.
func (r *ReleaseTester) ReconcileBlock(ctx context.Context) (uint64, error) {
	return r.store.GetReconcileBlock(ctx)
}

// WorkerBalance reads the workerBalances mapping for any address. Most tests
// pass r.WorkerAddress(), but the API allows checking other addresses for
// negative-space assertions.
func (r *ReleaseTester) WorkerBalance(ctx context.Context, worker common.Address) (*big.Int, error) {
	return r.chainClient.WorkerBalance(ctx, worker)
}

// Withdraw drains the worker's full workerBalance to the worker address.
// Returns only error — submitPreparedTx does not surface receipts; tests
// should infer outcome from pre/post WorkerBalance and the WorkerWithdrawal
// event log filtered orchestrator-side.
func (r *ReleaseTester) Withdraw(ctx context.Context) error {
	return r.chainClient.Withdraw(ctx)
}

// DisputeWindow returns the current AIConfig.getDisputeWindow() value,
// converted to time.Duration. Tests should advance Anvil time by
// (DisputeWindow + small buffer) instead of hardcoding 86401s.
func (r *ReleaseTester) DisputeWindow(ctx context.Context) (time.Duration, error) {
	return r.chainClient.GetDisputeWindow(ctx)
}

// Head returns the latest block number and timestamp.
func (r *ReleaseTester) Head(ctx context.Context) (uint64, int64, error) {
	h, err := r.chainClient.Head(ctx)
	if err != nil {
		return 0, 0, err
	}
	return h.Number, h.Timestamp, nil
}

// ResetStore wipes the on-disk state and re-opens with the same identity.
// Strict ordering (per plan):
//  1. Close the current store (releases the flock descriptor).
//  2. Remove the data file.
//  3. Remove the .lock sidecar file.
//  4. Re-open via release.NewFileStore with the same path + identity.
//
// Single-process safe (these tests are always single-process); deleting
// the lock file before Close would race a held descriptor.
func (r *ReleaseTester) ResetStore(t testing.TB) {
	t.Helper()

	if err := r.store.Close(); err != nil {
		t.Fatalf("workertest: close release store before reset: %v", err)
	}
	if err := os.Remove(r.statePath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("workertest: remove release state file: %v", err)
	}
	if err := os.Remove(r.statePath + ".lock"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("workertest: remove release state lock file: %v", err)
	}

	store, err := release.NewFileStore(r.statePath, r.identity, nil)
	if err != nil {
		t.Fatalf("workertest: re-open release store after reset: %v", err)
	}
	r.store = store
}

// Close releases the store and chain client. Idempotent. Tests do not need
// to call this directly — NewReleaseTester registers a t.Cleanup hook.
func (r *ReleaseTester) Close() error {
	var firstErr error
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			firstErr = fmt.Errorf("close store: %w", err)
		}
		r.store = nil
	}
	if r.chainClient != nil {
		r.chainClient.Close()
		r.chainClient = nil
	}
	return firstErr
}

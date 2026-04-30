// Package release implements the worker's on-chain settlement loop:
// tracking jobs that have been completed but not yet released,
// reconciling against on-chain JobCompleted events at startup and
// periodically, and submitting batched releaseJob/releaseJobs transactions
// from a background scheduler.
//
// The package is deliberately Redis-free so it works identically for
// direct-mode workers (Asynq + Redis) and gateway-mode workers (which talk
// to a worker-gateway over WebSocket and have no Redis access). State is
// persisted to a single local JSON file that may be safely shared between
// the running sidecar and a concurrently-invoked worker-cli.
package release

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/sys/unix"
)

// SchemaVersion is the on-disk schema version for the release state file.
// Bump when changing field semantics in PendingJob or Snapshot — Load will
// then reject older files and the operator must run a migration or wipe.
const SchemaVersion = 1

// PendingJob is one entry in the local "completed but not yet released" set.
// CompletedAt is best-effort: the pipeline writer fills it from time.Now()
// since submitPreparedTx does not surface the receipt; the reconciler
// overwrites it with the authoritative on-chain value (Job.completedAt) on
// the next pass. The scheduler always re-reads on-chain state via
// GetJobState before submitting a release, so a slightly stale CompletedAt
// can only delay or pre-filter a release — it cannot cause a wrongful one.
type PendingJob struct {
	JobID        uint64 `json:"job_id"`
	CompletedAt  int64  `json:"completed_at"`  // unix seconds
	FailCount    int    `json:"fail_count"`    // increments on per-job release failure
	BackoffUntil int64  `json:"backoff_until"` // unix seconds; scheduler skips until chain time ≥ this
}

// Snapshot is the full on-disk document. Identity fields are checked at load
// time against the runtime config so a worker can never accidentally
// settle jobs from a different chain or different worker key.
type Snapshot struct {
	SchemaVersion       int          `json:"schema_version"`
	ChainID             uint64       `json:"chain_id"`
	JobRegistry         string       `json:"job_registry"`   // checksummed hex
	WorkerAddress       string       `json:"worker_address"` // checksummed hex
	Pending             []PendingJob `json:"pending"`
	LastReconciledBlock uint64       `json:"last_reconciled_block"`
	LastReleaseTs       int64        `json:"last_release_ts"`
}

// StoreIdentity scopes a Store to one (chain, contract, worker) triple.
// Mismatches between this and an existing on-disk Snapshot are fatal.
type StoreIdentity struct {
	ChainID       uint64
	JobRegistry   common.Address
	WorkerAddress common.Address
}

// ErrIdentityMismatch is returned by NewFileStore when an existing state
// file's identity fields do not match the configured StoreIdentity.
// Operators must investigate (likely a wrong RPC, wrong contract address,
// or wrong keystore) — silently overwriting would cause silent fund loss.
var ErrIdentityMismatch = errors.New("release store identity mismatch")

// ErrSchemaVersionMismatch is returned when the on-disk file was written by
// a future or removed schema version. Operators must migrate or wipe.
var ErrSchemaVersionMismatch = errors.New("release store schema version mismatch")

// ErrEmptySnapshot is returned by readSnapshotLocked when the data file
// exists but is zero bytes — distinct from os.ErrNotExist so post-init
// callers can fail loud instead of treating truncation as a clean
// bootstrap. The legitimate first-run path is created by NewFileStore
// under exclusive lock; any later read seeing an empty file means the
// state was lost (truncated, partially written, or manually edited).
var ErrEmptySnapshot = errors.New("release store snapshot is empty")

// Store is the persistence interface the scheduler, reconciler, and tracker
// share. Defined at the consumer per the project's accept-interfaces
// convention. All methods are safe for concurrent use, including across
// processes (e.g. sidecar and worker-cli mutating the same file).
type Store interface {
	// AddEligible records a completed job as awaiting release. Idempotent —
	// duplicate jobIDs are silently ignored so the reconciler can be re-run
	// freely. completedAt is the best-effort timestamp; the reconciler may
	// later overwrite it via Add (which currently only adds new entries —
	// existing entries keep their first-recorded timestamp).
	AddEligible(ctx context.Context, jobID uint64, completedAt int64) error

	// Pending returns a snapshot of all pending jobs sorted by JobID for
	// deterministic batching.
	Pending(ctx context.Context) ([]PendingJob, error)

	// Remove deletes the named jobIDs from the pending set. Missing IDs are
	// silently ignored.
	Remove(ctx context.Context, jobIDs []uint64) error

	// RecordFailure increments fail_count and sets backoff_until for one job.
	// The scheduler uses this for exponential per-job backoff after a
	// per-job ReleaseJob failure.
	RecordFailure(ctx context.Context, jobID uint64, backoffUntil int64) error

	// GetReconcileBlock returns the highest block number we have safely
	// scanned for JobCompleted events. The reconciler resumes from this + 1.
	GetReconcileBlock(ctx context.Context) (uint64, error)

	// SetReconcileBlock advances the cursor.
	SetReconcileBlock(ctx context.Context, block uint64) error

	// GetLastReleaseTs returns the chain timestamp of the last attempted
	// release cycle. The scheduler uses this for the time-based trigger.
	GetLastReleaseTs(ctx context.Context) (int64, error)

	// SetLastReleaseTs records a cycle attempt (regardless of outcome).
	SetLastReleaseTs(ctx context.Context, ts int64) error

	// Close releases any retained OS resources. After Close, all other
	// methods return an error.
	Close() error
}

// FileStore is the local-file implementation of Store. It uses a separate
// `<path>.lock` file as the flock target so the data file can be replaced
// atomically via temp+rename without invalidating the lock.
type FileStore struct {
	dataPath string
	lockPath string
	identity StoreIdentity
	logger   *slog.Logger

	// mu guards the lockFile descriptor across concurrent in-process
	// callers. Without it, two goroutines could race to flock the same file
	// descriptor and one would unlock the other prematurely. The OS-level
	// flock still serializes against other processes.
	mu       sync.Mutex
	lockFile *os.File // long-lived; held for Store lifetime, locked per-op
	closed   bool
}

// NewFileStore opens (or creates) the state file at path. The directory is
// created if missing. Returns ErrIdentityMismatch when an existing file's
// chain_id/job_registry/worker_address do not match identity, and
// ErrSchemaVersionMismatch when the schema differs.
func NewFileStore(path string, identity StoreIdentity, logger *slog.Logger) (*FileStore, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if path == "" {
		return nil, fmt.Errorf("release store path must not be empty")
	}
	if identity.ChainID == 0 {
		return nil, fmt.Errorf("release store identity.ChainID must be non-zero")
	}
	if identity.WorkerAddress == (common.Address{}) {
		return nil, fmt.Errorf("release store identity.WorkerAddress must be non-zero")
	}
	if identity.JobRegistry == (common.Address{}) {
		return nil, fmt.Errorf("release store identity.JobRegistry must be non-zero")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create release store dir %q: %w", dir, err)
	}

	lockPath := path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file %q: %w", lockPath, err)
	}

	s := &FileStore{
		dataPath: path,
		lockPath: lockPath,
		identity: identity,
		logger:   logger,
		lockFile: lockFile,
	}

	// Verify identity (or initialize file if it does not exist) under an
	// exclusive lock so concurrent NewFileStore calls in two processes do
	// not race on initial write.
	if err := s.withExclusiveLock(func() error {
		snap, err := s.readSnapshotLocked()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return s.writeSnapshotLocked(s.emptySnapshot())
			}
			return err
		}
		return s.verifyIdentityLocked(snap)
	}); err != nil {
		_ = lockFile.Close()
		return nil, err
	}

	return s, nil
}

// Close releases the lock-file descriptor. Idempotent.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.lockFile != nil {
		err := s.lockFile.Close()
		s.lockFile = nil
		if err != nil {
			return fmt.Errorf("close release store lock file: %w", err)
		}
	}
	return nil
}

func (s *FileStore) emptySnapshot() Snapshot {
	return Snapshot{
		SchemaVersion: SchemaVersion,
		ChainID:       s.identity.ChainID,
		JobRegistry:   s.identity.JobRegistry.Hex(),
		WorkerAddress: s.identity.WorkerAddress.Hex(),
		Pending:       []PendingJob{},
	}
}

// withSharedLock runs fn with the lock file held LOCK_SH (read-only).
func (s *FileStore) withSharedLock(fn func() error) error {
	return s.withLock(unix.LOCK_SH, fn)
}

// withExclusiveLock runs fn with the lock file held LOCK_EX (read-modify-write).
func (s *FileStore) withExclusiveLock(fn func() error) error {
	return s.withLock(unix.LOCK_EX, fn)
}

func (s *FileStore) withLock(lockType int, fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.lockFile == nil {
		return fmt.Errorf("release store closed")
	}
	fd := int(s.lockFile.Fd())
	if err := unix.Flock(fd, lockType); err != nil {
		return fmt.Errorf("flock(%d): %w", lockType, err)
	}
	defer func() {
		_ = unix.Flock(fd, unix.LOCK_UN)
	}()
	return fn()
}

// readSnapshotLocked reads and parses the data file. Caller must hold the
// flock. Returns os.ErrNotExist (from os.ReadFile) when the file is
// absent — only NewFileStore should treat that as the first-run signal;
// every other caller must propagate it. Returns ErrEmptySnapshot for a
// zero-byte file so that post-init truncation is not silently treated as
// a fresh start.
func (s *FileStore) readSnapshotLocked() (Snapshot, error) {
	data, err := os.ReadFile(s.dataPath)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) == 0 {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrEmptySnapshot, s.dataPath)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("unmarshal release state %q: %w", s.dataPath, err)
	}
	return snap, nil
}

// writeSnapshotLocked atomically replaces the data file. Caller must hold
// the flock. Uses temp + Sync + Rename + parent-dir fsync for crash safety.
func (s *FileStore) writeSnapshotLocked(snap Snapshot) error {
	// Always pin the schema/identity fields on write so manual edits or
	// migrations cannot accidentally drop them.
	snap.SchemaVersion = SchemaVersion
	snap.ChainID = s.identity.ChainID
	snap.JobRegistry = s.identity.JobRegistry.Hex()
	snap.WorkerAddress = s.identity.WorkerAddress.Hex()
	if snap.Pending == nil {
		snap.Pending = []PendingJob{}
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal release state: %w", err)
	}

	dir := filepath.Dir(s.dataPath)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.dataPath)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp release state: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("write temp release state: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return fmt.Errorf("chmod temp release state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temp release state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp release state: %w", err)
	}
	if err := os.Rename(tmpPath, s.dataPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename release state: %w", err)
	}

	// Fsync the parent directory so the rename is durable across crashes.
	// Failure here is logged but non-fatal: the rename itself succeeded; on
	// some filesystems (and in tests using tmpfs) fsync of a directory
	// returns EINVAL or is a no-op.
	if dirF, dErr := os.Open(dir); dErr == nil {
		_ = dirF.Sync()
		_ = dirF.Close()
	}

	return nil
}

// verifyIdentityLocked rejects on-disk state that does not match the
// configured StoreIdentity. Caller must hold the flock.
func (s *FileStore) verifyIdentityLocked(snap Snapshot) error {
	if snap.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: file=%d expected=%d", ErrSchemaVersionMismatch, snap.SchemaVersion, SchemaVersion)
	}
	if snap.ChainID != s.identity.ChainID {
		return fmt.Errorf("%w: chain_id file=%d expected=%d", ErrIdentityMismatch, snap.ChainID, s.identity.ChainID)
	}
	if !addrEqualHex(snap.JobRegistry, s.identity.JobRegistry) {
		return fmt.Errorf("%w: job_registry file=%s expected=%s",
			ErrIdentityMismatch, snap.JobRegistry, s.identity.JobRegistry.Hex())
	}
	if !addrEqualHex(snap.WorkerAddress, s.identity.WorkerAddress) {
		return fmt.Errorf("%w: worker_address file=%s expected=%s",
			ErrIdentityMismatch, snap.WorkerAddress, s.identity.WorkerAddress.Hex())
	}
	return nil
}

// addrEqualHex compares a stored hex string to a common.Address. We
// case-insensitively normalize through HexToAddress so checksum casing
// differences (or missing 0x prefix) don't trigger false mismatches.
func addrEqualHex(stored string, want common.Address) bool {
	if !common.IsHexAddress(stored) {
		return false
	}
	return common.HexToAddress(stored) == want
}

// loadVerifiedLocked is the read-modify-write helper used by mutators.
// Caller must hold an exclusive flock. Post-init the data file must
// always exist and be non-empty — silently re-initializing on
// missing/empty would drop pending jobs, the reconcile cursor, and
// LastReleaseTs after a truncation or accidental delete. Fail loud so
// the operator can investigate; only NewFileStore creates the file.
func (s *FileStore) loadVerifiedLocked() (Snapshot, error) {
	snap, err := s.readSnapshotLocked()
	if err != nil {
		return Snapshot{}, err
	}
	if err := s.verifyIdentityLocked(snap); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// AddEligible records a completed job. Idempotent: duplicates are dropped,
// preserving the first-recorded CompletedAt and any accumulated FailCount.
func (s *FileStore) AddEligible(_ context.Context, jobID uint64, completedAt int64) error {
	return s.withExclusiveLock(func() error {
		snap, err := s.loadVerifiedLocked()
		if err != nil {
			return err
		}
		for _, p := range snap.Pending {
			if p.JobID == jobID {
				return nil
			}
		}
		snap.Pending = append(snap.Pending, PendingJob{
			JobID:       jobID,
			CompletedAt: completedAt,
		})
		return s.writeSnapshotLocked(snap)
	})
}

// Pending returns a sorted snapshot of the pending set. Post-init the
// data file must exist and be non-empty; missing/empty errors propagate
// rather than masquerading as "no pending jobs", which would otherwise
// silently disable the scheduler.
func (s *FileStore) Pending(_ context.Context) ([]PendingJob, error) {
	var out []PendingJob
	err := s.withSharedLock(func() error {
		snap, err := s.readSnapshotLocked()
		if err != nil {
			return err
		}
		if vErr := s.verifyIdentityLocked(snap); vErr != nil {
			return vErr
		}
		out = make([]PendingJob, len(snap.Pending))
		copy(out, snap.Pending)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out, nil
}

// Remove deletes the named jobIDs.
func (s *FileStore) Remove(_ context.Context, jobIDs []uint64) error {
	if len(jobIDs) == 0 {
		return nil
	}
	drop := make(map[uint64]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		drop[id] = struct{}{}
	}
	return s.withExclusiveLock(func() error {
		snap, err := s.loadVerifiedLocked()
		if err != nil {
			return err
		}
		filtered := snap.Pending[:0]
		for _, p := range snap.Pending {
			if _, removed := drop[p.JobID]; !removed {
				filtered = append(filtered, p)
			}
		}
		snap.Pending = filtered
		return s.writeSnapshotLocked(snap)
	})
}

// RecordFailure increments fail_count and updates backoff_until.
func (s *FileStore) RecordFailure(_ context.Context, jobID uint64, backoffUntil int64) error {
	return s.withExclusiveLock(func() error {
		snap, err := s.loadVerifiedLocked()
		if err != nil {
			return err
		}
		found := false
		for i := range snap.Pending {
			if snap.Pending[i].JobID == jobID {
				snap.Pending[i].FailCount++
				snap.Pending[i].BackoffUntil = backoffUntil
				found = true
				break
			}
		}
		if !found {
			// Job is not in pending — likely a CLI race or someone else
			// settled it. Log and treat as no-op rather than error.
			s.logger.Debug("RecordFailure called for unknown job",
				"job_id", jobID, "backoff_until", backoffUntil)
			return nil
		}
		return s.writeSnapshotLocked(snap)
	})
}

// GetReconcileBlock returns the cursor. Missing/empty state propagates
// as an error rather than returning 0, which would otherwise cause the
// reconciler to silently rescan from genesis.
func (s *FileStore) GetReconcileBlock(_ context.Context) (uint64, error) {
	var out uint64
	err := s.withSharedLock(func() error {
		snap, err := s.readSnapshotLocked()
		if err != nil {
			return err
		}
		if vErr := s.verifyIdentityLocked(snap); vErr != nil {
			return vErr
		}
		out = snap.LastReconciledBlock
		return nil
	})
	return out, err
}

// SetReconcileBlock advances the cursor (only forward — refuses to go
// backwards, which would cause double-scanning and is almost certainly a
// caller bug).
func (s *FileStore) SetReconcileBlock(_ context.Context, block uint64) error {
	return s.withExclusiveLock(func() error {
		snap, err := s.loadVerifiedLocked()
		if err != nil {
			return err
		}
		if block < snap.LastReconciledBlock {
			return fmt.Errorf("SetReconcileBlock: refusing to move cursor backward (current=%d new=%d)",
				snap.LastReconciledBlock, block)
		}
		snap.LastReconciledBlock = block
		return s.writeSnapshotLocked(snap)
	})
}

// GetLastReleaseTs returns the cycle-attempt timestamp. Missing/empty
// state propagates as an error rather than returning 0, which would
// otherwise cause the time-trigger to fire immediately.
func (s *FileStore) GetLastReleaseTs(_ context.Context) (int64, error) {
	var out int64
	err := s.withSharedLock(func() error {
		snap, err := s.readSnapshotLocked()
		if err != nil {
			return err
		}
		if vErr := s.verifyIdentityLocked(snap); vErr != nil {
			return vErr
		}
		out = snap.LastReleaseTs
		return nil
	})
	return out, err
}

// SetLastReleaseTs records a cycle attempt.
func (s *FileStore) SetLastReleaseTs(_ context.Context, ts int64) error {
	return s.withExclusiveLock(func() error {
		snap, err := s.loadVerifiedLocked()
		if err != nil {
			return err
		}
		snap.LastReleaseTs = ts
		return s.writeSnapshotLocked(snap)
	})
}

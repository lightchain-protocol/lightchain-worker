package release

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultIdentityForTest() StoreIdentity {
	return StoreIdentity{
		ChainID:       1337,
		JobRegistry:   common.HexToAddress("0x1111111111111111111111111111111111111111"),
		WorkerAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
	}
}

func newStore(t *testing.T) (*FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "release_state.json")
	store, err := NewFileStore(path, defaultIdentityForTest(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func TestNewFileStore_FirstRunCreatesFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "release_state.json")
	store, err := NewFileStore(path, defaultIdentityForTest(), nil)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	_, err = os.Stat(path)
	require.NoError(t, err, "data file should exist after NewFileStore")
}

func TestAddEligible_RoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, store.AddEligible(ctx, 1, 100))
	require.NoError(t, store.AddEligible(ctx, 3, 300))
	require.NoError(t, store.AddEligible(ctx, 2, 200))

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 3)

	assert.Equal(t, uint64(1), pending[0].JobID)
	assert.Equal(t, int64(100), pending[0].CompletedAt)
	assert.Equal(t, uint64(2), pending[1].JobID)
	assert.Equal(t, uint64(3), pending[2].JobID)
}

func TestAddEligible_IsIdempotent(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, store.AddEligible(ctx, 42, 1000))
	require.NoError(t, store.AddEligible(ctx, 42, 9999))

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, int64(1000), pending[0].CompletedAt, "first-recorded CompletedAt must be preserved")
}

func TestRemove_DropsNamedJobs(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	for _, id := range []uint64{1, 2, 3, 4, 5} {
		require.NoError(t, store.AddEligible(ctx, id, int64(id)))
	}

	require.NoError(t, store.Remove(ctx, []uint64{2, 4, 99}))

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 3)
	ids := []uint64{pending[0].JobID, pending[1].JobID, pending[2].JobID}
	assert.Equal(t, []uint64{1, 3, 5}, ids)
}

func TestRemove_EmptyIsNoop(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, store.AddEligible(ctx, 1, 100))
	require.NoError(t, store.Remove(ctx, nil))
	require.NoError(t, store.Remove(ctx, []uint64{}))

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
}

func TestRecordFailure_IncrementsAndSetsBackoff(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, store.AddEligible(ctx, 7, 100))
	require.NoError(t, store.RecordFailure(ctx, 7, 5000))
	require.NoError(t, store.RecordFailure(ctx, 7, 15000))

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, 2, pending[0].FailCount)
	assert.Equal(t, int64(15000), pending[0].BackoffUntil)
}

func TestRecordFailure_UnknownJobIsNoop(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	require.NoError(t, store.RecordFailure(ctx, 999, 5000))
}

func TestReconcileBlock_RoundTripAndForwardOnly(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	got, err := store.GetReconcileBlock(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got, "initial cursor is zero")

	require.NoError(t, store.SetReconcileBlock(ctx, 100))
	require.NoError(t, store.SetReconcileBlock(ctx, 200))

	got, err = store.GetReconcileBlock(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), got)

	err = store.SetReconcileBlock(ctx, 150)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backward")
}

func TestLastReleaseTs_RoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	got, err := store.GetLastReleaseTs(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)

	require.NoError(t, store.SetLastReleaseTs(ctx, 1234567890))
	got, err = store.GetLastReleaseTs(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1234567890), got)
}

func TestNewFileStore_RejectsIdentityMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "release_state.json")

	first, err := NewFileStore(path, defaultIdentityForTest(), nil)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	bad := defaultIdentityForTest()
	bad.WorkerAddress = common.HexToAddress("0x9999999999999999999999999999999999999999")
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIdentityMismatch)

	bad = defaultIdentityForTest()
	bad.ChainID = 2
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIdentityMismatch)

	bad = defaultIdentityForTest()
	bad.JobRegistry = common.HexToAddress("0x8888888888888888888888888888888888888888")
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIdentityMismatch)
}

func TestNewFileStore_RejectsSchemaMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "release_state.json")

	identity := defaultIdentityForTest()
	bad := Snapshot{
		SchemaVersion: SchemaVersion + 1,
		ChainID:       identity.ChainID,
		JobRegistry:   identity.JobRegistry.Hex(),
		WorkerAddress: identity.WorkerAddress.Hex(),
		Pending:       []PendingJob{},
	}
	data, err := json.Marshal(bad)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	_, err = NewFileStore(path, identity, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSchemaVersionMismatch)
}

func TestNewFileStore_RejectsEmptyOrZeroIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")

	_, err := NewFileStore("", defaultIdentityForTest(), nil)
	require.Error(t, err)

	bad := defaultIdentityForTest()
	bad.ChainID = 0
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)

	bad = defaultIdentityForTest()
	bad.WorkerAddress = common.Address{}
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)

	bad = defaultIdentityForTest()
	bad.JobRegistry = common.Address{}
	_, err = NewFileStore(path, bad, nil)
	require.Error(t, err)
}

func TestClose_IsIdempotent(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())

	err := store.AddEligible(context.Background(), 1, 100)
	require.Error(t, err)
}

func TestStore_InProcessConcurrentGoroutines(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	const goroutines = 32
	const perGoroutine = 25
	const expected = goroutines * perGoroutine

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		base := uint64(g * perGoroutine)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if err := store.AddEligible(ctx, base+uint64(i), int64(base+uint64(i))); err != nil {
					t.Errorf("AddEligible failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	pending, err := store.Pending(ctx)
	require.NoError(t, err)
	assert.Len(t, pending, expected, "every concurrent AddEligible must be persisted")
}

var _ = errors.Is

// --- W2: post-init missing/empty state must fail loud across every read
// path (Pending, GetReconcileBlock, GetLastReleaseTs) and mutator. The
// legitimate first-run path lives in NewFileStore; everything after that
// treats missing/empty as corruption so settlement state is never
// silently wiped.

func TestPostInit_MissingFile_AllReadsAndMutatorsFail(t *testing.T) {
	t.Parallel()
	store, path := newStore(t)
	ctx := context.Background()
	require.NoError(t, os.Remove(path))

	_, err := store.Pending(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, err = store.GetReconcileBlock(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, err = store.GetLastReleaseTs(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	require.Error(t, store.AddEligible(ctx, 1, 100))
	require.Error(t, store.Remove(ctx, []uint64{1}))
	require.Error(t, store.RecordFailure(ctx, 1, 200))
	require.Error(t, store.SetReconcileBlock(ctx, 5))
	require.Error(t, store.SetLastReleaseTs(ctx, 1234))
}

// W4a: NextAllowedAttempt is a separate cycle-backoff gate; round-trip
// it to ensure the new field persists across reads.
func TestNextAllowedAttempt_RoundTrip(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)
	ctx := context.Background()

	got, err := store.GetNextAllowedAttempt(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got, "fresh store starts with zero gate")

	require.NoError(t, store.SetNextAllowedAttempt(ctx, 1700000000))

	got, err = store.GetNextAllowedAttempt(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1700000000), got)

	// Setting LastReleaseTs must not clobber NextAllowedAttempt — they
	// are deliberately distinct fields.
	require.NoError(t, store.SetLastReleaseTs(ctx, 9999))
	got, err = store.GetNextAllowedAttempt(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1700000000), got)
}

// W4a: schema v1 files (pre-NextAllowedAttempt) must be rejected so
// operators are forced to migrate or wipe rather than silently lose
// the new gate semantics on first run after upgrade.
func TestNewFileStore_RejectsLegacySchemaV1(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "release_state.json")

	identity := defaultIdentityForTest()
	v1 := Snapshot{
		SchemaVersion: 1,
		ChainID:       identity.ChainID,
		JobRegistry:   identity.JobRegistry.Hex(),
		WorkerAddress: identity.WorkerAddress.Hex(),
		Pending:       []PendingJob{},
	}
	data, err := json.Marshal(v1)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	_, err = NewFileStore(path, identity, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSchemaVersionMismatch)
}

func TestPostInit_EmptyFile_DistinctFromMissing(t *testing.T) {
	t.Parallel()
	store, path := newStore(t)
	ctx := context.Background()
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	_, err := store.Pending(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptySnapshot, "empty file must surface ErrEmptySnapshot, not os.ErrNotExist")
	assert.NotErrorIs(t, err, os.ErrNotExist, "empty file must NOT alias missing-file")

	require.Error(t, store.AddEligible(ctx, 1, 100))
	require.Error(t, store.SetReconcileBlock(ctx, 5))
}

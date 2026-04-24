package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCheckpointStore(t *testing.T) (*CheckpointStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return NewCheckpointStore(client, 2*time.Hour, 10*time.Minute, 256*1024), mr
}

// TestCheckpointStore_GetMiss returns an empty checkpoint on a fresh store.
func TestCheckpointStore_GetMiss(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)

	ckpt, err := store.Get(context.Background(), 42)
	require.NoError(t, err)
	assert.True(t, ckpt.IsEmpty())
	assert.False(t, ckpt.HasCiphertext())
	assert.False(t, ckpt.HasVersionedHash())
}

// TestCheckpointStore_SetCiphertextIfAbsent_FirstWriterWins is the
// single-goroutine version of the race-resolution contract.
func TestCheckpointStore_SetCiphertextIfAbsent_FirstWriterWins(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)
	ctx := context.Background()

	canonical1, wasSet1, err := store.SetCiphertextIfAbsent(ctx, 1, []byte("first"))
	require.NoError(t, err)
	assert.True(t, wasSet1, "first call must set")
	assert.Equal(t, []byte("first"), canonical1)

	canonical2, wasSet2, err := store.SetCiphertextIfAbsent(ctx, 1, []byte("second"))
	require.NoError(t, err)
	assert.False(t, wasSet2, "second call must NOT set")
	assert.Equal(t, []byte("first"), canonical2,
		"second caller must observe the canonical (first) ciphertext")
}

// TestCheckpointStore_SetCiphertextIfAbsent_Concurrent asserts that under
// N concurrent callers with distinct proposals, exactly ONE wins the
// SETNX and all others observe the same canonical value.
func TestCheckpointStore_SetCiphertextIfAbsent_Concurrent(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)
	ctx := context.Background()

	const goroutines = 8
	proposals := make([][]byte, goroutines)
	for i := range proposals {
		proposals[i] = []byte{byte('a' + i)}
	}

	type result struct {
		canonical []byte
		wasSet    bool
		err       error
	}
	results := make([]result, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			c, s, e := store.SetCiphertextIfAbsent(ctx, 99, proposals[idx])
			results[idx] = result{canonical: c, wasSet: s, err: e}
		}(i)
	}
	wg.Wait()

	var setCount int
	var canonical []byte
	for i, r := range results {
		require.NoError(t, r.err, "goroutine %d", i)
		if r.wasSet {
			setCount++
			canonical = r.canonical
		}
	}
	assert.Equal(t, 1, setCount, "exactly one concurrent caller must win SETNX")

	// All losers must observe the same canonical value.
	for i, r := range results {
		if !r.wasSet {
			assert.Equal(t, canonical, r.canonical,
				"loser %d must observe canonical ciphertext", i)
		}
	}
}

// TestCheckpointStore_SetCiphertextIfAbsent_RejectsOversized returns
// ErrCiphertextTooLarge and does not write anything.
func TestCheckpointStore_SetCiphertextIfAbsent_RejectsOversized(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = client.Close() }()
	store := NewCheckpointStore(client, 2*time.Hour, 10*time.Minute, 10)

	_, _, err := store.SetCiphertextIfAbsent(context.Background(), 1, make([]byte, 100))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCiphertextTooLarge))

	// No record should be present.
	ckpt, err := store.Get(context.Background(), 1)
	require.NoError(t, err)
	assert.True(t, ckpt.IsEmpty())
}

// TestCheckpointStore_SetVersionedHash_AfterCiphertext confirms hash and
// ciphertext coexist and survive a Get round-trip.
func TestCheckpointStore_SetVersionedHash_AfterCiphertext(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)
	ctx := context.Background()
	hash := common.HexToHash("0xabc123")

	_, _, err := store.SetCiphertextIfAbsent(ctx, 7, []byte("ct"))
	require.NoError(t, err)
	require.NoError(t, store.SetVersionedHash(ctx, 7, hash))

	ckpt, err := store.Get(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, []byte("ct"), ckpt.Ciphertext)
	assert.Equal(t, hash, ckpt.VersionedHash)
	assert.True(t, ckpt.HasCiphertext())
	assert.True(t, ckpt.HasVersionedHash())
}

// TestCheckpointStore_MarkDelivered toggles the delivered flag.
func TestCheckpointStore_MarkDelivered(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)
	ctx := context.Background()

	_, _, err := store.SetCiphertextIfAbsent(ctx, 5, []byte("ct"))
	require.NoError(t, err)

	ckpt, _ := store.Get(ctx, 5)
	assert.False(t, ckpt.Delivered)

	require.NoError(t, store.MarkDelivered(ctx, 5))
	ckpt, _ = store.Get(ctx, 5)
	assert.True(t, ckpt.Delivered)
}

// TestCheckpointStore_Tombstone shrinks the TTL. We assert the key still
// exists immediately after; GC-via-TTL-expiry is a miniredis feature we
// trust, not something to poll here.
func TestCheckpointStore_Tombstone(t *testing.T) {
	t.Parallel()

	store, mr := newTestCheckpointStore(t)
	ctx := context.Background()

	_, _, err := store.SetCiphertextIfAbsent(ctx, 5, []byte("ct"))
	require.NoError(t, err)
	require.NoError(t, store.Tombstone(ctx, 5))

	// Key still present.
	ckpt, err := store.Get(ctx, 5)
	require.NoError(t, err)
	assert.True(t, ckpt.HasCiphertext(), "tombstone must not delete immediately")

	// Fast-forward past tombstone TTL; key should be GC'd.
	mr.FastForward(11 * time.Minute)
	ckpt, err = store.Get(ctx, 5)
	require.NoError(t, err)
	assert.True(t, ckpt.IsEmpty(), "tombstone TTL must expire the key")
}

// TestCheckpointStore_ConcurrentRaceDoesNotCorruptUnderRace exercises
// Set+Get concurrently under -race.
func TestCheckpointStore_ConcurrentSetGet(t *testing.T) {
	t.Parallel()

	store, _ := newTestCheckpointStore(t)
	const goroutines = 16
	const jobs = 4
	var wg sync.WaitGroup
	var getCount atomic.Int32

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < jobs; j++ {
				jobID := uint64(j)
				_, _, err := store.SetCiphertextIfAbsent(context.Background(), jobID, []byte{byte(g)})
				if err != nil {
					t.Errorf("set err: %v", err)
					return
				}
				_, err = store.Get(context.Background(), jobID)
				if err != nil {
					t.Errorf("get err: %v", err)
					return
				}
				getCount.Add(1)
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int32(goroutines*jobs), getCount.Load())
}

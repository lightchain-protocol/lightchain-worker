package keystore

import (
	"crypto/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randomKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func TestSessionKeyStore_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	key := randomKey(t)
	require.NoError(t, store.StoreKey(42, key))

	got, err := store.GetKey(42)
	require.NoError(t, err)
	assert.Equal(t, key, got)
}

func TestSessionKeyStore_GetKey_NotFound(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	_, err = store.GetKey(999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSessionKeyStore_GetKey_ReturnsCopy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	key := randomKey(t)
	require.NoError(t, store.StoreKey(1, key))

	got1, err := store.GetKey(1)
	require.NoError(t, err)

	// Mutate the returned copy
	got1[0] = 0xFF

	got2, err := store.GetKey(1)
	require.NoError(t, err)

	// Original should be unaffected
	assert.Equal(t, key, got2)
}

func TestSessionKeyStore_PersistenceAcrossInstances(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	// First instance — write keys
	store1, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	key1 := randomKey(t)
	key2 := randomKey(t)
	require.NoError(t, store1.StoreKey(10, key1))
	require.NoError(t, store1.StoreKey(20, key2))

	// Second instance — read keys from file
	store2, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	got1, err := store2.GetKey(10)
	require.NoError(t, err)
	assert.Equal(t, key1, got1)

	got2, err := store2.GetKey(20)
	require.NoError(t, err)
	assert.Equal(t, key2, got2)
}

func TestSessionKeyStore_WrongPassphrase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "correct-pass")
	require.NoError(t, err)

	require.NoError(t, store.StoreKey(1, randomKey(t)))

	_, err = NewSessionKeyStore(path, "wrong-pass")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestSessionKeyStore_RemoveKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	key := randomKey(t)
	require.NoError(t, store.StoreKey(1, key))

	require.NoError(t, store.RemoveKey(1))

	_, err = store.GetKey(1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify removal persists
	store2, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)
	_, err = store2.GetKey(1)
	require.Error(t, err)
}

func TestSessionKeyStore_ZeroAll(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	require.NoError(t, store.StoreKey(1, randomKey(t)))
	require.NoError(t, store.StoreKey(2, randomKey(t)))

	require.NoError(t, store.ZeroAll())

	_, err = store.GetKey(1)
	require.Error(t, err)
	_, err = store.GetKey(2)
	require.Error(t, err)

	reloaded, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	_, err = reloaded.GetKey(1)
	require.Error(t, err)
	_, err = reloaded.GetKey(2)
	require.Error(t, err)
}

func TestSessionKeyStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session-keys.enc")

	store, err := NewSessionKeyStore(path, "testpass")
	require.NoError(t, err)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id uint64) {
			defer wg.Done()
			key := randomKey(t)
			require.NoError(t, store.StoreKey(id, key))
			got, err := store.GetKey(id)
			require.NoError(t, err)
			assert.Equal(t, key, got)
		}(uint64(i))
	}
	wg.Wait()
}

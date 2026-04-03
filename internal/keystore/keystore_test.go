package keystore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerate_NewFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "worker-encryption.key")

	key, err := LoadOrGenerate(path, "test-passphrase")
	require.NoError(t, err)
	require.NotNil(t, key)

	// File should be created with correct permissions
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())

	// Public key must be a valid uncompressed P-256 point (65 bytes)
	pubKeyBytes := key.PublicKey().Bytes()
	assert.Equal(t, 65, len(pubKeyBytes), "uncompressed P-256 public key must be 65 bytes")
	assert.Equal(t, byte(0x04), pubKeyBytes[0], "uncompressed point must start with 0x04")
}

func TestLoadOrGenerate_ReloadSameKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "worker-encryption.key")
	passphrase := "stable-passphrase"

	key1, err := LoadOrGenerate(path, passphrase)
	require.NoError(t, err)

	key2, err := LoadOrGenerate(path, passphrase)
	require.NoError(t, err)

	// Both calls must return the identical private key
	assert.Equal(t, key1.Bytes(), key2.Bytes(), "reloaded key must match generated key")
}

func TestLoadOrGenerate_WrongPassphrase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "worker-encryption.key")

	_, err := LoadOrGenerate(path, "correct-passphrase")
	require.NoError(t, err)

	// Loading with wrong passphrase should fail (AES-GCM authentication failure)
	_, err = LoadOrGenerate(path, "wrong-passphrase")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt ECDH private key")
}

func TestLoadOrGenerate_CorruptFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "worker-encryption.key")

	// Write truncated/invalid JSON
	err := os.WriteFile(path, []byte(`{"version":1,"salt":"not`), 0600)
	require.NoError(t, err)

	_, err = LoadOrGenerate(path, "any-passphrase")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse keystore")
}

func TestLoadOrGenerate_NestedDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Path with a subdirectory that doesn't exist yet
	path := filepath.Join(dir, "data", "keys", "worker-encryption.key")

	key, err := LoadOrGenerate(path, "test-passphrase")
	require.NoError(t, err)
	require.NotNil(t, key)

	// Verify the parent directories were created
	_, err = os.Stat(filepath.Join(dir, "data", "keys"))
	require.NoError(t, err)
}

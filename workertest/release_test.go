package workertest

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lightchain/worker/internal/release"
)

// These tests deliberately exercise only the wrapper's option-defaulting
// logic. The wrapper holds a concrete *workerchain.ChainClient (not an
// interface) and is not designed for chain mocking — substantive coverage
// is provided by the orchestrator's e2e_chain test suite, which runs the
// wrapper against a real Anvil instance.
//
// Argument-validation paths in NewReleaseTester (RPCURL empty, ChainID
// non-positive, etc.) are plain `if x == ... { t.Fatal(...) }` checks
// over each opts field; their behavior is unambiguous from the source and
// any regression would be caught by every e2e test failing to construct.

func TestBuildReleaseConfigForTester_AppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg := buildReleaseConfigForTester(t, ReleaseTesterConfig{})
	defaults := release.DefaultConfig()

	// StatePath is always replaced (defaults to t.TempDir()/release_state.json).
	assert.NotEmpty(t, cfg.StatePath, "StatePath default must not be empty")
	assert.Equal(t, "release_state.json", filepath.Base(cfg.StatePath),
		"default StatePath basename should be release_state.json")

	// Anvil-friendly defaults (deviate from release.DefaultConfig()).
	assert.Equal(t, uint64(0), cfg.Confirmations, "Anvil default Confirmations=0")
	assert.Equal(t, uint64(1), cfg.StartBlock, "Anvil default StartBlock=1")
	assert.Equal(t, 100, cfg.MaxBatchSize, "wrapper default MaxBatchSize=100")

	// Pass-through defaults from release.DefaultConfig().
	assert.Equal(t, defaults.ChunkSize, cfg.ChunkSize)
	assert.Equal(t, defaults.BatchThreshold, cfg.BatchThreshold)
	assert.Equal(t, defaults.Interval, cfg.Interval)
	assert.Equal(t, defaults.ProbeInterval, cfg.ProbeInterval)
	assert.Equal(t, defaults.TxTimeout, cfg.TxTimeout)
	assert.Equal(t, defaults.DisputeWindowCacheTTL, cfg.DisputeWindowCacheTTL)

	// Production parity: zero override means "read from AIConfig".
	assert.Equal(t, time.Duration(0), cfg.DisputeWindowOverride)

	// Wrapper always enables the subsystem.
	assert.True(t, cfg.Enabled)
}

func TestBuildReleaseConfigForTester_AppliesOverrides(t *testing.T) {
	t.Parallel()

	override := ReleaseTesterConfig{
		StatePath:             "/tmp/explicit-path.json",
		Confirmations:         5,
		StartBlock:            42,
		ChunkSize:             999,
		MaxBatchSize:          7,
		BatchThreshold:        3,
		Interval:              90 * time.Minute,
		ProbeInterval:         11 * time.Second,
		TxTimeout:             45 * time.Second,
		DisputeWindowOverride: 30 * time.Minute,
		DisputeWindowCacheTTL: 2 * time.Minute,
	}

	cfg := buildReleaseConfigForTester(t, override)

	assert.Equal(t, "/tmp/explicit-path.json", cfg.StatePath)
	assert.Equal(t, uint64(5), cfg.Confirmations)
	assert.Equal(t, uint64(42), cfg.StartBlock)
	assert.Equal(t, uint64(999), cfg.ChunkSize)
	assert.Equal(t, 7, cfg.MaxBatchSize)
	assert.Equal(t, 3, cfg.BatchThreshold)
	assert.Equal(t, 90*time.Minute, cfg.Interval)
	assert.Equal(t, 11*time.Second, cfg.ProbeInterval)
	assert.Equal(t, 45*time.Second, cfg.TxTimeout)
	assert.Equal(t, 30*time.Minute, cfg.DisputeWindowOverride)
	assert.Equal(t, 2*time.Minute, cfg.DisputeWindowCacheTTL)
}

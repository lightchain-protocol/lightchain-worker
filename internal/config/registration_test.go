package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allRegEnvKeys lists every env var LoadRegistration() reads.
var allRegEnvKeys = []string{
	"WORKER_KEYSTORE_PATH", "WORKER_KEYSTORE_PASSWORD",
	"ENCRYPTION_KEYSTORE_PATH",
	"RPC_URL", "CHAIN_ID",
	"WORKER_REGISTRY_ADDRESS", "AI_CONFIG_ADDRESS",
	"SUPPORTED_MODELS", "WORKER_STAKE",
	"GAS_PRICE_MULTIPLIER_BPS", "MAX_GAS_PRICE",
	"LOG_LEVEL", "LOG_FORMAT",
}

func clearRegEnv(t *testing.T) {
	t.Helper()
	for _, key := range allRegEnvKeys {
		t.Setenv(key, "")
		os.Unsetenv(key) //nolint:tenv // intentionally clearing test state
	}
}

func validRegEnv(t *testing.T) {
	t.Helper()
	clearRegEnv(t)
	t.Setenv("CHAIN_ID", "1337")
	t.Setenv("WORKER_REGISTRY_ADDRESS", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv("AI_CONFIG_ADDRESS", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
}

func TestLoadRegistration_ValidConfig(t *testing.T) {
	validRegEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("SUPPORTED_MODELS", "llama3-8b,mistral-7b")

	cfg, err := LoadRegistration()
	require.NoError(t, err)
	assert.Equal(t, int64(1337), cfg.ChainID)
	assert.Equal(t, []string{"llama3-8b", "mistral-7b"}, cfg.SupportedModels)
	assert.Equal(t, "data/worker-encryption.key", cfg.EncryptionKeystorePath)
	assert.Nil(t, cfg.WorkerStake, "WorkerStake should be nil when WORKER_STAKE not set")
	assert.Equal(t, "100000000000", cfg.MaxGasPrice.String())
	assert.Equal(t, 11000, cfg.GasPriceMultiplierBps)

	errs := cfg.Validate()
	assert.Empty(t, errs)
}

func TestLoadRegistration_MissingChainID(t *testing.T) {
	validRegEnv(t)
	t.Setenv("CHAIN_ID", "")
	os.Unsetenv("CHAIN_ID") //nolint:tenv

	_, err := LoadRegistration()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHAIN_ID")
}

func TestLoadRegistration_MissingRegistryAddress(t *testing.T) {
	validRegEnv(t)
	t.Setenv("WORKER_REGISTRY_ADDRESS", "")
	os.Unsetenv("WORKER_REGISTRY_ADDRESS") //nolint:tenv

	_, err := LoadRegistration()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_REGISTRY_ADDRESS")
}

func TestLoadRegistration_ModelsOptional(t *testing.T) {
	validRegEnv(t)
	// SUPPORTED_MODELS not set — should load fine (needed for deregister/status)

	cfg, err := LoadRegistration()
	require.NoError(t, err)
	assert.Empty(t, cfg.SupportedModels)
}

func TestLoadRegistration_WorkerStakeProvided(t *testing.T) {
	validRegEnv(t)
	t.Setenv("WORKER_STAKE", "1000000000000000000")

	cfg, err := LoadRegistration()
	require.NoError(t, err)
	require.NotNil(t, cfg.WorkerStake)
	assert.Equal(t, "1000000000000000000", cfg.WorkerStake.String())
}

func TestLoadRegistration_NegativeStake(t *testing.T) {
	validRegEnv(t)
	t.Setenv("WORKER_STAKE", "-1000")

	_, err := LoadRegistration()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_STAKE: must be non-negative")
}

func TestLoadRegistration_Validate_MissingKeystore(t *testing.T) {
	validRegEnv(t)
	// WORKER_KEYSTORE_PATH and WORKER_KEYSTORE_PASSWORD not set

	cfg, err := LoadRegistration()
	require.NoError(t, err)

	errs := cfg.Validate()
	require.NotEmpty(t, errs)

	combined := ""
	for _, e := range errs {
		combined += e + " "
	}
	assert.Contains(t, combined, "WORKER_KEYSTORE_PATH")
	assert.Contains(t, combined, "WORKER_KEYSTORE_PASSWORD")
}

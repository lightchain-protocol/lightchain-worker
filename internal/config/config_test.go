package config

import (
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allEnvKeys lists every env var config.Load() reads, used to clear state between tests.
var allEnvKeys = []string{
	"WORKER_KEYSTORE_PATH", "WORKER_KEYSTORE_PASSWORD",
	"ENCRYPTION_KEYSTORE_PATH",
	"RPC_URL", "CHAIN_ID",
	"WORKER_REGISTRY_ADDRESS", "AI_CONFIG_ADDRESS", "JOB_REGISTRY_ADDRESS",
	"GAS_PRICE_MULTIPLIER_BPS", "MAX_GAS_PRICE",
	"SUPPORTED_MODELS",
	"REDIS_URL", "REDIS_PASSWORD",
	"HEARTBEAT_INTERVAL", "OLLAMA_URL", "OLLAMA_TIMEOUT",
	"BEACON_API_URL", "SESSION_KEY_FILE",
	"MAX_CONCURRENT_JOBS", "ACK_TX_TIMEOUT", "BLOB_TX_TIMEOUT", "BLOB_FETCH_TIMEOUT",
	"BLOB_FETCH_RETRIES", "RECEIPT_POLL_INTERVAL", "REDIS_PUBLISH_TIMEOUT",
	"SHUTDOWN_TIMEOUT",
	"LIGHTCHAIN_DRAIN_TTL", "LIGHTCHAIN_DRAIN_SLACK",
	"LOG_LEVEL", "LOG_FORMAT",
	// Sortition mode (Phase 3)
	"SORTITION_ENABLED", "SESSION_MANAGER_ADDRESS",
	"SORTITION_STATE_DIR", "SORTITION_CHUNK_SIZE",
	"SORTITION_POLL_INTERVAL", "SORTITION_CONFIRMATIONS",
	"SORTITION_SESSION_RETRY_LIMIT",
}

// clearEnv ensures all config-related env vars are unset before each test.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range allEnvKeys {
		t.Setenv(key, "")
		os.Unsetenv(key) //nolint:tenv // intentionally clearing test state
	}
}

// validEnv sets the minimal set of env vars required by Load() to succeed.
func validEnv(t *testing.T) {
	t.Helper()
	clearEnv(t)
	t.Setenv("CHAIN_ID", "1337")
	t.Setenv("WORKER_REGISTRY_ADDRESS", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv("AI_CONFIG_ADDRESS", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	t.Setenv("JOB_REGISTRY_ADDRESS", "0xcccccccccccccccccccccccccccccccccccccccc")
	t.Setenv("SUPPORTED_MODELS", "llama3-8b,mistral-7b")
}

func TestLoad_ValidConfig(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, int64(1337), cfg.ChainID)
	assert.Equal(t, []string{"llama3-8b", "mistral-7b"}, cfg.SupportedModels)
	assert.Equal(t, "data/worker-encryption.key", cfg.EncryptionKeystorePath)
	assert.Equal(t, "10s", cfg.HeartbeatInterval.String())
	// New 3-2 defaults
	assert.Equal(t, "http://localhost:3500", cfg.BeaconAPIURL)
	assert.Equal(t, 2, cfg.MaxConcurrentJobs)
	assert.Equal(t, 15*time.Second, cfg.AckTxTimeout)
	assert.Equal(t, 90*time.Second, cfg.BlobTxTimeout)
	assert.Equal(t, 10*time.Second, cfg.BlobFetchTimeout)
	assert.Equal(t, 3, cfg.BlobFetchRetries)
	assert.Equal(t, 120*time.Second, cfg.OllamaTimeout)
	assert.Equal(t, "data/session-keys.enc", cfg.SessionKeyFile)
	assert.Equal(t, 2*time.Second, cfg.ReceiptPollInterval)
	assert.NotNil(t, cfg.MaxGasPrice)
	// 100 gwei default
	assert.Equal(t, "100000000000", cfg.MaxGasPrice.String())
	// Stuck-nonce recovery defaults
	assert.Equal(t, 5, cfg.StuckNonceThreshold)
	assert.Equal(t, 3, cfg.StuckNonceMaxBumps)
	assert.True(t, cfg.StuckNonceAutoReplace, "auto-replace must default to true")
}

func TestLoad_StuckNonceOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("WORKER_STUCK_NONCE_THRESHOLD", "8")
	t.Setenv("WORKER_STUCK_NONCE_MAX_BUMPS", "1")
	t.Setenv("WORKER_STUCK_NONCE_AUTOREPLACE", "false")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 8, cfg.StuckNonceThreshold)
	assert.Equal(t, 1, cfg.StuckNonceMaxBumps)
	assert.False(t, cfg.StuckNonceAutoReplace)
}

func TestLoad_DrainTTLOverride_zeroIsUnset(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.DrainTTLOverride,
		"unset LIGHTCHAIN_DRAIN_TTL should leave override at zero")
}

func TestLoad_DrainTTLOverride_acceptsPositiveDuration(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("LIGHTCHAIN_DRAIN_TTL", "30m")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, cfg.DrainTTLOverride)
}

func TestLoad_DrainTTLOverride_rejectsNegativeDuration(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("LIGHTCHAIN_DRAIN_TTL", "-1h")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LIGHTCHAIN_DRAIN_TTL")
	assert.Contains(t, err.Error(), "must be >= 0")
}

func TestLoad_DrainSlack_defaultsTo2h(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, cfg.DrainSlack,
		"unset LIGHTCHAIN_DRAIN_SLACK should default to 2h")
}

func TestLoad_DrainSlack_acceptsShortDuration(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("LIGHTCHAIN_DRAIN_SLACK", "10s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.DrainSlack)
}

func TestLoad_DrainSlack_acceptsZero(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("LIGHTCHAIN_DRAIN_SLACK", "0s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.DrainSlack,
		"zero is valid (drain TTL == dispute window with no slack)")
}

func TestLoad_DrainSlack_rejectsNegativeDuration(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("LIGHTCHAIN_DRAIN_SLACK", "-5m")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LIGHTCHAIN_DRAIN_SLACK")
	assert.Contains(t, err.Error(), "must be >= 0")
}

func TestLoad_StuckNonceInvalidBoolean(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("WORKER_STUCK_NONCE_AUTOREPLACE", "maybe")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_STUCK_NONCE_AUTOREPLACE")
}

func TestLoad_MissingChainID(t *testing.T) {
	validEnv(t)
	t.Setenv("CHAIN_ID", "")
	os.Unsetenv("CHAIN_ID") //nolint:tenv

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CHAIN_ID")
}

func TestLoad_MissingWorkerRegistryAddress(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_REGISTRY_ADDRESS", "")
	os.Unsetenv("WORKER_REGISTRY_ADDRESS") //nolint:tenv

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_REGISTRY_ADDRESS")
}

func TestLoad_MultipleMissingFields(t *testing.T) {
	clearEnv(t)
	// Only SUPPORTED_MODELS set — CHAIN_ID, WORKER_REGISTRY_ADDRESS, AI_CONFIG_ADDRESS, JOB_REGISTRY_ADDRESS all missing
	t.Setenv("SUPPORTED_MODELS", "llama3-8b")

	_, err := Load()
	require.Error(t, err)
	errStr := err.Error()
	assert.Contains(t, errStr, "CHAIN_ID")
	assert.Contains(t, errStr, "WORKER_REGISTRY_ADDRESS")
	assert.Contains(t, errStr, "AI_CONFIG_ADDRESS")
	assert.Contains(t, errStr, "JOB_REGISTRY_ADDRESS")
}

func TestLoad_EmptySupportedModels(t *testing.T) {
	clearEnv(t)
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("WORKER_REGISTRY_ADDRESS", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv("AI_CONFIG_ADDRESS", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	// SUPPORTED_MODELS not set

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUPPORTED_MODELS")
}

func TestValidate_MissingKeystoreFields(t *testing.T) {
	validEnv(t)
	// WORKER_KEYSTORE_PATH and WORKER_KEYSTORE_PASSWORD not set

	cfg, err := Load()
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

func TestValidate_ValidConfig(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)

	errs := cfg.Validate()
	assert.Empty(t, errs)
}

func TestParseModelID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(t *testing.T, id [32]byte)
	}{
		{
			name:  "valid hex 0x-prefixed",
			input: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			check: func(t *testing.T, id [32]byte) {
				t.Helper()
				for _, b := range id {
					assert.Equal(t, byte(0xaa), b)
				}
			},
		},
		{
			name:  "human name keccak256",
			input: "llama3-8b",
			check: func(t *testing.T, id [32]byte) {
				t.Helper()
				var zero [32]byte
				assert.NotEqual(t, zero, id)
			},
		},
		{
			name:    "invalid hex wrong length",
			input:   "0xaabb",
			wantErr: true,
		},
		{
			name:    "invalid hex bad chars",
			input:   "0x" + "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, err := ParseModelID(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.check(t, id)
		})
	}
}

func TestParseModelID_Deterministic(t *testing.T) {
	t.Parallel()
	id1, err := ParseModelID("llama3-8b")
	require.NoError(t, err)
	id2, err := ParseModelID("llama3-8b")
	require.NoError(t, err)
	assert.Equal(t, id1, id2)
}

func TestLoad_NegativeValues(t *testing.T) {
	tests := []struct {
		name    string
		envKey  string
		envVal  string
		wantErr string
	}{
		{
			name:    "negative chain ID",
			envKey:  "CHAIN_ID",
			envVal:  "-1",
			wantErr: "CHAIN_ID must be positive",
		},
		{
			name:    "zero gas price multiplier",
			envKey:  "GAS_PRICE_MULTIPLIER_BPS",
			envVal:  "0",
			wantErr: "GAS_PRICE_MULTIPLIER_BPS must be positive",
		},
		{
			name:    "negative gas price multiplier",
			envKey:  "GAS_PRICE_MULTIPLIER_BPS",
			envVal:  "-100",
			wantErr: "GAS_PRICE_MULTIPLIER_BPS must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validEnv(t)
			t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
			t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
			t.Setenv(tt.envKey, tt.envVal)

			cfg, err := Load()
			if err != nil {
				// Load-time rejection (e.g. negative stake)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			// Validate-time rejection (e.g. chain ID, gas multiplier)
			errs := cfg.Validate()
			require.NotEmpty(t, errs, "expected validation error for %s=%s", tt.envKey, tt.envVal)
			combined := ""
			for _, e := range errs {
				combined += e + " "
			}
			assert.Contains(t, combined, tt.wantErr)
		})
	}
}

func TestLoad_OverflowValues(t *testing.T) {
	tests := []struct {
		name    string
		envKey  string
		envVal  string
		wantErr string
	}{
		{
			name:    "CHAIN_ID overflows int64",
			envKey:  "CHAIN_ID",
			envVal:  "9999999999999999999999",
			wantErr: "CHAIN_ID: value",
		},
		{
			name:    "GAS_PRICE_MULTIPLIER_BPS overflows int64",
			envKey:  "GAS_PRICE_MULTIPLIER_BPS",
			envVal:  "9999999999999999999999",
			wantErr: "GAS_PRICE_MULTIPLIER_BPS: value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validEnv(t)
			t.Setenv(tt.envKey, tt.envVal)

			_, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoad_JobRegistryAddress(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		wantErr string
	}{
		{
			name:    "missing",
			env:     "",
			wantErr: "JOB_REGISTRY_ADDRESS is required",
		},
		{
			name:    "invalid hex",
			env:     "not-an-address",
			wantErr: "JOB_REGISTRY_ADDRESS: invalid hex address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validEnv(t)
			t.Setenv("JOB_REGISTRY_ADDRESS", tt.env)
			if tt.env == "" {
				os.Unsetenv("JOB_REGISTRY_ADDRESS") //nolint:tenv
			}
			_, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoad_JobExecutionDefaults(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)

	errs := cfg.Validate()
	assert.Empty(t, errs)

	assert.Equal(t, 2, cfg.MaxConcurrentJobs)
	assert.Equal(t, 3, cfg.BlobFetchRetries)
	assert.Equal(t, 15*time.Second, cfg.AckTxTimeout)
	assert.Equal(t, 90*time.Second, cfg.BlobTxTimeout)
	assert.Equal(t, 10*time.Second, cfg.BlobFetchTimeout)
	assert.Equal(t, 120*time.Second, cfg.OllamaTimeout)
	assert.Equal(t, 2*time.Second, cfg.ReceiptPollInterval)
}

func TestLoad_JobExecutionOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("MAX_CONCURRENT_JOBS", "8")
	t.Setenv("BLOB_FETCH_RETRIES", "5")
	t.Setenv("ACK_TX_TIMEOUT", "30s")
	t.Setenv("BLOB_TX_TIMEOUT", "180s")
	t.Setenv("BLOB_FETCH_TIMEOUT", "20s")
	t.Setenv("OLLAMA_TIMEOUT", "60s")
	t.Setenv("BEACON_API_URL", "http://beacon:3500")
	t.Setenv("SESSION_KEY_FILE", "/data/keys.enc")
	t.Setenv("MAX_GAS_PRICE", "50000000000")
	t.Setenv("RECEIPT_POLL_INTERVAL", "5s")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 8, cfg.MaxConcurrentJobs)
	assert.Equal(t, 5, cfg.BlobFetchRetries)
	assert.Equal(t, 30*time.Second, cfg.AckTxTimeout)
	assert.Equal(t, 180*time.Second, cfg.BlobTxTimeout)
	assert.Equal(t, 20*time.Second, cfg.BlobFetchTimeout)
	assert.Equal(t, 60*time.Second, cfg.OllamaTimeout)
	assert.Equal(t, "http://beacon:3500", cfg.BeaconAPIURL)
	assert.Equal(t, "/data/keys.enc", cfg.SessionKeyFile)
	assert.Equal(t, "50000000000", cfg.MaxGasPrice.String())
	assert.Equal(t, 5*time.Second, cfg.ReceiptPollInterval)
}

func TestLoad_SearchDefaults(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/k")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "p")
	// SEARCH_* unset → search disabled, sane defaults.
	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.SearchEnabled)
	assert.Equal(t, "https://api.tavily.com", cfg.TavilyURL)
	assert.Equal(t, 10*time.Second, cfg.SearchTimeout)
	assert.Equal(t, 5, cfg.SearchMaxResults)
}

func TestLoad_SearchEnabled(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/k")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "p")
	t.Setenv("SEARCH_ENABLED", "true")
	t.Setenv("TAVILY_API_KEY", "tvly-xxx")
	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.SearchEnabled)
	assert.Equal(t, "tvly-xxx", cfg.TavilyAPIKey)
}

func TestValidate_JobRegistryZeroAddress(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	// Override JOB_REGISTRY_ADDRESS to zero
	t.Setenv("JOB_REGISTRY_ADDRESS", "0x0000000000000000000000000000000000000000")

	cfg, err := Load()
	require.NoError(t, err)

	errs := cfg.Validate()
	combined := ""
	for _, e := range errs {
		combined += e + " "
	}
	assert.Contains(t, combined, "JOB_REGISTRY_ADDRESS must be a non-zero address")
}

// loadValidConfig sets the minimal required env, overlays extras, calls Load(), and
// returns the result. Fails the test if Load() returns an error.
func loadValidConfig(t *testing.T, extra map[string]string) *Config {
	t.Helper()
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	for k, v := range extra {
		t.Setenv(k, v)
	}
	cfg, err := Load()
	require.NoError(t, err)
	return cfg
}

// requireConfigError sets the minimal required env, overlays extras, calls Load(),
// and asserts a Load-time error containing wantSubstr.
func requireConfigError(t *testing.T, extra map[string]string, wantSubstr string) {
	t.Helper()
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	for k, v := range extra {
		t.Setenv(k, v)
	}
	cfg, err := Load()
	// If Load() passes, check Validate() for cross-field errors
	if err == nil {
		errs := cfg.Validate()
		combined := ""
		for _, e := range errs {
			combined += e + " "
		}
		require.Contains(t, combined, wantSubstr,
			"expected config error containing %q but Validate() returned: %v", wantSubstr, errs)
		return
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), wantSubstr)
}

func TestConfig_SortitionDefaults(t *testing.T) {
	cfg := loadValidConfig(t, map[string]string{})
	require.False(t, cfg.SortitionEnabled)
	require.Equal(t, common.Address{}, cfg.SessionManagerAddress)
	require.Equal(t, "data/sortition-state", cfg.SortitionStateDir)
	require.Equal(t, uint64(5000), cfg.SortitionChunkSize)
	require.Equal(t, 4*time.Second, cfg.SortitionPollInterval)
	require.Equal(t, uint64(0), cfg.SortitionConfirmations)
	require.Equal(t, 10, cfg.SortitionSessionRetryLimit)
}

func TestConfig_SortitionSessionRetryLimit_Parses(t *testing.T) {
	cfg := loadValidConfig(t, map[string]string{
		"SORTITION_ENABLED":             "true",
		"SESSION_MANAGER_ADDRESS":       "0x000000000000000000000000000000000000dEaD",
		"SORTITION_SESSION_RETRY_LIMIT": "25",
	})
	require.Equal(t, 25, cfg.SortitionSessionRetryLimit)
}

func TestConfig_SortitionEnabled_RequiresSessionManager(t *testing.T) {
	// Enabling sortition without SESSION_MANAGER_ADDRESS must be a config error.
	requireConfigError(t, map[string]string{
		"SORTITION_ENABLED": "true",
	}, "SESSION_MANAGER_ADDRESS is required when SORTITION_ENABLED=true")
}

func TestConfig_SortitionEnabled_Parses(t *testing.T) {
	cfg := loadValidConfig(t, map[string]string{
		"SORTITION_ENABLED":       "true",
		"SESSION_MANAGER_ADDRESS": "0x000000000000000000000000000000000000dEaD",
		"SORTITION_POLL_INTERVAL": "2s",
		"SORTITION_CHUNK_SIZE":    "1000",
	})
	require.True(t, cfg.SortitionEnabled)
	require.Equal(t, common.HexToAddress("0x000000000000000000000000000000000000dEaD"), cfg.SessionManagerAddress)
	require.Equal(t, 2*time.Second, cfg.SortitionPollInterval)
	require.Equal(t, uint64(1000), cfg.SortitionChunkSize)
}

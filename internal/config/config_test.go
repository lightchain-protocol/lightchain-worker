package config

import (
	"os"
	"testing"
	"time"

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
	"OLLAMA_STREAM", "OLLAMA_KEEP_ALIVE", "OLLAMA_NUM_PREDICT", "OLLAMA_NUM_CTX",
	"OLLAMA_THINK", "MODEL_OPTIONS",
	"STREAM_CHUNK_TOKENS", "STREAM_CHUNK_INTERVAL",
	"DEADLINE_GUARD_ENABLED", "DEADLINE_SETTLE_RESERVE",
	"DEADLINE_COMPLETION_RESERVE", "DEADLINE_MIN_INFERENCE_BUDGET",
	"BEACON_API_URL", "SESSION_KEY_FILE",
	"MAX_CONCURRENT_JOBS", "ACK_TX_TIMEOUT", "BLOB_TX_TIMEOUT", "BLOB_FETCH_TIMEOUT",
	"BLOB_FETCH_RETRIES", "RECEIPT_POLL_INTERVAL", "REDIS_PUBLISH_TIMEOUT",
	"SHUTDOWN_TIMEOUT",
	"LIGHTCHAIN_DRAIN_TTL", "LIGHTCHAIN_DRAIN_SLACK",
	"STT_ENABLED", "STT_SIDECAR_URL", "STT_TIMEOUT",
	"TTS_ENABLED", "TTS_SIDECAR_URL", "TTS_VOICE",
	"TTS_MAX_CHARS", "TTS_TIMEOUT",
	"LOG_LEVEL", "LOG_FORMAT",
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
	// Token streaming defaults
	assert.True(t, cfg.OllamaStream, "streaming must default to on")
	assert.Equal(t, "-1", cfg.OllamaKeepAlive)
	assert.Equal(t, 1024, cfg.OllamaNumPredict)
	assert.Equal(t, 0, cfg.OllamaNumCtx, "0 means leave the model's context window alone")
	assert.False(t, cfg.OllamaThink, "thinking must default to off: the worker never transmits it")
	assert.Equal(t, 8, cfg.StreamChunkTokens)
	assert.Equal(t, 250*time.Millisecond, cfg.StreamChunkInterval)
}

func TestLoad_StreamingOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("OLLAMA_STREAM", "false")
	t.Setenv("OLLAMA_KEEP_ALIVE", "30m")
	t.Setenv("OLLAMA_NUM_PREDICT", "2048")
	t.Setenv("OLLAMA_NUM_CTX", "8192")
	t.Setenv("STREAM_CHUNK_TOKENS", "16")
	t.Setenv("STREAM_CHUNK_INTERVAL", "100ms")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.OllamaStream)
	assert.Equal(t, "30m", cfg.OllamaKeepAlive)
	assert.Equal(t, 2048, cfg.OllamaNumPredict)
	assert.Equal(t, 8192, cfg.OllamaNumCtx)
	assert.Equal(t, 16, cfg.StreamChunkTokens)
	assert.Equal(t, 100*time.Millisecond, cfg.StreamChunkInterval)

	assert.Empty(t, cfg.Validate())
}

func TestValidate_StreamingConstraints(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("OLLAMA_NUM_PREDICT", "0")
	t.Setenv("OLLAMA_NUM_CTX", "-1")
	t.Setenv("STREAM_CHUNK_TOKENS", "0")
	t.Setenv("STREAM_CHUNK_INTERVAL", "0s")

	cfg, err := Load()
	require.NoError(t, err)

	combined := ""
	for _, e := range cfg.Validate() {
		combined += e + " "
	}
	assert.Contains(t, combined, "OLLAMA_NUM_PREDICT")
	assert.Contains(t, combined, "OLLAMA_NUM_CTX")
	assert.Contains(t, combined, "STREAM_CHUNK_TOKENS")
	assert.Contains(t, combined, "STREAM_CHUNK_INTERVAL")
}

// TestValidate_NumPredictAcceptsOllamaSentinels pins that -1 (unlimited)
// and -2 (fill context) stay usable — an operator disabling the cap must
// not be blocked by the zero check.
func TestValidate_NumPredictAcceptsOllamaSentinels(t *testing.T) {
	for _, v := range []string{"-1", "-2"} {
		t.Run(v, func(t *testing.T) {
			validEnv(t)
			t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
			t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
			t.Setenv("OLLAMA_NUM_PREDICT", v)

			cfg, err := Load()
			require.NoError(t, err)
			assert.Empty(t, cfg.Validate())
		})
	}
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
	assert.True(t, cfg.AckOverlapEnabled, "ack overlap must default to on")
}

func TestLoad_AckOverlapOverride(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("ACK_OVERLAP_ENABLED", "false")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.AckOverlapEnabled)
}

func TestLoad_AckOverlapInvalidBoolean(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("ACK_OVERLAP_ENABLED", "sometimes")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ACK_OVERLAP_ENABLED")
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

func TestLoad_DeadlineGuardDefaults(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.DeadlineGuardEnabled)
	assert.Equal(t, 25*time.Second, cfg.SettleReserve)
	assert.Equal(t, 12*time.Second, cfg.CompletionReserve)
	assert.Equal(t, 10*time.Second, cfg.MinInferenceBudget)
	assert.Empty(t, cfg.Validate())
}

func TestLoad_DeadlineGuardOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("DEADLINE_GUARD_ENABLED", "false")
	t.Setenv("DEADLINE_SETTLE_RESERVE", "40s")
	t.Setenv("DEADLINE_COMPLETION_RESERVE", "20s")
	t.Setenv("DEADLINE_MIN_INFERENCE_BUDGET", "5s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.DeadlineGuardEnabled)
	assert.Equal(t, 40*time.Second, cfg.SettleReserve)
	assert.Equal(t, 20*time.Second, cfg.CompletionReserve)
	assert.Equal(t, 5*time.Second, cfg.MinInferenceBudget)
	// Disabled guard skips the reserve constraints entirely.
	assert.Empty(t, cfg.Validate())
}

func TestValidate_DeadlineGuardConstraints(t *testing.T) {
	validEnv(t)
	// Settle reserve below the completion floor is a misconfiguration: the
	// guard would clamp inference past the point where stage 8a still runs.
	t.Setenv("DEADLINE_SETTLE_RESERVE", "10s")
	t.Setenv("DEADLINE_COMPLETION_RESERVE", "15s")

	cfg, err := Load()
	require.NoError(t, err)

	combined := ""
	for _, e := range cfg.Validate() {
		combined += e + " "
	}
	assert.Contains(t, combined, "DEADLINE_SETTLE_RESERVE")
	assert.Contains(t, combined, "DEADLINE_COMPLETION_RESERVE")
}

// Voice sidecar gates: both features default off, so a stock worker is
// bit-identical to pre-voice behavior and never dials a sidecar.
func TestLoad_VoiceDefaults(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.STTEnabled, "STT must default to off")
	assert.False(t, cfg.TTSEnabled, "TTS must default to off")
	assert.Equal(t, "http://127.0.0.1:8100", cfg.STTSidecarURL)
	assert.Equal(t, "http://127.0.0.1:8101", cfg.TTSSidecarURL)
	assert.Equal(t, 10*time.Second, cfg.STTTimeout)
	assert.Equal(t, 20*time.Second, cfg.TTSTimeout)
	assert.Equal(t, "af_heart", cfg.TTSVoice)
	assert.Equal(t, 4000, cfg.TTSMaxChars)
	assert.Empty(t, cfg.Validate())
}

func TestLoad_VoiceOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("STT_ENABLED", "true")
	t.Setenv("STT_SIDECAR_URL", "http://10.0.0.3:9000")
	t.Setenv("STT_TIMEOUT", "15s")
	t.Setenv("TTS_ENABLED", "true")
	t.Setenv("TTS_SIDECAR_URL", "http://10.0.0.3:9001")
	t.Setenv("TTS_VOICE", "am_michael")
	t.Setenv("TTS_MAX_CHARS", "8000")
	t.Setenv("TTS_TIMEOUT", "30s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.STTEnabled)
	assert.Equal(t, "http://10.0.0.3:9000", cfg.STTSidecarURL)
	assert.Equal(t, 15*time.Second, cfg.STTTimeout)
	assert.True(t, cfg.TTSEnabled)
	assert.Equal(t, "http://10.0.0.3:9001", cfg.TTSSidecarURL)
	assert.Equal(t, "am_michael", cfg.TTSVoice)
	assert.Equal(t, 8000, cfg.TTSMaxChars)
	assert.Equal(t, 30*time.Second, cfg.TTSTimeout)
	assert.Empty(t, cfg.Validate())
}

// An enabled feature with an unparseable URL is a startup-fatal
// misconfiguration, not a runtime surprise on the first voice request.
func TestValidate_VoiceRequiresParseableURL(t *testing.T) {
	validEnv(t)
	t.Setenv("STT_ENABLED", "true")
	t.Setenv("STT_SIDECAR_URL", "not-a-url")
	t.Setenv("TTS_ENABLED", "true")
	t.Setenv("TTS_SIDECAR_URL", "ftp://sidecar:8801")

	cfg, err := Load()
	require.NoError(t, err)

	combined := ""
	for _, e := range cfg.Validate() {
		combined += e + " "
	}
	assert.Contains(t, combined, "STT_SIDECAR_URL")
	assert.Contains(t, combined, "TTS_SIDECAR_URL")
}

// Disabled features skip URL validation entirely: the default loopback
// URLs are never dialed and must not block startup.
func TestValidate_VoiceDisabledSkipsURLChecks(t *testing.T) {
	validEnv(t)
	t.Setenv("WORKER_KEYSTORE_PATH", "/tmp/keystore.json")
	t.Setenv("WORKER_KEYSTORE_PASSWORD", "secret")
	t.Setenv("STT_SIDECAR_URL", "not-a-url")
	t.Setenv("TTS_TIMEOUT", "0s")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Empty(t, cfg.Validate(),
		"voice constraints must not fire while both features are disabled")
}

// MODEL_OPTIONS is the heat-tier enforcement surface: a Max alias carries
// its token cap here and a typo must fail startup loudly, because a silent
// no-op would serve a paid Max job at the standard budget (fraud-by-config).
func TestLoad_ModelOptions_UnsetIsNil(t *testing.T) {
	validEnv(t)
	cfg, err := Load()
	require.NoError(t, err)
	assert.Nil(t, cfg.ModelOptions)
}

func TestLoad_ModelOptions_ParsesAliasEntries(t *testing.T) {
	validEnv(t)
	t.Setenv("SUPPORTED_MODELS", "agentworld-35b,agentworld-35b-max")
	// The exact production string for worker-6 (tier-catalog.json
	// string_match_verification.canonical_strings).
	t.Setenv("MODEL_OPTIONS", "agentworld-35b-max:num_predict=8192")

	cfg, err := Load()
	require.NoError(t, err)
	require.Len(t, cfg.ModelOptions, 1)
	opts, ok := cfg.ModelOptions["agentworld-35b-max"]
	require.True(t, ok)
	assert.Equal(t, 8192, opts.NumPredict)
	assert.Equal(t, 0, opts.NumCtx, "num_ctx omitted: worker-6 ruling keeps the process default")
	assert.Nil(t, opts.Temperature)
}

func TestLoad_ModelOptions_MultiEntryAndAllKeys(t *testing.T) {
	validEnv(t)
	t.Setenv("SUPPORTED_MODELS", "llama3-8b,mistral-7b")
	t.Setenv("MODEL_OPTIONS", "llama3-8b:num_predict=2048,num_ctx=8192,temperature=0.7;mistral-7b:num_predict=-1")

	cfg, err := Load()
	require.NoError(t, err)
	require.Len(t, cfg.ModelOptions, 2)

	llama := cfg.ModelOptions["llama3-8b"]
	assert.Equal(t, 2048, llama.NumPredict)
	assert.Equal(t, 8192, llama.NumCtx)
	require.NotNil(t, llama.Temperature)
	assert.InDelta(t, 0.7, *llama.Temperature, 1e-9)

	mistral := cfg.ModelOptions["mistral-7b"]
	assert.Equal(t, -1, mistral.NumPredict, "Ollama's unlimited sentinel survives")
}

// Six of the whitelisted model names carry a colon of their own
// (gemma4:e2b, gpt-oss:20b/120b, qwen3-vl:8b/30b, qwen3-embedding:0.6b).
// The name must survive both the SUPPORTED_MODELS split and the
// MODEL_OPTIONS split, or the worker refuses to start for the very models
// external operators are most likely to run.
func TestLoad_ColonInModelName(t *testing.T) {
	validEnv(t)
	t.Setenv("SUPPORTED_MODELS", "gemma4:e2b, gpt-oss:20b")
	t.Setenv("MODEL_OPTIONS", "gemma4:e2b:num_predict=2048;gpt-oss:20b:num_ctx=8192")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"gemma4:e2b", "gpt-oss:20b"}, cfg.SupportedModels,
		"the colon is part of the name — keccak256 of a truncated name is not a whitelisted model id")
	require.Len(t, cfg.ModelOptions, 2)
	assert.Equal(t, 2048, cfg.ModelOptions["gemma4:e2b"].NumPredict)
	assert.Equal(t, 8192, cfg.ModelOptions["gpt-oss:20b"].NumCtx)
}

// Every malformed shape is a startup failure. These are the fraud-by-config
// guards: none of these may silently degrade to default behavior.
func TestLoad_ModelOptions_ValidationFailures(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantErr string
	}{
		{"typo model not in SUPPORTED_MODELS", "mistral-7b-MAX:num_predict=8192", "not in SUPPORTED_MODELS"},
		{"unknown model entirely", "qwen2.5-7b:num_predict=100", "not in SUPPORTED_MODELS"},
		{"unknown option key", "llama3-8b:top_p=0.9", "unknown option key"},
		{"missing pairs", "llama3-8b", "malformed entry"},
		{"missing value", "llama3-8b:num_predict=", "malformed option"},
		{"missing key", "llama3-8b:=1024", "malformed option"},
		{"duplicate model entry", "llama3-8b:num_predict=100;llama3-8b:num_ctx=4096", "duplicate entry"},
		{"duplicate key in entry", "llama3-8b:num_predict=100,num_predict=200", "duplicate key"},
		{"num_predict zero", "llama3-8b:num_predict=0", "num_predict must be positive"},
		{"num_predict below sentinels", "llama3-8b:num_predict=-3", "num_predict must be positive"},
		{"num_predict non-integer", "llama3-8b:num_predict=eightk", "num_predict must be positive"},
		{"num_ctx zero is the unset sentinel", "llama3-8b:num_ctx=0", "num_ctx must be positive"},
		{"num_ctx negative", "llama3-8b:num_ctx=-1", "num_ctx must be positive"},
		{"temperature above range", "llama3-8b:temperature=2.5", "temperature must be"},
		{"temperature negative", "llama3-8b:temperature=-0.1", "temperature must be"},
		{"temperature non-float", "llama3-8b:temperature=hot", "temperature must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validEnv(t)
			t.Setenv("MODEL_OPTIONS", tc.value)
			_, err := Load()
			require.Error(t, err, "MODEL_OPTIONS=%q must fail startup", tc.value)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The two production alias caps must satisfy the tier-catalog admission
// rule: maxTokens / measuredP50TokPerSec + 5s variance margin must fit the
// 93s effective generation window (120s completion timeout - 25s settle
// reserve - ~2s pre-inference). Measured rates are from
// provisioning/tier-catalog.json (measuredAt 2026-08-23); if either alias
// cap is raised, re-measure and update both files together.
func TestLoad_ModelOptions_AliasCapsFitDeadlineWindow(t *testing.T) {
	const effectiveWindowSeconds = 93.0
	const varianceMarginSeconds = 5.0

	cases := []struct {
		modelOptionsEnv string // the exact production worker.env string
		alias           string
		wantCap         int
		measuredP50     float64 // tok/s, tier-catalog.json measurementSamples
	}{
		{"agentworld-35b-max:num_predict=8192", "agentworld-35b-max", 8192, 199.1},
		{"gpt-oss-20b-max:num_predict=6144", "gpt-oss-20b-max", 6144, 159.7},
	}
	for _, tc := range cases {
		t.Run(tc.alias, func(t *testing.T) {
			validEnv(t)
			t.Setenv("SUPPORTED_MODELS", tc.alias)
			t.Setenv("MODEL_OPTIONS", tc.modelOptionsEnv)

			cfg, err := Load()
			require.NoError(t, err)
			cap := cfg.ModelOptions[tc.alias].NumPredict
			require.Equal(t, tc.wantCap, cap,
				"alias cap drifted from tier-catalog.json — update both together")

			decodeSeconds := float64(cap) / tc.measuredP50
			assert.LessOrEqual(t, decodeSeconds+varianceMarginSeconds, effectiveWindowSeconds,
				"full-cap decode at the measured p50 rate no longer fits the deadline window")
		})
	}
}

// The full production MODEL_OPTIONS sets — one per live worker, every
// advertised model at its post-redeploy catalogue cap (tier-catalog.json
// genesis_seed + tier_aliases; residency-contract.json for the per-worker
// model assignment). These strings are pinned verbatim so a config edit
// without a matching catalogue change fails loudly here.
func TestLoad_ModelOptions_ProductionWorkerSets(t *testing.T) {
	cases := []struct {
		worker          string
		supportedModels string
		modelOptions    string
		wantCaps        map[string]int
	}{
		{
			worker:          "worker-1",
			supportedModels: "llama3-8b,gpt-oss-20b",
			modelOptions:    "llama3-8b:num_predict=4096;gpt-oss-20b:num_predict=4096",
			wantCaps:        map[string]int{"llama3-8b": 4096, "gpt-oss-20b": 4096},
		},
		{
			worker:          "worker-2",
			supportedModels: "qwen3-8b,qwen3-vl-8b,gemma4-12b",
			modelOptions:    "qwen3-8b:num_predict=2048;qwen3-vl-8b:num_predict=2048;gemma4-12b:num_predict=2048",
			wantCaps:        map[string]int{"qwen3-8b": 2048, "qwen3-vl-8b": 2048, "gemma4-12b": 2048},
		},
		{
			worker:          "worker-3",
			supportedModels: "devstral-24b,gpt-oss-20b,qwen3-coder-30b,gpt-oss-20b-max",
			modelOptions:    "devstral-24b:num_predict=4096;gpt-oss-20b:num_predict=4096;qwen3-coder-30b:num_predict=4096;gpt-oss-20b-max:num_predict=6144",
			wantCaps: map[string]int{
				"devstral-24b": 4096, "gpt-oss-20b": 4096, "qwen3-coder-30b": 4096,
				"gpt-oss-20b-max": 6144,
			},
		},
		{
			worker:          "worker-5",
			supportedModels: "deepseek-r1-32b,qwen3.6-27b,gemma4-26b,mistral-small-24b",
			modelOptions:    "deepseek-r1-32b:num_predict=3000;qwen3.6-27b:num_predict=4096;gemma4-26b:num_predict=4096;mistral-small-24b:num_predict=4096",
			wantCaps: map[string]int{
				// r1-32b carries the R7-fixed 3000 cap: 4096 at the measured
				// 40.3 tok/s is ~107 s of decode — over the 93 s window.
				"deepseek-r1-32b": 3000, "qwen3.6-27b": 4096, "gemma4-26b": 4096,
				"mistral-small-24b": 4096,
			},
		},
		{
			worker:          "worker-6",
			supportedModels: "qwen3.8-27b,ornith-1.5-35b,agentworld-35b,kat-coder-32b,lfm2.5-8b,agentworld-35b-max",
			modelOptions:    "qwen3.8-27b:num_predict=4096;ornith-1.5-35b:num_predict=4096;agentworld-35b:num_predict=4096;kat-coder-32b:num_predict=4096;lfm2.5-8b:num_predict=2048;agentworld-35b-max:num_predict=8192",
			wantCaps: map[string]int{
				"qwen3.8-27b": 4096, "ornith-1.5-35b": 4096, "agentworld-35b": 4096,
				"kat-coder-32b": 4096, "lfm2.5-8b": 2048, "agentworld-35b-max": 8192,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.worker, func(t *testing.T) {
			validEnv(t)
			t.Setenv("SUPPORTED_MODELS", tc.supportedModels)
			t.Setenv("MODEL_OPTIONS", tc.modelOptions)

			cfg, err := Load()
			require.NoError(t, err, "%s production MODEL_OPTIONS must parse", tc.worker)
			require.Len(t, cfg.ModelOptions, len(tc.wantCaps),
				"every advertised model on %s must carry its catalogue cap", tc.worker)
			for model, want := range tc.wantCaps {
				opts, ok := cfg.ModelOptions[model]
				require.True(t, ok, "%s missing MODEL_OPTIONS entry", model)
				assert.Equal(t, want, opts.NumPredict,
					"%s cap drifted from tier-catalog.json — update both together", model)
				assert.Equal(t, 0, opts.NumCtx, "num_ctx stays at the process default (per-worker ruling)")
			}
		})
	}
}

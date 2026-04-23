// Package config loads and validates worker sidecar configuration from environment variables.
package config

import (
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Config holds all worker sidecar configuration parsed from environment variables.
type Config struct {
	// Identity — Ethereum signing key
	WorkerKeystorePath     string
	WorkerKeystorePassword string

	// Identity — ECDH encryption key (separate from signing key)
	EncryptionKeystorePath string

	// Chain
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address
	JobRegistryAddress    common.Address

	// Gas
	GasPriceMultiplierBps int
	MaxGasPrice           *big.Int

	// Models — comma-separated list
	SupportedModels []string

	// Redis
	RedisURL      string
	RedisPassword string

	// Heartbeat
	HeartbeatInterval time.Duration

	// Ollama
	OllamaURL     string
	OllamaTimeout time.Duration

	// Beacon API (CL node)
	BeaconAPIURL string

	// Job execution
	MaxConcurrentJobs int
	AckTxTimeout      time.Duration
	BlobTxTimeout     time.Duration
	BlobFetchTimeout  time.Duration
	BlobFetchRetries  int
	SessionKeyFile    string
	ReceiptPollInterval time.Duration

	// Stuck-nonce recovery (Hazard B). When the worker's signing key has a
	// tx stuck in the mempool — e.g. BlobFeeCap underbid the current blob
	// base fee — every SendTransaction is rejected with "address already
	// reserved" and the ResetNonce+refetch loop cannot clear it. These
	// settings bound detection and automated replacement.
	//
	// StuckNonceThreshold — consecutive rejections at the same nonce before
	// declaring it stuck. Default 5 tolerates transient pool flakiness.
	//
	// StuckNonceMaxBumps — max replacement-tx attempts per stuck nonce
	// before returning a loud error. Default 3 caps the worst-case gas
	// cost of automated recovery.
	//
	// StuckNonceAutoReplace — operator kill-switch. When false the tracker
	// still detects and logs, but no replacement tx is submitted.
	StuckNonceThreshold    int
	StuckNonceMaxBumps     int
	StuckNonceAutoReplace  bool

	// Shutdown
	ShutdownTimeout time.Duration

	// Logging
	LogLevel  string
	LogFormat string
}

// Load reads all environment variables and returns a populated Config.
// It does not validate — call Validate() after Load() to check constraints.
func Load() (*Config, error) {
	cfg := &Config{
		WorkerKeystorePath:     os.Getenv("WORKER_KEYSTORE_PATH"),
		WorkerKeystorePassword: os.Getenv("WORKER_KEYSTORE_PASSWORD"),
		EncryptionKeystorePath: envOrDefault("ENCRYPTION_KEYSTORE_PATH", "data/worker-encryption.key"),
		RPCURL:                 envOrDefault("RPC_URL", "http://localhost:8545"),
		RedisURL:               envOrDefault("REDIS_URL", "redis://localhost:6379"),
		RedisPassword:          os.Getenv("REDIS_PASSWORD"),
		OllamaURL:              envOrDefault("OLLAMA_URL", "http://localhost:11434"),
		BeaconAPIURL:           envOrDefault("BEACON_API_URL", "http://localhost:3500"),
		SessionKeyFile:         envOrDefault("SESSION_KEY_FILE", "data/session-keys.enc"),
		LogLevel:               envOrDefault("LOG_LEVEL", "info"),
		LogFormat:              envOrDefault("LOG_FORMAT", "json"),
	}

	var errs []string

	// ChainID — required int64
	chainIDStr := os.Getenv("CHAIN_ID")
	if chainIDStr == "" {
		errs = append(errs, "CHAIN_ID is required")
	} else {
		v := new(big.Int)
		if _, ok := v.SetString(chainIDStr, 10); !ok {
			errs = append(errs, fmt.Sprintf("CHAIN_ID: invalid integer %q", chainIDStr))
		} else if !v.IsInt64() {
			errs = append(errs, fmt.Sprintf("CHAIN_ID: value %q overflows int64", chainIDStr))
		} else {
			cfg.ChainID = v.Int64()
		}
	}

	// WorkerRegistryAddress — required
	registryStr := os.Getenv("WORKER_REGISTRY_ADDRESS")
	if registryStr == "" {
		errs = append(errs, "WORKER_REGISTRY_ADDRESS is required")
	} else if !common.IsHexAddress(registryStr) {
		errs = append(errs, fmt.Sprintf("WORKER_REGISTRY_ADDRESS: invalid hex address %q", registryStr))
	} else {
		cfg.WorkerRegistryAddress = common.HexToAddress(registryStr)
	}

	// AIConfigAddress — required
	aiConfigStr := os.Getenv("AI_CONFIG_ADDRESS")
	if aiConfigStr == "" {
		errs = append(errs, "AI_CONFIG_ADDRESS is required")
	} else if !common.IsHexAddress(aiConfigStr) {
		errs = append(errs, fmt.Sprintf("AI_CONFIG_ADDRESS: invalid hex address %q", aiConfigStr))
	} else {
		cfg.AIConfigAddress = common.HexToAddress(aiConfigStr)
	}

	// JobRegistryAddress — required
	jobRegStr := os.Getenv("JOB_REGISTRY_ADDRESS")
	if jobRegStr == "" {
		errs = append(errs, "JOB_REGISTRY_ADDRESS is required")
	} else if !common.IsHexAddress(jobRegStr) {
		errs = append(errs, fmt.Sprintf("JOB_REGISTRY_ADDRESS: invalid hex address %q", jobRegStr))
	} else {
		cfg.JobRegistryAddress = common.HexToAddress(jobRegStr)
	}

	// GasPriceMultiplierBps — default 11000 (1.1x)
	cfg.GasPriceMultiplierBps = parseInt("GAS_PRICE_MULTIPLIER_BPS", 11000, &errs)

	// MaxGasPrice — optional; default 100 gwei
	if maxGasStr := os.Getenv("MAX_GAS_PRICE"); maxGasStr != "" {
		maxGas := new(big.Int)
		if _, ok := maxGas.SetString(maxGasStr, 10); !ok {
			errs = append(errs, fmt.Sprintf("MAX_GAS_PRICE: invalid decimal wei value %q", maxGasStr))
		} else if maxGas.Sign() <= 0 {
			errs = append(errs, "MAX_GAS_PRICE: must be positive")
		} else {
			cfg.MaxGasPrice = maxGas
		}
	} else {
		cfg.MaxGasPrice = new(big.Int).Mul(big.NewInt(100), big.NewInt(1_000_000_000)) // 100 gwei
	}

	// SupportedModels — required, comma-separated
	modelsStr := os.Getenv("SUPPORTED_MODELS")
	if modelsStr == "" {
		errs = append(errs, "SUPPORTED_MODELS is required")
	} else {
		for _, m := range strings.Split(modelsStr, ",") {
			if trimmed := strings.TrimSpace(m); trimmed != "" {
				cfg.SupportedModels = append(cfg.SupportedModels, trimmed)
			}
		}
		if len(cfg.SupportedModels) == 0 {
			errs = append(errs, "SUPPORTED_MODELS: no valid model entries after parsing")
		}
	}

	// Durations
	cfg.HeartbeatInterval = parseDuration("HEARTBEAT_INTERVAL", "10s", &errs)
	cfg.ShutdownTimeout = parseDuration("SHUTDOWN_TIMEOUT", "30s", &errs)
	cfg.OllamaTimeout = parseDuration("OLLAMA_TIMEOUT", "120s", &errs)
	cfg.AckTxTimeout = parseDuration("ACK_TX_TIMEOUT", "15s", &errs)
	// BlobTxTimeout bounds stage 8 (submit_blob): slot wait + SendTransaction
	// + WaitMined. Default 90s = ~15 blocks at 6s block time, generous buffer
	// over the ~12s typical to absorb network jitter and queued broadcasts.
	cfg.BlobTxTimeout = parseDuration("BLOB_TX_TIMEOUT", "90s", &errs)
	cfg.BlobFetchTimeout = parseDuration("BLOB_FETCH_TIMEOUT", "10s", &errs)
	cfg.ReceiptPollInterval = parseDuration("RECEIPT_POLL_INTERVAL", "2s", &errs)

	// Job execution integers
	cfg.MaxConcurrentJobs = parseInt("MAX_CONCURRENT_JOBS", 2, &errs)
	cfg.BlobFetchRetries = parseInt("BLOB_FETCH_RETRIES", 3, &errs)

	// Stuck-nonce recovery config
	cfg.StuckNonceThreshold = parseInt("WORKER_STUCK_NONCE_THRESHOLD", 5, &errs)
	cfg.StuckNonceMaxBumps = parseInt("WORKER_STUCK_NONCE_MAX_BUMPS", 3, &errs)
	cfg.StuckNonceAutoReplace = parseBool("WORKER_STUCK_NONCE_AUTOREPLACE", true, &errs)

	if len(errs) > 0 {
		return nil, fmt.Errorf("config load errors:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

// Validate checks all constraints on a loaded Config and returns a slice of human-readable
// error strings — one per invalid field. The caller should log all of them then os.Exit(1).
// Validate is separate from Load so callers can validate after post-load adjustments.
func (c *Config) Validate() []string {
	var errs []string

	if c.WorkerKeystorePath == "" {
		errs = append(errs, "WORKER_KEYSTORE_PATH is required")
	}
	if c.WorkerKeystorePassword == "" {
		errs = append(errs, "WORKER_KEYSTORE_PASSWORD is required")
	}
	if c.ChainID <= 0 {
		errs = append(errs, "CHAIN_ID must be positive")
	}
	if c.WorkerRegistryAddress == (common.Address{}) {
		errs = append(errs, "WORKER_REGISTRY_ADDRESS must be a non-zero address")
	}
	if c.AIConfigAddress == (common.Address{}) {
		errs = append(errs, "AI_CONFIG_ADDRESS must be a non-zero address")
	}
	if c.JobRegistryAddress == (common.Address{}) {
		errs = append(errs, "JOB_REGISTRY_ADDRESS must be a non-zero address")
	}
	if len(c.SupportedModels) == 0 {
		errs = append(errs, "SUPPORTED_MODELS: at least one model is required")
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, "HEARTBEAT_INTERVAL must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, "SHUTDOWN_TIMEOUT must be positive")
	}
	if c.GasPriceMultiplierBps <= 0 {
		errs = append(errs, "GAS_PRICE_MULTIPLIER_BPS must be positive")
	}
	if c.MaxConcurrentJobs <= 0 {
		errs = append(errs, "MAX_CONCURRENT_JOBS must be positive")
	}
	if c.OllamaTimeout <= 0 {
		errs = append(errs, "OLLAMA_TIMEOUT must be positive")
	}
	if c.AckTxTimeout <= 0 {
		errs = append(errs, "ACK_TX_TIMEOUT must be positive")
	}
	if c.BlobTxTimeout <= 0 {
		errs = append(errs, "BLOB_TX_TIMEOUT must be positive")
	}
	if c.BlobFetchTimeout <= 0 {
		errs = append(errs, "BLOB_FETCH_TIMEOUT must be positive")
	}
	if c.BlobFetchRetries <= 0 {
		errs = append(errs, "BLOB_FETCH_RETRIES must be positive")
	}
	if c.ReceiptPollInterval <= 0 {
		errs = append(errs, "RECEIPT_POLL_INTERVAL must be positive")
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT: must be \"json\" or \"text\", got %q", c.LogFormat))
	}
	if c.StuckNonceThreshold <= 0 {
		errs = append(errs, "WORKER_STUCK_NONCE_THRESHOLD must be positive")
	}
	if c.StuckNonceMaxBumps <= 0 {
		errs = append(errs, "WORKER_STUCK_NONCE_MAX_BUMPS must be positive")
	}

	return errs
}

// ParseModelID converts a string to the bytes32 model ID format used on-chain.
// Accepts:
//   - "0x{64 hex chars}" — decoded as-is into [32]byte
//   - any other string   — keccak256([]byte(s)), keeping first 32 bytes
func ParseModelID(s string) ([32]byte, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		hex := s[2:]
		if len(hex) != 64 {
			return [32]byte{}, fmt.Errorf("ParseModelID: hex model ID must be 64 hex chars (got %d): %q", len(hex), s)
		}
		b, err := hexDecode(hex)
		if err != nil {
			return [32]byte{}, fmt.Errorf("ParseModelID: invalid hex %q: %w", s, err)
		}
		var id [32]byte
		copy(id[:], b)
		return id, nil
	}

	// keccak256 of the UTF-8 name
	hash := crypto.Keccak256([]byte(s))
	var id [32]byte
	copy(id[:], hash)
	return id, nil
}

// hexDecode decodes a hex string (without 0x prefix) into bytes.
func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex string")
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi, ok1 := hexNibble(s[i])
		lo, ok2 := hexNibble(s[i+1])
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("invalid hex character at position %d", i)
		}
		b[i/2] = hi<<4 | lo
	}
	return b, nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func parseDuration(key, defaultVal string, errs *[]string) time.Duration {
	raw := envOrDefault(key, defaultVal)
	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: invalid duration %q: %v", key, raw, err))
		return 0
	}
	return d
}

// parseBool reads an env var and parses standard truthy/falsy strings.
// Accepts: true/false, 1/0, yes/no, on/off (case-insensitive). Empty
// defaults to defaultVal.
func parseBool(key string, defaultVal bool, errs *[]string) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultVal
	}
	switch strings.ToLower(raw) {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		*errs = append(*errs, fmt.Sprintf("%s: invalid boolean %q", key, raw))
		return defaultVal
	}
}

func parseInt(key string, defaultVal int, errs *[]string) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultVal
	}
	v := new(big.Int)
	if _, ok := v.SetString(raw, 10); !ok {
		*errs = append(*errs, fmt.Sprintf("%s: invalid integer %q", key, raw))
		return defaultVal
	}
	if !v.IsInt64() {
		*errs = append(*errs, fmt.Sprintf("%s: value %q overflows int64", key, raw))
		return defaultVal
	}
	i64 := v.Int64()
	if int64(int(i64)) != i64 {
		*errs = append(*errs, fmt.Sprintf("%s: value %q overflows int", key, raw))
		return defaultVal
	}
	return int(i64)
}

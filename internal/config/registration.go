package config

import (
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// RegistrationConfig holds configuration for the operator CLI (lightchain-worker).
// It avoids requiring Redis, Ollama, heartbeat, or other sidecar-only env vars,
// keeping the operator workflow minimal.
type RegistrationConfig struct {
	// Identity — Ethereum signing key
	WorkerKeystorePath     string
	WorkerKeystorePassword string

	// Identity — ECDH encryption key
	EncryptionKeystorePath string

	// Chain
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address

	// Models — comma-separated list
	SupportedModels []string

	// Staking — nil means "query getMinWorkerStake() at startup"
	WorkerStake *big.Int

	// Gas
	GasPriceMultiplierBps int
	MaxGasPrice           *big.Int

	// Logging
	LogLevel  string
	LogFormat string
}

// LoadRegistration reads environment variables for the CLI registration config.
// It does not validate — call Validate() after LoadRegistration().
func LoadRegistration() (*RegistrationConfig, error) {
	cfg := &RegistrationConfig{
		WorkerKeystorePath:     os.Getenv("WORKER_KEYSTORE_PATH"),
		WorkerKeystorePassword: os.Getenv("WORKER_KEYSTORE_PASSWORD"),
		EncryptionKeystorePath: envOrDefault("ENCRYPTION_KEYSTORE_PATH", "data/worker-encryption.key"),
		RPCURL:                 envOrDefault("RPC_URL", "http://localhost:8545"),
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

	// SupportedModels — required for register/add-models; may be empty for other commands
	modelsStr := os.Getenv("SUPPORTED_MODELS")
	if modelsStr != "" {
		for _, m := range strings.Split(modelsStr, ",") {
			if trimmed := strings.TrimSpace(m); trimmed != "" {
				cfg.SupportedModels = append(cfg.SupportedModels, trimmed)
			}
		}
	}

	// WorkerStake — optional; nil means auto-query
	if stakeStr := os.Getenv("WORKER_STAKE"); stakeStr != "" {
		stake := new(big.Int)
		if _, ok := stake.SetString(stakeStr, 10); !ok {
			errs = append(errs, fmt.Sprintf("WORKER_STAKE: invalid decimal wei value %q", stakeStr))
		} else if stake.Sign() < 0 {
			errs = append(errs, "WORKER_STAKE: must be non-negative")
		} else {
			cfg.WorkerStake = stake
		}
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

	if len(errs) > 0 {
		return nil, fmt.Errorf("config load errors:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

// Validate checks all constraints on a loaded RegistrationConfig.
func (c *RegistrationConfig) Validate() []string {
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
	if c.GasPriceMultiplierBps <= 0 {
		errs = append(errs, "GAS_PRICE_MULTIPLIER_BPS must be positive")
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT: must be \"json\" or \"text\", got %q", c.LogFormat))
	}

	return errs
}

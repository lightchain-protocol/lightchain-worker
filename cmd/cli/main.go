// Binary lightchain-worker is the operator CLI for managing worker on-chain registration.
// It handles registration, deregistration, model management, and status checks — all
// operations that control stake and should be separated from the runtime sidecar.
//
// Usage:
//
//	lightchain-worker <command>
//
// Commands:
//
//	keygen      Generate/load ECDH encryption key and print public key hex
//	register    Register worker on-chain (stake + encryption key + models)
//	add-models  Add models to an already-registered worker
//	deregister  Deregister worker and withdraw stake
//	status      Check on-chain registration status
package main

import (
	"context"
	"crypto/ecdsa"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	ethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/cli"
	"github.com/lightchain/worker/internal/config"
	"github.com/lightchain/worker/internal/keystore"
)

const txTimeout = 120 * time.Second

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "import-key":
		runImportKey()
	case "keygen":
		runKeygen()
	case "register":
		runRegister()
	case "add-models":
		runAddModels()
	case "deregister":
		runDeregister()
	case "status":
		runStatus()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `lightchain-worker — operator CLI for worker on-chain registration

Usage: lightchain-worker <command>

Commands:
  import-key  Import a hex private key into an encrypted keystore file
  keygen      Generate/load ECDH encryption key and print public key hex
  register    Register worker on-chain (stake + encryption key + models)
  add-models  Add models to an already-registered worker
  deregister  Deregister worker and withdraw stake
  status      Check on-chain registration status

All configuration is via environment variables. See docs/worker-cli.md for details.

import-key flags:
  --private-key <hex>   Hex-encoded private key (without 0x prefix)
  --password <string>   Password to encrypt the keystore
  --output <dir>        Directory to write the keystore file (default: ./eth-keystore)`)
}

func runImportKey() {
	fs := flag.NewFlagSet("import-key", flag.ExitOnError)
	privKeyHex := fs.String("private-key", "", "Hex-encoded private key (without 0x prefix)")
	password := fs.String("password", "", "Password to encrypt the keystore")
	outputDir := fs.String("output", "./eth-keystore", "Directory to write the keystore file")
	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(1)
	}

	if *privKeyHex == "" || *password == "" {
		fmt.Fprintln(os.Stderr, "import-key requires --private-key and --password flags")
		fs.Usage()
		os.Exit(1)
	}

	// Strip 0x prefix if present.
	hex := strings.TrimPrefix(*privKeyHex, "0x")

	privateKey, err := crypto.HexToECDSA(hex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid private key: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*outputDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "create output directory: %v\n", err)
		os.Exit(1)
	}

	ks := ethkeystore.NewKeyStore(*outputDir, ethkeystore.StandardScryptN, ethkeystore.StandardScryptP)
	account, err := ks.ImportECDSA(privateKey, *password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "import key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Address:  %s\n", account.Address.Hex())
	fmt.Printf("Keystore: %s\n", account.URL.Path)
}

func runKeygen() {
	cfg, logger := loadAndValidateCfg()

	h := &cli.Handler{
		ECDHKeyPath: cfg.EncryptionKeystorePath,
		ECDHPass:    cfg.WorkerKeystorePassword,
		LoadECDHKey: keystore.LoadOrGenerate,
		Out:         os.Stdout,
		Logger:      logger,
	}

	if err := h.Keygen(); err != nil {
		logger.Error("keygen failed", "error", err)
		os.Exit(1)
	}
}

func runRegister() {
	cfg, logger := loadAndValidateCfg()
	signingKey, workerAddr := loadSigningKey(cfg, logger)
	modelIDs, modelNames := parseModels(cfg, logger)

	chainClient := dialChain(cfg, signingKey, logger)
	defer chainClient.Close()

	h := &cli.Handler{
		Client:      chainClient,
		WorkerAddr:  workerAddr,
		ModelIDs:    modelIDs,
		ModelNames:  modelNames,
		ECDHKeyPath: cfg.EncryptionKeystorePath,
		ECDHPass:    cfg.WorkerKeystorePassword,
		WorkerStake: cfg.WorkerStake,
		LoadECDHKey: keystore.LoadOrGenerate,
		Out:         os.Stdout,
		Logger:      logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	if err := h.Register(ctx); err != nil {
		logger.Error("registration failed", "error", err)
		os.Exit(1)
	}
}

func runAddModels() {
	cfg, logger := loadAndValidateCfg()
	signingKey, workerAddr := loadSigningKey(cfg, logger)
	modelIDs, modelNames := parseModels(cfg, logger)

	chainClient := dialChain(cfg, signingKey, logger)
	defer chainClient.Close()

	h := &cli.Handler{
		Client:     chainClient,
		WorkerAddr: workerAddr,
		ModelIDs:   modelIDs,
		ModelNames: modelNames,
		Out:        os.Stdout,
		Logger:     logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	if err := h.AddModels(ctx); err != nil {
		logger.Error("add-models failed", "error", err)
		os.Exit(1)
	}
}

func runDeregister() {
	cfg, logger := loadAndValidateCfg()
	signingKey, workerAddr := loadSigningKey(cfg, logger)

	chainClient := dialChain(cfg, signingKey, logger)
	defer chainClient.Close()

	h := &cli.Handler{
		Client:     chainClient,
		WorkerAddr: workerAddr,
		Out:        os.Stdout,
		Logger:     logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	if err := h.Deregister(ctx); err != nil {
		logger.Error("deregistration failed", "error", err)
		os.Exit(1)
	}
}

func runStatus() {
	cfg, logger := loadAndValidateCfg()
	signingKey, workerAddr := loadSigningKey(cfg, logger)

	chainClient := dialChain(cfg, signingKey, logger)
	defer chainClient.Close()

	h := &cli.Handler{
		Client:      chainClient,
		WorkerAddr:  workerAddr,
		ECDHKeyPath: cfg.EncryptionKeystorePath,
		ECDHPass:    cfg.WorkerKeystorePassword,
		LoadECDHKey: keystore.LoadOrGenerate,
		Out:         os.Stdout,
		Logger:      logger,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := h.Status(ctx); err != nil {
		logger.Error("status check failed", "error", err)
		os.Exit(1)
	}
}

// loadAndValidateCfg loads and validates the RegistrationConfig, exiting on error.
func loadAndValidateCfg() (*config.RegistrationConfig, *slog.Logger) {
	cfg, err := config.LoadRegistration()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			slog.Error("config invalid", "field", e)
		}
		slog.Error("config validation failed", "count", len(errs), "fields", strings.Join(errs, "; "))
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel, cfg.LogFormat)
	return cfg, logger
}

// loadSigningKey reads the Ethereum keystore and returns the ECDSA private key + derived address.
func loadSigningKey(cfg *config.RegistrationConfig, logger *slog.Logger) (*ecdsa.PrivateKey, common.Address) {
	keystoreJSON, err := os.ReadFile(cfg.WorkerKeystorePath)
	if err != nil {
		logger.Error("failed to read keystore", "path", cfg.WorkerKeystorePath, "error", err)
		os.Exit(1)
	}

	ethKey, err := ethkeystore.DecryptKey(keystoreJSON, cfg.WorkerKeystorePassword)
	if err != nil {
		logger.Error("failed to decrypt keystore", "error", err)
		os.Exit(1)
	}

	workerAddr := crypto.PubkeyToAddress(ethKey.PrivateKey.PublicKey)
	logger.Info("loaded signing key", "address", workerAddr.Hex())

	return ethKey.PrivateKey, workerAddr
}

// parseModels converts SupportedModels strings to bytes32 IDs using the cli package.
func parseModels(cfg *config.RegistrationConfig, logger *slog.Logger) ([][32]byte, []string) {
	modelIDs, err := cli.ParseAndDeduplicateModelIDs(cfg.SupportedModels)
	if err != nil {
		logger.Error("invalid model configuration", "error", err)
		os.Exit(1)
	}
	return modelIDs, cfg.SupportedModels
}

// dialChain creates a ChainClient for the CLI. cfg.JobRegistryAddress may be
// the zero address for non-settlement subcommands; chain.NewChainClient
// conditionally binds JobRegistry only when the address is non-zero.
// Settlement subcommands (balance, withdraw, release) must call
// cfg.ValidateForSettlement() before calling dialChain so misconfiguration
// fails loudly rather than producing nil-binding panics later.
func dialChain(cfg *config.RegistrationConfig, signingKey *ecdsa.PrivateKey, logger *slog.Logger) *chain.ChainClient {
	client, err := chain.NewChainClient(
		cfg.RPCURL,
		cfg.ChainID,
		cfg.WorkerRegistryAddress,
		cfg.AIConfigAddress,
		cfg.JobRegistryAddress,
		signingKey,
		cfg.GasPriceMultiplierBps,
		// CLI is single-threaded — no concurrent broadcasts possible, so
		// a private coordinator is correct. Not shared with a blob path
		// because the CLI never submits blobs.
		chain.NewSubpoolCoordinator(logger, 0),
		chain.NewStuckNonceTracker(),
	)
	if err != nil {
		logger.Error("failed to connect to chain", "error", err)
		os.Exit(1)
	}

	return client
}

// newLogger builds a slog.Logger based on log level and format settings.
func newLogger(level, format string) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: logLevel}
	var handler slog.Handler
	if format == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(handler)
}

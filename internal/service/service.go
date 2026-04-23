// Package service wires all worker sidecar components together and manages the
// startup + graceful shutdown lifecycle.
package service

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	ethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/lightchain/worker/internal/blob"
	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/config"
	"github.com/lightchain/worker/internal/heartbeat"
	"github.com/lightchain/worker/internal/keystore"
	"github.com/lightchain/worker/internal/ollama"
	"github.com/lightchain/worker/internal/pipeline"
	"github.com/lightchain/worker/internal/registration"
)

// startupHeartbeatTimeout is the maximum time allowed for the initial heartbeat
// write during service startup. It validates Redis connectivity before accepting traffic.
const startupHeartbeatTimeout = 5 * time.Second

// Service owns all worker sidecar components and coordinates startup and shutdown.
type Service struct {
	cfg             *config.Config
	chainClient     *chain.ChainClient
	redis           *redis.Client
	monitor         *heartbeat.Monitor
	asynqServer     *asynq.Server
	asynqMux        *asynq.ServeMux
	sessionKeyStore *keystore.SessionKeyStore
	jobCounter      *atomic.Int32
	logger          *slog.Logger
}

// New initializes all components: loads keys, dials chain + Redis, registers on-chain,
// creates the job pipeline, and returns a fully ready Service. Call Run to start.
func New(cfg *config.Config) (*Service, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config must not be nil")
	}

	logger := newLogger(cfg.LogLevel, cfg.LogFormat)

	// Load Ethereum signing key from the go-ethereum keystore file
	keystoreJSON, err := os.ReadFile(cfg.WorkerKeystorePath)
	if err != nil {
		return nil, fmt.Errorf("read keystore %s: %w", cfg.WorkerKeystorePath, err)
	}

	ethKey, err := ethkeystore.DecryptKey(keystoreJSON, cfg.WorkerKeystorePassword)
	if err != nil {
		return nil, fmt.Errorf("decrypt Ethereum keystore: %w", err)
	}

	signingKey := ethKey.PrivateKey
	workerAddr := crypto.PubkeyToAddress(signingKey.PublicKey)

	// Load or generate the ECDH encryption key pair
	ecdhKey, err := keystore.LoadOrGenerate(cfg.EncryptionKeystorePath, cfg.WorkerKeystorePassword)
	if err != nil {
		return nil, fmt.Errorf("load ECDH keystore: %w", err)
	}

	// Parse model IDs from config strings
	modelIDs := make([][32]byte, 0, len(cfg.SupportedModels))
	for _, s := range cfg.SupportedModels {
		id, err := config.ParseModelID(s)
		if err != nil {
			return nil, fmt.Errorf("parse model ID %q: %w", s, err)
		}
		modelIDs = append(modelIDs, id)
	}

	// Reject duplicate model IDs before any on-chain I/O
	seen := make(map[[32]byte]bool, len(modelIDs))
	for _, id := range modelIDs {
		if seen[id] {
			return nil, fmt.Errorf("duplicate model ID in SUPPORTED_MODELS: %s", modelIDToHex(id))
		}
		seen[id] = true
	}

	// Build hex model ID strings for heartbeat payload
	modelHexStrings := make([]string, len(modelIDs))
	modelIDToName := make(map[string]string, len(modelIDs)*2)
	for i, id := range modelIDs {
		modelHex := modelIDToHex(id)
		modelHexStrings[i] = modelHex
		modelIDToName[strings.ToLower(modelHex)] = cfg.SupportedModels[i]
		modelIDToName[strings.TrimPrefix(strings.ToLower(modelHex), "0x")] = cfg.SupportedModels[i]
	}

	// Dial chain (now includes JobRegistry binding)
	chainClient, err := chain.NewChainClient(
		cfg.RPCURL,
		cfg.ChainID,
		cfg.WorkerRegistryAddress,
		cfg.AIConfigAddress,
		cfg.JobRegistryAddress,
		signingKey,
		cfg.GasPriceMultiplierBps,
	)
	if err != nil {
		return nil, fmt.Errorf("connect to chain: %w", err)
	}

	// Validate on-chain registration (read-only — registration is handled by the operator CLI)
	validator := registration.NewValidator(chainClient, workerAddr, logger)

	valCtx, valCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer valCancel()

	ecdhPubKeyBytes := ecdhKey.PublicKey().Bytes()
	if err := validator.Validate(valCtx, ecdhPubKeyBytes); err != nil {
		chainClient.Close()
		return nil, fmt.Errorf("validate registration: %w", err)
	}

	// Session key store
	sessionKeyStore, err := keystore.NewSessionKeyStore(cfg.SessionKeyFile, cfg.WorkerKeystorePassword)
	if err != nil {
		chainClient.Close()
		return nil, fmt.Errorf("create session key store: %w", err)
	}

	// Ollama client
	ollamaClient := ollama.NewOllamaClient(cfg.OllamaURL, cfg.OllamaTimeout)
	if err := ollamaClient.VerifyModels(context.Background(), cfg.SupportedModels); err != nil {
		logger.Warn("ollama model verification failed (non-fatal)", "error", err)
	}

	// Dial Redis (before blob setup — RedisBlobFetcher needs the client)
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		chainClient.Close()
		return nil, fmt.Errorf("parse Redis URL %q: %w", cfg.RedisURL, err)
	}
	if cfg.RedisPassword != "" {
		redisOpts.Password = cfg.RedisPassword
	}
	redisClient := redis.NewClient(redisOpts)

	// Blob fetcher + submitter (mode-dependent)
	blobMode := strings.ToLower(strings.TrimSpace(os.Getenv("BLOB_MODE")))
	var blobFetcher blob.BlobFetcher
	var blobSubmitter blob.BlobSubmitter
	switch blobMode {
	case "redis":
		blobFetcher = blob.NewRedisBlobFetcher(redisClient)
		blobSubmitter = blob.NewRedisBlobSubmitter(redisClient)
		logger.Info("blob mode: redis (dev)")
	case "", "beacon":
		blobFetcher = blob.NewBeaconClient(
			cfg.BeaconAPIURL,
			chainClient.EthClient(),
			cfg.BlobFetchTimeout,
			cfg.BlobFetchRetries,
		)
		blobSubmitter = blob.NewBlobTxSubmitter(
			chainClient.EthClient(),
			signingKey,
			big.NewInt(cfg.ChainID),
			chainClient.NonceManager(),
			cfg.MaxGasPrice,
			logger,
		)
		logger.Info("blob mode: eip-4844 (beacon)")
	default:
		_ = redisClient.Close()
		chainClient.Close()
		return nil, fmt.Errorf("unsupported BLOB_MODE %q", blobMode)
	}

	// Redis already dialed above

	// Shared job counter between pipeline handler and heartbeat monitor
	jobCounter := &atomic.Int32{}

	// Job pipeline handler
	handler := pipeline.NewJobHandler(
		chainClient,
		blobFetcher,
		blobSubmitter,
		sessionKeyStore,
		ollamaClient,
		redisClient,
		signingKey,
		ecdhKey,
		jobCounter,
		logger,
		pipeline.HandlerConfig{
			AckTxTimeout:    cfg.AckTxTimeout,
			ModelIDToName:   modelIDToName,
			ChainID:         big.NewInt(cfg.ChainID),
			JobRegistryAddr: cfg.JobRegistryAddress,
		},
	)

	// Asynq server — listens on worker-specific queue
	// Queue name must match dispatcher's workerQueueName(): "worker:{lowercase_hex_with_0x}"
	queueName := fmt.Sprintf("worker:%s", strings.ToLower(workerAddr.Hex()))
	asynqSrv := asynq.NewServer(
		asynqRedisClientOptFromRedisOptions(redisOpts),
		asynq.Config{
			Concurrency: cfg.MaxConcurrentJobs,
			Queues:      map[string]int{queueName: 1},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(pipeline.TaskTypeJobInference, handler.HandleTask)

	// Heartbeat monitor — shared job counter for dynamic ActiveJobs/MaxJobs
	monitorCfg := heartbeat.MonitorConfig{
		Interval:  cfg.HeartbeatInterval,
		OllamaURL: cfg.OllamaURL,
	}
	addrHex := checksumHexNoPrefix(workerAddr)
	monitor := heartbeat.NewMonitor(redisClient, monitorCfg, addrHex, modelHexStrings, jobCounter, cfg.MaxConcurrentJobs, logger)

	// Gate startup on a real heartbeat write
	startCtx, startCancel := context.WithTimeout(context.Background(), startupHeartbeatTimeout)
	defer startCancel()
	if err := monitor.EmitOnce(startCtx); err != nil {
		_ = redisClient.Close()
		chainClient.Close()
		return nil, fmt.Errorf("initial heartbeat write failed — check Redis config: %w", err)
	}

	logger.Info("worker service initialized",
		"address", workerAddr.Hex(),
		"models", len(modelIDs),
		"maxConcurrentJobs", cfg.MaxConcurrentJobs,
		"queue", queueName,
	)

	return &Service{
		cfg:             cfg,
		chainClient:     chainClient,
		redis:           redisClient,
		monitor:         monitor,
		asynqServer:     asynqSrv,
		asynqMux:        mux,
		sessionKeyStore: sessionKeyStore,
		jobCounter:      jobCounter,
		logger:          logger,
	}, nil
}

// Run starts the heartbeat goroutine and Asynq server, waits for SIGINT/SIGTERM,
// then gracefully shuts down. Shutdown must complete within cfg.ShutdownTimeout.
func (s *Service) Run(ctx context.Context) error {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	s.monitor.Start(runCtx)

	asynqErrCh := make(chan error, 1)
	go func() {
		asynqErrCh <- s.asynqServer.Run(s.asynqMux)
	}()

	sigCtx, stop := signal.NotifyContext(runCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.logger.Info("worker sidecar running — waiting for shutdown signal")
	var runErr error

	select {
	case <-sigCtx.Done():
		stop()
	case err := <-asynqErrCh:
		runCancel()
		stop()
		if err != nil {
			runErr = fmt.Errorf("asynq server: %w", err)
			s.logger.Error("asynq server exited unexpectedly", "error", err)
		} else {
			runErr = fmt.Errorf("asynq server exited unexpectedly")
			s.logger.Error("asynq server exited unexpectedly")
		}
	}

	s.logger.Info("shutdown signal received, stopping gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	shutdownErr := s.shutdown(shutdownCtx)

	if shutdownErr != nil {
		s.logger.Error("worker sidecar stopped with shutdown errors", "error", shutdownErr)
		return errors.Join(runErr, shutdownErr)
	}

	s.logger.Info("worker sidecar stopped cleanly")
	return runErr
}

// shutdown coordinates all cleanup steps within the given context deadline.
func (s *Service) shutdown(ctx context.Context) error {
	var shutdownErr error

	// Stop Asynq server — waits for in-flight jobs
	s.asynqServer.Shutdown()

	// Zero all session keys
	if err := s.sessionKeyStore.ZeroAll(); err != nil {
		s.logger.Error("failed to zero session keys on shutdown", "error", err)
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("zero session keys: %w", err))
	}

	monitorDone := make(chan struct{})
	go func() {
		s.monitor.Stop()
		close(monitorDone)
	}()
	select {
	case <-monitorDone:
	case <-ctx.Done():
		s.logger.Warn("heartbeat monitor stop timed out")
	}

	if err := s.redis.Close(); err != nil {
		s.logger.Warn("redis close error", "error", err)
	}

	s.chainClient.Close()

	select {
	case <-ctx.Done():
		return errors.Join(shutdownErr, fmt.Errorf("shutdown timeout exceeded: %w", ctx.Err()))
	default:
		return shutdownErr
	}
}

func asynqRedisClientOptFromRedisOptions(opts *redis.Options) asynq.RedisClientOpt {
	return asynq.RedisClientOpt{
		Network:      opts.Network,
		Addr:         opts.Addr,
		Username:     opts.Username,
		Password:     opts.Password,
		DB:           opts.DB,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		PoolSize:     opts.PoolSize,
		TLSConfig:    opts.TLSConfig,
	}
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

// modelIDToHex converts a [32]byte model ID to a 0x-prefixed lowercase hex string.
func modelIDToHex(id [32]byte) string {
	return "0x" + hex.EncodeToString(id[:])
}

// checksumHexNoPrefix returns the EIP-55 mixed-case checksummed hex address without 0x prefix.
func checksumHexNoPrefix(addr common.Address) string {
	hex := addr.Hex()
	return hex[2:]
}

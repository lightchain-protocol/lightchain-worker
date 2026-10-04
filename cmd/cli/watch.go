package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	"github.com/lightchain/worker/internal/cli"
	"github.com/lightchain/worker/internal/config"
	"github.com/lightchain/worker/internal/heartbeat"
)

// runWatch is the `watch` daemon. It reads the worker's own env file plus
// WATCH_WEBHOOK_URL and only ever reads: the keystore's address field (never
// decrypted), the RPC, Redis's heartbeat hash and two HTTP endpoints.
func runWatch() {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	workerFlag := fs.String("worker", "", "Worker address (default: WATCH_WORKER_ADDRESS, then the address field of WORKER_KEYSTORE_PATH)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		quitProcess(1)
	}

	cfg, err := config.LoadRegistration()
	if err != nil {
		slog.Error("config load failed", "error", err)
		quitProcess(1)
	}
	// No keystore password is needed: the address is read, not decrypted.
	errs := slices.DeleteFunc(cfg.Validate(), func(e string) bool { return strings.HasPrefix(e, "WORKER_KEYSTORE") })
	if len(errs) > 0 {
		slog.Error("config validation failed", "fields", strings.Join(errs, "; "))
		quitProcess(1)
	}
	logger := newLogger(cfg.LogLevel, cfg.LogFormat)
	wcfg, err := config.LoadWatch()
	if err != nil {
		logger.Error("watch config invalid", "error", err)
		quitProcess(1)
	}

	workerAddr, err := cli.WatchWorkerAddress(*workerFlag, wcfg.WorkerAddress, cfg.WorkerKeystorePath)
	if err != nil {
		logger.Error("read worker address", "error", err)
		quitProcess(1)
	}

	rpc, err := ethclient.Dial(cfg.RPCURL)
	if err != nil {
		logger.Error("dial RPC", "error", err)
		quitProcess(1)
	}
	defer rpc.Close()
	chainReader, err := cli.NewReadOnlyChain(rpc, cfg.WorkerRegistryAddress, cfg.AIConfigAddress, wcfg.SessionManagerAddress)
	if err != nil {
		logger.Error("bind contracts", "error", err)
		quitProcess(1)
	}

	h := &cli.WatchHandler{
		Chain:           chainReader,
		WorkerAddr:      workerAddr,
		ChainID:         cfg.ChainID,
		LivenessURL:     "http://" + wcfg.MetricsAddr + "/healthz",
		OllamaURL:       cfg.OllamaURL,
		HeartbeatMaxAge: heartbeat.KeyTTL(wcfg.HeartbeatInterval),
		WebhookURL:      wcfg.WebhookURL,
		Interval:        wcfg.Interval,
		Cooldown:        wcfg.Cooldown,
		Logger:          logger,
	}
	if wcfg.SessionManagerAddress != (common.Address{}) {
		h.MissedClaims = wcfg.MissedClaims
	}
	// Gateway profiles heartbeat through the worker-gateway, not Redis.
	if cfg.WorkerGatewayURL == "" {
		opts, err := config.RedisOptions(wcfg.RedisURL, wcfg.RedisPassword)
		if err != nil {
			logger.Error("parse REDIS_URL", "error", err)
			quitProcess(1)
		}
		rdb := redis.NewClient(opts)
		defer func() { _ = rdb.Close() }()
		h.Heartbeat = rdb
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("watch started", "worker", workerAddr.Hex(), "chain", cfg.ChainID,
		"interval", h.Interval, "cooldown", h.Cooldown, "heartbeat", h.Heartbeat != nil, "claims", h.MissedClaims > 0)
	h.Run(ctx)
}

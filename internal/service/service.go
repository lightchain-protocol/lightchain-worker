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
	"net/http"
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

	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/blob"
	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/config"
	gw "github.com/lightchain/worker/internal/gateway"
	"github.com/lightchain/worker/internal/heartbeat"
	"github.com/lightchain/worker/internal/keystore"
	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
	"github.com/lightchain/worker/internal/pipeline"
	"github.com/lightchain/worker/internal/registration"
	"github.com/lightchain/worker/internal/release"
)

// startupHeartbeatTimeout is the maximum time allowed for the initial heartbeat
// write during service startup. It validates Redis connectivity before accepting traffic.
const startupHeartbeatTimeout = 5 * time.Second

// shutdownDrainTimeout is the maximum time allowed for the SIGTERM-driven
// drain marker write. Set short — the goal is best-effort signalling, not
// blocking the shutdown sequence on a network round trip.
const shutdownDrainTimeout = 10 * time.Second

// drainTTLFallback is used when the on-chain dispute window cannot be read
// at drain time (RPC down, context cancelled, etc.). 24h matches the live
// testnet dispute window, with a small implicit slack via the shutdown
// timeout. See docs/worker-drain-plan.md.
const drainTTLFallback = 24 * time.Hour

// drainSlack is added to the on-chain dispute window when computing the
// drain TTL. Gives the operator time to run claimTimeout/releaseJobs/
// deregister/withdraw after the dispute window passes without the drain
// marker silently expiring mid-cleanup.
const drainSlack = 2 * time.Hour

// Service owns all worker sidecar components and coordinates startup and shutdown.
type Service struct {
	cfg             *config.Config
	workerAddr      common.Address
	chainClient     *chain.ChainClient
	coordinator     *chain.SubpoolCoordinator
	redis           *redis.Client
	monitor         *heartbeat.Monitor
	asynqServer     *asynq.Server
	asynqMux        *asynq.ServeMux
	sessionKeyStore *keystore.SessionKeyStore
	jobCounter      *atomic.Int32
	logger          *slog.Logger

	// metrics owns the Prometheus registry; metricsServer is the HTTP server
	// exposing /metrics. Server is non-nil iff cfg.MetricsListenAddr != "".
	metrics       *metrics.Metrics
	metricsServer *http.Server

	// Gateway mode (non-nil when WORKER_GATEWAY_URL is set)
	gwClient  *gw.Client
	gwHandler *pipeline.JobHandler

	// Release subsystem. Store is always non-nil (the Tracker writes to it
	// from the pipeline regardless of cfg.ReleaseEnabled). Scheduler and
	// Reconciler are nil when ReleaseEnabled=false; in that case the
	// operator is expected to settle via worker-cli release.
	releaseStore      release.Store
	releaseTracker    *release.Tracker
	releaseScheduler  *release.Scheduler
	releaseReconciler *release.Reconciler
}

// releaseMetricsAdapter bridges release.Metrics (a tiny consumer-side
// interface) to the worker's Prometheus collectors. Service constructs
// one instance and passes it to both the Scheduler and the Reconciler.
type releaseMetricsAdapter struct{ m *metrics.Metrics }

func (a releaseMetricsAdapter) SetPending(n int)  { a.m.ReleasePending.Set(float64(n)) }
func (a releaseMetricsAdapter) IncReleased(n int) { a.m.ReleaseReleasedTotal.Add(float64(n)) }
func (a releaseMetricsAdapter) IncFailed(n int)   { a.m.ReleaseFailedTotal.Add(float64(n)) }
func (a releaseMetricsAdapter) IncDropped(reason string) {
	a.m.ReleaseDroppedTotal.WithLabelValues(reason).Inc()
}
func (a releaseMetricsAdapter) IncPauseEvent() { a.m.ReleasePauseEventsTotal.Inc() }
func (a releaseMetricsAdapter) SetLastSuccessTimestamp(ts int64) {
	a.m.ReleaseLastSuccessTimestamp.Set(float64(ts))
}
func (a releaseMetricsAdapter) SetReconcileLastBlock(block uint64) {
	a.m.ReleaseReconcileLastBlock.Set(float64(block))
}

// buildReleaseConfig translates the flat env-var fields on config.Config
// into the release package's typed Config. Lives in the service package so
// the release package never has to import config (which would cycle).
func buildReleaseConfig(cfg *config.Config) release.Config {
	return release.Config{
		Enabled:               cfg.ReleaseEnabled,
		StatePath:             cfg.ReleaseStatePath,
		Interval:              cfg.ReleaseInterval,
		ProbeInterval:         cfg.ReleaseProbeInterval,
		BatchThreshold:        cfg.ReleaseBatchThreshold,
		MaxBatchSize:          cfg.ReleaseMaxBatchSize,
		TxTimeout:             cfg.ReleaseTxTimeout,
		StartBlock:            cfg.ReleaseStartBlock,
		ChunkSize:             cfg.ReleaseChunkSize,
		Confirmations:         cfg.ReleaseConfirmations,
		ReconcileInterval:     cfg.ReleaseReconcileInterval,
		BackoffBase:           cfg.ReleaseBackoffBase,
		BackoffMax:            cfg.ReleaseBackoffMax,
		PausedCycleBackoff:    cfg.ReleasePausedCycleBackoff,
		StaleDisputeWarnAfter: cfg.ReleaseStaleDisputeWarnAfter,
		DisputeWindowOverride: cfg.ReleaseDisputeWindowOverride,
		DisputeWindowCacheTTL: cfg.ReleaseDisputeWindowCacheTTL,
	}
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

	// Per-signing-key subpool coordinator. ONE instance is shared across
	// every tx submission path (BlobTxSubmitter and ChainClient). It
	// serializes ACROSS tx classes (blob vs legacy) — which geth requires
	// at the sender-reservation boundary — while allowing unbounded
	// within-class concurrency. See internal/chain/subpool_coordinator.go.
	coordinator := chain.NewSubpoolCoordinator(logger, 0)

	// Per-signing-key stuck-nonce tracker. Shared across submitters so
	// "address already reserved" hits from ACK/CompleteJob broadcasts
	// accumulate into the blob path's replacement decision.
	stuckTracker := chain.NewStuckNonceTracker()

	// Dial chain (now includes JobRegistry binding)
	chainClient, err := chain.NewChainClient(
		cfg.RPCURL,
		cfg.ChainID,
		cfg.WorkerRegistryAddress,
		cfg.AIConfigAddress,
		cfg.JobRegistryAddress,
		signingKey,
		cfg.GasPriceMultiplierBps,
		coordinator,
		stuckTracker,
	)
	if err != nil {
		coordinator.Close()
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

	// Blob fetcher + submitter (mode-dependent)
	blobMode := strings.ToLower(strings.TrimSpace(os.Getenv("BLOB_MODE")))
	var blobFetcher blob.BlobFetcher
	var blobSubmitter blob.BlobSubmitter
	var redisClient *redis.Client
	var redisOpts *redis.Options

	switch blobMode {
	case "redis":
		// Redis blob mode requires a Redis connection for blob I/O.
		redisOpts, err = redis.ParseURL(cfg.RedisURL)
		if err != nil {
			chainClient.Close()
			return nil, fmt.Errorf("parse Redis URL %q: %w", cfg.RedisURL, err)
		}
		if cfg.RedisPassword != "" {
			redisOpts.Password = cfg.RedisPassword
		}
		redisClient = redis.NewClient(redisOpts)
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
			coordinator,
			stuckTracker,
			blob.StuckNonceConfig{
				Threshold:   cfg.StuckNonceThreshold,
				MaxBumps:    cfg.StuckNonceMaxBumps,
				AutoReplace: cfg.StuckNonceAutoReplace,
			},
			logger,
		)
		logger.Info("blob mode: eip-4844 (beacon)")
	default:
		chainClient.Close()
		return nil, fmt.Errorf("unsupported BLOB_MODE %q", blobMode)
	}

	// In direct Redis mode (non-gateway), we always need a Redis client for
	// heartbeat, Asynq job queue, and response publishing. Create it now if
	// blob mode didn't already create one.
	if cfg.WorkerGatewayURL == "" && redisClient == nil {
		redisOpts, err = redis.ParseURL(cfg.RedisURL)
		if err != nil {
			chainClient.Close()
			return nil, fmt.Errorf("parse Redis URL %q: %w", cfg.RedisURL, err)
		}
		if cfg.RedisPassword != "" {
			redisOpts.Password = cfg.RedisPassword
		}
		redisClient = redis.NewClient(redisOpts)
	}

	// Redis already dialed above

	// Shared job counter between pipeline handler and heartbeat monitor
	jobCounter := &atomic.Int32{}

	// Prometheus metrics. Owns its own registry — no global pollution.
	// MaxJobs is set immediately because it's a config-derived constant;
	// Bind wires GaugeFunc/CounterFunc collectors to the live atomics on
	// jobCounter, coordinator, and stuckTracker so each scrape reflects
	// current state without any background goroutine.
	metricsCollector := metrics.New(cfg.SupportedModels)
	metricsCollector.MaxJobs.Set(float64(cfg.MaxConcurrentJobs))
	if err := metricsCollector.Bind(
		jobCounter,
		func() int { return coordinator.Inflight(chain.ClassBlob) },
		func() int { return coordinator.Inflight(chain.ClassLegacy) },
		coordinator.OrphanCount,
		stuckTracker.Size,
		stuckTracker.MaxConsecutiveHits,
	); err != nil {
		// Bind only fails on duplicate-call; can't happen here unless someone
		// shares the *Metrics across two service.New invocations, which is
		// itself a bug. Surface loudly.
		chainClient.Close()
		return nil, fmt.Errorf("metrics.Bind: %w", err)
	}

	// Retry-safety checkpoint store. Backed by the same Redis client so
	// cache records ride the same connection pool as heartbeat and
	// response pub/sub. Tombstone TTL is fixed at 10 minutes — long enough
	// that a late retry observes "completed" via the cached record, short
	// enough that stale entries don't accumulate.
	// In gateway+beacon mode redisClient is nil — skip checkpoints (the
	// handler treats nil checkpoints as a no-op).
	var checkpoints *pipeline.CheckpointStore
	if redisClient != nil {
		checkpoints = pipeline.NewCheckpointStore(
			redisClient,
			cfg.CheckpointTTL,
			10*time.Minute,
			cfg.CheckpointMaxBytes,
		)
	}

	// Release subsystem. The Store opens with chain/contract/worker
	// identity — mismatches fail loudly so a worker can never settle the
	// wrong chain's jobs. The Tracker is wired into the pipeline handler
	// below regardless of cfg.ReleaseEnabled; if the scheduler is off, the
	// Tracker still records eligibility so worker-cli release can settle
	// later.
	releaseStore, err := release.NewFileStore(cfg.ReleaseStatePath, release.StoreIdentity{
		ChainID:       uint64(cfg.ChainID),
		JobRegistry:   cfg.JobRegistryAddress,
		WorkerAddress: workerAddr,
	}, logger)
	if err != nil {
		if redisClient != nil {
			_ = redisClient.Close()
		}
		chainClient.Close()
		return nil, fmt.Errorf("open release store: %w", err)
	}
	releaseTracker := release.NewTracker(releaseStore, logger)
	chainClient.SetDisputeWindowCacheTTL(cfg.ReleaseDisputeWindowCacheTTL)

	releaseCfg := buildReleaseConfig(cfg)
	releaseMetrics := releaseMetricsAdapter{m: metricsCollector}
	var releaseScheduler *release.Scheduler
	var releaseReconciler *release.Reconciler
	if cfg.ReleaseEnabled {
		releaseReconciler = release.NewReconciler(releaseStore, chainClient, workerAddr, releaseCfg, logger)
		releaseReconciler.SetMetrics(releaseMetrics)
		// Run a startup reconciliation pass best-effort. Errors are logged
		// but never fatal — the periodic reconciler retries on its own
		// timer.
		startupRecCtx, startupRecCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if recErr := releaseReconciler.Run(startupRecCtx); recErr != nil {
			logger.Warn("startup reconciliation failed; periodic reconciler will retry",
				"error", recErr)
		}
		startupRecCancel()
		releaseScheduler = release.NewScheduler(releaseStore, chainClient, workerAddr, releaseCfg, logger)
		releaseScheduler.SetMetrics(releaseMetrics)
	}

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
			AckTxTimeout:        cfg.AckTxTimeout,
			BlobTxTimeout:       cfg.BlobTxTimeout,
			RedisPublishTimeout: cfg.RedisPublishTimeout,
			ModelIDToName:       modelIDToName,
			ChainID:             big.NewInt(cfg.ChainID),
			JobRegistryAddr:     cfg.JobRegistryAddress,
		},
		nil, // publisher — fallback wires RedisResponsePublisher from redisClient
		checkpoints,
		metricsCollector,
		metrics.DeliveryAsynq,
	)
	handler.SetReleaseTracker(releaseTracker)

	// --- Gateway mode: skip Asynq and direct Redis heartbeat ---
	if cfg.WorkerGatewayURL != "" {
		gwClient := gw.NewClient(cfg.WorkerGatewayURL, signingKey, logger)

		gwCtx, gwCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer gwCancel()
		if err := gwClient.Authenticate(gwCtx); err != nil {
			if redisClient != nil {
				_ = redisClient.Close()
			}
			chainClient.Close()
			return nil, fmt.Errorf("authenticate with worker-gateway: %w", err)
		}

		// Create handler with a gateway-based response publisher.
		// RedisPublishTimeout is intentionally omitted from HandlerConfig: gateway
		// mode publishes via gwPublisher (HTTP), which has its own timeout via
		// gwClient.httpClient. checkpoints is shared with the direct-mode handler
		// (would be, if both ran together) — the store is jobID-keyed and stateless,
		// so the cache benefits any redelivered job regardless of delivery channel.
		gwPublisher := &gatewayResponsePublisher{client: gwClient, logger: logger, metrics: metricsCollector}
		gwHandler := pipeline.NewJobHandler(
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
				BlobTxTimeout:   cfg.BlobTxTimeout,
				ModelIDToName:   modelIDToName,
				ChainID:         big.NewInt(cfg.ChainID),
				JobRegistryAddr: cfg.JobRegistryAddress,
			},
			gwPublisher,
			checkpoints,
			metricsCollector,
			metrics.DeliveryGateway,
		)
		gwHandler.SetReleaseTracker(releaseTracker)

		logger.Info("worker service initialized (gateway mode)",
			"address", workerAddr.Hex(),
			"gateway", cfg.WorkerGatewayURL,
			"models", len(modelIDs),
		)

		// Seed the coordinator from the chain's pending-vs-latest nonce gap
		// so gateway mode also recovers from a previous crash's stuck txs.
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		pendingNonce, perr := chainClient.EthClient().PendingNonceAt(seedCtx, workerAddr)
		latestNonce, lerr := chainClient.EthClient().NonceAt(seedCtx, workerAddr, nil)
		seedCancel()
		if perr == nil && lerr == nil {
			coordinator.Seed(pendingNonce, latestNonce)
		} else {
			logger.Warn("coordinator seed skipped — pending/latest nonce read failed",
				"pendingErr", perr,
				"latestErr", lerr,
				"hint", "first legacy broadcast may race a stuck pool tx; existing ShouldResetNonceOnSendError will recover",
			)
		}

		return &Service{
			cfg:               cfg,
			workerAddr:        workerAddr,
			chainClient:       chainClient,
			coordinator:       coordinator,
			redis:             redisClient,
			sessionKeyStore:   sessionKeyStore,
			jobCounter:        jobCounter,
			logger:            logger,
			metrics:           metricsCollector,
			metricsServer:     buildMetricsServer(cfg, metricsCollector),
			gwClient:          gwClient,
			gwHandler:         gwHandler,
			releaseStore:      releaseStore,
			releaseTracker:    releaseTracker,
			releaseScheduler:  releaseScheduler,
			releaseReconciler: releaseReconciler,
		}, nil
	}

	// --- Direct Redis mode (default) ---

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
	monitor := heartbeat.NewMonitor(redisClient, monitorCfg, addrHex, modelHexStrings, jobCounter, cfg.MaxConcurrentJobs, logger, metricsCollector)

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
		"ackTxTimeout", cfg.AckTxTimeout.String(),
		"blobTxTimeout", cfg.BlobTxTimeout.String(),
		"minExpectedTaskBudget", (cfg.AckTxTimeout + cfg.BlobTxTimeout + 10*time.Second).String(),
		"stuckNonceThreshold", cfg.StuckNonceThreshold,
		"stuckNonceMaxBumps", cfg.StuckNonceMaxBumps,
		"stuckNonceAutoReplace", cfg.StuckNonceAutoReplace,
	)

	// Seed the coordinator from the chain's pending-vs-latest nonce gap.
	// If the previous process crashed between SendTransaction and WaitMined,
	// geth's reserver still holds those pending txs and our fresh in-memory
	// coordinator must block cross-class broadcasts until the pool drains.
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	pendingNonce, perr := chainClient.EthClient().PendingNonceAt(seedCtx, workerAddr)
	latestNonce, lerr := chainClient.EthClient().NonceAt(seedCtx, workerAddr, nil)
	seedCancel()
	if perr == nil && lerr == nil {
		coordinator.Seed(pendingNonce, latestNonce)
	} else {
		logger.Warn("coordinator seed skipped — pending/latest nonce read failed",
			"pendingErr", perr,
			"latestErr", lerr,
			"hint", "first legacy broadcast may race a stuck pool tx; existing ShouldResetNonceOnSendError will recover",
		)
	}

	return &Service{
		cfg:               cfg,
		workerAddr:        workerAddr,
		chainClient:       chainClient,
		coordinator:       coordinator,
		redis:             redisClient,
		monitor:           monitor,
		asynqServer:       asynqSrv,
		asynqMux:          mux,
		sessionKeyStore:   sessionKeyStore,
		jobCounter:        jobCounter,
		logger:            logger,
		metrics:           metricsCollector,
		metricsServer:     buildMetricsServer(cfg, metricsCollector),
		releaseStore:      releaseStore,
		releaseTracker:    releaseTracker,
		releaseScheduler:  releaseScheduler,
		releaseReconciler: releaseReconciler,
	}, nil
}

// buildMetricsServer returns the configured Prometheus HTTP server, or nil
// if metrics are disabled via empty MetricsListenAddr. Constructed here
// rather than inline in New() so both the direct and gateway paths share
// the same configuration logic.
func buildMetricsServer(cfg *config.Config, m *metrics.Metrics) *http.Server {
	if cfg.MetricsListenAddr == "" {
		return nil
	}
	return m.Server(cfg.MetricsListenAddr)
}

// startMetricsServer launches the Prometheus /metrics HTTP listener in a
// background goroutine. No-op when metricsServer is nil (cfg disabled the
// endpoint). Errors are logged but never returned — the main lifecycle
// must continue serving jobs even if observability fails.
func (s *Service) startMetricsServer() {
	if s.metricsServer == nil {
		s.logger.Info("metrics endpoint disabled (WORKER_METRICS_ADDR is empty)")
		return
	}
	s.logger.Info("starting metrics endpoint", "addr", s.metricsServer.Addr)
	go func() {
		if err := s.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("metrics endpoint failed (job processing continues)",
				"addr", s.metricsServer.Addr,
				"error", err,
			)
		}
	}()
}

// gatewayResponsePublisher publishes responses via the worker-gateway HTTP API.
type gatewayResponsePublisher struct {
	client  *gw.Client
	logger  *slog.Logger
	metrics *metrics.Metrics
}

func (p *gatewayResponsePublisher) PublishResponse(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	signature string,
	ciphertext []byte,
) {
	if err := p.client.PublishResponse(ctx, jobID, sessionID, correlationID, signature, ciphertext); err != nil {
		p.logger.Warn("gateway response publish failed (non-fatal)", "jobID", jobID, "error", err)
		if p.metrics != nil {
			p.metrics.RedisPublishFailures.Inc()
		}
	}
}

// Run starts the heartbeat goroutine and Asynq server (or gateway poll loop),
// waits for SIGINT/SIGTERM, then gracefully shuts down.
func (s *Service) Run(ctx context.Context) error {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	sigCtx, stop := signal.NotifyContext(runCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Metrics endpoint runs in both modes. Non-fatal: a bind failure or
	// transient ListenAndServe error logs loudly but does not stop job
	// processing — observability outages must not cascade into job
	// outages. Shutdown is handled in s.shutdown() via srv.Shutdown(ctx).
	s.startMetricsServer()

	// Gateway mode: poll loop + gateway heartbeat
	if s.gwClient != nil {
		return s.runGatewayMode(sigCtx, runCancel)
	}

	// Direct Redis mode
	s.monitor.Start(runCtx)
	s.startReleaseSubsystem(runCtx)

	asynqErrCh := make(chan error, 1)
	go func() {
		asynqErrCh <- s.asynqServer.Run(s.asynqMux)
	}()

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

	// Set the drain marker BEFORE asynq.Shutdown waits for in-flight jobs.
	// New sessions stop being routed to this worker while existing jobs
	// drain naturally. Use a fresh context — sigCtx is cancelled.
	s.markDrainOnShutdown(false)

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

// runGatewayMode runs the worker in gateway mode: connects to the worker-gateway
// via WebSocket for instant job delivery, and sends heartbeats via HTTP.
func (s *Service) runGatewayMode(ctx context.Context, cancel context.CancelFunc) error {
	s.logger.Info("worker sidecar running (gateway mode) — waiting for shutdown signal")

	// Release subsystem runs identically in both modes.
	s.startReleaseSubsystem(ctx)

	// Heartbeat loop (HTTP POST, unchanged)
	go func() {
		ticker := time.NewTicker(s.cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			payload := gw.HeartbeatPayload{
				ActiveJobs:   int(s.jobCounter.Load()),
				MaxJobs:      s.cfg.MaxConcurrentJobs,
				OllamaStatus: "ready",
				Uptime:       0, // simplified for MVP
			}
			if err := s.gwClient.SendHeartbeat(ctx, payload); err != nil {
				s.logger.Warn("gateway heartbeat failed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Job stream via WebSocket (BRPOP-backed, instant delivery)
	go s.gwClient.StreamJobs(ctx, s.cfg.MaxConcurrentJobs, func(jobCtx context.Context, job pipeline.JobPayload) error {
		if err := s.gwHandler.HandleJobPayload(jobCtx, job); err != nil {
			s.logger.Error("gateway job processing failed", "jobID", job.JobID, "error", err)
			return err
		}
		return nil
	})

	<-ctx.Done()
	s.logger.Info("shutdown signal received, stopping gracefully")

	// Set the drain marker via the gateway BEFORE shutdown closes the
	// gateway client. New sessions stop being routed; in-flight stream
	// jobs continue draining via the WebSocket reader until ctx
	// propagation closes it.
	s.markDrainOnShutdown(true)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer shutdownCancel()
	return s.shutdown(shutdownCtx)
}

// markDrainOnShutdown writes the drain marker as the worker exits. Failure
// is non-fatal — a missing drain marker just means the worker will be
// filtered out by stale-detection (TTL expiry on the heartbeat key)
// instead of by drain. The shutdown sequence proceeds regardless.
//
// gatewayMode selects between writing via the gateway HTTP API (external
// workers) and writing directly to Redis (internal workers).
func (s *Service) markDrainOnShutdown(gatewayMode bool) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
	defer cancel()

	if gatewayMode {
		if s.gwClient == nil {
			return
		}
		if err := s.gwClient.SendDrain(ctx); err != nil {
			s.logger.Warn("drain_write_failed",
				"mode", "gateway",
				"worker", s.workerAddr.Hex(),
				"error", err,
			)
			return
		}
		s.logger.Info("drain marker set via gateway", "worker", s.workerAddr.Hex())
		return
	}

	if s.redis == nil {
		return
	}
	ttl := s.computeDrainTTL(ctx)
	if err := pkgtypes.SetDraining(ctx, s.redis, s.workerAddr.Hex(), ttl); err != nil {
		s.logger.Warn("drain_write_failed",
			"mode", "direct",
			"worker", s.workerAddr.Hex(),
			"ttl", ttl,
			"error", err,
		)
		return
	}
	s.logger.Info("drain marker set via redis",
		"worker", s.workerAddr.Hex(),
		"ttl", ttl,
	)
}

// computeDrainTTL derives the drain marker TTL from the on-chain dispute
// window plus operator slack. Falls back to drainTTLFallback if the chain
// read fails — drain still works, but the TTL may drift from governance
// changes until the next read succeeds.
func (s *Service) computeDrainTTL(ctx context.Context) time.Duration {
	if s.chainClient == nil {
		return drainTTLFallback
	}
	disputeWindow, err := s.chainClient.GetDisputeWindow(ctx)
	if err != nil {
		s.logger.Warn("get dispute window for drain TTL failed; using fallback",
			"fallback", drainTTLFallback,
			"error", err,
		)
		return drainTTLFallback
	}
	return disputeWindow + drainSlack
}

// startReleaseSubsystem launches the Scheduler and the periodic Reconciler.
// No-op when ReleaseEnabled=false. Safe to call exactly once per Service.
func (s *Service) startReleaseSubsystem(ctx context.Context) {
	if s.releaseScheduler == nil && s.releaseReconciler == nil {
		s.logger.Info("release subsystem disabled (RELEASE_ENABLED=false); operator must run worker-cli release")
		return
	}
	if s.releaseReconciler != nil {
		s.releaseReconciler.StartPeriodic(ctx)
	}
	if s.releaseScheduler != nil {
		s.releaseScheduler.Start(ctx)
	}
	s.logger.Info("release subsystem started",
		"interval", s.cfg.ReleaseInterval,
		"probe_interval", s.cfg.ReleaseProbeInterval,
		"reconcile_interval", s.cfg.ReleaseReconcileInterval,
	)
}

// shutdown coordinates all cleanup steps within the given context deadline.
func (s *Service) shutdown(ctx context.Context) error {
	var shutdownErr error

	// Stop the metrics endpoint first — its handlers don't hold locks on
	// anything else we need to drain, and stopping it early prevents new
	// scrapes during teardown that might observe inconsistent state.
	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(ctx); err != nil {
			s.logger.Warn("metrics server shutdown error (non-fatal)", "error", err)
		}
	}

	// Stop Asynq server — waits for in-flight jobs (nil in gateway mode)
	if s.asynqServer != nil {
		s.asynqServer.Shutdown()
	}

	// Zero all session keys
	if err := s.sessionKeyStore.ZeroAll(); err != nil {
		s.logger.Error("failed to zero session keys on shutdown", "error", err)
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("zero session keys: %w", err))
	}

	// Stop heartbeat monitor (nil in gateway mode)
	if s.monitor != nil {
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
	}

	// Stop release subsystem before closing the chain client, since the
	// scheduler/reconciler goroutines may still be holding a chain call.
	if s.releaseScheduler != nil {
		schedDone := make(chan struct{})
		go func() {
			s.releaseScheduler.Stop()
			close(schedDone)
		}()
		select {
		case <-schedDone:
		case <-ctx.Done():
			s.logger.Warn("release scheduler stop timed out")
		}
	}
	if s.releaseReconciler != nil {
		recDone := make(chan struct{})
		go func() {
			s.releaseReconciler.StopPeriodic()
			close(recDone)
		}()
		select {
		case <-recDone:
		case <-ctx.Done():
			s.logger.Warn("release reconciler stop timed out")
		}
	}
	if s.releaseStore != nil {
		if err := s.releaseStore.Close(); err != nil {
			s.logger.Warn("release store close error", "error", err)
		}
	}

	if s.redis != nil {
		if err := s.redis.Close(); err != nil {
			s.logger.Warn("redis close error", "error", err)
		}
	}

	s.chainClient.Close()
	if s.coordinator != nil {
		s.coordinator.Close()
	}

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

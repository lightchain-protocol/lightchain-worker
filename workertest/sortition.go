package workertest

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/redis/go-redis/v9"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	workerchain "github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/keystore"
	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/pipeline"
	"github.com/lightchain/worker/internal/registration"
	"github.com/lightchain/worker/internal/search"
	"github.com/lightchain/worker/internal/sortition"
)

// SortitionOptions configures an in-process dispatcher-free worker harness:
// the real SessionWatcher (sortition claim), JobWatcher (chain job intake),
// and pipeline handler wired against a live RPC endpoint.
type SortitionOptions struct {
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address
	JobRegistryAddress    common.Address
	SessionManagerAddress common.Address
	SigningKey            *ecdsa.PrivateKey
	ECDHKey               *ecdh.PrivateKey
	RedisClient           *redis.Client
	BlobFetcher           BlobFetcher
	BlobSubmitter         BlobSubmitter
	InferenceClient       InferenceClient
	ModelIDToName         map[string]string
	// TavilyURL, when non-empty, enables web search exactly like the production
	// SEARCH_ENABLED path: the harness self-declares the "search" capability
	// on-chain at construction and wires a real Tavily client at this base URL.
	TavilyURL string
	Logger    *slog.Logger
}

// SortitionHarness drives the worker's real sortition-mode components with
// deterministic single-pass steps instead of background polling loops.
type SortitionHarness struct {
	workerAddress  common.Address
	sessionWatcher *sortition.SessionWatcher
	jobWatcher     *sortition.JobWatcher
	sessionStore   *keystore.SessionKeyStore
	served         chan error
}

// ecdhChecker mirrors the service's KeyChecker adapter: a session key is
// serveable only if it decrypts with this worker's ECDH private key.
type ecdhChecker struct{ key *ecdh.PrivateKey }

func (c ecdhChecker) CanDecryptSessionKey(enc []byte) bool {
	_, err := pkgcrypto.DecryptSessionKey(enc, c.key)
	return err == nil
}

// notifySink wraps the pipeline handler so ServeOnce can wait for the
// JobWatcher's asynchronous serve goroutine to finish deterministically.
type notifySink struct {
	inner  sortition.JobSink
	served chan error
}

func (s *notifySink) HandleJobPayload(ctx context.Context, p pipeline.JobPayload) error {
	err := s.inner.HandleJobPayload(ctx, p)
	select {
	case s.served <- err:
	default:
	}
	return err
}

// NewSortitionHarness wires the worker's real sortition components. It
// performs the production startup sequence: capability self-declaration
// on-chain (when TavilyURL is set), then a read-back of the final mask for
// the SessionWatcher's politeness skip.
func NewSortitionHarness(t testing.TB, opts SortitionOptions) *SortitionHarness {
	t.Helper()

	if opts.SigningKey == nil {
		t.Fatal("workertest: signing key must not be nil")
	}
	if opts.ECDHKey == nil {
		t.Fatal("workertest: ECDH key must not be nil")
	}
	if opts.RedisClient == nil {
		t.Fatal("workertest: redis client must not be nil")
	}
	if opts.BlobFetcher == nil || opts.BlobSubmitter == nil || opts.InferenceClient == nil {
		t.Fatal("workertest: blob fetcher, blob submitter, and inference client must not be nil")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	workerAddr := crypto.PubkeyToAddress(opts.SigningKey.PublicKey)

	chainClient, err := workerchain.NewChainClient(
		opts.RPCURL,
		opts.ChainID,
		opts.WorkerRegistryAddress,
		opts.AIConfigAddress,
		opts.JobRegistryAddress,
		opts.SessionManagerAddress,
		opts.SigningKey,
		11000,
		workerchain.NewSubpoolCoordinator(nil, 0),
		workerchain.NewStuckNonceTracker(),
	)
	if err != nil {
		t.Fatalf("workertest: create sortition chain client: %v", err)
	}
	t.Cleanup(chainClient.Close)

	// Production startup path (service.go): declare capabilities implied by the
	// search configuration, then read the final mask back for the watcher.
	var searcher search.Searcher
	var desiredCaps []string
	if opts.TavilyURL != "" {
		searcher = search.NewTavilyClient(opts.TavilyURL, "workertest-key", 10*time.Second)
		desiredCaps = append(desiredCaps, "search")
	}
	capCtx, capCancel := context.WithTimeout(context.Background(), 30*time.Second)
	registration.NewManager(chainClient, workerAddr, nil, opts.Logger).EnsureCapabilities(capCtx, desiredCaps)
	ownCaps, err := chainClient.GetWorkerCapabilities(capCtx, workerAddr)
	capCancel()
	if err != nil {
		t.Fatalf("workertest: read worker capability mask: %v", err)
	}

	sessionStore, err := keystore.NewSessionKeyStore(
		filepath.Join(t.TempDir(), "session-keys.enc"), "workertest-passphrase",
	)
	if err != nil {
		t.Fatalf("workertest: create session store: %v", err)
	}
	t.Cleanup(func() { _ = sessionStore.ZeroAll() })

	jobCounter := &atomic.Int32{}
	checkpoints := pipeline.NewCheckpointStore(opts.RedisClient, 2*time.Hour, 10*time.Minute, 256*1024)
	handler := pipeline.NewJobHandler(
		chainClient,
		opts.BlobFetcher,
		opts.BlobSubmitter,
		sessionStore,
		opts.InferenceClient,
		opts.RedisClient,
		opts.SigningKey,
		opts.ECDHKey,
		jobCounter,
		opts.Logger,
		pipeline.HandlerConfig{
			AckTxTimeout:        30 * time.Second,
			RedisPublishTimeout: 5 * time.Second,
			ModelIDToName:       opts.ModelIDToName,
			ChainID:             big.NewInt(opts.ChainID),
			JobRegistryAddr:     opts.JobRegistryAddress,
		},
		nil, // publisher — fallback wires RedisResponsePublisher from RedisClient
		checkpoints,
		metrics.New(modelTagsFromMap(opts.ModelIDToName)),
		metrics.DeliveryAsynq, // pure sortition keeps the asynq delivery label (service.go parity)
		searcher,
	)

	cursorStore, err := sortition.NewCursorStore(t.TempDir())
	if err != nil {
		t.Fatalf("workertest: open sortition cursor store: %v", err)
	}

	served := make(chan error, 16)
	h := &SortitionHarness{
		workerAddress: workerAddr,
		sessionStore:  sessionStore,
		served:        served,
		sessionWatcher: sortition.NewSessionWatcher(sortition.SessionWatcherOpts{
			Client:          chainClient,
			Cursor:          cursorStore,
			Worker:          workerAddr,
			JobCounter:      jobCounter,
			MaxConcurrent:   1,
			ChunkSize:       2048,
			Confirmations:   0,
			PollInterval:    time.Second,
			Logger:          opts.Logger,
			OwnCapabilities: ownCaps,
		}),
		jobWatcher: sortition.NewJobWatcher(sortition.JobWatcherOpts{
			Client:                chainClient,
			KeyChecker:            ecdhChecker{key: opts.ECDHKey},
			Sink:                  &notifySink{inner: handler, served: served},
			Cursor:                cursorStore,
			Worker:                workerAddr,
			JobCounter:            jobCounter,
			MaxConcurrent:         1,
			ChunkSize:             2048,
			Confirmations:         0,
			HistoryLookbackBlocks: 5000,
			SessionRetryLimit:     3,
			PollInterval:          time.Second,
			Logger:                opts.Logger,
		}),
	}
	return h
}

// WorkerAddress returns the worker's signing address.
func (h *SortitionHarness) WorkerAddress() common.Address {
	return h.workerAddress
}

// ClaimOnce runs a single SessionWatcher pass: scan for SessionRequested
// events, evaluate eligibility (including the capability politeness skip),
// and claim at most MaxConcurrent sessions.
func (h *SortitionHarness) ClaimOnce(ctx context.Context) error {
	return h.sessionWatcher.RunOnce(ctx)
}

// ServeOnce runs a single JobWatcher pass and, if a job assigned to this
// worker was dispatched to the pipeline, waits for that serve to finish.
// Returns (false, nil) when no serve completed within the wait window — the
// window is deliberately generous because the JobWatcher dispatches serves
// asynchronously and a slow pipeline pass is indistinguishable from no job.
func (h *SortitionHarness) ServeOnce(ctx context.Context) (bool, error) {
	// Drain any stale results from previous passes.
	for {
		select {
		case <-h.served:
			continue
		default:
		}
		break
	}

	if err := h.jobWatcher.RunOnce(ctx); err != nil {
		return false, err
	}

	select {
	case err := <-h.served:
		return true, err
	case <-time.After(60 * time.Second):
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Package workertest exposes a public in-process worker harness for integration tests.
// It lives inside the worker module so it can wire real internal components together.
package workertest

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	pkgtypes "github.com/lightchain/pkg/types"
	workerchain "github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/keystore"
	"github.com/lightchain/worker/internal/ollama"
	"github.com/lightchain/worker/internal/pipeline"
)

// ChainClient is the worker pipeline's chain-facing dependency surface.
type ChainClient interface {
	AcknowledgeJob(ctx context.Context, jobID uint64) error
	CompleteJob(ctx context.Context, jobID uint64, responseBlobHash [32]byte, responseCiphertextHash [32]byte) error
	HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error)
	HasJobCompleted(ctx context.Context, jobID uint64) (bool, error)
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash common.Hash, responseHash common.Hash, submitBlock uint64, completionBlock uint64, err error)
}

// BlobFetcher is the worker pipeline's blob-fetch dependency surface.
type BlobFetcher interface {
	FetchBlob(ctx context.Context, versionedHash common.Hash, blockNumber uint64) ([]byte, error)
}

// BlobSubmitter is the worker pipeline's blob-submit dependency surface.
type BlobSubmitter interface {
	SubmitBlobTx(ctx context.Context, data []byte) ([][32]byte, error)
}

// ChatMessage represents a single turn in a multi-turn conversation.
// Re-exported from internal/ollama for use by external test consumers.
type ChatMessage = ollama.ChatMessage

// InferenceClient is the worker pipeline's inference dependency surface.
type InferenceClient interface {
	Generate(ctx context.Context, model, prompt string) (string, error)
	Chat(ctx context.Context, model string, messages []ChatMessage) (string, error)
}

// Options configures the in-process worker harness.
type Options struct {
	RedisClient            *redis.Client
	SigningKey             *ecdsa.PrivateKey
	ECDHKey                *ecdh.PrivateKey
	ChainClient            ChainClient
	BlobFetcher            BlobFetcher
	BlobSubmitter          BlobSubmitter
	InferenceClient        InferenceClient
	ModelIDToName          map[string]string
	MaxConcurrentJobs      int
	AckTxTimeout           time.Duration
	SessionStorePath       string
	SessionStorePassphrase string
	Logger                 *slog.Logger
	// ChainID and JobRegistryAddr are required for the worker's
	// disputeResponseMismatch-compatible response signing. Tests that do not
	// exercise the Redis publish path may leave them zero.
	ChainID         *big.Int
	JobRegistryAddr common.Address
}

// RealChainClientOptions configures a real worker chain client for E2E tests.
type RealChainClientOptions struct {
	RPCURL                string
	ChainID               int64
	WorkerRegistryAddress common.Address
	AIConfigAddress       common.Address
	JobRegistryAddress    common.Address
	SigningKey            *ecdsa.PrivateKey
	GasPriceMultiplierBps int
}

// ResponsePayload mirrors the worker's Redis response payload for external tests.
type ResponsePayload = pkgtypes.PubSubMessage

// Harness runs the worker's real Asynq consumer path in-process.
type Harness struct {
	workerAddress    common.Address
	queueName        string
	sessionStorePath string
	asynqServer      *asynq.Server
	asynqMux         *asynq.ServeMux
	asynqInspector   *asynq.Inspector
	sessionStore     *keystore.SessionKeyStore
	readyCh          chan struct{}
	readyOnce        sync.Once

	cancelRun context.CancelFunc // set in Run(), called in Close()
	mu        sync.Mutex         // protects cancelRun

	closeOnce sync.Once
	closeErr  error
}

// New creates a worker harness backed by the worker's real Asynq server and job handler.
func New(t testing.TB, opts Options) *Harness {
	t.Helper()

	if opts.RedisClient == nil {
		t.Fatal("workertest: redis client must not be nil")
	}
	if opts.SigningKey == nil {
		t.Fatal("workertest: signing key must not be nil")
	}
	if opts.ECDHKey == nil {
		t.Fatal("workertest: ECDH key must not be nil")
	}
	if opts.ChainClient == nil {
		t.Fatal("workertest: chain client must not be nil")
	}
	if opts.BlobFetcher == nil {
		t.Fatal("workertest: blob fetcher must not be nil")
	}
	if opts.BlobSubmitter == nil {
		t.Fatal("workertest: blob submitter must not be nil")
	}
	if opts.InferenceClient == nil {
		t.Fatal("workertest: inference client must not be nil")
	}

	if opts.MaxConcurrentJobs <= 0 {
		opts.MaxConcurrentJobs = 1
	}
	if opts.AckTxTimeout <= 0 {
		opts.AckTxTimeout = 5 * time.Second
	}
	if opts.SessionStorePassphrase == "" {
		opts.SessionStorePassphrase = "workertest-passphrase"
	}
	if opts.SessionStorePath == "" {
		opts.SessionStorePath = filepath.Join(t.TempDir(), "session-keys.enc")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	}

	sessionStore, err := keystore.NewSessionKeyStore(opts.SessionStorePath, opts.SessionStorePassphrase)
	if err != nil {
		t.Fatalf("workertest: create session store: %v", err)
	}

	redisOpts := opts.RedisClient.Options()
	workerAddr := crypto.PubkeyToAddress(opts.SigningKey.PublicKey)
	queueName := workerQueueName(workerAddr)

	jobCounter := &atomic.Int32{}
	handler := pipeline.NewJobHandler(
		opts.ChainClient,
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
			AckTxTimeout:    opts.AckTxTimeout,
			ModelIDToName:   opts.ModelIDToName,
			ChainID:         opts.ChainID,
			JobRegistryAddr: opts.JobRegistryAddr,
		},
	)

	redisConnOpt := asynq.RedisClientOpt{
		Network:      redisOpts.Network,
		Addr:         redisOpts.Addr,
		Username:     redisOpts.Username,
		Password:     redisOpts.Password,
		DB:           redisOpts.DB,
		DialTimeout:  redisOpts.DialTimeout,
		ReadTimeout:  redisOpts.ReadTimeout,
		WriteTimeout: redisOpts.WriteTimeout,
		PoolSize:     redisOpts.PoolSize,
		TLSConfig:    redisOpts.TLSConfig,
	}
	asynqServer := asynq.NewServer(redisConnOpt, asynq.Config{
		Concurrency: opts.MaxConcurrentJobs,
		Queues:      map[string]int{queueName: 1},
	})
	asynqInspector := asynq.NewInspector(redisConnOpt)

	mux := asynq.NewServeMux()
	mux.HandleFunc(pipeline.TaskTypeJobInference, handler.HandleTask)

	h := &Harness{
		workerAddress:    workerAddr,
		queueName:        queueName,
		sessionStorePath: opts.SessionStorePath,
		asynqServer:      asynqServer,
		asynqMux:         mux,
		asynqInspector:   asynqInspector,
		sessionStore:     sessionStore,
		readyCh:          make(chan struct{}),
	}
	t.Cleanup(func() {
		_ = h.Close()
	})

	return h
}

// Run starts the worker consumer and blocks until ctx is cancelled or Close is called.
func (h *Harness) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.mu.Lock()
	h.cancelRun = cancel
	h.mu.Unlock()

	h.readyOnce.Do(func() {
		close(h.readyCh)
	})

	if err := h.asynqServer.Start(h.asynqMux); err != nil {
		return err
	}

	<-ctx.Done()
	h.asynqServer.Shutdown()
	return nil
}

// Ready is closed once Run has started.
func (h *Harness) Ready() <-chan struct{} {
	return h.readyCh
}

// Close cancels the Run context, shuts down the Asynq server, and zeroes the session-key store.
func (h *Harness) Close() error {
	h.closeOnce.Do(func() {
		h.mu.Lock()
		cancel := h.cancelRun
		h.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		h.asynqServer.Shutdown()
		_ = h.asynqInspector.Close()
		h.closeErr = h.sessionStore.ZeroAll()
	})
	return h.closeErr
}

// RunAllRetryTasks moves all tasks that are waiting to be retried into the
// active queue immediately, bypassing the retry backoff delay. Useful in
// tests where waiting for Asynq's default 15-44 second backoff is impractical.
func (h *Harness) RunAllRetryTasks() (int, error) {
	return h.asynqInspector.RunAllRetryTasks(h.queueName)
}

// WorkerAddress returns the worker's signing address.
func (h *Harness) WorkerAddress() common.Address {
	return h.workerAddress
}

// QueueName returns the exact queue name the harness is consuming.
func (h *Harness) QueueName() string {
	return h.queueName
}

// SessionStorePath returns the underlying encrypted session-key store path.
func (h *Harness) SessionStorePath() string {
	return h.sessionStorePath
}

// RecoverResponseSigner recovers the signer from a Redis response payload signature.
// The digest matches JobRegistry.disputeResponseMismatch verification so the returned
// signature is directly usable as on-chain mismatch evidence.
func RecoverResponseSigner(chainID *big.Int, jobRegistryAddr common.Address, resp ResponsePayload) (common.Address, error) {
	digest, err := responseDigest(chainID, jobRegistryAddr, uint64(resp.JobID), uint64(resp.SessionID), resp.Payload)
	if err != nil {
		return common.Address{}, err
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(resp.Signature, "0x"))
	if err != nil {
		return common.Address{}, fmt.Errorf("decode response signature: %w", err)
	}
	pubKey, err := crypto.SigToPub(accounts.TextHash(digest), sig)
	if err != nil {
		return common.Address{}, fmt.Errorf("recover response signer: %w", err)
	}
	return crypto.PubkeyToAddress(*pubKey), nil
}

func workerQueueName(addr common.Address) string {
	return fmt.Sprintf("worker:%s", strings.ToLower(addr.Hex()))
}

// responseDigestSigArgs mirrors pipeline.responseMismatchSigArgs in the
// worker package — kept here so workertest does not depend on internal/pipeline
// implementation details.
var responseDigestSigArgs abi.Arguments

func init() {
	uint256Ty, err := abi.NewType("uint256", "", nil)
	if err != nil {
		panic(fmt.Sprintf("workertest: abi.NewType(uint256): %v", err))
	}
	addressTy, err := abi.NewType("address", "", nil)
	if err != nil {
		panic(fmt.Sprintf("workertest: abi.NewType(address): %v", err))
	}
	bytesTy, err := abi.NewType("bytes", "", nil)
	if err != nil {
		panic(fmt.Sprintf("workertest: abi.NewType(bytes): %v", err))
	}
	responseDigestSigArgs = abi.Arguments{
		{Type: uint256Ty}, // block.chainid
		{Type: addressTy}, // address(jobRegistry)
		{Type: uint256Ty}, // jobId
		{Type: uint256Ty}, // sessionId
		{Type: bytesTy},   // ciphertext
	}
}

func responseDigest(chainID *big.Int, jobRegistryAddr common.Address, jobID, sessionID uint64, ciphertext []byte) ([]byte, error) {
	if chainID == nil {
		return nil, fmt.Errorf("responseDigest: chainID is nil")
	}
	encoded, err := responseDigestSigArgs.Pack(
		new(big.Int).Set(chainID),
		jobRegistryAddr,
		new(big.Int).SetUint64(jobID),
		new(big.Int).SetUint64(sessionID),
		ciphertext,
	)
	if err != nil {
		return nil, fmt.Errorf("pack mismatch payload: %w", err)
	}
	return crypto.Keccak256(encoded), nil
}

// NewRealChainClient creates the worker's real chain client behind the public
// workertest interface for cross-module E2E tests.
func NewRealChainClient(t testing.TB, opts RealChainClientOptions) ChainClient {
	t.Helper()

	if opts.RPCURL == "" {
		t.Fatal("workertest: RPC URL must not be empty")
	}
	if opts.SigningKey == nil {
		t.Fatal("workertest: signing key must not be nil")
	}
	if opts.GasPriceMultiplierBps <= 0 {
		opts.GasPriceMultiplierBps = 11000
	}

	client, err := workerchain.NewChainClient(
		opts.RPCURL,
		opts.ChainID,
		opts.WorkerRegistryAddress,
		opts.AIConfigAddress,
		opts.JobRegistryAddress,
		opts.SigningKey,
		opts.GasPriceMultiplierBps,
		workerchain.NewSubpoolCoordinator(nil, 0),
		workerchain.NewStuckNonceTracker(),
	)
	if err != nil {
		t.Fatalf("workertest: create real chain client: %v", err)
	}

	t.Cleanup(client.Close)
	return client
}

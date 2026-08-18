package chain

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/lightchain/pkg/chain/bindings"
)

// defaultDisputeWindowCacheTTL is used when SetDisputeWindowCacheTTL has not
// been called. Governance changes the dispute window rarely; 15m balances
// staleness against contract round-trips.
const defaultDisputeWindowCacheTTL = 15 * time.Minute

// Compile-time interface assertions.
var (
	_ RegistrationClient = (*ChainClient)(nil)
	_ ValidationClient   = (*ChainClient)(nil)
	_ JobExecutionClient = (*ChainClient)(nil)
	_ SettlementClient   = (*ChainClient)(nil)
)

// ChainClient implements RegistrationClient, ValidationClient, and JobExecutionClient
// using the go-ethereum ethclient and the generated pkg/chain/bindings for WorkerRegistry,
// AIConfig, and JobRegistry.
type ChainClient struct {
	ethClient       *ethclient.Client
	jobTxBackend    jobTxBackend
	registry        *bindings.WorkerRegistry
	aiConfig        *bindings.AIConfig
	jobRegistry     *bindings.JobRegistry
	jobRegistryAddr common.Address
	sessionManager  *bindings.SessionManager
	signingKey      *ecdsa.PrivateKey
	workerAddr      common.Address
	chainID         *big.Int
	gasPriceMulBps  int
	nonceMgr        *NonceManager
	// coordinator is the shared per-signing-key subpool coordinator. It
	// serializes broadcasts ACROSS tx classes (blob vs legacy) while
	// allowing unbounded within-class parallelism. Shared with
	// BlobTxSubmitter via injection at the service layer. Nil-safe via
	// broadcastCoordinator() lazy init for tests that don't inject one.
	coordinator     *SubpoolCoordinator
	coordinatorOnce sync.Once
	// stuckTracker is the shared per-sender stuck-nonce tracker. Also
	// shared with BlobTxSubmitter. Nil-safe via stuckNonceTracker()
	// lazy init for tests that don't inject one.
	stuckTracker     *StuckNonceTracker
	stuckTrackerOnce sync.Once
	// disputeWindowCache memoizes AIConfig.getDisputeWindow() to avoid an
	// eth_call on every release-cycle eligibility check. TTL is bounded so
	// governance changes propagate without a worker restart. Guarded by
	// disputeWindowMu.
	disputeWindowMu       sync.Mutex
	disputeWindowValue    time.Duration
	disputeWindowExpires  time.Time
	disputeWindowCacheTTL time.Duration // zero means defaultDisputeWindowCacheTTL
}

type jobTxBackend interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// NewChainClient dials the RPC endpoint, instantiates the contract bindings, and
// returns a ChainClient ready to submit registration and job transactions.
// coordinator and stuckTracker must be the shared per-signing-key instances.
func NewChainClient(
	rpcURL string,
	chainID int64,
	registryAddr common.Address,
	aiConfigAddr common.Address,
	jobRegistryAddr common.Address,
	sessionManagerAddr common.Address,
	signingKey *ecdsa.PrivateKey,
	gasMulBps int,
	coordinator *SubpoolCoordinator,
	stuckTracker *StuckNonceTracker,
) (*ChainClient, error) {
	if signingKey == nil {
		return nil, fmt.Errorf("signingKey must not be nil")
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("chainID must be positive, got %d", chainID)
	}
	if gasMulBps <= 0 {
		return nil, fmt.Errorf("gasMulBps must be positive, got %d", gasMulBps)
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dial RPC %s: %w", rpcURL, err)
	}

	registry, err := bindings.NewWorkerRegistry(registryAddr, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("bind WorkerRegistry at %s: %w", registryAddr.Hex(), err)
	}

	aiCfg, err := bindings.NewAIConfig(aiConfigAddr, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("bind AIConfig at %s: %w", aiConfigAddr.Hex(), err)
	}

	var jobReg *bindings.JobRegistry
	if jobRegistryAddr != (common.Address{}) {
		jobReg, err = bindings.NewJobRegistry(jobRegistryAddr, client)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("bind JobRegistry at %s: %w", jobRegistryAddr.Hex(), err)
		}
	}

	var sessMgr *bindings.SessionManager
	if sessionManagerAddr != (common.Address{}) {
		sessMgr, err = bindings.NewSessionManager(sessionManagerAddr, client)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("bind SessionManager at %s: %w", sessionManagerAddr.Hex(), err)
		}
	}

	workerAddr := crypto.PubkeyToAddress(signingKey.PublicKey)
	nonceMgr := NewNonceManager(client, workerAddr)

	return &ChainClient{
		ethClient:       client,
		jobTxBackend:    client,
		registry:        registry,
		aiConfig:        aiCfg,
		jobRegistry:     jobReg,
		jobRegistryAddr: jobRegistryAddr,
		sessionManager:  sessMgr,
		signingKey:      signingKey,
		workerAddr:      workerAddr,
		chainID:         big.NewInt(chainID),
		gasPriceMulBps:  gasMulBps,
		nonceMgr:        nonceMgr,
		coordinator:     coordinator,
		stuckTracker:    stuckTracker,
	}, nil
}

// broadcastCoordinator returns the shared coordinator, lazy-initializing a
// private one if none was injected (useful for tests that construct
// ChainClient via struct literal and don't need cross-submitter sharing).
func (c *ChainClient) broadcastCoordinator() *SubpoolCoordinator {
	c.coordinatorOnce.Do(func() {
		if c.coordinator == nil {
			c.coordinator = NewSubpoolCoordinator(nil, 0)
		}
	})
	return c.coordinator
}

// stuckNonceTracker returns the shared tracker, lazy-initializing a private
// one if none was injected. Shared with BlobTxSubmitter in production so
// hits from the non-blob path accumulate into the blob path's replacement
// decision.
func (c *ChainClient) stuckNonceTracker() *StuckNonceTracker {
	c.stuckTrackerOnce.Do(func() {
		if c.stuckTracker == nil {
			c.stuckTracker = NewStuckNonceTracker()
		}
	})
	return c.stuckTracker
}

// Close shuts down the underlying ethclient connection.
func (c *ChainClient) Close() {
	c.ethClient.Close()
}

// IsWorkerRegistered returns true if this worker's address is registered on-chain.
func (c *ChainClient) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	registered, err := c.registry.IsWorkerRegistered(&bind.CallOpts{Context: ctx}, worker)
	if err != nil {
		return false, fmt.Errorf("IsWorkerRegistered %s: %w", worker.Hex(), err)
	}
	return registered, nil
}

// RegisterWorker submits a registerWorker transaction with the ECDH public key and stake.
func (c *ChainClient) RegisterWorker(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error {
	return c.submitPreparedTx(ctx, "RegisterWorker", stake, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.registry.RegisterWorker(opts, encryptionPubKey)
	})
}

// AddSupportedModel submits an addSupportedModel transaction (non-payable per Amendment A-2).
func (c *ChainClient) AddSupportedModel(ctx context.Context, modelID [32]byte) error {
	return c.submitPreparedTx(ctx, "AddSupportedModel", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.registry.AddSupportedModel(opts, modelID)
	})
}

// SetCapabilities declares this worker's full on-chain capability mask (overwrite, not merge)..
func (c *ChainClient) SetCapabilities(ctx context.Context, mask *big.Int) error {
	return c.submitPreparedTx(ctx, "SetCapabilities", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.registry.SetCapabilities(opts, mask)
	})
}

// GetWorkerCapabilities reads the declared capability mask for the given worker..
func (c *ChainClient) GetWorkerCapabilities(ctx context.Context, worker common.Address) (*big.Int, error) {
	return c.registry.GetWorkerCapabilities(&bind.CallOpts{Context: ctx}, worker)
}

// GetCapabilityMask resolves a capability name to its bitmask (zero = not registered on-chain)..
func (c *ChainClient) GetCapabilityMask(ctx context.Context, name string) (*big.Int, error) {
	return c.registry.GetCapabilityMask(&bind.CallOpts{Context: ctx}, name)
}

// DeregisterWorker submits a deregisterWorker transaction, withdrawing all stake.
func (c *ChainClient) DeregisterWorker(ctx context.Context) error {
	return c.submitPreparedTx(ctx, "DeregisterWorker", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.registry.DeregisterWorker(opts)
	})
}

// GetMinWorkerStake reads the minimum stake required to register from the AIConfig contract.
func (c *ChainClient) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	stake, err := c.aiConfig.GetMinWorkerStake(&bind.CallOpts{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("GetMinWorkerStake: %w", err)
	}
	return stake, nil
}

// GetWorkerEncryptionKey returns the ECDH public key stored on-chain for the given worker.
func (c *ChainClient) GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error) {
	key, err := c.registry.GetWorkerEncryptionKey(&bind.CallOpts{Context: ctx}, worker)
	if err != nil {
		return nil, fmt.Errorf("GetWorkerEncryptionKey %s: %w", worker.Hex(), err)
	}
	return key, nil
}

// checkReceipt returns an error if the receipt is nil or reports a reverted status.
func checkReceipt(receipt *types.Receipt, txName string) error {
	if receipt == nil {
		return fmt.Errorf("%s: WaitMined returned nil receipt", txName)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return fmt.Errorf("%s transaction reverted (status 0, tx %s)", txName, receipt.TxHash.Hex())
	}
	return nil
}

// classifyReceiptRevert wraps a checkReceipt error with ErrContractPaused
// when the original transaction reverted because the callee contract is
// paused. Replays the tx via eth_call against the receipt's block to
// extract revert data; classification is best-effort and degrades to
// the generic revert error on any decode failure (so a transient RPC
// problem never escalates a generic revert into "paused").
//
// The wrapped error preserves the status-0 context the operator sees
// in logs while letting the scheduler use errors.Is(err,
// chain.ErrContractPaused) to apply cycle-level backoff.
func (c *ChainClient) classifyReceiptRevert(
	ctx context.Context,
	txName string,
	tx *types.Transaction,
	receipt *types.Receipt,
	revertErr error,
) error {
	if !decodePauseRevert(ctx, c.ethClient, c.workerAddr, tx, receipt.BlockNumber) {
		return revertErr
	}
	return fmt.Errorf("%s transaction reverted (status 0, tx %s): %w",
		txName, receipt.TxHash.Hex(), ErrContractPaused)
}

// adjustGasPrice applies the configured gas price multiplier: basePrice * gasPriceMulBps / 10000.
func (c *ChainClient) adjustGasPrice(basePrice *big.Int) *big.Int {
	adjusted := new(big.Int).Mul(basePrice, big.NewInt(int64(c.gasPriceMulBps)))
	return adjusted.Div(adjusted, big.NewInt(10000))
}

// submitPreparedTx handles transactions with explicit pre-send and post-send phases.
// If tx construction fails after reserving a nonce but before broadcast, the nonce manager is reset.
func (c *ChainClient) submitPreparedTx(
	ctx context.Context,
	txName string,
	value *big.Int,
	build func(opts *bind.TransactOpts) (*types.Transaction, error),
) error {
	backend := c.jobTxBackend
	if backend == nil {
		backend = c.ethClient
	}

	gasPrice, err := backend.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("suggest gas price: %w", err)
	}

	// Enter the coordinator's legacy lane before reserving a nonce. This
	// blocks only if the BLOB lane has pending txs (cross-subpool exclusion
	// required by geth's sender reservation). Same-class callers run in
	// parallel — geth's legacypool orders them by nonce.
	coordinator := c.broadcastCoordinator()
	token, err := coordinator.Enter(ctx, ClassLegacy)
	if err != nil {
		return fmt.Errorf("wait for %s broadcast slot: %w", txName, err)
	}
	// Token.Done is idempotent, so deferring on every exit path is safe.
	defer token.Done()

	auth, err := bind.NewKeyedTransactorWithChainID(c.signingKey, c.chainID)
	if err != nil {
		return fmt.Errorf("create transactor: %w", err)
	}

	nonce, err := c.nonceMgr.NextNonce(ctx)
	if err != nil {
		return fmt.Errorf("nonce manager: %w", err)
	}

	auth.Context = ctx
	auth.Nonce = new(big.Int).SetUint64(nonce)
	auth.GasPrice = c.adjustGasPrice(gasPrice)
	auth.Value = value
	auth.NoSend = true

	tx, err := build(auth)
	if err != nil {
		c.nonceMgr.ResetNonce()
		return fmt.Errorf("%s transaction: %w", txName, err)
	}

	if err := ctx.Err(); err != nil {
		c.nonceMgr.ResetNonce()
		return fmt.Errorf("broadcast %s tx: %w", txName, err)
	}

	if err := backend.SendTransaction(ctx, tx); err != nil {
		if ShouldResetNonceOnSendError(err) {
			c.nonceMgr.ResetNonce()
		}
		// Stuck-nonce detection (Hazard B): record "address already
		// reserved" hits at this nonce into the shared tracker. The
		// blob-tx path watches this counter and triggers replacement
		// when it crosses the threshold. Non-blob txs cannot themselves
		// evict a stuck blob from the pool (go-ethereum blob pool only
		// accepts blob replacements), so we defer the actual fix to
		// the next blob broadcast's decision.
		//
		// Under the SubpoolCoordinator, IsAlreadyReservedError in
		// normal flow is an anomaly — the coordinator should have
		// prevented cross-subpool contention. A hit here suggests an
		// orphaned blob token in the coordinator (previous pipeline
		// crashed between Send and release). The tracker's per-nonce
		// counter makes this detectable.
		if IsAlreadyReservedError(err) {
			c.stuckNonceTracker().Record(tx.Nonce())
		}
		return fmt.Errorf("send %s tx: %w", txName, err)
	}

	// Broadcast succeeded — record the hash on the token so janitor
	// eviction logs can identify stuck entries by tx hash.
	token.Registered(tx.Hash())

	receipt, err := bind.WaitMined(ctx, c.ethClient, tx)
	if err != nil {
		// Symmetric with BlobTxSubmitter.SubmitBlobTx: SendTransaction
		// already succeeded, so the tx is on the wire and the local nonce
		// has advanced past it. A WaitMined failure (ctx deadline, dead
		// EL) without a reset here leaves the counter drifting from chain
		// pending, which is the same cascading-gap failure mode seen on
		// testnet. Reset forces the next NextNonce() to refetch pending
		// from chain. Unlike the blob path, no logger is threaded through
		// ChainClient — the chain-refetch on subsequent NextNonce is
		// currently silent, tracked as a separate observability follow-up.
		c.nonceMgr.ResetNonce()
		return fmt.Errorf("wait for %s tx %s: %w", txName, tx.Hash().Hex(), err)
	}
	if err := checkReceipt(receipt, txName); err != nil {
		return c.classifyReceiptRevert(ctx, txName, tx, receipt, err)
	}

	// A mined receipt means this tx is no longer in the pool. Clear the
	// stuck tracker — future rejections at a new nonce start fresh.
	c.stuckNonceTracker().Clear()

	return nil
}

// requireJobRegistry returns an error if the jobRegistry binding is nil.
// All job-facing methods must call this before dereferencing c.jobRegistry.
func (c *ChainClient) requireJobRegistry() error {
	if c.jobRegistry == nil {
		return fmt.Errorf("jobRegistry not configured: zero jobRegistryAddr was provided to NewChainClient")
	}
	return nil
}

// requireSessionManager returns an error if the sessionManager binding is nil.
// All sortition-facing methods must call this before dereferencing c.sessionManager.
func (c *ChainClient) requireSessionManager() error {
	if c.sessionManager == nil {
		return fmt.Errorf("session manager binding not configured")
	}
	return nil
}

// ClaimSession sends a claimSession tx for the given request id (sortition claim).
func (c *ChainClient) ClaimSession(ctx context.Context, reqID uint64) error {
	if err := c.requireSessionManager(); err != nil {
		return err
	}
	return c.submitPreparedTx(ctx, "ClaimSession", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.sessionManager.ClaimSession(opts, new(big.Int).SetUint64(reqID))
	})
}

// GetRequiredCapabilities reads a session request's required-capability mask (zero = unconstrained)..
func (c *ChainClient) GetRequiredCapabilities(ctx context.Context, reqID uint64) (*big.Int, error) {
	if err := c.requireSessionManager(); err != nil {
		return nil, err
	}
	return c.sessionManager.GetRequiredCapabilities(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(reqID))
}

// EligibleNow returns whether `worker` currently clears the sortition threshold for the request.
func (c *ChainClient) EligibleNow(ctx context.Context, reqID uint64, worker common.Address) (bool, error) {
	if err := c.requireSessionManager(); err != nil {
		return false, err
	}
	ok, err := c.sessionManager.EligibleNow(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(reqID), worker)
	if err != nil {
		return false, fmt.Errorf("eligibleNow req %d worker %s: %w", reqID, worker.Hex(), err)
	}
	return ok, nil
}

// AcknowledgeJob submits an acknowledgeJob transaction for the given job ID.
func (c *ChainClient) AcknowledgeJob(ctx context.Context, jobID uint64) error {
	if err := c.requireJobRegistry(); err != nil {
		return err
	}
	return c.submitPreparedTx(ctx, "AcknowledgeJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.jobRegistry.AcknowledgeJob(opts, new(big.Int).SetUint64(jobID))
	})
}

// CompleteJob submits a completeJob transaction with a single bytes32 response
// blob hash. Post-audit the contract takes one bytes32 and enforces
// `blobhash(0) == responseBlobHash`, so the blob-carrying TX must contain
// exactly one blob matching this hash.
func (c *ChainClient) CompleteJob(
	ctx context.Context,
	jobID uint64,
	responseBlobHash [32]byte,
	responseCiphertextHash [32]byte,
) error {
	if err := c.requireJobRegistry(); err != nil {
		return err
	}
	return c.submitPreparedTx(ctx, "CompleteJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.jobRegistry.CompleteJob(opts, new(big.Int).SetUint64(jobID), responseBlobHash, responseCiphertextHash)
	})
}

// HasJobAcknowledged reports whether a JobAcknowledged event already exists for this worker+job pair.
func (c *ChainClient) HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error) {
	if err := c.requireJobRegistry(); err != nil {
		return false, err
	}
	iter, err := c.jobRegistry.FilterJobAcknowledged(
		&bind.FilterOpts{Context: ctx},
		[]*big.Int{new(big.Int).SetUint64(jobID)},
		[]common.Address{c.workerAddr},
	)
	if err != nil {
		return false, fmt.Errorf("filter JobAcknowledged for job %d: %w", jobID, err)
	}
	defer iter.Close()

	if iter.Next() {
		return true, nil
	}
	if err := iter.Error(); err != nil {
		return false, fmt.Errorf("iterate JobAcknowledged events for job %d: %w", jobID, err)
	}

	return false, nil
}

// HasJobCompleted reports whether a JobCompleted event already exists for this worker+job pair.
func (c *ChainClient) HasJobCompleted(ctx context.Context, jobID uint64) (bool, error) {
	if err := c.requireJobRegistry(); err != nil {
		return false, err
	}
	iter, err := c.jobRegistry.FilterJobCompleted(
		&bind.FilterOpts{Context: ctx},
		[]*big.Int{new(big.Int).SetUint64(jobID)},
		[]common.Address{c.workerAddr},
	)
	if err != nil {
		return false, fmt.Errorf("filter JobCompleted for job %d: %w", jobID, err)
	}
	defer iter.Close()

	if iter.Next() {
		return true, nil
	}
	if err := iter.Error(); err != nil {
		return false, fmt.Errorf("iterate JobCompleted events for job %d: %w", jobID, err)
	}

	return false, nil
}

// sessionStatusActive matches the Solidity enum JobRegistry.SessionStatus.Active (index 0).
// Source: contracts/src/interfaces/IJobRegistry.sol — enum SessionStatus { Active, ... }
const sessionStatusActive uint8 = 0

// ErrSessionNotActive marks a session that is not in Active status — a
// condition retrying cannot fix from the worker's side (Closed, or
// Reassigning away from this worker). Callers classify it as permanent
// (bounded retry, then skip) rather than transient (unlimited retry).
var ErrSessionNotActive = errors.New("session not active")

// sessionNotActiveErr builds the ErrSessionNotActive-wrapped error returned
// by GetSessionEncWorkerKey when a session's status is not Active. Split out
// from GetSessionEncWorkerKey so the classification is unit-testable without
// a live chain backend (jobRegistry is a concrete generated binding with no
// mockable interface seam).
func sessionNotActiveErr(sessionID uint64, status uint8) error {
	return fmt.Errorf("session %d: %w (status=%d)", sessionID, ErrSessionNotActive, status)
}

// GetSessionEncWorkerKey retrieves the current encrypted worker key for a session
// from JobRegistry session storage. Sessions that are not currently Active are
// blocked until on-chain failover has completed.
func (c *ChainClient) GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	sess, err := c.jobRegistry.GetSession(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(sessionID))
	if err != nil {
		return nil, fmt.Errorf("GetSession %d: %w", sessionID, err)
	}
	if sess.Status != sessionStatusActive {
		return nil, sessionNotActiveErr(sessionID, sess.Status)
	}
	encWorkerKey := sess.EncWorkerKey
	if len(encWorkerKey) == 0 {
		return nil, fmt.Errorf("session %d has empty encWorkerKey", sessionID)
	}

	return encWorkerKey, nil
}

// GetJobBlobInfo reads a job from the contract and returns its prompt blob
// hash, response blob hash, submitBlockNumber, and completionBlockNumber.
// Post-audit each job carries a single prompt blob and a single
// response blob.
func (c *ChainClient) GetJobBlobInfo(ctx context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	if err := c.requireJobRegistry(); err != nil {
		return common.Hash{}, common.Hash{}, 0, 0, err
	}

	job, err := c.jobRegistry.GetJob(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(jobID))
	if err != nil {
		return common.Hash{}, common.Hash{}, 0, 0, fmt.Errorf("GetJob %d: %w", jobID, err)
	}

	return common.Hash(job.PromptBlobHash),
		common.Hash(job.ResponseBlobHash),
		job.SubmitBlockNumber.Uint64(),
		job.CompletionBlockNumber.Uint64(),
		nil
}

// EthClient returns the underlying ethclient for use by blob layer components.
func (c *ChainClient) EthClient() *ethclient.Client {
	return c.ethClient
}

// NonceManager returns the nonce manager for use by blob TX submission.
func (c *ChainClient) NonceManager() *NonceManager {
	return c.nonceMgr
}

// WorkerAddr returns the worker's Ethereum address.
func (c *ChainClient) WorkerAddr() common.Address {
	return c.workerAddr
}

// SetDisputeWindowCacheTTL configures how long GetDisputeWindow memoizes the
// on-chain value. Pass <= 0 to fall back to defaultDisputeWindowCacheTTL.
// Service wiring sets this from cfg.ReleaseDisputeWindowCacheTTL.
func (c *ChainClient) SetDisputeWindowCacheTTL(ttl time.Duration) {
	c.disputeWindowMu.Lock()
	defer c.disputeWindowMu.Unlock()
	c.disputeWindowCacheTTL = ttl
	// Invalidate the cached value so the next call reflects the new TTL
	// regime (and fetches fresh data).
	c.disputeWindowExpires = time.Time{}
}

// ReleaseJob settles a single job's escrowed fee. The contract reverts if the
// job is not Completed-past-window or Resolved; callers should pre-filter via
// GetJobState to avoid wasted gas.
func (c *ChainClient) ReleaseJob(ctx context.Context, jobID uint64) error {
	if err := c.requireJobRegistry(); err != nil {
		return err
	}
	return c.submitPreparedTx(ctx, "ReleaseJob", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.jobRegistry.ReleaseJob(opts, new(big.Int).SetUint64(jobID))
	})
}

// ReleaseJobs is the batch variant. The contract reverts the entire batch if
// any single job is not in a releasable state, so callers must pre-filter.
func (c *ChainClient) ReleaseJobs(ctx context.Context, jobIDs []uint64) error {
	if err := c.requireJobRegistry(); err != nil {
		return err
	}
	if len(jobIDs) == 0 {
		return fmt.Errorf("ReleaseJobs: empty jobIDs slice")
	}
	idsBig := make([]*big.Int, len(jobIDs))
	for i, id := range jobIDs {
		idsBig[i] = new(big.Int).SetUint64(id)
	}
	return c.submitPreparedTx(ctx, "ReleaseJobs", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.jobRegistry.ReleaseJobs(opts, idsBig)
	})
}

// GetJobState reads the on-chain Job struct and returns the subset of fields
// needed by the release scheduler.
func (c *ChainClient) GetJobState(ctx context.Context, jobID uint64) (JobStateInfo, error) {
	if err := c.requireJobRegistry(); err != nil {
		return JobStateInfo{}, err
	}
	job, err := c.jobRegistry.GetJob(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(jobID))
	if err != nil {
		return JobStateInfo{}, fmt.Errorf("GetJob %d: %w", jobID, err)
	}
	completedAt := int64(0)
	if job.CompletedAt != nil {
		completedAt = job.CompletedAt.Int64()
	}
	fee := job.EscrowedFee
	if fee == nil {
		fee = new(big.Int)
	}
	return JobStateInfo{
		State:       JobState(job.State),
		Worker:      job.Worker,
		CompletedAt: completedAt,
		EscrowedFee: fee,
	}, nil
}

// GetDisputeWindow reads AIConfig.getDisputeWindow() with TTL'd caching. The
// returned duration is the contract's window in seconds (converted to
// time.Duration).
func (c *ChainClient) GetDisputeWindow(ctx context.Context) (time.Duration, error) {
	c.disputeWindowMu.Lock()
	if !c.disputeWindowExpires.IsZero() && time.Now().Before(c.disputeWindowExpires) {
		v := c.disputeWindowValue
		c.disputeWindowMu.Unlock()
		return v, nil
	}
	c.disputeWindowMu.Unlock()

	raw, err := c.aiConfig.GetDisputeWindow(&bind.CallOpts{Context: ctx})
	if err != nil {
		return 0, fmt.Errorf("AIConfig.getDisputeWindow: %w", err)
	}
	if raw == nil || raw.Sign() <= 0 {
		return 0, fmt.Errorf("AIConfig.getDisputeWindow returned non-positive value: %v", raw)
	}
	window := time.Duration(raw.Int64()) * time.Second

	c.disputeWindowMu.Lock()
	defer c.disputeWindowMu.Unlock()
	ttl := c.disputeWindowCacheTTL
	if ttl <= 0 {
		ttl = defaultDisputeWindowCacheTTL
	}
	c.disputeWindowValue = window
	c.disputeWindowExpires = time.Now().Add(ttl)
	return window, nil
}

// WorkerBalance returns the withdrawable balance accumulated for `worker`.
func (c *ChainClient) WorkerBalance(ctx context.Context, worker common.Address) (*big.Int, error) {
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	bal, err := c.jobRegistry.WorkerBalance(&bind.CallOpts{Context: ctx}, worker)
	if err != nil {
		return nil, fmt.Errorf("WorkerBalance %s: %w", worker.Hex(), err)
	}
	if bal == nil {
		return new(big.Int), nil
	}
	return bal, nil
}

// Withdraw moves the calling address's full workerBalance to itself. The
// contract sends ETH to msg.sender; there is no destination parameter.
func (c *ChainClient) Withdraw(ctx context.Context) error {
	if err := c.requireJobRegistry(); err != nil {
		return err
	}
	return c.submitPreparedTx(ctx, "Withdraw", nil, func(opts *bind.TransactOpts) (*types.Transaction, error) {
		return c.jobRegistry.Withdraw(opts)
	})
}

// Head returns the latest block number and timestamp. Used to make settlement
// decisions against block.timestamp instead of wall-clock time.
func (c *ChainClient) Head(ctx context.Context) (HeadInfo, error) {
	header, err := c.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return HeadInfo{}, fmt.Errorf("HeaderByNumber(latest): %w", err)
	}
	if header.Number == nil {
		return HeadInfo{}, fmt.Errorf("HeaderByNumber returned nil block number")
	}
	return HeadInfo{
		Number:    header.Number.Uint64(),
		Timestamp: int64(header.Time),
	}, nil
}

// FilterJobCompleted iterates JobCompleted events emitted by `worker` in
// [fromBlock, toBlock] (both inclusive). The worker filter uses the indexed
// event topic so filtering happens on the node side.
func (c *ChainClient) FilterJobCompleted(
	ctx context.Context,
	worker common.Address,
	fromBlock, toBlock uint64,
) ([]JobCompletedEvent, error) {
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("FilterJobCompleted: toBlock %d < fromBlock %d", toBlock, fromBlock)
	}
	endBlock := toBlock
	opts := &bind.FilterOpts{
		Context: ctx,
		Start:   fromBlock,
		End:     &endBlock,
	}
	iter, err := c.jobRegistry.FilterJobCompleted(opts, nil, []common.Address{worker})
	if err != nil {
		return nil, fmt.Errorf("filter JobCompleted [%d..%d] for %s: %w", fromBlock, toBlock, worker.Hex(), err)
	}
	defer iter.Close()

	var events []JobCompletedEvent
	for iter.Next() {
		ev := iter.Event
		if ev == nil || ev.JobId == nil {
			continue
		}
		events = append(events, JobCompletedEvent{
			JobID:       ev.JobId.Uint64(),
			Worker:      ev.Worker,
			BlockNumber: ev.Raw.BlockNumber,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate JobCompleted [%d..%d]: %w", fromBlock, toBlock, err)
	}
	return events, nil
}

// SessionRequestedEvent is the decoded view of the on-chain SessionRequested
// log. ReqID and ModelID use Go-idiomatic casing even though the abigen struct
// uses ReqId/ModelId.
type SessionRequestedEvent struct {
	ReqID        uint64
	User         common.Address
	ModelID      [32]byte
	RequestBlock uint64
	BlockNumber  uint64
}

// JobSubmittedEvent is the decoded view of the on-chain JobSubmitted log.
type JobSubmittedEvent struct {
	JobID       uint64
	SessionID   uint64
	Worker      common.Address
	BlockNumber uint64
}

// SessionInfo is a trimmed view of IJobRegistrySession returned by GetSession.
type SessionInfo struct {
	User    common.Address
	ModelID [32]byte
	Worker  common.Address
	Status  uint8
}

// RequestInfo is the subset of on-chain SessionManager.Request storage the
// SessionWatcher needs to decide whether a pending request is still actionable.
// ReqStatus enum on-chain: Open=0, Claimed=1, Ready=2, Expired=3.
type RequestInfo struct {
	Status uint8
	Expiry uint64
	Worker common.Address
}

// FilterSessionRequested returns all SessionRequested events emitted by the
// SessionManager contract in [fromBlock, toBlock] (both inclusive). No indexed
// filter is applied — the caller is expected to further filter by reqId/user/modelId
// as needed.
func (c *ChainClient) FilterSessionRequested(ctx context.Context, fromBlock, toBlock uint64) ([]SessionRequestedEvent, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("FilterSessionRequested: toBlock %d < fromBlock %d", toBlock, fromBlock)
	}
	if err := c.requireSessionManager(); err != nil {
		return nil, err
	}
	end := toBlock
	opts := &bind.FilterOpts{Context: ctx, Start: fromBlock, End: &end}
	iter, err := c.sessionManager.FilterSessionRequested(opts, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("filter SessionRequested [%d,%d]: %w", fromBlock, toBlock, err)
	}
	defer iter.Close()
	var out []SessionRequestedEvent
	for iter.Next() {
		ev := iter.Event
		if ev == nil || ev.ReqId == nil {
			continue
		}
		out = append(out, SessionRequestedEvent{
			ReqID:        ev.ReqId.Uint64(),
			User:         ev.User,
			ModelID:      ev.ModelId,
			RequestBlock: ev.RequestBlock.Uint64(),
			BlockNumber:  ev.Raw.BlockNumber,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate SessionRequested [%d,%d]: %w", fromBlock, toBlock, err)
	}
	return out, nil
}

// FilterJobSubmitted returns all JobSubmitted events in [fromBlock, toBlock]
// (both inclusive). No indexed filter is applied — callers that need only jobs
// for a specific worker must filter the returned slice themselves.
func (c *ChainClient) FilterJobSubmitted(ctx context.Context, fromBlock, toBlock uint64) ([]JobSubmittedEvent, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("FilterJobSubmitted: toBlock %d < fromBlock %d", toBlock, fromBlock)
	}
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	end := toBlock
	opts := &bind.FilterOpts{Context: ctx, Start: fromBlock, End: &end}
	iter, err := c.jobRegistry.FilterJobSubmitted(opts, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("filter JobSubmitted [%d,%d]: %w", fromBlock, toBlock, err)
	}
	defer iter.Close()
	var out []JobSubmittedEvent
	for iter.Next() {
		ev := iter.Event
		if ev == nil || ev.JobId == nil {
			continue
		}
		out = append(out, JobSubmittedEvent{
			JobID:       ev.JobId.Uint64(),
			SessionID:   ev.SessionId.Uint64(),
			Worker:      ev.Worker,
			BlockNumber: ev.Raw.BlockNumber,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate JobSubmitted [%d,%d]: %w", fromBlock, toBlock, err)
	}
	return out, nil
}

// GetPriorSessionJobIDs returns the ascending list of job IDs submitted for
// sessionID strictly before currentJobID, scanning JobSubmitted events (indexed
// by sessionId) in [fromBlock, toBlock]. Used by the sortition JobWatcher to
// reconstruct the conversation-history job list the dispatcher used to supply.
func (c *ChainClient) GetPriorSessionJobIDs(ctx context.Context, sessionID, currentJobID, fromBlock, toBlock uint64) ([]uint64, error) {
	if toBlock < fromBlock {
		return nil, fmt.Errorf("GetPriorSessionJobIDs: toBlock %d < fromBlock %d", toBlock, fromBlock)
	}
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	end := toBlock
	opts := &bind.FilterOpts{Context: ctx, Start: fromBlock, End: &end}
	sid := []*big.Int{new(big.Int).SetUint64(sessionID)}
	iter, err := c.jobRegistry.FilterJobSubmitted(opts, nil, sid)
	if err != nil {
		return nil, fmt.Errorf("filter JobSubmitted session %d [%d,%d]: %w", sessionID, fromBlock, toBlock, err)
	}
	defer iter.Close()
	var out []uint64
	for iter.Next() {
		ev := iter.Event
		if ev == nil || ev.JobId == nil {
			continue
		}
		if jid := ev.JobId.Uint64(); jid < currentJobID {
			out = append(out, jid)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterate JobSubmitted session %d: %w", sessionID, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// GetSessionInfo fetches session metadata for the given session ID from the
// JobRegistry. Only the fields needed by the sortition watcher are returned.
func (c *ChainClient) GetSessionInfo(ctx context.Context, sessionID uint64) (SessionInfo, error) {
	if err := c.requireJobRegistry(); err != nil {
		return SessionInfo{}, err
	}
	s, err := c.jobRegistry.GetSession(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(sessionID))
	if err != nil {
		return SessionInfo{}, fmt.Errorf("GetSession %d: %w", sessionID, err)
	}
	return SessionInfo{
		User:    s.User,
		ModelID: s.ModelId,
		Worker:  s.Worker,
		Status:  s.Status,
	}, nil
}

// GetRequestInfo fetches the status, expiry, and assigned worker for the given
// sortition request ID from the SessionManager contract. Used by the SessionWatcher
// to re-evaluate pending requests across multiple poll passes.
func (c *ChainClient) GetRequestInfo(ctx context.Context, reqID uint64) (RequestInfo, error) {
	if err := c.requireSessionManager(); err != nil {
		return RequestInfo{}, err
	}
	r, err := c.sessionManager.GetRequest(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(reqID))
	if err != nil {
		return RequestInfo{}, fmt.Errorf("getRequest %d: %w", reqID, err)
	}
	return RequestInfo{Status: r.Status, Expiry: r.Expiry, Worker: r.Worker}, nil
}

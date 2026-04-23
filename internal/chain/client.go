package chain

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/lightchain/pkg/chain/bindings"
)

// Compile-time interface assertions.
var (
	_ RegistrationClient = (*ChainClient)(nil)
	_ ValidationClient   = (*ChainClient)(nil)
	_ JobExecutionClient = (*ChainClient)(nil)
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
	signingKey      *ecdsa.PrivateKey
	workerAddr      common.Address
	chainID         *big.Int
	gasPriceMulBps  int
	nonceMgr        *NonceManager
}

type jobTxBackend interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// NewChainClient dials the RPC endpoint, instantiates the contract bindings, and
// returns a ChainClient ready to submit registration and job transactions.
func NewChainClient(
	rpcURL string,
	chainID int64,
	registryAddr common.Address,
	aiConfigAddr common.Address,
	jobRegistryAddr common.Address,
	signingKey *ecdsa.PrivateKey,
	gasMulBps int,
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

	workerAddr := crypto.PubkeyToAddress(signingKey.PublicKey)
	nonceMgr := NewNonceManager(client, workerAddr)

	return &ChainClient{
		ethClient:       client,
		jobTxBackend:    client,
		registry:        registry,
		aiConfig:        aiCfg,
		jobRegistry:     jobReg,
		jobRegistryAddr: jobRegistryAddr,
		signingKey:      signingKey,
		workerAddr:      workerAddr,
		chainID:         big.NewInt(chainID),
		gasPriceMulBps:  gasMulBps,
		nonceMgr:        nonceMgr,
	}, nil
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

	if err := backend.SendTransaction(ctx, tx); err != nil {
		if ShouldResetNonceOnSendError(err) {
			c.nonceMgr.ResetNonce()
		}
		return fmt.Errorf("send %s tx: %w", txName, err)
	}

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
		return err
	}

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

// GetSessionEncWorkerKey returns the most recent encrypted worker key for a
// session. It queries both SessionCreated and SessionKeyUpdated event logs
// and returns whichever was emitted in the highest-numbered block, so the
// post-audit updateSessionKey flow (LSC-15) transparently rotates the key
// worker-side without any explicit event subscription.
//
// Event iteration is required because the JobRegistry ABI has no view functions
// that expose session data.
func (c *ChainClient) GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	if err := c.requireJobRegistry(); err != nil {
		return nil, err
	}
	sessionIDBig := new(big.Int).SetUint64(sessionID)
	filterOpts := &bind.FilterOpts{Context: ctx}

	// Baseline: the SessionCreated event carries the original encWorkerKey and
	// is the only event guaranteed to exist for any valid session.
	createdIter, err := c.jobRegistry.FilterSessionCreated(filterOpts, []*big.Int{sessionIDBig}, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("GetSession %d: %w", sessionID, err)
	}
	defer createdIter.Close()

	if !createdIter.Next() {
		if createdIter.Error() != nil {
			return nil, fmt.Errorf("iterate SessionCreated events: %w", createdIter.Error())
		}
		return nil, fmt.Errorf("no SessionCreated event found for session %d", sessionID)
	}

	latestBlock := createdIter.Event.Raw.BlockNumber
	latestKey := createdIter.Event.EncWorkerKey
	if len(latestKey) == 0 {
		return nil, fmt.Errorf("SessionCreated event for session %d has empty encWorkerKey", sessionID)
	}

	// Layer any SessionKeyUpdated events over the baseline. Each such event
	// carries a replacement encWorkerKey; the newest one wins.
	updatedIter, err := c.jobRegistry.FilterSessionKeyUpdated(filterOpts, []*big.Int{sessionIDBig})
	if err != nil {
		return nil, fmt.Errorf("filter SessionKeyUpdated for session %d: %w", sessionID, err)
	}
	defer updatedIter.Close()

	for updatedIter.Next() {
		block := updatedIter.Event.Raw.BlockNumber
		if block < latestBlock {
			continue
		}
		if len(updatedIter.Event.EncWorkerKey) == 0 {
			// Defensive: ignore an empty rotation rather than crash — contract
			// enforces 125-byte length, so this should never happen in practice.
			continue
		}
		latestBlock = block
		latestKey = updatedIter.Event.EncWorkerKey
	}
	if err := updatedIter.Error(); err != nil {
		return nil, fmt.Errorf("iterate SessionKeyUpdated events: %w", err)
	}

	return latestKey, nil
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

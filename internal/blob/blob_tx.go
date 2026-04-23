package blob

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/holiman/uint256"

	pkgblob "github.com/lightchain/pkg/blob"
	workerchain "github.com/lightchain/worker/internal/chain"
)

// BlobSubmitter submits EIP-4844 blob transactions and returns the versioned hashes.
type BlobSubmitter = pkgblob.BlobSubmitter

// NonceManager provides the next nonce for transaction submission and can be
// reset if reservation fails before the tx is broadcast.
type NonceManager interface {
	NextNonce(ctx context.Context) (uint64, error)
	ResetNonce()
}

// BlobTxBackend is the subset of execution-layer functionality needed for blob
// submission. The concrete backend is the ethclient, but this narrow interface
// keeps the pre-send failure path unit-testable.
type BlobTxBackend interface {
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// BlobTxSubmitter builds and submits type-3 (EIP-4844) blob transactions.
//
// broadcastSlot serializes the SendTransaction..WaitMined window so that at
// most one blob tx per submitter is in-flight on the execution layer at a
// time. Go-ethereum's txpool rejects a second blob tx from the same sender
// with ErrAlreadyReserved while a prior one is still pending; since a single
// worker has a single signing key, this slot enforces that invariant across
// concurrent asynq worker goroutines.
//
// Unlike sync.Mutex, the slot is a buffered channel of size 1 used as a
// ctx-aware binary semaphore: empty = unlocked, sending acquires, receiving
// releases. Acquisition respects ctx.Done() so a cancelled job (e.g. during
// worker shutdown) aborts promptly instead of waiting out the current
// holder's WaitMined. broadcastSlotOnce lazy-initializes the channel so tests
// that construct via struct literal do not need to pre-seed it.
type BlobTxSubmitter struct {
	ethClient         *ethclient.Client
	txBackend         BlobTxBackend
	signingKey        *ecdsa.PrivateKey
	signTx            func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error)
	waitMined         func(ctx context.Context, b bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error)
	chainID           *big.Int
	nonceMgr          NonceManager
	maxGasPrice       *big.Int
	broadcastSlot     chan struct{}
	broadcastSlotOnce sync.Once
	// logger is optional. When nil, no structured logs are emitted. Tests
	// that construct BlobTxSubmitter via struct literal may leave this nil;
	// the production constructor wires in the service-level logger.
	logger *slog.Logger
}

// slot lazily initializes and returns the broadcast serialization channel.
// Safe for concurrent use via sync.Once.
func (s *BlobTxSubmitter) slot() chan struct{} {
	s.broadcastSlotOnce.Do(func() {
		if s.broadcastSlot == nil {
			s.broadcastSlot = make(chan struct{}, 1)
		}
	})
	return s.broadcastSlot
}

// NewBlobTxSubmitter creates a BlobTxSubmitter. The logger may be nil; when
// nil, structured logs are suppressed (useful for tests that inject their own
// instrumentation via struct literals).
func NewBlobTxSubmitter(
	ethClient *ethclient.Client,
	signingKey *ecdsa.PrivateKey,
	chainID *big.Int,
	nonceMgr NonceManager,
	maxGasPrice *big.Int,
	logger *slog.Logger,
) *BlobTxSubmitter {
	return &BlobTxSubmitter{
		ethClient:     ethClient,
		txBackend:     ethClient,
		signingKey:    signingKey,
		signTx:        types.SignTx,
		waitMined:     bind.WaitMined,
		chainID:       chainID,
		nonceMgr:      nonceMgr,
		maxGasPrice:   maxGasPrice,
		broadcastSlot: make(chan struct{}, 1),
		logger:        logger,
	}
}

// SubmitBlobTx creates a type-3 blob transaction containing the given data.
// Returns the versioned hashes of the submitted blobs.
func (s *BlobTxSubmitter) SubmitBlobTx(ctx context.Context, data []byte) ([][32]byte, error) {
	txBackend := s.txBackend
	if txBackend == nil {
		txBackend = s.ethClient
	}
	signTx := s.signTx
	if signTx == nil {
		signTx = types.SignTx
	}
	waitMined := s.waitMined
	if waitMined == nil {
		waitMined = bind.WaitMined
	}

	b, err := pkgblob.EncodeBlobData(data)
	if err != nil {
		return nil, fmt.Errorf("encode blob payload: %w", err)
	}

	commitment, err := kzg4844.BlobToCommitment(&b)
	if err != nil {
		return nil, fmt.Errorf("compute KZG commitment: %w", err)
	}

	proof, err := kzg4844.ComputeBlobProof(&b, commitment)
	if err != nil {
		return nil, fmt.Errorf("compute KZG proof: %w", err)
	}

	versionedHash := pkgblob.VersionedHash(commitment[:])

	gasTipCap, err := txBackend.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas tip cap: %w", err)
	}

	nonce, err := s.nonceMgr.NextNonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("get nonce for blob tx: %w", err)
	}

	senderAddr := crypto.PubkeyToAddress(s.signingKey.PublicKey)
	signer := types.NewCancunSigner(s.chainID)

	tx := types.NewTx(&types.BlobTx{
		ChainID:    uint256.MustFromBig(s.chainID),
		Nonce:      nonce,
		GasTipCap:  uint256.MustFromBig(gasTipCap),
		GasFeeCap:  uint256.MustFromBig(s.maxGasPrice),
		Gas:        21000,
		To:         senderAddr,
		BlobFeeCap: uint256.NewInt(1_000_000_000), // 1 gwei blob fee cap
		BlobHashes: []common.Hash{versionedHash},
		Sidecar: &types.BlobTxSidecar{
			Blobs:       []kzg4844.Blob{b},
			Commitments: []kzg4844.Commitment{commitment},
			Proofs:      []kzg4844.Proof{proof},
		},
	})

	signedTx, err := signTx(tx, signer, s.signingKey)
	if err != nil {
		s.nonceMgr.ResetNonce()
		return nil, fmt.Errorf("sign blob tx: %w", err)
	}

	txHash := signedTx.Hash()

	if s.logger != nil {
		s.logger.Debug("acquiring blob broadcast slot",
			"stage", "submit_blob",
			"txHash", txHash.Hex(),
			"nonce", nonce,
		)
	}
	slot := s.slot()
	mutexWaitStart := time.Now()
	select {
	case slot <- struct{}{}:
		// acquired
	case <-ctx.Done():
		mutexWaitMs := time.Since(mutexWaitStart).Milliseconds()
		if s.logger != nil {
			s.logger.Warn("ctx cancelled waiting for broadcast slot",
				"stage", "submit_blob",
				"txHash", txHash.Hex(),
				"nonce", nonce,
				"mutexWaitMs", mutexWaitMs,
				"error", ctx.Err(),
			)
		}
		return nil, fmt.Errorf("wait for blob broadcast slot: %w", ctx.Err())
	}
	defer func() { <-slot }()
	mutexWaitMs := time.Since(mutexWaitStart).Milliseconds()

	broadcastStart := time.Now()
	if err := txBackend.SendTransaction(ctx, signedTx); err != nil {
		resetNonce := workerchain.ShouldResetNonceOnSendError(err)
		if resetNonce {
			s.nonceMgr.ResetNonce()
		}
		if s.logger != nil {
			s.logger.Warn("blob tx rejected by SendTransaction",
				"stage", "submit_blob",
				"txHash", txHash.Hex(),
				"nonce", nonce,
				"mutexWaitMs", mutexWaitMs,
				"nonceReset", resetNonce,
				"error", err,
			)
		}
		return nil, fmt.Errorf("send blob tx: %w", err)
	}

	if s.logger != nil {
		s.logger.Info("blob tx broadcast, waiting for receipt",
			"stage", "submit_blob",
			"txHash", txHash.Hex(),
			"nonce", nonce,
			"mutexWaitMs", mutexWaitMs,
			"broadcastMs", time.Since(broadcastStart).Milliseconds(),
		)
	}

	waitStart := time.Now()
	receipt, err := waitMined(ctx, s.ethClient, signedTx)
	if err != nil {
		return nil, fmt.Errorf("wait for blob tx %s: %w", signedTx.Hash().Hex(), err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, fmt.Errorf("blob tx reverted (status 0, tx %s)", receipt.TxHash.Hex())
	}

	if s.logger != nil {
		s.logger.Info("blob tx mined",
			"stage", "submit_blob",
			"txHash", txHash.Hex(),
			"blockNumber", receipt.BlockNumber.Uint64(),
			"waitMs", time.Since(waitStart).Milliseconds(),
		)
	}

	return [][32]byte{versionedHash}, nil
}


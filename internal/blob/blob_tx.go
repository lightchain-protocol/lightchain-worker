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
// The SendTransaction..WaitMined window is serialized by a shared
// chain.BroadcastSerializer (see internal/chain/broadcast.go). The
// serializer is per-signing-key, so the same instance is injected into
// BOTH this submitter and ChainClient — go-ethereum's txpool reservation
// is sender-wide, meaning a blob tx pending for sender S blocks any
// concurrent non-blob tx from S as well.
//
// serializerOnce lazy-initializes the serializer when constructed via
// struct literal without one (the NewBlobTxSubmitter constructor injects
// the shared instance from the service layer).
type BlobTxSubmitter struct {
	ethClient      *ethclient.Client
	txBackend      BlobTxBackend
	signingKey     *ecdsa.PrivateKey
	signTx         func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error)
	waitMined      func(ctx context.Context, b bind.DeployBackend, tx *types.Transaction) (*types.Receipt, error)
	chainID        *big.Int
	nonceMgr       NonceManager
	maxGasPrice    *big.Int
	serializer     *workerchain.BroadcastSerializer
	serializerOnce sync.Once
	// logger is optional. When nil, no structured logs are emitted. Tests
	// that construct BlobTxSubmitter via struct literal may leave this nil;
	// the production constructor wires in the service-level logger.
	logger *slog.Logger
}

// broadcastSerializer returns the shared serializer, lazy-initializing a
// private one if none was injected (useful for tests that construct via
// struct literal and don't care about cross-submitter sharing).
func (s *BlobTxSubmitter) broadcastSerializer() *workerchain.BroadcastSerializer {
	s.serializerOnce.Do(func() {
		if s.serializer == nil {
			s.serializer = workerchain.NewBroadcastSerializer()
		}
	})
	return s.serializer
}

// NewBlobTxSubmitter creates a BlobTxSubmitter. The logger may be nil; when
// nil, structured logs are suppressed (useful for tests that inject their own
// instrumentation via struct literals). The serializer must be the shared
// per-signing-key instance — see BroadcastSerializer's package docs.
func NewBlobTxSubmitter(
	ethClient *ethclient.Client,
	signingKey *ecdsa.PrivateKey,
	chainID *big.Int,
	nonceMgr NonceManager,
	maxGasPrice *big.Int,
	serializer *workerchain.BroadcastSerializer,
	logger *slog.Logger,
) *BlobTxSubmitter {
	return &BlobTxSubmitter{
		ethClient:   ethClient,
		txBackend:   ethClient,
		signingKey:  signingKey,
		signTx:      types.SignTx,
		waitMined:   bind.WaitMined,
		chainID:     chainID,
		nonceMgr:    nonceMgr,
		maxGasPrice: maxGasPrice,
		serializer:  serializer,
		logger:      logger,
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
	serializer := s.broadcastSerializer()
	mutexWaitStart := time.Now()
	if err := serializer.Acquire(ctx); err != nil {
		mutexWaitMs := time.Since(mutexWaitStart).Milliseconds()
		if s.logger != nil {
			s.logger.Warn("ctx cancelled waiting for broadcast slot",
				"stage", "submit_blob",
				"txHash", txHash.Hex(),
				"nonce", nonce,
				"mutexWaitMs", mutexWaitMs,
				"error", err,
			)
		}
		return nil, fmt.Errorf("wait for blob broadcast slot: %w", err)
	}
	defer serializer.Release()
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
		// SendTransaction already succeeded: the tx is on the wire and the
		// local NonceManager counter has advanced past this nonce. If
		// WaitMined failed (ctx deadline, dead EL connection, propagation
		// blip), the next broadcast at counter+1 would desync from the
		// chain's pending nonce — producing the cascading gap observed in
		// the 2026-04-23 testnet incident. Force the next NextNonce() to
		// refetch pending from chain instead. On refetch we either (a)
		// see the pool evicted the stuck tx, (b) see it still reserved
		// and hit "address already reserved" (now reset-eligible via
		// Commit A), or (c) see it mined — all recoverable.
		//
		// NOT reset on receipt.Status != Successful below, because a
		// reverted receipt means the tx landed on-chain; the nonce was
		// consumed for real.
		s.nonceMgr.ResetNonce()
		if s.logger != nil {
			s.logger.Warn("blob tx WaitMined failed, resetting nonce for retry",
				"stage", "submit_blob",
				"txHash", signedTx.Hash().Hex(),
				"nonce", nonce,
				"waitMs", time.Since(waitStart).Milliseconds(),
				"error", err,
			)
		}
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


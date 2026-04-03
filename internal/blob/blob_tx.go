package blob

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"

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
type BlobTxSubmitter struct {
	ethClient   *ethclient.Client
	txBackend   BlobTxBackend
	signingKey  *ecdsa.PrivateKey
	signTx      func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error)
	chainID     *big.Int
	nonceMgr    NonceManager
	maxGasPrice *big.Int
}

// NewBlobTxSubmitter creates a BlobTxSubmitter.
func NewBlobTxSubmitter(
	ethClient *ethclient.Client,
	signingKey *ecdsa.PrivateKey,
	chainID *big.Int,
	nonceMgr NonceManager,
	maxGasPrice *big.Int,
) *BlobTxSubmitter {
	return &BlobTxSubmitter{
		ethClient:   ethClient,
		txBackend:   ethClient,
		signingKey:  signingKey,
		signTx:      types.SignTx,
		chainID:     chainID,
		nonceMgr:    nonceMgr,
		maxGasPrice: maxGasPrice,
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

	if err := txBackend.SendTransaction(ctx, signedTx); err != nil {
		if workerchain.ShouldResetNonceOnSendError(err) {
			s.nonceMgr.ResetNonce()
		}
		return nil, fmt.Errorf("send blob tx: %w", err)
	}

	receipt, err := bind.WaitMined(ctx, s.ethClient, signedTx)
	if err != nil {
		return nil, fmt.Errorf("wait for blob tx %s: %w", signedTx.Hash().Hex(), err)
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return nil, fmt.Errorf("blob tx reverted (status 0, tx %s)", receipt.TxHash.Hex())
	}

	return [][32]byte{versionedHash}, nil
}


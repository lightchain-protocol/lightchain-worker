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

// replacementBlobPayload is the smallest valid placeholder payload used when
// constructing a bumped-fee replacement blob tx to evict a stuck entry. Real
// response data isn't relevant — the replacement exists purely to occupy the
// stuck nonce with a higher-fee tx that the pool will accept in place of the
// original. Go-ethereum's blobpool requires a valid KZG commitment + proof,
// so we still run the full blob encoding over this single byte.
var replacementBlobPayload = []byte{0x00}

const defaultBlobFeeCapWei = 1_000_000_000

type blobTxPayload struct {
	blob          kzg4844.Blob
	commitment    kzg4844.Commitment
	proof         kzg4844.Proof
	versionedHash common.Hash
}

func encodeBlobTxPayload(data []byte, label string) (*blobTxPayload, error) {
	b, err := pkgblob.EncodeBlobData(data)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", label, err)
	}

	commitment, err := kzg4844.BlobToCommitment(&b)
	if err != nil {
		return nil, fmt.Errorf("compute %s KZG commitment: %w", label, err)
	}

	proof, err := kzg4844.ComputeBlobProof(&b, commitment)
	if err != nil {
		return nil, fmt.Errorf("compute %s KZG proof: %w", label, err)
	}

	return &blobTxPayload{
		blob:          b,
		commitment:    commitment,
		proof:         proof,
		versionedHash: pkgblob.VersionedHash(commitment[:]),
	}, nil
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
	// stuckTracker is the shared per-sender stuck-nonce tracker. Nil-safe
	// via stuckNonceTracker() lazy init for tests that don't inject one.
	stuckTracker     *workerchain.StuckNonceTracker
	stuckTrackerOnce sync.Once
	stuckCfg         StuckNonceConfig
	// logger is optional. When nil, no structured logs are emitted. Tests
	// that construct BlobTxSubmitter via struct literal may leave this nil;
	// the production constructor wires in the service-level logger.
	logger *slog.Logger
}

// StuckNonceConfig bounds stuck-nonce detection and replacement. Mirrors
// the env vars WORKER_STUCK_NONCE_THRESHOLD / _MAX_BUMPS / _AUTOREPLACE.
type StuckNonceConfig struct {
	Threshold   int
	MaxBumps    int
	AutoReplace bool
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

// stuckNonceTracker returns the shared tracker, lazy-initializing a private
// one if none was injected. Behaves identically to broadcastSerializer — if
// the caller shares an instance with ChainClient, both sides see the same
// counts; otherwise detection still works but is submitter-local.
func (s *BlobTxSubmitter) stuckNonceTracker() *workerchain.StuckNonceTracker {
	s.stuckTrackerOnce.Do(func() {
		if s.stuckTracker == nil {
			s.stuckTracker = workerchain.NewStuckNonceTracker()
		}
	})
	return s.stuckTracker
}

// NewBlobTxSubmitter creates a BlobTxSubmitter. The logger may be nil; when
// nil, structured logs are suppressed (useful for tests that inject their own
// instrumentation via struct literals). The serializer and stuckTracker must
// be the shared per-signing-key instances — see their package docs.
func NewBlobTxSubmitter(
	ethClient *ethclient.Client,
	signingKey *ecdsa.PrivateKey,
	chainID *big.Int,
	nonceMgr NonceManager,
	maxGasPrice *big.Int,
	serializer *workerchain.BroadcastSerializer,
	stuckTracker *workerchain.StuckNonceTracker,
	stuckCfg StuckNonceConfig,
	logger *slog.Logger,
) *BlobTxSubmitter {
	return &BlobTxSubmitter{
		ethClient:    ethClient,
		txBackend:    ethClient,
		signingKey:   signingKey,
		signTx:       types.SignTx,
		waitMined:    bind.WaitMined,
		chainID:      chainID,
		nonceMgr:     nonceMgr,
		maxGasPrice:  maxGasPrice,
		serializer:   serializer,
		stuckTracker: stuckTracker,
		stuckCfg:     stuckCfg,
		logger:       logger,
	}
}

func (s *BlobTxSubmitter) signBlobTx(
	payload *blobTxPayload,
	nonce uint64,
	gasTipCap *big.Int,
	gasFeeCap *big.Int,
	blobFeeCap *big.Int,
	signTx func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error),
) (*types.Transaction, error) {
	senderAddr := crypto.PubkeyToAddress(s.signingKey.PublicKey)
	signer := types.NewCancunSigner(s.chainID)

	tx := types.NewTx(&types.BlobTx{
		ChainID:    uint256.MustFromBig(s.chainID),
		Nonce:      nonce,
		GasTipCap:  uint256.MustFromBig(gasTipCap),
		GasFeeCap:  uint256.MustFromBig(gasFeeCap),
		Gas:        21000,
		To:         senderAddr,
		BlobFeeCap: uint256.MustFromBig(blobFeeCap),
		BlobHashes: []common.Hash{payload.versionedHash},
		Sidecar: &types.BlobTxSidecar{
			Blobs:       []kzg4844.Blob{payload.blob},
			Commitments: []kzg4844.Commitment{payload.commitment},
			Proofs:      []kzg4844.Proof{payload.proof},
		},
	})

	return signTx(tx, signer, s.signingKey)
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

	payload, err := encodeBlobTxPayload(data, "blob payload")
	if err != nil {
		return nil, err
	}

	gasTipCap, err := txBackend.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas tip cap: %w", err)
	}

	if s.logger != nil {
		s.logger.Debug("acquiring blob broadcast slot",
			"stage", "submit_blob",
		)
	}
	serializer := s.broadcastSerializer()
	mutexWaitStart := time.Now()
	if err := serializer.Acquire(ctx); err != nil {
		mutexWaitMs := time.Since(mutexWaitStart).Milliseconds()
		if s.logger != nil {
			s.logger.Warn("ctx cancelled waiting for broadcast slot",
				"stage", "submit_blob",
				"mutexWaitMs", mutexWaitMs,
				"error", err,
			)
		}
		return nil, fmt.Errorf("wait for blob broadcast slot: %w", err)
	}
	defer serializer.Release()
	mutexWaitMs := time.Since(mutexWaitStart).Milliseconds()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("wait for blob broadcast slot: %w", err)
	}

	nonce, err := s.nonceMgr.NextNonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("get nonce for blob tx: %w", err)
	}

	signedTx, err := s.signBlobTx(payload, nonce, gasTipCap, s.maxGasPrice, big.NewInt(defaultBlobFeeCapWei), signTx)
	if err != nil {
		s.nonceMgr.ResetNonce()
		return nil, fmt.Errorf("sign blob tx: %w", err)
	}
	if err := ctx.Err(); err != nil {
		s.nonceMgr.ResetNonce()
		return nil, fmt.Errorf("broadcast blob tx: %w", err)
	}

	txHash := signedTx.Hash()

	broadcastStart := time.Now()
	if err := txBackend.SendTransaction(ctx, signedTx); err != nil {
		s.handleSendError(ctx, txBackend, signTx, err, nonce, txHash, mutexWaitMs)
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

	// A mined receipt means the tx at this nonce is no longer in the pool.
	// Clear the stuck tracker so a subsequent rejection at a different nonce
	// starts from a clean slate rather than an inherited state.
	s.stuckNonceTracker().Clear()

	if s.logger != nil {
		s.logger.Info("blob tx mined",
			"stage", "submit_blob",
			"txHash", txHash.Hex(),
			"blockNumber", receipt.BlockNumber.Uint64(),
			"waitMs", time.Since(waitStart).Milliseconds(),
		)
	}

	return [][32]byte{[32]byte(payload.versionedHash)}, nil
}

func (s *BlobTxSubmitter) handleSendError(
	ctx context.Context,
	txBackend BlobTxBackend,
	signTx func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error),
	err error,
	nonce uint64,
	txHash common.Hash,
	mutexWaitMs int64,
) {
	resetNonce := workerchain.ShouldResetNonceOnSendError(err)
	if resetNonce {
		s.nonceMgr.ResetNonce()
	}

	stuckHits := 0
	stuck := workerchain.IsAlreadyReservedError(err)
	if stuck {
		stuckHits = s.stuckNonceTracker().Record(nonce)
	}

	if s.logger != nil {
		s.logger.Warn("blob tx rejected by SendTransaction",
			"stage", "submit_blob",
			"txHash", txHash.Hex(),
			"nonce", nonce,
			"mutexWaitMs", mutexWaitMs,
			"nonceReset", resetNonce,
			"stuckHits", stuckHits,
			"error", err,
		)
	}

	if stuck {
		s.maybeSubmitReplacementBlobTx(ctx, txBackend, signTx, nonce, stuckHits)
	}
}

func (s *BlobTxSubmitter) maybeSubmitReplacementBlobTx(
	ctx context.Context,
	txBackend BlobTxBackend,
	signTx func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error),
	nonce uint64,
	stuckHits int,
) {
	if s.stuckCfg.Threshold <= 0 || stuckHits < s.stuckCfg.Threshold {
		return
	}
	if !s.stuckCfg.AutoReplace {
		if s.logger != nil {
			s.logger.Error("stuck-nonce threshold reached; auto-replace disabled, manual intervention required",
				"stage", "submit_blob",
				"nonce", nonce,
				"consecutiveHits", stuckHits,
				"threshold", s.stuckCfg.Threshold,
			)
		}
		return
	}

	tracker := s.stuckNonceTracker()
	bumpsUsed := tracker.BumpAttemptsUsed()
	if bumpsUsed >= s.stuckCfg.MaxBumps {
		if s.logger != nil {
			s.logger.Error("stuck-nonce replacement bumps exhausted; manual intervention required",
				"stage", "submit_blob",
				"nonce", nonce,
				"bumpsUsed", bumpsUsed,
				"maxBumps", s.stuckCfg.MaxBumps,
			)
		}
		return
	}

	bumpNumber := bumpsUsed + 1
	if err := s.submitReplacementBlobTx(ctx, txBackend, signTx, nonce, bumpNumber); err != nil {
		if s.logger != nil {
			s.logger.Error("stuck-nonce replacement broadcast failed",
				"stage", "submit_blob",
				"nonce", nonce,
				"bumpNumber", bumpNumber,
				"error", err,
			)
		}
		return
	}

	if s.logger != nil {
		s.logger.Warn("stuck-nonce replacement broadcast succeeded",
			"stage", "submit_blob",
			"nonce", nonce,
			"bumpNumber", bumpNumber,
			"maxBumps", s.stuckCfg.MaxBumps,
			"hint", "next asynq retry should observe the replacement result",
		)
	}
}

// submitReplacementBlobTx broadcasts a bumped-fee blob tx at the given
// nonce to evict a stuck mempool entry. Called from within SubmitBlobTx's
// error branch while the shared broadcast serializer is still held, so no
// additional Acquire is needed here.
//
// Fee bumps are 2^bumpNumber × the configured baseline. First replacement
// (bumpNumber=1) is 2× everything, second (bumpNumber=2) is 4×, etc. Cap
// is enforced by the caller via stuckCfg.MaxBumps — each broadcast
// attempt here increments the tracker's bump counter.
//
// Go-ethereum blobpool replacement rules: same sender + same nonce + ALL
// three fee fields bumped by at least the configured minimum (100% by
// default). A non-blob tx cannot replace a blob tx at the same nonce, so
// we deliberately reuse the blob path with a 1-byte placeholder payload.
//
// WaitMined is NOT called on the replacement — we return success/failure
// of the broadcast itself and let the next asynq retry observe the
// resulting state (either mined, still stuck, or different nonce). This
// keeps the original stuck job's task deadline from being spent twice
// inside the same invocation.
func (s *BlobTxSubmitter) submitReplacementBlobTx(
	ctx context.Context,
	txBackend BlobTxBackend,
	signTx func(tx *types.Transaction, signer types.Signer, key *ecdsa.PrivateKey) (*types.Transaction, error),
	nonce uint64,
	bumpNumber int,
) error {
	if bumpNumber < 1 {
		return fmt.Errorf("bumpNumber must be >= 1, got %d", bumpNumber)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("replacement context done before build: %w", err)
	}

	payload, err := encodeBlobTxPayload(replacementBlobPayload, "replacement blob payload")
	if err != nil {
		return err
	}

	tipSuggested, err := txBackend.SuggestGasTipCap(ctx)
	if err != nil {
		return fmt.Errorf("suggest replacement gas tip: %w", err)
	}

	// Compute bumped fees. multiplier = 2^bumpNumber.
	multiplier := big.NewInt(1)
	multiplier.Lsh(multiplier, uint(bumpNumber))

	bumpedTip := new(big.Int).Mul(tipSuggested, multiplier)
	bumpedGasFeeCap := new(big.Int).Mul(s.maxGasPrice, multiplier)
	// Baseline blob fee cap in the normal path is 1 gwei (hardcoded at
	// tx build). Bump from there for replacement.
	baselineBlobFeeCap := big.NewInt(defaultBlobFeeCapWei)
	bumpedBlobFeeCap := new(big.Int).Mul(baselineBlobFeeCap, multiplier)

	signedTx, err := s.signBlobTx(payload, nonce, bumpedTip, bumpedGasFeeCap, bumpedBlobFeeCap, signTx)
	if err != nil {
		return fmt.Errorf("sign replacement blob tx: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("replacement context done before broadcast: %w", err)
	}

	if s.logger != nil {
		s.logger.Warn("stuck-nonce replacement: broadcasting bumped blob tx",
			"stage", "submit_blob_replacement",
			"nonce", nonce,
			"bumpNumber", bumpNumber,
			"multiplier", multiplier.String(),
			"gasTipCap", bumpedTip.String(),
			"gasFeeCap", bumpedGasFeeCap.String(),
			"blobFeeCap", bumpedBlobFeeCap.String(),
			"txHash", signedTx.Hash().Hex(),
		)
	}

	// Increment BEFORE broadcast: even a failed send should count against
	// the max-bumps budget so a misconfigured EL doesn't let us bump
	// forever without progress.
	s.stuckNonceTracker().IncrementBumpAttempts()

	if err := txBackend.SendTransaction(ctx, signedTx); err != nil {
		return fmt.Errorf("broadcast replacement blob tx: %w", err)
	}

	return nil
}

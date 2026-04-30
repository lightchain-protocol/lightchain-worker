package chain

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ErrContractPaused is returned by submitPreparedTx when a transaction
// reverted because the JobRegistry (or any other Pausable contract we
// call) is paused. Callers should detect it with errors.Is so the
// scheduler can apply cycle-level backoff instead of treating the
// revert as a generic per-job failure.
//
// We classify pause separately because OpenZeppelin Pausable v5 uses
// the parameter-less custom error EnforcedPause(); go-ethereum returns
// only the 4-byte selector in eth_call revert data, which would be
// indistinguishable from any other custom revert if we matched on the
// generic "execution reverted" string.
var ErrContractPaused = errors.New("contract paused")

// enforcedPauseSelector is the first 4 bytes of keccak256("EnforcedPause()"),
// the function selector OpenZeppelin Pausable v5 emits on a paused revert
// (= 0xd93c0665). Computed at init from the canonical signature so the
// source documents itself rather than relying on a magic constant.
var enforcedPauseSelector = crypto.Keccak256([]byte("EnforcedPause()"))[:4]

// contractCaller is the narrow seam decodePauseRevert needs from
// ethclient.Client. Defined locally so the decoder is unit-testable
// without a live RPC.
type contractCaller interface {
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
}

// decodePauseRevert is a best-effort classifier: it replays a reverted
// transaction via eth_call and returns true only when the revert data
// starts with the EnforcedPause() selector. Any RPC failure, missing
// revert data, or selector mismatch returns false so a transient node
// problem never escalates a generic revert into a "paused" diagnosis.
//
// blockNumber selects the state used for replay; pass receipt.BlockNumber.
// Replay runs against end-of-block state rather than exact pre-tx state,
// which is fine for the pause flag (it persists for the rest of the
// block and beyond) but would not be appropriate for state-dependent
// classifications.
func decodePauseRevert(
	ctx context.Context,
	caller contractCaller,
	from common.Address,
	tx *types.Transaction,
	blockNumber *big.Int,
) bool {
	if caller == nil || tx == nil || tx.To() == nil {
		// Contract creation (tx.To == nil) cannot revert with a custom
		// error from a contract — there is no contract yet. The
		// JobRegistry calls we care about always have a To, so this
		// guards a path we do not otherwise expect to hit.
		return false
	}

	msg := ethereum.CallMsg{
		From:  from,
		To:    tx.To(),
		Value: tx.Value(),
		Data:  tx.Data(),
		// Gas/GasPrice intentionally left zero: eth_call uses the
		// node's default gas cap and ignores fee fields.
	}

	_, callErr := caller.CallContract(ctx, msg, blockNumber)
	if callErr == nil {
		// Replay succeeded — the original revert was likely caused by
		// transient state (e.g. nonce ordering) rather than a
		// reproducible contract condition. Not pause.
		return false
	}

	revertData, ok := ethclient.RevertErrorData(callErr)
	if !ok || len(revertData) < 4 {
		// No revert data exposed (non-Geth node, or generic revert
		// without ABI-encoded payload). We cannot classify; treat
		// as not-paused so per-job fallback handles the failure.
		return false
	}
	return bytes.Equal(revertData[:4], enforcedPauseSelector)
}

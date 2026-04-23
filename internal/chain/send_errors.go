package chain

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
)

const (
	rpcCodeInvalidParams        = -32602
	rpcCodeNonceTooLow          = -38010
	rpcCodeNonceTooHigh         = -38011
	rpcCodeIntrinsicGas         = -38013
	rpcCodeInsufficientFunds    = -38014
	rpcCodeBlockGasLimitReached = -38015
	rpcCodeSenderNotEOA         = -38024
	rpcCodeMaxInitCodeSize      = -38025
)

// alreadyReservedSubstr matches go-ethereum's txpool.ErrAlreadyReserved, raised
// when a sender already has a blob (type-3) tx pending while submitting another
// blob tx (or a non-blob tx against a pending blob, and vice versa). The
// sentinel error is plain (errors.New) and its string is stable across
// go-ethereum releases, so substring matching survives RPC wrapping.
//
// nonceTooHighSubstr and nonceTooLowSubstr match go-ethereum's core txpool
// nonce-desync rejections. They arrive from nodes that surface plain (non
// -RPC-coded) errors under specific txpool paths — notably when the local
// NonceManager counter drifts from the chain's pending nonce after a
// WaitMined timeout leaves a tx stuck in the mempool. Both are emitted as
// strings like "nonce too high: tx nonce 126, gapped nonce 122" or "nonce
// too low: next nonce 126, tx nonce 124", so a directional substring match
// is safe (and deliberately narrower than a bare "nonce" match, which would
// swallow unrelated errors).
const (
	alreadyReservedSubstr = "address already reserved"
	nonceTooHighSubstr    = "nonce too high"
	nonceTooLowSubstr     = "nonce too low"
)

// isNonceDesyncError reports whether err is any of the three known local/chain
// nonce-desync conditions that should trigger a NonceManager reset so the next
// NextNonce() refetches the chain's pending nonce.
func isNonceDesyncError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, alreadyReservedSubstr) ||
		strings.Contains(msg, nonceTooHighSubstr) ||
		strings.Contains(msg, nonceTooLowSubstr)
}

// ShouldResetNonceOnSendError reports whether a SendTransaction failure was a
// definite pre-broadcast rejection and the locally reserved nonce should be discarded.
func ShouldResetNonceOnSendError(err error) bool {
	if isNonceDesyncError(err) {
		return true
	}

	var rpcErr rpc.Error
	if !errors.As(err, &rpcErr) {
		return false
	}

	switch rpcErr.ErrorCode() {
	case rpcCodeInvalidParams,
		rpcCodeNonceTooLow,
		rpcCodeNonceTooHigh,
		rpcCodeIntrinsicGas,
		rpcCodeInsufficientFunds,
		rpcCodeBlockGasLimitReached,
		rpcCodeSenderNotEOA,
		rpcCodeMaxInitCodeSize:
		return true
	default:
		return false
	}
}

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
const alreadyReservedSubstr = "address already reserved"

// isAlreadyReserved reports whether err is (or wraps, or contains via RPC
// message) go-ethereum's txpool reservation error.
func isAlreadyReserved(err error) bool {
	return err != nil && strings.Contains(err.Error(), alreadyReservedSubstr)
}

// ShouldResetNonceOnSendError reports whether a SendTransaction failure was a
// definite pre-broadcast rejection and the locally reserved nonce should be discarded.
func ShouldResetNonceOnSendError(err error) bool {
	if isAlreadyReserved(err) {
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

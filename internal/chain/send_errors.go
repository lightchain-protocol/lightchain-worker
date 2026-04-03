package chain

import (
	"errors"

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

// ShouldResetNonceOnSendError reports whether a SendTransaction failure was a
// definite pre-broadcast rejection and the locally reserved nonce should be discarded.
func ShouldResetNonceOnSendError(err error) bool {
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

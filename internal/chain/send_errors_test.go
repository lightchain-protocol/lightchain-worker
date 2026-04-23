package chain

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldResetNonceOnSendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},

		// Existing RPC-coded rejection paths (pre-broadcast).
		{name: "invalid params", err: mockRPCError{code: rpcCodeInvalidParams}, want: true},
		{name: "nonce too low", err: mockRPCError{code: rpcCodeNonceTooLow}, want: true},
		{name: "nonce too high", err: mockRPCError{code: rpcCodeNonceTooHigh}, want: true},
		{name: "intrinsic gas", err: mockRPCError{code: rpcCodeIntrinsicGas}, want: true},
		{name: "insufficient funds", err: mockRPCError{code: rpcCodeInsufficientFunds}, want: true},
		{name: "block gas limit", err: mockRPCError{code: rpcCodeBlockGasLimitReached}, want: true},
		{name: "sender not EOA", err: mockRPCError{code: rpcCodeSenderNotEOA}, want: true},
		{name: "max init code size", err: mockRPCError{code: rpcCodeMaxInitCodeSize}, want: true},

		// Regression guard: unrecognized RPC codes must not trigger a reset.
		{name: "unrecognized rpc code", err: mockRPCError{code: -32000}, want: false},

		// Reservation error variants (go-ethereum txpool.ErrAlreadyReserved).
		{
			name: "plain reservation error",
			err:  errors.New("address already reserved"),
			want: true,
		},
		{
			name: "wrapped reservation error",
			err:  fmt.Errorf("eth_sendRawTransaction: %w", errors.New("address already reserved")),
			want: true,
		},
		{
			name: "rpc-wrapped reservation error",
			err:  mockRPCError{code: -32000, msg: "eth_sendRawTransaction: address already reserved"},
			want: true,
		},

		// Unrelated transport error must not trigger a reset.
		{name: "unrelated network error", err: errors.New("connection refused"), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ShouldResetNonceOnSendError(tc.err))
		})
	}
}

// mockRPCError (declared in client_test.go) implements go-ethereum's rpc.Error
// interface. Its optional msg field is used here to exercise the substring
// match on RPC-wrapped reservation errors.

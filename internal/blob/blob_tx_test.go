package blob

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobTxSubmitter_ResetNonceOnSignFailure(t *testing.T) {
	t.Parallel()

	nonceMgr := &mockNonceManager{next: 7}
	submitter := &BlobTxSubmitter{
		txBackend:  mockBlobTxBackend{gasTipCap: big.NewInt(1)},
		signingKey: testSigningKey(t),
		signTx: func(*types.Transaction, types.Signer, *ecdsa.PrivateKey) (*types.Transaction, error) {
			return nil, assert.AnError
		},
		chainID:     big.NewInt(1),
		nonceMgr:    nonceMgr,
		maxGasPrice: big.NewInt(2),
	}

	_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sign blob tx")
	assert.Equal(t, 1, nonceMgr.resetCalls)
}

func TestBlobTxSubmitter_ResetNonceOnPreBroadcastSendFailure(t *testing.T) {
	t.Parallel()

	nonceMgr := &mockNonceManager{next: 7}
	submitter := &BlobTxSubmitter{
		txBackend:   mockBlobTxBackend{gasTipCap: big.NewInt(1), sendErr: mockRPCError{code: -38014}},
		signingKey:  testSigningKey(t),
		chainID:     big.NewInt(1),
		nonceMgr:    nonceMgr,
		maxGasPrice: big.NewInt(2),
	}

	_, err := submitter.SubmitBlobTx(context.Background(), []byte("ciphertext"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send blob tx")
	assert.Equal(t, 1, nonceMgr.resetCalls)
}

// Compile-time assertion
var _ BlobSubmitter = (*BlobTxSubmitter)(nil)

type mockNonceManager struct {
	next       uint64
	resetCalls int
}

func (m *mockNonceManager) NextNonce(context.Context) (uint64, error) {
	return m.next, nil
}

func (m *mockNonceManager) ResetNonce() {
	m.resetCalls++
}

type mockBlobTxBackend struct {
	gasTipCap *big.Int
	sendErr   error
}

func (m mockBlobTxBackend) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return m.gasTipCap, nil
}

func (m mockBlobTxBackend) SendTransaction(context.Context, *types.Transaction) error {
	return m.sendErr
}

func testSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return key
}

type mockRPCError struct {
	code int
}

func (e mockRPCError) Error() string {
	return "rpc error"
}

func (e mockRPCError) ErrorCode() int {
	return e.code
}

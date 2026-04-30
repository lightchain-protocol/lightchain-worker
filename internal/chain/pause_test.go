package chain

import (
	"context"
	"errors"
	"math/big"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rpcCodeDataError implements both rpc.Error and rpc.DataError, the
// shape ethclient.RevertErrorData requires (errorCode == 3 + ErrorData
// returning a hex string). Used to simulate the error CallContract
// returns for a real reverted call against a Geth node.
type rpcCodeDataError struct {
	msg  string
	code int
	data string
}

func (e *rpcCodeDataError) Error() string         { return e.msg }
func (e *rpcCodeDataError) ErrorCode() int        { return e.code }
func (e *rpcCodeDataError) ErrorData() interface{} { return e.data }

// fakeCaller records the CallMsg passed to CallContract so we can
// assert the decoder builds the replay correctly (From, To, Value,
// Data must mirror the original tx).
type fakeCaller struct {
	gotMsg     ethereum.CallMsg
	gotBlock   *big.Int
	returnErr  error
	returnData []byte
	calls      int
}

func (f *fakeCaller) CallContract(_ context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	f.calls++
	f.gotMsg = msg
	f.gotBlock = blockNumber
	return f.returnData, f.returnErr
}

func newReleaseTx(t *testing.T) *types.Transaction {
	t.Helper()
	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	return types.NewTx(&types.LegacyTx{
		Nonce:    7,
		GasPrice: big.NewInt(1_000_000_000),
		Gas:      200_000,
		To:       &to,
		Value:    big.NewInt(0),
		Data:     []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02},
	})
}

func TestDecodePauseRevert_MatchesEnforcedPauseSelector(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x2222222222222222222222222222222222222222")

	caller := &fakeCaller{
		returnErr: &rpcCodeDataError{
			msg:  "execution reverted",
			code: 3,
			// "0x" + hex(EnforcedPause selector). Any trailing bytes are
			// allowed (the v5 error has none, but the matcher only
			// inspects the first 4 bytes).
			data: "0xd93c0665",
		},
	}

	assert.True(t, decodePauseRevert(context.Background(), caller, from, tx, big.NewInt(100)),
		"selector 0xd93c0665 must classify as paused")
}

func TestDecodePauseRevert_RejectsOtherRevertSelectors(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x2222222222222222222222222222222222222222")

	caller := &fakeCaller{
		returnErr: &rpcCodeDataError{
			msg:  "execution reverted",
			code: 3,
			// Some unrelated custom error (e.g. JobNotReleasable or
			// ZeroEscrow). Decoder must NOT misclassify these as pause;
			// they should fall through to per-job fallback.
			data: "0xdeadbeef",
		},
	}

	assert.False(t, decodePauseRevert(context.Background(), caller, from, tx, big.NewInt(100)),
		"unrelated custom-error selector must not be classified as paused")
}

func TestDecodePauseRevert_HandlesPlainExecutionReverted(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// CallContract surfaces only "execution reverted" — no rpc.DataError
	// payload. ethclient.RevertErrorData returns ok=false, so we cannot
	// classify and must return false.
	caller := &fakeCaller{
		returnErr: errors.New("execution reverted"),
	}

	assert.False(t, decodePauseRevert(context.Background(), caller, from, tx, big.NewInt(100)),
		"errors without ABI-encoded revert data must NOT be classified as paused")
}

func TestDecodePauseRevert_BestEffortOnRPCFailure(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Network-level failure during replay. Pause classification is a
	// best-effort signal; we degrade to per-job fallback rather than
	// escalate a transient RPC issue into a "paused" diagnosis.
	caller := &fakeCaller{
		returnErr: errors.New("connection refused"),
	}

	assert.False(t, decodePauseRevert(context.Background(), caller, from, tx, big.NewInt(100)),
		"transient RPC errors must NOT be classified as paused")
}

func TestDecodePauseRevert_UsesOriginalTxCallFields(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x3333333333333333333333333333333333333333")
	block := big.NewInt(42)

	caller := &fakeCaller{
		returnErr: &rpcCodeDataError{
			msg:  "execution reverted",
			code: 3,
			data: "0xd93c0665",
		},
	}

	require.True(t, decodePauseRevert(context.Background(), caller, from, tx, block))
	require.Equal(t, 1, caller.calls)

	// Replay must use the original tx's destination, calldata, and
	// value — otherwise the contract may take a different code path
	// and the revert reason would not be reproducible.
	assert.Equal(t, from, caller.gotMsg.From, "From must be passed explicitly so msg.sender checks match")
	assert.Equal(t, tx.To(), caller.gotMsg.To, "To must mirror the original tx")
	assert.Equal(t, tx.Value(), caller.gotMsg.Value, "Value must mirror the original tx")
	assert.Equal(t, tx.Data(), caller.gotMsg.Data, "Calldata must mirror the original tx")
	assert.Equal(t, block, caller.gotBlock, "block number must be the receipt's block")
	assert.Equal(t, uint64(0), caller.gotMsg.Gas, "Gas left zero so eth_call uses node default cap")
}

func TestDecodePauseRevert_NilTxReturnsFalse(t *testing.T) {
	t.Parallel()
	from := common.HexToAddress("0x4444444444444444444444444444444444444444")
	caller := &fakeCaller{}
	assert.False(t, decodePauseRevert(context.Background(), caller, from, nil, big.NewInt(1)))
	assert.Equal(t, 0, caller.calls, "nil tx must not trigger a CallContract")
}

func TestDecodePauseRevert_NilToReturnsFalse(t *testing.T) {
	t.Parallel()
	from := common.HexToAddress("0x4444444444444444444444444444444444444444")
	// Contract-creation tx (nil To). Should not be classified as a
	// pause revert from a contract — there is no callee contract yet.
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    1,
		GasPrice: big.NewInt(1),
		Gas:      100_000,
		Data:     []byte{0x60, 0x80},
	})
	caller := &fakeCaller{}
	assert.False(t, decodePauseRevert(context.Background(), caller, from, tx, big.NewInt(1)))
	assert.Equal(t, 0, caller.calls, "contract-creation tx must not trigger a CallContract")
}

func TestDecodePauseRevert_NilCallerReturnsFalse(t *testing.T) {
	t.Parallel()
	tx := newReleaseTx(t)
	from := common.HexToAddress("0x4444444444444444444444444444444444444444")
	assert.False(t, decodePauseRevert(context.Background(), nil, from, tx, big.NewInt(1)),
		"nil caller is a programmer error but must not panic")
}

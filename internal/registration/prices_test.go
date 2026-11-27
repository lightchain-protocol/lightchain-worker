package registration

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
)

type fakePriceClient struct {
	prices   map[[32]byte]*big.Int
	readErr  error
	sets     []string
	floor    *big.Int // nil = 0
	floorErr error
	fee      *big.Int // nil = 1e18
}

func (f *fakePriceClient) MinWorkerPrice(context.Context, [32]byte) (*big.Int, error) {
	if f.floorErr != nil {
		return nil, f.floorErr
	}
	if f.floor == nil {
		return big.NewInt(0), nil
	}
	return f.floor, nil
}

func (f *fakePriceClient) ModelFee(context.Context, [32]byte) (*big.Int, error) {
	if f.fee == nil {
		return new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil), nil
	}
	return f.fee, nil
}

func (f *fakePriceClient) GetWorkerPrice(_ context.Context, _ common.Address, id [32]byte) (*big.Int, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	if p, ok := f.prices[id]; ok {
		return p, nil
	}
	return big.NewInt(0), nil
}

func (f *fakePriceClient) SetWorkerPrice(_ context.Context, id [32]byte, price *big.Int) error {
	f.prices[id] = price
	f.sets = append(f.sets, common.Hash(id).Hex()[:10]+"="+price.String())
	return nil
}

func TestEnsurePrices_SetsOnlyChangedPrices(t *testing.T) {
	gpt := crypto.Keccak256Hash([]byte("gpt-oss:20b"))
	qwen := crypto.Keccak256Hash([]byte("qwen3-vl:8b"))
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{gpt: big.NewInt(5), qwen: big.NewInt(7)}}

	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{
		"gpt-oss:20b": big.NewInt(5), // already on chain
		"qwen3-vl:8b": big.NewInt(0), // cleared back to the governance fee
	}, newLogger())

	assert.Equal(t, []string{qwen.Hex()[:10] + "=0"}, f.sets)
}

func TestEnsurePrices_ReadFailureSendsNothing(t *testing.T) {
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{}, readErr: errors.New("rpc down")}
	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{"gpt-oss:20b": big.NewInt(1)}, newLogger())
	assert.Empty(t, f.sets)
}

// deadlineClient records each SetWorkerPrice call's deadline.
type deadlineClient struct {
	fakePriceClient
	deadlines []time.Time
}

func (d *deadlineClient) SetWorkerPrice(ctx context.Context, id [32]byte, price *big.Int) error {
	dl, _ := ctx.Deadline()
	d.deadlines = append(d.deadlines, dl)
	return d.fakePriceClient.SetWorkerPrice(ctx, id, price)
}

func TestEnsurePrices_EachModelGetsItsOwnDeadline(t *testing.T) {
	d := &deadlineClient{fakePriceClient: fakePriceClient{prices: map[[32]byte]*big.Int{}}}
	start := time.Now()
	EnsurePrices(context.Background(), d, testAddr, map[string]*big.Int{
		"a": big.NewInt(1), "b": big.NewInt(2),
	}, newLogger())
	assert.Len(t, d.deadlines, 2)
	for _, dl := range d.deadlines {
		assert.WithinDuration(t, start.Add(priceTimeout), dl, 5*time.Second)
	}
}

func TestEnsurePrices_BelowTheFloorIsNotSent(t *testing.T) {
	gpt := crypto.Keccak256Hash([]byte("gpt-oss:20b"))
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{}, floor: big.NewInt(500)}
	logs := &bytes.Buffer{}
	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{
		"gpt-oss:20b": big.NewInt(499),
		"qwen3-vl:8b": big.NewInt(500), // the floor itself is fine
	}, slog.New(slog.NewTextHandler(logs, nil)))

	assert.Equal(t, []string{crypto.Keccak256Hash([]byte("qwen3-vl:8b")).Hex()[:10] + "=500"}, f.sets)
	assert.Nil(t, f.prices[gpt])
	assert.Contains(t, logs.String(), "below the on-chain floor")
	assert.Contains(t, logs.String(), "minWei=500")
}

func TestEnsurePrices_ClearingNeedsNoFloorRead(t *testing.T) {
	gpt := crypto.Keccak256Hash([]byte("gpt-oss:20b"))
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{gpt: big.NewInt(9)}, floorErr: errors.New("must not be read")}
	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{"gpt-oss:20b": big.NewInt(0)}, newLogger())
	assert.Equal(t, []string{gpt.Hex()[:10] + "=0"}, f.sets)
}

func TestEnsurePrices_FloorReadFailureLetsTheTxDecide(t *testing.T) {
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{}, floorErr: errors.New("rpc down")}
	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{"gpt-oss:20b": big.NewInt(7)}, newLogger())
	assert.Len(t, f.sets, 1)
}

func TestEnsurePrices_WarnsAboveTheModelFee(t *testing.T) {
	f := &fakePriceClient{prices: map[[32]byte]*big.Int{}, fee: big.NewInt(100)}
	logs := &bytes.Buffer{}
	EnsurePrices(context.Background(), f, testAddr, map[string]*big.Int{"gpt-oss:20b": big.NewInt(101)},
		slog.New(slog.NewTextHandler(logs, nil)))
	assert.Len(t, f.sets, 1, "still declared: requests with a higher maxFee can take it")
	assert.Contains(t, logs.String(), "above the model's fee")
}

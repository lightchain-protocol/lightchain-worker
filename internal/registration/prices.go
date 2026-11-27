package registration

import (
	"context"
	"log/slog"
	"math/big"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// priceTimeout bounds one model's declaration: the read, and the tx with its
// wait until mined. Each model gets its own, so a slow block cannot use up
// the time of the models after it.
const priceTimeout = 30 * time.Second

// PriceClient is the chain surface EnsurePrices needs (SessionManager).
type PriceClient interface {
	GetWorkerPrice(ctx context.Context, worker common.Address, modelID [32]byte) (*big.Int, error)
	SetWorkerPrice(ctx context.Context, modelID [32]byte, price *big.Int) error
	MinWorkerPrice(ctx context.Context, modelID [32]byte) (*big.Int, error)
	ModelFee(ctx context.Context, modelID [32]byte) (*big.Int, error)
}

// EnsurePrices declares WORKER_PRICES on chain: for every configured model
// whose on-chain price differs, one setWorkerPrice tx. A model left out of
// prices is not touched, so a price set by hand (cast send) survives a
// restart; listing it as 0 clears it back to the governance fee.
//
// Best-effort like EnsureCapabilities: a failure is logged and the worker
// comes up anyway. It then claims at whatever price the chain holds, and
// SessionManager enforces the consumer's cap against that price at claim.
func EnsurePrices(ctx context.Context, client PriceClient, worker common.Address, prices map[string]*big.Int, logger *slog.Logger) {
	names := make([]string, 0, len(prices))
	for name := range prices {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic tx order
	for _, name := range names {
		ensurePrice(ctx, client, worker, name, prices[name], logger)
	}
}

func ensurePrice(ctx context.Context, client PriceClient, worker common.Address, name string, want *big.Int, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, priceTimeout)
	defer cancel()
	modelID := crypto.Keccak256Hash([]byte(name))
	current, err := client.GetWorkerPrice(ctx, worker, modelID)
	if err != nil {
		logger.Warn("worker price read failed; skipping declaration", "model", name, "error", err)
		return
	}
	if want.Sign() != 0 {
		if !priceAllowed(ctx, client, name, modelID, want, logger) {
			return
		}
	}
	if current.Cmp(want) == 0 {
		return
	}
	if err := client.SetWorkerPrice(ctx, modelID, want); err != nil {
		// A tx broadcast before the deadline may still land; the next start
		// reads the price back and only resends if it did not.
		logger.Warn("setWorkerPrice failed or not confirmed", "model", name, "price", want.String(), "error", err)
		return
	}
	logger.Info("declared worker price on-chain", "model", name, "priceWei", want.String(), "previousWei", current.String())
}

// priceAllowed checks a price against the chain's floor before sending it
// (setWorkerPrice reverts below it), and warns about a price above the
// governance fee: plain requests are capped at that fee, so only requests
// whose maxFee allows more can be claimed. A read failure lets the tx decide.
func priceAllowed(ctx context.Context, client PriceClient, name string, modelID [32]byte, want *big.Int, logger *slog.Logger) bool {
	floor, err := client.MinWorkerPrice(ctx, modelID)
	if err != nil {
		logger.Warn("worker price floor read failed; sending the price anyway", "model", name, "error", err)
	} else if want.Cmp(floor) < 0 {
		logger.Error("WORKER_PRICES is below the on-chain floor; price not declared",
			"model", name, "priceWei", want.String(), "minWei", floor.String(),
			"hint", "raise it to at least minWei, or remove it to earn the model's fee")
		return false
	}
	fee, err := client.ModelFee(ctx, modelID)
	if err == nil && want.Cmp(fee) > 0 {
		logger.Warn("WORKER_PRICES is above the model's fee: plain requests are capped at the fee, so only requests with a higher maxFee can be claimed",
			"model", name, "priceWei", want.String(), "feeWei", fee.String())
	}
	return true
}

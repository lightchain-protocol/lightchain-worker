package cli

import (
	"context"
	"fmt"
	"math/big"

	"github.com/lightchain/worker/internal/release"
)

// weiPerEther is 10^18, used to render wei balances as ETH.
var weiPerEther = new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

// formatBalance renders a wei amount as both wei and ETH.
func formatBalance(wei *big.Int) string {
	if wei == nil {
		return "0 wei (0 ETH)"
	}
	asFloat := new(big.Float).SetInt(wei)
	asFloat.Quo(asFloat, weiPerEther)
	return fmt.Sprintf("%s wei (%s ETH)", wei.String(), asFloat.Text('f', 9))
}

// Balance prints the worker's withdrawable on-chain balance.
func (h *Handler) Balance(ctx context.Context) error {
	if h.Settlement == nil {
		return fmt.Errorf("Balance: Settlement client not configured")
	}
	bal, err := h.Settlement.WorkerBalance(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read worker balance: %w", err)
	}
	fmt.Fprintf(h.Out, "address: %s\nbalance: %s\n", h.WorkerAddr.Hex(), formatBalance(bal))
	return nil
}

// Withdraw drains the worker's accumulated balance to the worker address.
//
// We display the pre-read balance as the withdrawn amount because
// submitPreparedTx does not surface the receipt; an optional post-read
// confirms the new balance for operators who want belt-and-braces
// confirmation.
func (h *Handler) Withdraw(ctx context.Context) error {
	if h.Settlement == nil {
		return fmt.Errorf("Withdraw: Settlement client not configured")
	}
	preBalance, err := h.Settlement.WorkerBalance(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read pre-withdraw balance: %w", err)
	}
	if preBalance == nil || preBalance.Sign() == 0 {
		fmt.Fprintf(h.Out, "no balance to withdraw for %s\n", h.WorkerAddr.Hex())
		return nil
	}

	if err := h.Settlement.Withdraw(ctx); err != nil {
		return fmt.Errorf("withdraw tx: %w", err)
	}

	fmt.Fprintf(h.Out, "withdraw submitted\n  address: %s\n  amount:  %s\n",
		h.WorkerAddr.Hex(), formatBalance(preBalance))

	// Optional post-read for confirmation. Failures are logged but not
	// propagated — the withdraw tx already succeeded.
	postBalance, postErr := h.Settlement.WorkerBalance(ctx, h.WorkerAddr)
	if postErr != nil {
		fmt.Fprintf(h.Out, "  post-balance read failed: %v\n", postErr)
	} else {
		fmt.Fprintf(h.Out, "  post-balance: %s\n", formatBalance(postBalance))
	}
	return nil
}

// Release runs the reconciler and (optionally) one release cycle. Used
// for operator-initiated catch-up or as the only settlement path when
// the sidecar's RELEASE_ENABLED is false. The store's flock serializes
// against a concurrently-running sidecar.
func (h *Handler) Release(ctx context.Context, reconcileOnly bool) error {
	if h.Settlement == nil {
		return fmt.Errorf("Release: Settlement client not configured")
	}
	if h.ReleaseStore == nil {
		return fmt.Errorf("Release: release store not configured")
	}

	reconciler := release.NewReconciler(h.ReleaseStore, h.Settlement, h.WorkerAddr, h.ReleaseConfig, h.Logger)
	if err := reconciler.Run(ctx); err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	fmt.Fprintln(h.Out, "reconciliation complete")

	if reconcileOnly {
		return nil
	}

	scheduler := release.NewScheduler(h.ReleaseStore, h.Settlement, h.WorkerAddr, h.ReleaseConfig, h.Logger)
	if err := scheduler.RunOnce(ctx); err != nil {
		return fmt.Errorf("release cycle: %w", err)
	}
	fmt.Fprintln(h.Out, "release cycle complete")

	pending, err := h.ReleaseStore.Pending(ctx)
	if err != nil {
		return fmt.Errorf("read pending after cycle: %w", err)
	}
	fmt.Fprintf(h.Out, "pending after cycle: %d job(s)\n", len(pending))
	return nil
}

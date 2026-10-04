package cli

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
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
	result, err := scheduler.RunOnce(ctx)
	if err != nil {
		return fmt.Errorf("release cycle: %w", err)
	}

	// Surface accurate status. Previously the CLI printed
	// "release cycle complete" unconditionally even when the
	// scheduler swallowed batch reverts, paused-contract backoffs,
	// and per-job failures (CodeRabbit PR #23). The CycleResult
	// from RunOnce makes the real outcome visible to operators and
	// drives a non-zero exit when settlement did not fully succeed.
	switch {
	case result.BackoffActive:
		fmt.Fprintf(h.Out, "release cycle blocked: backoff active until chain time %d\n", result.NextAllowedAt)
	case result.Paused:
		fmt.Fprintf(h.Out, "release cycle paused: contract paused; backoff active until chain time %d\n", result.NextAllowedAt)
	case result.Candidates == 0:
		fmt.Fprintln(h.Out, "release cycle complete: no jobs ready for release")
	case result.BatchReverted && result.Failed == 0:
		fmt.Fprintf(h.Out, "release cycle complete: batch reverted, %d job(s) recovered via per-job fallback (%d dropped)\n",
			result.Released, result.Dropped)
	case result.Failed > 0:
		fmt.Fprintf(h.Out, "release cycle completed with failures: %d released, %d failed, %d dropped\n",
			result.Released, result.Failed, result.Dropped)
	default:
		fmt.Fprintf(h.Out, "release cycle complete: %d released (%d dropped)\n", result.Released, result.Dropped)
	}

	pending, err := h.ReleaseStore.Pending(ctx)
	if err != nil {
		return fmt.Errorf("read pending after cycle: %w", err)
	}
	fmt.Fprintf(h.Out, "pending after cycle: %d job(s)\n", len(pending))

	// Non-zero exit on Paused or Failed > 0 so an operator scripting
	// `lightchain-worker release` (e.g. cron, smoke test) can detect
	// silent settlement failures instead of treating every invocation
	// as success.
	switch {
	case result.Paused:
		return fmt.Errorf("release cycle paused: contract paused; next attempt allowed at chain time %d", result.NextAllowedAt)
	case result.Failed > 0:
		return fmt.Errorf("release cycle: %d per-job release(s) failed", result.Failed)
	}
	return nil
}

// ReconcileSeed is what Register needs to start a new worker's release
// reconciler at the safe head it registered at instead of at the chain's
// first block.
type ReconcileSeed struct {
	Chain ReconcileSeedChain
	// ChainID and JobRegistry, with the worker address, are the identity of
	// the release state file at StatePath (RELEASE_STATE_PATH).
	ChainID     uint64
	JobRegistry common.Address
	StatePath   string
	// Confirmations is the reconciler's own depth behind the head
	// (RELEASE_RECONCILE_CONFIRMATIONS): the seed never passes its safe head.
	Confirmations uint64
}

// ReconcileSeedChain is the chain reads the seed is taken from.
type ReconcileSeedChain interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	NonceAt(ctx context.Context, addr common.Address, block uint64) (uint64, error)
}

// newWorkerSafeHead returns the reconciler's safe head when the worker is new
// to the chain: not registered, and its key had sent no transaction by that
// block. Otherwise it returns 0. It is read before the registration is sent.
// Such a worker has no completed job at or before that block: registerWorker
// and completeJob both act for their sender only, so every JobCompleted event
// of a worker follows a transaction from its key. A key that was used before
// gets 0, and its reconciler scans from the start.
func (h *Handler) newWorkerSafeHead(ctx context.Context) (uint64, error) {
	s := h.ReconcileSeed
	if s == nil {
		return 0, nil
	}
	registered, err := h.Client.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil || registered {
		return 0, err
	}
	head, err := s.Chain.Head(ctx)
	if err != nil || head.Number < s.Confirmations {
		return 0, err
	}
	safeHead := head.Number - s.Confirmations
	nonce, err := s.Chain.NonceAt(ctx, h.WorkerAddr, safeHead)
	if err != nil || nonce != 0 {
		return 0, err
	}
	return safeHead, nil
}

// seedReconcileBlock stores block (from newWorkerSafeHead, 0 for none) as the
// release reconcile cursor once the registration has gone through. It never
// fails the registration: when the state cannot be written the worker is left
// as it was, scanning from the start.
func (h *Handler) seedReconcileBlock(ctx context.Context, block uint64, err error) {
	if err == nil && block != 0 {
		err = h.storeReconcileBlock(ctx, block)
	}
	if err != nil {
		fmt.Fprintf(h.Out, "note: the release state was not prepared (%v) — registration is not affected; "+
			"the worker's first start looks for its completed jobs from the chain's first block, as before\n", err)
	}
}

func (h *Handler) storeReconcileBlock(ctx context.Context, block uint64) error {
	s := h.ReconcileSeed
	store, err := release.NewFileStore(s.StatePath, release.StoreIdentity{
		ChainID:       s.ChainID,
		JobRegistry:   s.JobRegistry,
		WorkerAddress: h.WorkerAddr,
	}, h.Logger)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// A stored cursor is the reconciler's own progress and is never replaced.
	if stored, err := store.GetReconcileBlock(ctx); err != nil || stored != 0 {
		return err
	}
	if err := store.SetReconcileBlock(ctx, block); err != nil {
		return err
	}
	h.Logger.Info("release reconciler starts after this block: the worker key had sent no transaction by it", "block", block)
	return nil
}

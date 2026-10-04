// Package cli provides testable command handlers for the worker operator CLI.
// Each handler method returns an error instead of calling os.Exit, enabling
// unit tests with dependency injection.
package cli

import (
	"bufio"
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/registration"
	"github.com/lightchain/worker/internal/release"
)

// ECDHKeyLoader loads or generates an ECDH key pair from an encrypted keystore file.
type ECDHKeyLoader func(path, passphrase string) (*ecdh.PrivateKey, error)

// Handler encapsulates CLI command logic with injected dependencies.
// Each method returns an error instead of calling os.Exit, making commands testable.
type Handler struct {
	Client      chain.RegistrationClient
	WorkerAddr  common.Address
	ModelIDs    [][32]byte
	ModelNames  []string // parallel to ModelIDs — human-readable names for output
	ECDHKeyPath string
	ECDHPass    string
	WorkerStake *big.Int // nil = auto-query min stake
	LoadECDHKey ECDHKeyLoader
	Out         io.Writer // captures stdout in tests
	Logger      *slog.Logger

	// Settlement is required only by the balance/withdraw/release
	// subcommands. The same ChainClient that satisfies Client also
	// satisfies SettlementClient, but the field is kept separate so test
	// doubles can be smaller.
	Settlement chain.SettlementClient
	// ReleaseStore is required only by the `release` subcommand. Opened
	// with the same StoreIdentity the sidecar uses; the underlying flock
	// serializes against a concurrently-running sidecar.
	ReleaseStore release.Store
	// ReleaseConfig configures the on-demand release cycle and reconciler
	// run invoked by `worker-cli release`.
	ReleaseConfig release.Config
	// Reinstatement is required only by the `reinstate` and `top-up-stake`
	// subcommands.
	Reinstatement ReinstateChain
	// Yes and In are used only by `top-up-stake`: it asks on Out and reads
	// the answer from In before sending, unless Yes is set.
	Yes bool
	In  *bufio.Reader
}

// ReinstateChain is what `reinstate` and `top-up-stake` drive: the reads that
// tell whether the chain would accept them, then the transaction that lifts
// the suspension or adds the stake.
type ReinstateChain interface {
	IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error)
	IsWorkerSuspended(ctx context.Context, worker common.Address) (bool, error)
	Balance(ctx context.Context, addr common.Address) (*big.Int, error)
	GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error)
	GetMinWorkerStake(ctx context.Context) (*big.Int, error)
	Reinstate(ctx context.Context) error
	TopUpStake(ctx context.Context, amount *big.Int) error
}

// Keygen loads or generates the ECDH encryption key and prints the public key hex.
func (h *Handler) Keygen() error {
	ecdhKey, err := h.LoadECDHKey(h.ECDHKeyPath, h.ECDHPass)
	if err != nil {
		return fmt.Errorf("load/generate ECDH key: %w", err)
	}

	pubKeyHex := "0x" + hex.EncodeToString(ecdhKey.PublicKey().Bytes())
	fmt.Fprintf(h.Out, "ECDH Public Key: %s\n", pubKeyHex)
	fmt.Fprintf(h.Out, "Keystore file:   %s\n", h.ECDHKeyPath)
	return nil
}

// Register registers the worker on-chain with stake, encryption key, and models.
func (h *Handler) Register(ctx context.Context) error {
	if len(h.ModelIDs) == 0 {
		return fmt.Errorf("SUPPORTED_MODELS is required for registration")
	}

	ecdhKey, err := h.LoadECDHKey(h.ECDHKeyPath, h.ECDHPass)
	if err != nil {
		return fmt.Errorf("load/generate ECDH key: %w", err)
	}

	mgr := registration.NewManager(h.Client, h.WorkerAddr, h.ModelIDs, h.Logger)

	ecdhPubKeyBytes := ecdhKey.PublicKey().Bytes()
	if err := mgr.EnsureRegistered(ctx, ecdhPubKeyBytes, h.WorkerStake); err != nil {
		return fmt.Errorf("registration: %w", err)
	}

	stakeStr := "min (auto-queried)"
	if h.WorkerStake != nil {
		stakeStr = h.WorkerStake.String() + " wei"
	}
	fmt.Fprintf(h.Out, "Worker %s registered with stake %s, %d models added\n",
		h.WorkerAddr.Hex(), stakeStr, len(h.ModelIDs))
	return nil
}

// AddModels adds models to an already-registered worker.
func (h *Handler) AddModels(ctx context.Context) error {
	if len(h.ModelIDs) == 0 {
		return fmt.Errorf("SUPPORTED_MODELS is required for add-models")
	}
	if len(h.ModelNames) != len(h.ModelIDs) {
		return fmt.Errorf("ModelNames length %d does not match ModelIDs length %d", len(h.ModelNames), len(h.ModelIDs))
	}

	// One bad name must not strand the rest: a model the worker already
	// supports, or one that is not whitelisted, reverts with a custom
	// error that the RPC returns as a bare "execution reverted". Adding
	// the remaining models is still the right thing to do, so failures
	// are collected and reported together.
	var failed []string
	added := 0
	for i, modelID := range h.ModelIDs {
		if err := h.Client.AddSupportedModel(ctx, modelID); err != nil {
			h.Logger.Error("add model failed", "model", h.ModelNames[i], "index", i, "error", err)
			failed = append(failed, fmt.Sprintf("%q: %v", h.ModelNames[i], err))
			continue
		}
		added++
		h.Logger.Info("model added", "model", h.ModelNames[i])
	}

	fmt.Fprintf(h.Out, "Added %d of %d models for worker %s\n", added, len(h.ModelIDs), h.WorkerAddr.Hex())
	if len(failed) > 0 {
		return fmt.Errorf("%d model(s) not added (run `lightchain-worker preflight` for the per-model reason): %s",
			len(failed), strings.Join(failed, "; "))
	}
	return nil
}

// Deregister removes the worker from the registry and withdraws stake.
func (h *Handler) Deregister(ctx context.Context) error {
	mgr := registration.NewManager(h.Client, h.WorkerAddr, nil, h.Logger)

	if err := mgr.Deregister(ctx); err != nil {
		return fmt.Errorf("deregistration: %w", err)
	}

	fmt.Fprintf(h.Out, "Worker %s deregistered, stake withdrawn\n", h.WorkerAddr.Hex())
	return nil
}

// Reinstate lifts the worker's suspension. It reads the chain first and sends
// nothing unless the worker is registered, suspended and staked to the
// on-chain minimum.
func (h *Handler) Reinstate(ctx context.Context) error {
	registered, err := h.Reinstatement.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read registration: %w", err)
	}
	if !registered {
		return fmt.Errorf("worker %s is not registered — there is nothing to reinstate", h.WorkerAddr.Hex())
	}
	suspended, err := h.Reinstatement.IsWorkerSuspended(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read suspension: %w", err)
	}
	if !suspended {
		return fmt.Errorf("worker %s is not suspended — there is nothing to reinstate", h.WorkerAddr.Hex())
	}
	stake, err := h.Reinstatement.GetWorkerStake(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read stake: %w", err)
	}
	minStake, err := h.Reinstatement.GetMinWorkerStake(ctx)
	if err != nil {
		return fmt.Errorf("read the minimum stake: %w", err)
	}
	if stake.Cmp(minStake) < 0 {
		return fmt.Errorf("stake %s is below the on-chain minimum %s — top it up by at least %s (`lightchain-worker top-up-stake <amount>`) first; nothing was sent",
			lcai(stake), lcai(minStake), lcai(new(big.Int).Sub(minStake, stake)))
	}

	if err := h.Reinstatement.Reinstate(ctx); err != nil {
		// ponytail: the cooldown is not read before sending (this chain client
		// has no reader for its end), so a revert is explained instead. Read
		// getSuspendedUntil and refuse with the end time if this gets run early.
		if strings.Contains(err.Error(), "reverted") {
			return fmt.Errorf("%w — the chain rejects reinstate while the suspension cooldown is still running; "+
				"if that is the cause, run `lightchain-worker reinstate` again once it is over", err)
		}
		return err
	}
	fmt.Fprintf(h.Out, "Worker %s reinstated, suspension lifted\n", h.WorkerAddr.Hex())
	return nil
}

// TopUpStake adds amount (wei) to the worker's stake and prints the new stake
// against the on-chain minimum. Once every check has passed it asks before
// sending, unless Yes is set.
func (h *Handler) TopUpStake(ctx context.Context, amount *big.Int) error {
	registered, err := h.Reinstatement.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read registration: %w", err)
	}
	if !registered {
		return fmt.Errorf("worker %s is not registered — there is no stake to top up; `lightchain-worker register` stakes it", h.WorkerAddr.Hex())
	}
	bal, err := h.Reinstatement.Balance(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read balance: %w", err)
	}
	need := new(big.Int).Add(amount, lcaiToWei(gasBufferLCAI))
	if bal.Cmp(amount) <= 0 {
		return fmt.Errorf("worker %s holds %s; topping up %s needs gas on top — send at least %s to it first; nothing was sent",
			h.WorkerAddr.Hex(), lcai(bal), lcai(amount), lcai(new(big.Int).Sub(need, bal)))
	}
	stake, err := h.Reinstatement.GetWorkerStake(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read stake: %w", err)
	}
	minStake, err := h.Reinstatement.GetMinWorkerStake(ctx)
	if err != nil {
		return fmt.Errorf("read the minimum stake: %w", err)
	}
	suspended, err := h.Reinstatement.IsWorkerSuspended(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read suspension: %w", err)
	}
	// ponytail: the new stake is the stake read before sending plus the
	// amount, so every read that can fail does so before anything is sent.
	stake = new(big.Int).Add(stake, amount)
	against := fmt.Sprintf(" (minimum %s)", lcai(minStake))
	if stake.Cmp(minStake) < 0 {
		against = fmt.Sprintf(", still %s below the minimum %s", lcai(new(big.Int).Sub(minStake, stake)), lcai(minStake))
	}
	if bal.Cmp(need) < 0 {
		fmt.Fprintf(h.Out, "Note: this top-up leaves under %d LCAI for gas — send the worker more before it runs out\n", gasBufferLCAI)
	}
	if !h.Yes {
		asked := time.Now()
		if !confirm(h.Out, h.In, fmt.Sprintf("Add %s to the stake of worker %s? It will be %s%s. [y/N] ",
			lcai(amount), h.WorkerAddr.Hex(), lcai(stake), against)) {
			return fmt.Errorf("top-up not confirmed, nothing was sent — run it again when ready (or pass --yes)")
		}
		// The wait for the answer is not on the caller's clock: after a slow
		// yes the transaction would go out with no time left to see it mined,
		// and a failure reported for a top-up that landed invites a second one.
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(time.Since(asked)))
			defer cancel()
		}
	}

	if err := h.Reinstatement.TopUpStake(ctx, amount); err != nil {
		return err
	}
	fmt.Fprintf(h.Out, "Worker %s stake topped up by %s: now %s%s\n", h.WorkerAddr.Hex(), lcai(amount), lcai(stake), against)
	if suspended {
		fmt.Fprintln(h.Out, "The worker is still suspended: a top-up does not lift a suspension — "+
			"run `lightchain-worker reinstate` once the stake meets the minimum and the cooldown is over")
	}
	return nil
}

// Status checks the worker's on-chain registration status and key info.
func (h *Handler) Status(ctx context.Context) error {
	registered, err := h.Client.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("check registration status: %w", err)
	}

	if !registered {
		fmt.Fprintf(h.Out, "Worker %s: NOT REGISTERED\n", h.WorkerAddr.Hex())
		return nil
	}

	fmt.Fprintf(h.Out, "Worker %s: REGISTERED\n", h.WorkerAddr.Hex())

	onChainKey, err := h.Client.GetWorkerEncryptionKey(ctx, h.WorkerAddr)
	if err != nil {
		h.Logger.Warn("failed to fetch on-chain encryption key", "error", err)
		return nil
	}
	fmt.Fprintf(h.Out, "On-chain ECDH key: 0x%s\n", hex.EncodeToString(onChainKey))

	ecdhKey, err := h.LoadECDHKey(h.ECDHKeyPath, h.ECDHPass)
	if err != nil {
		fmt.Fprintln(h.Out, "Local ECDH key:    not found")
		fmt.Fprintln(h.Out, "Keys match:        unknown")
		return nil
	}

	localKeyHex := "0x" + hex.EncodeToString(ecdhKey.PublicKey().Bytes())
	fmt.Fprintf(h.Out, "Local ECDH key:    %s\n", localKeyHex)

	if hex.EncodeToString(onChainKey) == hex.EncodeToString(ecdhKey.PublicKey().Bytes()) {
		fmt.Fprintln(h.Out, "Keys match:        yes")
	} else {
		fmt.Fprintln(h.Out, "Keys match:        no")
	}
	return nil
}

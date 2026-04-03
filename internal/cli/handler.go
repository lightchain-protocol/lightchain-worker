// Package cli provides testable command handlers for the worker operator CLI.
// Each handler method returns an error instead of calling os.Exit, enabling
// unit tests with dependency injection.
package cli

import (
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/registration"
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

	for i, modelID := range h.ModelIDs {
		if err := h.Client.AddSupportedModel(ctx, modelID); err != nil {
			return fmt.Errorf("add model %q (index %d): %w", h.ModelNames[i], i, err)
		}
		h.Logger.Info("model added", "model", h.ModelNames[i])
	}

	fmt.Fprintf(h.Out, "Added %d models for worker %s\n", len(h.ModelIDs), h.WorkerAddr.Hex())
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

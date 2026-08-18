// Package registration manages the worker's on-chain registration lifecycle.
// EnsureRegistered is idempotent: it skips registration entirely if already registered.
package registration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// P256UncompressedPubKeyLength is the expected length of an uncompressed P-256
// public key — 1-byte 0x04 prefix followed by 32-byte X and 32-byte Y coords.
// Post-audit WorkerRegistry.registerWorker reverts any key
// whose length is not exactly this value or whose first byte is not 0x04.
const (
	P256UncompressedPubKeyLength = 65
	P256UncompressedPrefix       = 0x04
)

// validateEncryptionPubKey enforces the same 65-byte / 0x04-prefix check that
// WorkerRegistry.registerWorker applies on-chain, so registration fails fast
// with a clear local error instead of burning a tx on a contract revert.
func validateEncryptionPubKey(key []byte) error {
	if len(key) != P256UncompressedPubKeyLength {
		return fmt.Errorf("encryption pubkey must be %d bytes, got %d",
			P256UncompressedPubKeyLength, len(key))
	}
	if key[0] != P256UncompressedPrefix {
		return fmt.Errorf("encryption pubkey must start with 0x%02x prefix, got 0x%02x",
			P256UncompressedPrefix, key[0])
	}
	return nil
}

// RegistrationManager handles startup registration and graceful deregistration.
type RegistrationManager struct {
	client     chain.RegistrationClient
	workerAddr common.Address
	modelIDs   [][32]byte
	logger     *slog.Logger
}

// NewManager creates a RegistrationManager. modelIDs are the bytes32 identifiers to register.
func NewManager(
	client chain.RegistrationClient,
	workerAddr common.Address,
	modelIDs [][32]byte,
	logger *slog.Logger,
) *RegistrationManager {
	return &RegistrationManager{
		client:     client,
		workerAddr: workerAddr,
		modelIDs:   modelIDs,
		logger:     logger,
	}
}

// EnsureRegistered checks if the worker is already registered and, if not, registers it
// with the given ECDH public key and stake. If stake is nil, the minimum stake is queried
// from the AIConfig contract.
//
// Registration is idempotent: if IsWorkerRegistered returns true, this is a no-op.
// If AddSupportedModel fails after RegisterWorker succeeds, DeregisterWorker is called to roll back.
//
// The ecdhPubKey must be an uncompressed P-256 public key — 65 bytes with a
// leading 0x04 byte. The contract enforces this format and reverts otherwise;
// the same check is applied locally so misconfigured workers fail fast.
func (m *RegistrationManager) EnsureRegistered(ctx context.Context, ecdhPubKey []byte, stake *big.Int) error {
	if err := validateEncryptionPubKey(ecdhPubKey); err != nil {
		return fmt.Errorf("invalid encryption pubkey: %w", err)
	}

	registered, err := m.client.IsWorkerRegistered(ctx, m.workerAddr)
	if err != nil {
		return fmt.Errorf("check registration status: %w", err)
	}

	if registered {
		onChainKey, err := m.client.GetWorkerEncryptionKey(ctx, m.workerAddr)
		if err != nil {
			return fmt.Errorf("fetch on-chain encryption key: %w", err)
		}
		if !bytes.Equal(onChainKey, ecdhPubKey) {
			return fmt.Errorf(
				"local ECDH public key does not match on-chain key for %s — "+
					"deregister and re-register to reset",
				m.workerAddr.Hex(),
			)
		}
		m.logger.Info("worker already registered on-chain with matching encryption key",
			"address", m.workerAddr.Hex(),
		)
		return nil
	}

	// Auto-query stake if not provided
	if stake == nil {
		minStake, err := m.client.GetMinWorkerStake(ctx)
		if err != nil {
			return fmt.Errorf("query min worker stake: %w", err)
		}
		if minStake == nil || minStake.Sign() <= 0 {
			return fmt.Errorf("min worker stake from contract is nil or non-positive")
		}
		stake = minStake
		m.logger.Info("using minimum stake from AIConfig", "stakeWei", stake.String())
	}

	if stake.Sign() <= 0 {
		return fmt.Errorf("stake must be positive, got %s", stake.String())
	}

	if err := m.client.RegisterWorker(ctx, ecdhPubKey, stake); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	m.logger.Info("worker registered on-chain",
		"address", m.workerAddr.Hex(),
		"stakeWei", stake.String(),
	)

	for i, modelID := range m.modelIDs {
		if err := m.client.AddSupportedModel(ctx, modelID); err != nil {
			modelErr := fmt.Errorf("add supported model at index %d: %w", i, err)
			m.logger.Warn("AddSupportedModel failed, rolling back registration",
				"modelIndex", i,
				"error", err,
			)
			if rollbackErr := m.client.DeregisterWorker(ctx); rollbackErr != nil {
				m.logger.Error("rollback deregistration also failed",
					"error", rollbackErr,
				)
				return errors.Join(modelErr, fmt.Errorf("rollback deregister: %w", rollbackErr))
			}
			m.logger.Info("registration rolled back after model failure",
				"address", m.workerAddr.Hex(),
			)
			return modelErr
		}
	}

	m.logger.Info("all models registered",
		"address", m.workerAddr.Hex(),
		"modelCount", len(m.modelIDs),
	)

	return nil
}

// Deregister calls DeregisterWorker on-chain, withdrawing all stake.
func (m *RegistrationManager) Deregister(ctx context.Context) error {
	if err := m.client.DeregisterWorker(ctx); err != nil {
		return fmt.Errorf("deregister worker: %w", err)
	}
	m.logger.Info("worker deregistered", "address", m.workerAddr.Hex())
	return nil
}

// EnsureCapabilities syncs the worker's on-chain capability mask to the
// desired capability names — the worker's configuration is the source of
// truth, matching setCapabilities' overwrite (not merge) semantics. A worker
// whose config drops a capability (e.g. search disabled after a revoked API
// key) must stop advertising it, or it keeps winning constrained claims it
// can no longer serve.
//
// Best-effort by design: every failure is logged and skipped, never fatal —
// the worker must come up even when a capability is not yet registered
// on-chain or the RPC read fails. If any desired name fails to resolve, the
// sync degrades to add-only (never clears bits on partial knowledge). The
// on-chain claimSession check is the enforcement point, not this call.
func (m *RegistrationManager) EnsureCapabilities(ctx context.Context, names []string) {
	want := new(big.Int)
	resolvedAll := true
	for _, name := range names {
		mask, err := m.client.GetCapabilityMask(ctx, name)
		if err != nil {
			m.logger.Warn("capability mask lookup failed", "name", name, "error", err)
			resolvedAll = false
			continue
		}
		if mask.Sign() == 0 {
			m.logger.Warn("capability not registered on-chain; skipping declaration", "name", name)
			resolvedAll = false
			continue
		}
		want.Or(want, mask)
	}

	current, err := m.client.GetWorkerCapabilities(ctx, m.workerAddr)
	if err != nil {
		m.logger.Warn("worker capability read failed; skipping declaration", "error", err)
		return
	}

	target := want
	if !resolvedAll {
		// Partial knowledge: only add the bits that resolved; clearing a bit
		// we merely failed to look up would un-declare a live capability.
		target = new(big.Int).Or(current, want)
	}
	if target.Cmp(current) == 0 {
		return // already in sync
	}

	if err := m.client.SetCapabilities(ctx, target); err != nil {
		m.logger.Warn("setCapabilities failed", "error", err)
		return
	}
	m.logger.Info("declared worker capabilities on-chain",
		"address", m.workerAddr.Hex(),
		"mask", target.String(),
	)
}

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
func (m *RegistrationManager) EnsureRegistered(ctx context.Context, ecdhPubKey []byte, stake *big.Int) error {
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

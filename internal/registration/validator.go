package registration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// RegistrationValidator performs read-only startup validation to confirm
// the worker is registered on-chain with a matching encryption key.
// Unlike RegistrationManager, it never submits transactions — registration
// is the responsibility of the operator CLI.
type RegistrationValidator struct {
	client     chain.ValidationClient
	workerAddr common.Address
	logger     *slog.Logger
}

// NewValidator creates a RegistrationValidator that checks on-chain state at startup.
func NewValidator(
	client chain.ValidationClient,
	workerAddr common.Address,
	logger *slog.Logger,
) *RegistrationValidator {
	return &RegistrationValidator{
		client:     client,
		workerAddr: workerAddr,
		logger:     logger,
	}
}

// Validate checks that the worker is registered on-chain and that the local
// ECDH public key matches the on-chain record. Returns a descriptive error
// if validation fails, or nil if the worker is ready to operate.
func (v *RegistrationValidator) Validate(ctx context.Context, ecdhPubKey []byte) error {
	registered, err := v.client.IsWorkerRegistered(ctx, v.workerAddr)
	if err != nil {
		return fmt.Errorf("check registration status: %w", err)
	}

	if !registered {
		return fmt.Errorf(
			"worker %s is not registered on-chain; run 'lightchain-worker register' before starting the sidecar",
			v.workerAddr.Hex(),
		)
	}

	onChainKey, err := v.client.GetWorkerEncryptionKey(ctx, v.workerAddr)
	if err != nil {
		return fmt.Errorf("fetch on-chain encryption key: %w", err)
	}

	if !bytes.Equal(onChainKey, ecdhPubKey) {
		return fmt.Errorf(
			"local ECDH public key does not match on-chain key for %s — "+
				"deregister and re-register to reset",
			v.workerAddr.Hex(),
		)
	}

	v.logger.Info("worker registration validated — on-chain key matches local key",
		"address", v.workerAddr.Hex(),
	)

	return nil
}

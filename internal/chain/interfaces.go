// Package chain defines the narrow chain interfaces the worker sidecar needs for
// registration and job execution. Interfaces are defined at the consumer (worker),
// not re-exported from pkg/chain, keeping coupling to only the methods actually required.
package chain

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// RegistrationClient is the narrow chain interface the worker sidecar needs for startup
// registration and heartbeat. Defined at the consumer (worker) — not re-exported from pkg/chain.
type RegistrationClient interface {
	// IsWorkerRegistered returns true if the given address is already registered on-chain.
	IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error)

	// RegisterWorker registers the worker with the given ECDH public key and sends the
	// stake as msg.value. Per Amendment A-2, the stake is a single global value.
	RegisterWorker(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error

	// AddSupportedModel adds a model ID to the worker's supported model set on-chain.
	// Per Amendment A-2, this is non-payable — no additional ETH is required per model.
	AddSupportedModel(ctx context.Context, modelID [32]byte) error

	// DeregisterWorker removes the worker from the registry and withdraws all stake.
	DeregisterWorker(ctx context.Context) error

	// GetMinWorkerStake returns the minimum stake required to register from AIConfig.
	GetMinWorkerStake(ctx context.Context) (*big.Int, error)

	// GetWorkerEncryptionKey returns the ECDH public key stored on-chain for the given worker.
	GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error)
}

// ValidationClient is the narrow read-only chain interface the sidecar uses at
// startup to validate that the worker is properly registered on-chain. It does
// not include any write methods — registration is handled by the operator CLI.
type ValidationClient interface {
	// IsWorkerRegistered returns true if the given address is already registered on-chain.
	IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error)

	// GetWorkerEncryptionKey returns the ECDH public key stored on-chain for the given worker.
	GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error)
}

// JobExecutionClient is the chain interface for job lifecycle transactions.
// Separate from RegistrationClient to follow interface segregation — the pipeline
// handler only needs these methods.
type JobExecutionClient interface {
	// AcknowledgeJob submits an acknowledgeJob transaction for the given job ID.
	AcknowledgeJob(ctx context.Context, jobID uint64) error

	// CompleteJob submits a completeJob transaction with a single bytes32 response
	// blob hash and the keccak256 hash of the full encrypted response ciphertext.
	// Post-audit the contract's completeJob takes one bytes32 and enforces
	// `blobhash(0) == responseBlobHash`, so the caller must have included exactly
	// one blob in the same blob-carrying transaction.
	CompleteJob(
		ctx context.Context,
		jobID uint64,
		responseBlobHash [32]byte,
		responseCiphertextHash [32]byte,
	) error

	// HasJobAcknowledged reports whether JobAcknowledged has already been observed for the job.
	HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error)

	// HasJobCompleted reports whether JobCompleted has already been observed for the job.
	HasJobCompleted(ctx context.Context, jobID uint64) (bool, error)

	// GetSessionEncWorkerKey returns the most recent encrypted worker key for a
	// session by filtering historical SessionCreated and SessionKeyUpdated event
	// logs, returning whichever was emitted in the highest-numbered block. This
	// picks up post-creation key rotations.
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)

	// GetJobBlobInfo returns the prompt blob hash, response blob hash,
	// submitBlockNumber, and completionBlockNumber for a completed job.
	// Used to build conversation history. Post-audit each job carries one
	// prompt blob and one response blob.
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash common.Hash, responseHash common.Hash, submitBlock uint64, completionBlock uint64, err error)
}

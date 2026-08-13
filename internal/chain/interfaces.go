// Package chain defines the narrow chain interfaces the worker sidecar needs for
// registration and job execution. Interfaces are defined at the consumer (worker),
// not re-exported from pkg/chain, keeping coupling to only the methods actually required.
package chain

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// JobState mirrors the Solidity enum JobRegistry.JobState (see
// contracts/src/interfaces/IJobRegistry.sol). Indices are part of the on-chain
// ABI and must not be reordered.
type JobState uint8

const (
	JobStateSubmitted    JobState = 0
	JobStateAcknowledged JobState = 1
	JobStateCompleted    JobState = 2
	JobStateTimedOut     JobState = 3
	JobStateDisputed     JobState = 4
	JobStateResolved     JobState = 5
	JobStateReleased     JobState = 6
)

// String renders a JobState for logging.
func (s JobState) String() string {
	switch s {
	case JobStateSubmitted:
		return "Submitted"
	case JobStateAcknowledged:
		return "Acknowledged"
	case JobStateCompleted:
		return "Completed"
	case JobStateTimedOut:
		return "TimedOut"
	case JobStateDisputed:
		return "Disputed"
	case JobStateResolved:
		return "Resolved"
	case JobStateReleased:
		return "Released"
	default:
		return "Unknown"
	}
}

// JobStateInfo is the subset of on-chain Job storage the release scheduler
// needs to make per-cycle settlement decisions. See contracts/src/JobRegistry.sol
// (struct Job).
type JobStateInfo struct {
	State       JobState
	Worker      common.Address
	CompletedAt int64 // unix seconds; zero when never set
	EscrowedFee *big.Int
}

// HeadInfo describes the latest block. Used by the scheduler to make
// chain-time-based eligibility decisions instead of trusting the worker's
// wall clock.
type HeadInfo struct {
	Number    uint64
	Timestamp int64
}

// JobCompletedEvent is the worker-facing projection of the on-chain
// JobCompleted event. BlockNumber is preserved so callers can advance their
// reconciliation cursor.
type JobCompletedEvent struct {
	JobID       uint64
	Worker      common.Address
	BlockNumber uint64
}

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

	// GetCapabilityMask resolves a capability name to its on-chain bitmask (zero = not registered). LC-30.
	GetCapabilityMask(ctx context.Context, name string) (*big.Int, error)

	// GetWorkerCapabilities returns the declared capability mask for the given worker. LC-30.
	GetWorkerCapabilities(ctx context.Context, worker common.Address) (*big.Int, error)

	// SetCapabilities declares the worker's full capability mask (overwrite, not merge). LC-30.
	SetCapabilities(ctx context.Context, mask *big.Int) error
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

	// GetSessionEncWorkerKey retrieves the current encrypted worker key for a session
	// from JobRegistry session storage. After reassignment, the consumer is expected
	// to re-wrap the same underlying symmetric session key for the replacement worker.
	// Implementations must reject sessions that are not currently active.
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)

	// GetJobBlobInfo returns the prompt blob hash, response blob hash,
	// submitBlockNumber, and completionBlockNumber for a completed job.
	// Used to build conversation history. Post-audit each job carries one
	// prompt blob and one response blob.
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash common.Hash, responseHash common.Hash, submitBlock uint64, completionBlock uint64, err error)
}

// SettlementClient is the chain interface for the release scheduler and
// withdrawal CLI. Defined here at the consumer per the project's
// accept-interfaces-return-structs convention.
//
// The release scheduler uses these methods together to:
//  1. Discover newly-completed jobs (FilterJobCompleted)
//  2. Decide eligibility against chain time (Head) and dispute window (GetDisputeWindow)
//  3. Validate per-job state before settlement (GetJobState)
//  4. Settle in batches with per-job fallback (ReleaseJobs / ReleaseJob)
//
// The CLI uses WorkerBalance + Withdraw to display and cash out earned funds.
type SettlementClient interface {
	// ReleaseJob settles a single job's escrowed fee. Reverts on the contract
	// side if the job is not in Completed (after dispute window) or Resolved
	// state. Permissionless — any caller may invoke.
	ReleaseJob(ctx context.Context, jobID uint64) error

	// ReleaseJobs is the batch variant. The contract iterates and calls
	// _releaseJob for each id; a single bad job reverts the entire batch, so
	// callers must pre-filter via GetJobState.
	ReleaseJobs(ctx context.Context, jobIDs []uint64) error

	// GetJobState reads the authoritative on-chain Job struct and returns the
	// fields the scheduler needs.
	GetJobState(ctx context.Context, jobID uint64) (JobStateInfo, error)

	// GetDisputeWindow returns AIConfig.getDisputeWindow(). Implementations
	// MAY cache the value with a short TTL since governance changes are rare.
	GetDisputeWindow(ctx context.Context) (time.Duration, error)

	// WorkerBalance returns the worker's accumulated, withdrawable balance
	// from JobRegistry.workerBalances.
	WorkerBalance(ctx context.Context, worker common.Address) (*big.Int, error)

	// Withdraw moves the caller's full workerBalance to the caller's address
	// (msg.sender). The CLI uses this; the sidecar should not.
	Withdraw(ctx context.Context) error

	// Head returns the latest block number and timestamp for chain-time
	// arithmetic. The scheduler uses this instead of wall-clock time to match
	// the contract's block.timestamp semantics.
	Head(ctx context.Context) (HeadInfo, error)

	// FilterJobCompleted returns JobCompleted events emitted by this worker
	// in the closed block range [fromBlock, toBlock] (both endpoints
	// inclusive, per ethclient.FilterLogs semantics). Filtering by worker
	// uses the indexed topic so the call is cheap even on large block
	// ranges.
	FilterJobCompleted(ctx context.Context, worker common.Address, fromBlock, toBlock uint64) ([]JobCompletedEvent, error)
}

// Package pipeline implements the Asynq task handler for AI inference jobs.
package pipeline

import "github.com/ethereum/go-ethereum/common"

// TaskTypeJobInference is the Asynq task type for inference jobs.
// Must match dispatcher/internal/types.TaskTypeJobInference exactly.
const TaskTypeJobInference = "job:inference"

// JobPayload mirrors dispatcher/internal/types.JobPayload.
// JSON tags must match exactly for correct deserialization.
//
// Post-audit there is exactly one prompt blob per job —
// the on-chain submitJob carries a single bytes32 blobHash rather than a
// dynamic array.
type JobPayload struct {
	JobID          uint64         `json:"jobId"`
	SessionID      uint64         `json:"sessionId"`
	Consumer       common.Address `json:"consumer"`
	Worker         common.Address `json:"worker"`
	ModelID        string         `json:"modelId"`
	PromptBlobHash common.Hash    `json:"promptBlobHash"`
	BlockNumber    uint64         `json:"blockNumber"`
	Timestamp      int64          `json:"timestamp"`
	CorrelationID  string         `json:"correlationId"`
	PriorJobIDs    []uint64       `json:"priorJobIds,omitempty"`
}

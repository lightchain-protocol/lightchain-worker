// Package pipeline implements the Asynq task handler for AI inference jobs.
package pipeline

import "github.com/ethereum/go-ethereum/common"

// TaskTypeJobInference is the Asynq task type for inference jobs.
// Must match dispatcher/internal/types.TaskTypeJobInference exactly.
const TaskTypeJobInference = "job:inference"

// JobPayload mirrors dispatcher/internal/types.JobPayload.
// JSON tags must match exactly for correct deserialization.
type JobPayload struct {
	JobID            uint64         `json:"jobId"`
	SessionID        uint64         `json:"sessionId"`
	Consumer         common.Address `json:"consumer"`
	Worker           common.Address `json:"worker"`
	ModelID          string         `json:"modelId"`
	PromptBlobHash   common.Hash    `json:"promptBlobHash"`             // Deprecated: first hash for backward compat.
	PromptBlobHashes []common.Hash  `json:"promptBlobHashes,omitempty"` // Full list of prompt blob hashes.
	BlockNumber      uint64         `json:"blockNumber"`
	Timestamp        int64          `json:"timestamp"`
	CorrelationID    string         `json:"correlationId"`
	PriorJobIDs      []uint64       `json:"priorJobIds,omitempty"`
}

// EffectivePromptHashes returns the full list of prompt blob hashes,
// falling back to the singular PromptBlobHash for old payloads.
func (p *JobPayload) EffectivePromptHashes() []common.Hash {
	if len(p.PromptBlobHashes) > 0 {
		return p.PromptBlobHashes
	}
	if p.PromptBlobHash != (common.Hash{}) {
		return []common.Hash{p.PromptBlobHash}
	}
	return nil
}

package pipeline

// Generative-UI / agent-mode delivery scaffold.
//
// FrameKindArtifact already exists in the shared protocol types as the
// channel for "descriptors of out-of-band content - JSON metadata, not the
// artifact itself". This file defines the wire convention future job
// classes (generative UI, agent tool-call traces) emit on that channel and
// provides the single emitter they will share. Nothing calls it yet: the
// job classes that produce artifacts are Phase-2 work (they depend on a
// per-model completion timeout / async job class, because tool loops
// cannot fit the fixed 120 s completion window). It lands now so the wire
// convention is pinned, reviewed, and unit-tested before any producer
// exists.
//
// Design doc: lightchain-agents/research/ai-1-generative-ui-design.md.

import (
	"encoding/json"
	"fmt"

	pkgtypes "github.com/lightchain/pkg/types"
)

// artifactDescriptor is the JSON payload convention for FrameKindArtifact
// frames. Like every streamer frame it is encrypted under the session key,
// so only session participants can read it.
//
// Artifacts are delivered, not settled: they never enter the stage-6
// settlement ciphertext, exactly like audio. Consumers must treat them as
// convenience renderings until the non-text settlement commitment lands
// (contract-side scope).
type artifactDescriptor struct {
	// ArtifactType names the producer family: "genui" for generative-UI
	// payloads, "tool_call" / "tool_result" for agent-mode traces.
	ArtifactType string `json:"artifactType"`
	// Schema names the payload contract and its version, e.g.
	// "lightchain.genui.v1". Consumers must refuse descriptors whose
	// schema they do not recognize rather than guessing at the payload.
	Schema string `json:"schema"`
	// Payload is the artifact metadata itself. For genui this is the
	// component tree description; for tool traces the call/result record.
	Payload json.RawMessage `json:"payload"`
	// Settled is always false in v1 - see above.
	Settled bool `json:"settled"`
}

// maxArtifactDescriptorBytes caps one descriptor frame. Artifact payloads
// are metadata by contract; anything larger belongs in a DA blob with a
// pointer descriptor (the audio pattern), not on the relay.
const maxArtifactDescriptorBytes = 8192

// emitArtifact publishes one artifact descriptor frame. Best-effort like
// every non-terminal frame: a dropped artifact must never fail a job whose
// text answer is deliverable. Nil-safe on the streamer so batch-mode jobs
// silently skip delivery.
func emitArtifact(streamer *chunkStreamer, artifactType, schema string, payload json.RawMessage) error {
	if streamer == nil {
		return nil
	}
	if artifactType == "" || schema == "" {
		return fmt.Errorf("artifact descriptor requires artifactType and schema")
	}
	if len(payload) == 0 {
		return fmt.Errorf("artifact descriptor requires a payload")
	}
	raw, err := json.Marshal(artifactDescriptor{
		ArtifactType: artifactType,
		Schema:       schema,
		Payload:      payload,
		Settled:      false,
	})
	if err != nil {
		return fmt.Errorf("encode artifact descriptor: %w", err)
	}
	if len(raw) > maxArtifactDescriptorBytes {
		return fmt.Errorf("artifact descriptor exceeds %d bytes (%d)", maxArtifactDescriptorBytes, len(raw))
	}
	return streamer.PublishBytes(pkgtypes.FrameKindArtifact, raw)
}

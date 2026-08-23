package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
)

// promptEnvelope is the decrypted prompt payload.
//
// Prompts used to be raw UTF-8 text, which meant a deployed vision model had
// no way to receive an image: the blob carried a bare string and the worker
// passed it straight through. The envelope adds a version and an image list
// while staying backward compatible - a payload that is not a versioned
// envelope is treated as text, so older clients keep working unchanged.
type promptEnvelope struct {
	// Version is 1 for the first envelope format. Its presence is what
	// distinguishes an envelope from a prompt that merely happens to be
	// valid JSON.
	Version int    `json:"v"`
	Text    string `json:"text"`
	// Images are base64-encoded, without a data: prefix, in the form Ollama
	// expects. They ride on the final user turn.
	Images []string `json:"images,omitempty"`
}

// maxPromptImages bounds how many images one prompt may carry. The prompt
// blob is capped at 126,972 bytes by EIP-4844 encoding, so this is a guard
// against a pathological payload rather than the real limit - the blob
// encoder rejects anything oversized long before this matters.
const maxPromptImages = 8

// errTooManyImages is returned rather than silently truncating, because a
// dropped image changes the answer the consumer paid for.
var errTooManyImages = errors.New("prompt carries too many images")

// decodePrompt interprets a decrypted prompt payload.
//
// Anything that is not a well-formed versioned envelope is returned as plain
// text. That fallback is deliberate and load-bearing: a user prompt can
// legitimately be a JSON document, and misreading one as an envelope would
// silently drop the actual question.
func decodePrompt(raw []byte) (promptEnvelope, error) {
	var env promptEnvelope
	if err := json.Unmarshal(raw, &env); err != nil || env.Version < 1 {
		return promptEnvelope{Text: string(raw)}, nil
	}
	if len(env.Images) > maxPromptImages {
		return promptEnvelope{}, fmt.Errorf("%w: %d > %d", errTooManyImages, len(env.Images), maxPromptImages)
	}
	return env, nil
}

// bytes reports the plaintext size for logging, counting image data because
// that is what dominates a multimodal prompt.
func (p promptEnvelope) bytes() int {
	n := len(p.Text)
	for _, img := range p.Images {
		n += len(img)
	}
	return n
}

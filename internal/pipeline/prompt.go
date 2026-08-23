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
	// Version is 1 for the first envelope format, 2 once any voice field is
	// in use. Its presence is what distinguishes an envelope from a prompt
	// that merely happens to be valid JSON. decodePrompt accepts any
	// version >= 1: unknown fields are ignored by encoding/json, so an old
	// worker decoding a v2 payload simply drops the voice fields (forward
	// compatibility) instead of rejecting a prompt it could still answer.
	Version int    `json:"v"`
	Text    string `json:"text"`
	// Images are base64-encoded, without a data: prefix, in the form Ollama
	// expects. They ride on the final user turn.
	Images []string `json:"images,omitempty"`

	// --- Voice fields (envelope v2) ---
	//
	// Audio is a base64-encoded audio clip (no data: prefix) carrying a
	// spoken prompt. The worker transcribes it via the STT sidecar and
	// merges the transcript into Text before inference; see
	// pipeline/voice.go. AudioFormat names the container/codec
	// ("wav", "mp3", ...) so the transcriber can pick a decoder; empty
	// defaults to "wav".
	//
	// The audio bytes stay inside the encrypted prompt blob on DA, so the
	// input side of settlement (what the consumer submitted) still covers
	// the voice prompt bit-for-bit. The *transcript* is derived context,
	// not settled content: it is re-derivable from the blob but not
	// bit-deterministic across whisper versions, so the response hash
	// continues to cover only the model's output text until the non-text
	// settlement commitment lands (contract-side scope, tracked
	// separately).
	Audio       string `json:"audio,omitempty"`
	AudioFormat string `json:"audioFormat,omitempty"`
	// AudioResponse opts in to spoken output: the worker synthesizes the
	// response text via the TTS sidecar and delivers the audio by
	// reference (encrypted DA blob + `audio` frame descriptor). The audio
	// is deliberately NOT part of the settlement ciphertext - the
	// text-only settlement invariant is unchanged.
	AudioResponse bool `json:"audioResponse,omitempty"`
	// Voice selects the TTS voice (sidecar-specific name, e.g. a Kokoro
	// voice id). Empty falls back to the worker's configured default.
	Voice string `json:"voice,omitempty"`
}

// maxPromptImages bounds how many images one prompt may carry. The prompt
// blob is capped at 126,972 bytes by EIP-4844 encoding, so this is a guard
// against a pathological payload rather than the real limit - the blob
// encoder rejects anything oversized long before this matters.
const maxPromptImages = 8

// maxPromptAudioB64 bounds the base64 audio payload one prompt may carry
// (~96 KiB decoded). Same rationale as maxPromptImages: the blob cap is the
// real limit, this rejects pathological envelopes early and keeps an audio
// prompt from crowding out the response blob budget unnoticed.
const maxPromptAudioB64 = 131072

// errTooManyImages is returned rather than silently truncating, because a
// dropped image changes the answer the consumer paid for.
var errTooManyImages = errors.New("prompt carries too many images")

// errAudioTooLarge rejects an oversized audio payload rather than silently
// truncating it, for the same reason: a clipped clip transcribes to
// different words than the consumer spoke.
var errAudioTooLarge = errors.New("prompt audio exceeds size limit")

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
	if len(env.Audio) > maxPromptAudioB64 {
		return promptEnvelope{}, fmt.Errorf("%w: %d > %d base64 chars", errAudioTooLarge, len(env.Audio), maxPromptAudioB64)
	}
	return env, nil
}

// bytes reports the plaintext size for logging, counting image and audio
// data because that is what dominates a multimodal prompt.
func (p promptEnvelope) bytes() int {
	n := len(p.Text) + len(p.Audio)
	for _, img := range p.Images {
		n += len(img)
	}
	return n
}

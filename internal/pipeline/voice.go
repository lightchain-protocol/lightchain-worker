package pipeline

// Voice input/output: STT transcription of an audio prompt and TTS
// synthesis of the response, both via local sidecars (whisper / Kokoro).
//
// Settlement posture (deliberate, do not weaken silently):
//
//   - Input: the audio clip rides inside the encrypted prompt blob on DA,
//     so the submitted input is covered bit-for-bit. The transcript is
//     derived context, NOT settled content - it is re-derivable from the
//     blob but not bit-deterministic across whisper versions. The response
//     hash continues to cover model output text only.
//   - Output: synthesized audio never enters the stage-6 settlement
//     ciphertext (text-only invariant, unchanged). It streams to the
//     consumer as `audio` frames - delivered, not settled. Wiring audio
//     into the settlement commitment is contract-side scope (the Phase-2
//     manifestHash commitment). To make that future commitment possible
//     without re-derivation, the pipeline computes and ships a keccak256
//     content hash over the delivered PCM bytes in the terminal audio
//     descriptor; it is not yet committed on-chain.
//
// Delivery mechanism: audio rides chunk frames, not a DA blob. PCM s16le
// at 24 kHz is ~48 KB/s, so anything over ~2.6 s of speech exceeds the
// 126,972-byte blob cap; frames have no such constraint, arrive
// incrementally (first audio ~0.9 s into synthesis), and reuse the exact
// channel the consumer already demultiplexes.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"

	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/voice"
)

// VoiceEngine abstracts the STT/TTS sidecars. *voice.Client satisfies it;
// tests substitute a mock. The interface is installed via SetVoiceEngine so
// the 15-parameter constructor and its ~30 call sites stay untouched (same
// precedent as SetReleaseTracker).
//
// Error contract: implementations must wrap request-deterministic failures
// (HTTP 400/413/422, undecodable transcript) in voice.ErrBadAudio so the
// pipeline can map them to no-retry; transport errors and 5xx/503 stay
// retryable.
type VoiceEngine interface {
	// Transcribe turns one audio clip into text. format is the
	// container/codec hint from the envelope; the sidecar decodes WAV
	// only.
	Transcribe(ctx context.Context, audio []byte, format string) (string, error)
	// Synthesize starts streaming synthesis and returns the raw PCM
	// s16le 24 kHz mono chunk stream (voice.AudioMIME). The caller closes
	// the stream. Per the sidecar contract the stream may end early
	// without an error; a missing audioFinal frame marks truncation.
	Synthesize(ctx context.Context, text, voiceName string) (stream io.ReadCloser, err error)
}

// SetVoiceEngine installs the voice sidecar engine after construction.
// Service wiring calls this once before Run when STT or TTS is enabled;
// nil leaves every voice path inert.
func (h *JobHandler) SetVoiceEngine(v VoiceEngine) {
	h.voiceEngine = v
}

// errSTTUnavailable marks an audio-only prompt that cannot be served: no
// STT sidecar is configured/enabled, and there is no text to fall back to.
// Deterministic for this worker, so no-retry.
var errSTTUnavailable = errors.New("prompt is audio-only but STT is not enabled on this worker")

// errEmptyTranscript marks a successful sidecar call that produced no
// words (silence, noise). Like ollama.ErrEmptyGeneration it repeats
// deterministically under identical input, so no-retry.
var errEmptyTranscript = errors.New("STT sidecar returned an empty transcript")

// maybeTranscribeAudio implements voice input. When the envelope carries
// audio it is transcribed and the transcript merged into the prompt text:
// the transcript becomes the text when no text was provided, otherwise it
// is appended after a newline (spoken question refining typed context).
// Runs under the deadline-clamped inference context so a slow sidecar
// cannot push the job past its settlement window.
func (h *JobHandler) maybeTranscribeAudio(ctx context.Context, logger *slog.Logger, envelope *promptEnvelope) error {
	if envelope.Audio == "" {
		return nil
	}
	if h.voiceEngine == nil || !h.cfg.STTEnabled {
		if envelope.Text == "" {
			return noRetry(fmt.Errorf("stage 5 (transcribe audio): %w", errSTTUnavailable))
		}
		// Text present: answer the text and drop the audio. Dropping is
		// safe here only because a text fallback exists; the audio remains
		// verifiable in the prompt blob on DA.
		logger.Warn("STT disabled; ignoring audio prompt and answering text only",
			"stage", "transcribe",
			"audioB64Chars", len(envelope.Audio),
		)
		envelope.Audio = ""
		return nil
	}

	raw, err := base64.StdEncoding.DecodeString(envelope.Audio)
	if err != nil {
		return noRetry(fmt.Errorf("stage 5 (transcribe audio): undecodable base64 audio: %w", err))
	}
	format := envelope.AudioFormat
	if format == "" {
		format = "wav"
	}

	start := time.Now()
	transcript, err := h.voiceEngine.Transcribe(ctx, raw, format)
	if err != nil {
		if errors.Is(err, voice.ErrBadAudio) {
			return noRetry(fmt.Errorf("stage 5 (transcribe audio): %w", err))
		}
		return fmt.Errorf("stage 5 (transcribe audio): %w", err)
	}
	transcript = strings.TrimSpace(transcript)
	if transcript == "" {
		return noRetry(fmt.Errorf("stage 5 (transcribe audio): %w", errEmptyTranscript))
	}

	if envelope.Text == "" {
		envelope.Text = transcript
	} else {
		envelope.Text = envelope.Text + "\n" + transcript
	}
	// The audio payload has served its purpose; drop it so later stages
	// (history build, logging) handle the merged text only. The bytes
	// remain anchored in the prompt blob on DA.
	envelope.Audio = ""

	logger.Info("stage 5: audio prompt transcribed",
		"stage", "transcribe",
		"audioBytes", len(raw),
		"format", format,
		"transcriptChars", len(transcript),
		"durationMs", time.Since(start).Milliseconds(),
	)
	return nil
}

// audioFrameBytes is the PCM payload per audio chunk frame: 32 KiB is
// ~0.68 s of 24 kHz s16le speech, small enough to keep the relay's
// per-frame and rate budgets comfortably clear while bounding frame count
// (a 30 s answer is ~45 frames).
const audioFrameBytes = 32 * 1024

// audioMeta is the JSON marker identifying the non-PCM audio frames. Every
// `audio` frame payload is either raw PCM bytes or a JSON object carrying
// this field; consumers distinguish by JSON-parsing for "audioMeta" (a PCM
// s16le chunk cannot be valid UTF-8 JSON with this key in practice).
type audioMeta struct {
	// Header fields (first audio frame; sent before synthesis output).
	Format     string `json:"format"`     // "pcm-s16le"
	SampleRate int    `json:"sampleRate"` // 24000
	Channels   int    `json:"channels"`   // 1
	Mime       string `json:"mime"`       // voice.AudioMIME
	Voice      string `json:"voice"`
	Engine     string `json:"engine"`    // "kokoro"
	Chars      int    `json:"chars"`     // characters sent to synthesis
	Truncated  bool   `json:"truncated"` // text was cut to TTSMaxChars
	Delivered  bool   `json:"delivered"` // always true; semantic is "Delivered", never "verified"
	Settled    bool   `json:"settled"`   // false: outside the settlement commitment (Phase-2 manifestHash)
	// Final fields (last audio frame; absent on a truncated stream).
	Final       bool   `json:"final,omitempty"`
	Bytes       int    `json:"bytes,omitempty"`
	Chunks      int    `json:"chunks,omitempty"`
	ContentHash string `json:"contentHash,omitempty"` // keccak256 of delivered PCM plaintext, 0x-prefixed
}

// audioMetaFrame wraps the marker so PCM and JSON frames cannot collide.
type audioMetaFrame struct {
	Meta audioMeta `json:"audioMeta"`
}

// voiceStreamMargin is the headroom the deadline budget check requires
// beyond the TTS timeout itself: frame encryption + publish for the
// trailing descriptor, plus jitter. There is no audio blob transaction in
// the frame-streaming design, so the margin is small.
const voiceStreamMargin = 5 * time.Second

// defaultTTSTimeout mirrors the config default; used when HandlerConfig
// carries a zero (tests, CLI constructions).
const defaultTTSTimeout = 20 * time.Second

// maybeDeliverAudio implements voice output: synthesize the response text
// and stream the PCM to the consumer as `audio` frames, bracketed by JSON
// header/final descriptor frames on the same kind.
//
// It runs after stage 6 (encrypt) and before runInferencePipeline returns
// so the audio frames are counted in streamer.frames() and the terminal
// frame's TotalChunks stays honest (frame accounting invariant).
//
// The whole path is best-effort: any failure (synthesis, budget, publish)
// logs and skips. Voice output is a convenience rendering of an answer the
// consumer already received as text; it must never fail or retry a job
// whose text answer is deliverable. A mid-stream failure publishes what it
// has and omits the final frame, matching the sidecar's own truncation
// contract.
//
// Requires a non-nil streamer: audio frames ARE the delivery channel, so
// batch-mode jobs skip audio. (A retry that hits a delivered checkpoint
// also runs streamer-less, which correctly avoids double-delivering audio.)
func (h *JobHandler) maybeDeliverAudio(
	ctx context.Context,
	logger *slog.Logger,
	streamer *chunkStreamer,
	response string,
	envelope promptEnvelope,
	guard *deadlineGuard,
) {
	if h.voiceEngine == nil || !h.cfg.TTSEnabled || !envelope.AudioResponse {
		return
	}
	if streamer == nil {
		logger.Warn("TTS requested but streaming is off; skipping audio delivery",
			"stage", "tts",
			"reason", "no_streamer",
		)
		return
	}
	if response == "" {
		return
	}

	text := response
	truncated := false
	if max := h.cfg.TTSMaxChars; max > 0 && len(text) > max {
		// Truncation is safe here (unlike prompt audio): the settled text
		// answer is complete, audio is a convenience rendering of its
		// prefix. 4000 chars is also the sidecar's own hard limit.
		text = text[:max]
		truncated = true
	}

	voiceName := envelope.Voice
	if voiceName == "" {
		voiceName = h.cfg.TTSVoice
	}
	ttsTimeout := h.cfg.TTSTimeout
	if ttsTimeout <= 0 {
		ttsTimeout = defaultTTSTimeout
	}

	// Deadline interaction: voice streaming must not eat the settlement
	// window. Fail-open like the rest of the guard: when the deadline
	// cannot be read, the work proceeds.
	if !guard.voiceBudgetOK(ctx, logger, ttsTimeout+voiceStreamMargin) {
		return
	}

	synthStart := time.Now()
	stream, err := h.voiceEngine.Synthesize(ctx, text, voiceName)
	if err != nil {
		logger.Warn("TTS synthesis failed; skipping audio delivery",
			"stage", "tts",
			"error", err,
			"chars", len(text),
		)
		return
	}
	defer stream.Close()

	// Header frame first: the consumer learns the wire format, voice, and
	// truncation state before the first PCM byte.
	if err := h.publishAudioMeta(streamer, audioMeta{
		Format:     "pcm-s16le",
		SampleRate: 24000,
		Channels:   1,
		Mime:       voice.AudioMIME,
		Voice:      voiceName,
		Engine:     "kokoro",
		Chars:      len(text),
		Truncated:  truncated,
		Delivered:  true,
		Settled:    false,
	}); err != nil {
		logger.Warn("failed to publish audio header; skipping audio delivery",
			"stage", "tts",
			"error", err,
		)
		return
	}

	buf := make([]byte, audioFrameBytes)
	totalBytes := 0
	chunks := 0
	// Content hash over the delivered PCM plaintext: computed now so a
	// future Phase-2 manifestHash commitment can cover audio without
	// re-derivation; shipped in the final descriptor, not settled on-chain.
	contentHash := ethcrypto.NewKeccakState()
	readErr := error(nil)
	for {
		n, rErr := stream.Read(buf)
		if n > 0 {
			contentHash.Write(buf[:n])
			if pErr := streamer.PublishBytes(pkgtypes.FrameKindAudio, buf[:n]); pErr != nil {
				logger.Warn("failed to publish audio chunk; continuing stream",
					"stage", "tts",
					"error", pErr,
				)
			} else {
				chunks++
			}
			totalBytes += n
		}
		if rErr != nil {
			if rErr != io.EOF {
				readErr = rErr
			}
			break
		}
	}

	if readErr != nil || totalBytes == 0 {
		// Truncated or empty stream: no final frame, per the sidecar's own
		// truncation contract. Whatever PCM frames already went out are
		// still playable.
		logger.Warn("audio stream ended abnormally; omitting final descriptor",
			"stage", "tts",
			"error", readErr,
			"audioBytes", totalBytes,
			"chunks", chunks,
		)
		return
	}

	var hashOut [32]byte
	copy(hashOut[:], contentHash.Sum(nil))
	if err := h.publishAudioMeta(streamer, audioMeta{
		Format:      "pcm-s16le",
		SampleRate:  24000,
		Channels:    1,
		Mime:        voice.AudioMIME,
		Voice:       voiceName,
		Engine:      "kokoro",
		Chars:       len(text),
		Truncated:   truncated,
		Delivered:   true,
		Settled:     false,
		Final:       true,
		Bytes:       totalBytes,
		Chunks:      chunks,
		ContentHash: fmt.Sprintf("0x%x", hashOut),
	}); err != nil {
		logger.Warn("failed to publish audio final descriptor",
			"stage", "tts",
			"error", err,
		)
		return
	}
	logger.Info("stage 6b: audio response streamed",
		"stage", "tts",
		"audioBytes", totalBytes,
		"chunks", chunks,
		"voice", voiceName,
		"truncated", truncated,
		"durationMs", time.Since(synthStart).Milliseconds(),
	)
}

// publishAudioMeta marshals and emits one audio descriptor frame.
func (h *JobHandler) publishAudioMeta(streamer *chunkStreamer, meta audioMeta) error {
	raw, err := json.Marshal(audioMetaFrame{Meta: meta})
	if err != nil {
		return fmt.Errorf("encode audio descriptor: %w", err)
	}
	return streamer.PublishBytes(pkgtypes.FrameKindAudio, raw)
}

// voiceBudgetOK reports whether the remaining completion window can absorb
// optional voice work without endangering settlement. Mirrors
// gateBlobSubmit semantics: the deadline is read regardless of the
// acknowledged flag (a retry sees the completion deadline; for a fresh job
// the ack has landed by the time voice output runs, since stage 1b joins
// the ack before inference). Fail-open like the other guard methods: a
// read error allows the work, and a nil guard (disabled) allows it too.
func (g *deadlineGuard) voiceBudgetOK(ctx context.Context, logger *slog.Logger, need time.Duration) bool {
	if g == nil {
		return true
	}
	deadline, _, ok := g.read(ctx, logger, "deadline_voice")
	if !ok {
		return true
	}
	remaining := time.Until(deadline)
	if remaining <= g.completionReserve+need {
		logger.Warn("deadline guard: skipping voice work; window too small",
			"stage", "deadline_voice",
			"jobID", g.jobID,
			"remainingMs", remaining.Milliseconds(),
			"requiredMs", (g.completionReserve + need).Milliseconds(),
		)
		return false
	}
	return true
}

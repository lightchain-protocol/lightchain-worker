// Package voice implements the worker-side HTTP client for the whisper STT
// and Kokoro TTS sidecars running loopback-only on the worker host.
//
// Contract: provisioning/worker/voice-sidecars.md (ai-2, deployed
// 2026-08-23 on worker-3). The endpoints are NOT OpenAI-compatible:
//
//	STT: POST {sttURL}/v1/transcribe
//	     body: raw WAV bytes (Content-Type: audio/wav) or multipart `file`;
//	     other containers are not decoded - send WAV. Max body 25 MB (413).
//	     -> 200 {"text":"...","language":"en",...}
//	     errors: 400 bad/empty body, 413 too large, 422 undecodable audio
//	             (deterministic), 500 inference failure, 503 model loading
//	             (retryable)
//	TTS: POST {ttsURL}/v1/tts
//	     JSON {"text","voice","speed":1.0,"stream":true} -> chunked raw PCM
//	     s16le 24 kHz mono (audio/L16;rate=24000;channels=1), one HTTP chunk
//	     per sentence segment. text <= 4000 chars (413), unknown voice (400).
//	     Mid-stream failure = connection ends early; consumers must tolerate
//	     truncated streams.
package voice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrBadAudio wraps errors that will repeat deterministically on retry:
// the sidecar rejected the request payload itself (HTTP 400/413/422) or
// the transcript body was undecodable. The pipeline maps these to a
// no-retry failure (STT) or a skip (TTS, which is best-effort regardless).
// Transport errors, 500, and 503 (model still loading) are left unwrapped
// so the caller treats them as retryable.
var ErrBadAudio = errors.New("voice sidecar rejected the request")

// maxTranscriptionBody caps the STT JSON response; the real thing is a few
// hundred bytes, so 1 MiB is generous headroom against a malfunctioning
// sidecar.
const maxTranscriptionBody = 1 << 20

// AudioMIME is the wire format of every TTS audio frame: raw PCM s16le,
// 24 kHz mono, no WAV header (Kokoro stream:true contract).
const AudioMIME = "audio/L16;rate=24000;channels=1"

// Client speaks to both sidecars. Safe for concurrent use; each call
// derives a timeout-bounded context from the caller's.
type Client struct {
	httpClient *http.Client
	sttURL     string
	ttsURL     string
	sttTimeout time.Duration
	ttsTimeout time.Duration
}

// New builds a client. sttURL/ttsURL are the sidecar base URLs (no path);
// either may be empty when the corresponding feature is disabled, in which
// case the matching method returns an error if still called. Timeouts must
// be positive; zero falls back to the config defaults (10 s STT, 20 s TTS).
func New(sttURL, ttsURL string, sttTimeout, ttsTimeout time.Duration) *Client {
	if sttTimeout <= 0 {
		sttTimeout = 10 * time.Second
	}
	if ttsTimeout <= 0 {
		ttsTimeout = 20 * time.Second
	}
	return &Client{
		httpClient: &http.Client{},
		sttURL:     strings.TrimRight(sttURL, "/"),
		ttsURL:     strings.TrimRight(ttsURL, "/"),
		sttTimeout: sttTimeout,
		ttsTimeout: ttsTimeout,
	}
}

// transcriptionResponse is the JSON body the whisper sidecar returns. Only
// the text is consumed; language/duration/segments are available if a
// future consumer wants them surfaced.
type transcriptionResponse struct {
	Text string `json:"text"`
}

// Transcribe sends one audio clip to the whisper sidecar and returns the
// transcript. format is the container hint from the prompt envelope: the
// sidecar only decodes WAV, so anything else fails fast as a deterministic
// bad request rather than burning a sidecar round trip.
func (c *Client) Transcribe(ctx context.Context, audio []byte, format string) (string, error) {
	if c.sttURL == "" {
		return "", fmt.Errorf("%w: STT sidecar URL not configured", ErrBadAudio)
	}
	if format != "" && !strings.EqualFold(format, "wav") {
		return "", fmt.Errorf("%w: sidecar only decodes WAV, got format %q", ErrBadAudio, format)
	}
	if len(audio) == 0 {
		return "", fmt.Errorf("%w: empty audio body", ErrBadAudio)
	}

	ctx, cancel := context.WithTimeout(ctx, c.sttTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.sttURL+"/v1/transcribe", bytes.NewReader(audio))
	if err != nil {
		return "", fmt.Errorf("build transcription request: %w", err)
	}
	req.Header.Set("Content-Type", "audio/wav")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("transcription request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxTranscriptionBody))
	if err != nil {
		return "", fmt.Errorf("read transcription response: %w", err)
	}
	if err := classifyStatus(resp.StatusCode, respBody); err != nil {
		return "", err
	}

	var parsed transcriptionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("%w: undecodable transcription body: %v", ErrBadAudio, err)
	}
	return parsed.Text, nil
}

// ttsRequest is the JSON body the Kokoro sidecar accepts. Speed is pinned
// at 1.0; surfacing it is a consumer-envelope decision for later.
type ttsRequest struct {
	Text   string  `json:"text"`
	Voice  string  `json:"voice"`
	Speed  float64 `json:"speed"`
	Stream bool    `json:"stream"`
}

// Synthesize starts a streaming synthesis and returns the PCM chunk stream.
// The caller must close the stream. The stream yields raw PCM s16le 24 kHz
// mono bytes (AudioMIME); per the contract it may end early without error
// if synthesis fails mid-stream - callers must treat a missing completion
// marker as truncation, not corruption.
func (c *Client) Synthesize(ctx context.Context, text, voice string) (stream io.ReadCloser, err error) {
	if c.ttsURL == "" {
		return nil, fmt.Errorf("%w: TTS sidecar URL not configured", ErrBadAudio)
	}

	body, err := json.Marshal(ttsRequest{Text: text, Voice: voice, Speed: 1.0, Stream: true})
	if err != nil {
		return nil, fmt.Errorf("build speech request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.ttsTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.ttsURL+"/v1/tts", bytes.NewReader(body))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build speech request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("speech request: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxTranscriptionBody))
		cancel()
		return nil, classifyStatus(resp.StatusCode, errBody)
	}
	// The cancel func must fire when the stream is closed so the timeout
	// context does not outlive the response body.
	return &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}, nil
}

// cancelOnClose ties the request context's lifetime to the response body.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// classifyStatus maps sidecar HTTP status codes onto the retry contract:
// 400/413/422 mean the request payload is at fault and will fail
// identically on retry (ErrBadAudio); 500/503 and transport errors are
// retryable (503 is "model not loaded yet" - the sidecar self-heals).
func classifyStatus(status int, body []byte) error {
	if status >= 200 && status < 300 {
		return nil
	}
	snippet := string(body)
	if len(snippet) > 200 {
		snippet = snippet[:200]
	}
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: status %d: %s", ErrBadAudio, status, snippet)
	default:
		return fmt.Errorf("voice sidecar status %d (retryable): %s", status, snippet)
	}
}

// Package ollama provides an HTTP client for the Ollama inference API.
package ollama

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// streamLineInitialBytes / streamLineMaxBytes size the NDJSON scanner.
// A per-token line is well under 1 KiB, but the terminal done=true object
// carries the full timing block and (for /api/chat) can be larger, so the
// ceiling is generous relative to bufio's 64 KiB default.
const (
	streamLineInitialBytes = 8 * 1024
	streamLineMaxBytes     = 1024 * 1024
)

// deterministicSeed is the fixed random seed sent to Ollama alongside
// temperature=0. Both the worker (original execution) and the disputer
// (re-execution) rely on greedy decoding being reproducible across services.
const deterministicSeed = 42

// Options mirrors the Ollama request "options" object. Only the knobs the
// worker needs are modelled. Every field is omitempty so an unset option
// leaves the server-side default untouched.
type Options struct {
	// NumPredict caps generated tokens. Ollama treats -1 as "unlimited"
	// and -2 as "fill context", both of which survive omitempty.
	NumPredict int `json:"num_predict,omitempty"`
	// NumCtx sets the context window in tokens. Zero omits the field.
	NumCtx int `json:"num_ctx,omitempty"`
	// Temperature overrides the model's sampling temperature. Sent as a
	// pointer so that an explicit 0 (fully deterministic) is
	// distinguishable from "not set".
	Temperature *float64 `json:"temperature,omitempty"`
	// Seed pins the sampler's RNG. Fixed rather than configurable: the
	// disputer re-executes a job and scores the result by similarity, so
	// an honest worker's output has to stay reproducible.
	Seed int `json:"seed,omitempty"`
}

// GenerateRequest is the JSON body sent to POST /api/generate.
type GenerateRequest struct {
	Model     string   `json:"model"`
	Prompt    string   `json:"prompt"`
	Stream    bool     `json:"stream"`
	KeepAlive any      `json:"keep_alive,omitempty"`
	Options   *Options `json:"options,omitempty"`
	// Think toggles a reasoning model's hidden thinking channel. Nil omits
	// the field. Ollama accepts it against non-reasoning models too, where
	// it is simply ignored, so it is safe to send unconditionally.
	Think *bool `json:"think,omitempty"`
	// Format constrains the output shape. "json" puts the model in JSON
	// mode; a JSON-schema object constrains it further. Needed so an agent
	// plan can be parsed rather than scraped out of prose with a regex.
	Format any `json:"format,omitempty"`
	// Images carries base64-encoded images for vision models on the
	// /api/generate path.
	Images []string `json:"images,omitempty"`
}

// GenerateResponse is the JSON body returned from POST /api/generate (stream=false).
type GenerateResponse struct {
	Model    string `json:"model"`
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

// ChatMessage is a single message in a chat conversation.
//
// Thinking carries a reasoning model's hidden chain of thought, which Ollama
// returns alongside — never inside — Content. It is deserialized so it can be
// streamed to the consumer on its own frame kind, and so an answer that
// arrives entirely as thinking can be detected rather than silently counted
// as an empty response. It is never sent back up in a request.
//
// Images holds base64-encoded image data for vision models. Without it a
// deployed vision model such as qwen3-vl-8b can only ever be given text.
type ChatMessage struct {
	Role     string   `json:"role"`
	Content  string   `json:"content"`
	Thinking string   `json:"thinking,omitempty"`
	Images   []string `json:"images,omitempty"`
}

// ChatRequest is the JSON body sent to POST /api/chat.
type ChatRequest struct {
	Model     string        `json:"model"`
	Messages  []ChatMessage `json:"messages"`
	Stream    bool          `json:"stream"`
	KeepAlive any           `json:"keep_alive,omitempty"`
	Options   *Options      `json:"options,omitempty"`
	Think     *bool         `json:"think,omitempty"`
	Format    any           `json:"format,omitempty"`
}

// ChatResponse is the JSON body returned from POST /api/chat (stream=false).
type ChatResponse struct {
	Model   string      `json:"model"`
	Message ChatMessage `json:"message"`
	Done    bool        `json:"done"`
}

// timings are the model's own measurements, present only on the terminal
// done=true object. Nanoseconds, as Ollama reports them.
type timings struct {
	TotalDuration      int64 `json:"total_duration"`
	LoadDuration       int64 `json:"load_duration"`
	PromptEvalCount    int   `json:"prompt_eval_count"`
	PromptEvalDuration int64 `json:"prompt_eval_duration"`
	EvalCount          int   `json:"eval_count"`
	EvalDuration       int64 `json:"eval_duration"`
}

// generateStreamLine is one NDJSON object from POST /api/generate with
// stream=true. Every object carries an incremental `response` delta; the
// last one sets done=true and adds the timing block.
type generateStreamLine struct {
	Response string `json:"response"`
	Thinking string `json:"thinking"`
	Done     bool   `json:"done"`
	Error    string `json:"error"`
	timings
}

// chatStreamLine is the /api/chat equivalent: the delta arrives in
// message.content instead of a top-level response field.
type chatStreamLine struct {
	Message ChatMessage `json:"message"`
	Done    bool        `json:"done"`
	Error   string      `json:"error"`
	timings
}

// TokenFunc is invoked once per token delta as Ollama emits it. Returning
// an error aborts the stream and surfaces from the calling
// GenerateStream/ChatStream.
type TokenFunc func(delta string) error

// StreamHandlers collects the per-delta callbacks for one streaming call.
//
// OnThinking exists because reasoning models spend a large share of their
// generation in the thinking channel: one measured job produced 962 tokens
// and surfaced roughly 320 of them, leaving the user watching a spinner
// through the rest. A nil OnThinking preserves the old behaviour of counting
// thinking bytes and discarding them.
type StreamHandlers struct {
	OnToken    TokenFunc
	OnThinking TokenFunc
}

// StreamStats reports what the model itself measured for one generation.
// Ollama sends these on the terminal object; the worker used to drop them,
// so the UI had no way to show tokens or throughput.
type StreamStats struct {
	PromptTokens       int
	EvalTokens         int
	ThinkingBytes      int
	LoadDuration       time.Duration
	PromptEvalDuration time.Duration
	EvalDuration       time.Duration
	TotalDuration      time.Duration
}

// TokensPerSecond is generation throughput, or 0 when the model reported no
// eval time (a fully prompt-cached response).
func (s StreamStats) TokensPerSecond() float64 {
	if s.EvalDuration <= 0 || s.EvalTokens == 0 {
		return 0
	}
	return float64(s.EvalTokens) / s.EvalDuration.Seconds()
}

func (t timings) stats(thinkingBytes int) StreamStats {
	return StreamStats{
		PromptTokens:       t.PromptEvalCount,
		EvalTokens:         t.EvalCount,
		ThinkingBytes:      thinkingBytes,
		LoadDuration:       time.Duration(t.LoadDuration),
		PromptEvalDuration: time.Duration(t.PromptEvalDuration),
		EvalDuration:       time.Duration(t.EvalDuration),
		TotalDuration:      time.Duration(t.TotalDuration),
	}
}

// ClientOptions carries the request-body knobs applied to every inference
// call. Zero values mean "omit the field and take the Ollama default", so
// the zero ClientOptions reproduces the pre-streaming request bodies byte
// for byte.
type ClientOptions struct {
	// KeepAlive is passed through as the request's keep_alive field.
	// "-1" pins the model in memory indefinitely, removing the model
	// reload that Ollama's 5-minute idle default otherwise imposes on
	// the first job after a quiet period.
	KeepAlive string
	// NumPredict bounds generated tokens so a runaway response cannot
	// consume the entire OLLAMA_TIMEOUT budget and fail the whole job.
	NumPredict int
	// NumCtx sets the context window. Zero leaves the model default.
	NumCtx int
	// Think controls the reasoning channel on models that have one. Nil
	// omits the field and takes the model's own default.
	//
	// Worth disabling: the worker only ever transmits Content, so thinking
	// tokens are generated, paid for by the consumer, and then discarded.
	// Worse, a model that spends its whole budget reasoning returns empty
	// Content, which settles on chain as an answer of zero bytes.
	Think *bool
	// Temperature overrides the model's sampling temperature when set.
	Temperature *float64
	// Format constrains output shape ("json", or a JSON schema object).
	// Nil omits the field.
	Format any
}

// ThinkSetting renders a ClientOptions.Think value.
func ThinkSetting(enabled bool) *bool {
	return &enabled
}

// TagsResponse is the JSON body returned from GET /api/tags.
type TagsResponse struct {
	Models []ModelInfo `json:"models"`
}

// ModelInfo describes a single loaded model in the Ollama server.
type ModelInfo struct {
	Name string `json:"name"`
}

// OllamaClient talks to the Ollama HTTP API for AI inference.
type OllamaClient struct {
	baseURL    string
	httpClient *http.Client
	opts       ClientOptions
}

// NewOllamaClient creates a client configured for the given Ollama server
// with no keep_alive and no options object â€” identical request bodies to
// the pre-streaming client. Callers that want those knobs (the sidecar,
// via config) use NewOllamaClientWithOptions.
func NewOllamaClient(baseURL string, timeout time.Duration) *OllamaClient {
	return NewOllamaClientWithOptions(baseURL, timeout, ClientOptions{})
}

// NewOllamaClientWithOptions creates a client that attaches keep_alive and
// an options object to every /api/generate and /api/chat request.
func NewOllamaClientWithOptions(baseURL string, timeout time.Duration, opts ClientOptions) *OllamaClient {
	return &OllamaClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		opts: opts,
	}
}

// requestOptions returns the options object to embed in a request body, or
// nil when nothing is configured so the field is omitted entirely.
func (c *OllamaClient) requestOptions() *Options {
	// Determinism is a protocol property here, not a tuning knob, so the
	// options object is always sent: an omitted temperature lets Ollama
	// fall back to its default of 0.8 and a random seed, which is what the
	// pre-merge testnet client existed to prevent.
	temperature := c.opts.Temperature
	if temperature == nil {
		greedy := 0.0
		temperature = &greedy
	}
	return &Options{
		NumPredict:  c.opts.NumPredict,
		NumCtx:      c.opts.NumCtx,
		Temperature: temperature,
		Seed:        deterministicSeed,
	}
}

// WithOverrides returns a shallow copy of the client whose per-request knobs
// are replaced. Used for one-off calls that need a different shape from the
// worker default - JSON mode for agent planning, or a caller-supplied
// temperature - without mutating the shared client.
func (c *OllamaClient) WithOverrides(mutate func(*ClientOptions)) *OllamaClient {
	next := *c
	opts := c.opts
	mutate(&opts)
	next.opts = opts
	return &next
}

// Options returns the client's current per-request knobs. Read-only access
// for audit surfaces (stage-5 log fields, the stats frame's appliedMaxTokens)
// that must report the values actually sent on the wire.
func (c *OllamaClient) Options() ClientOptions {
	return c.opts
}

// keepAlive renders the configured keep_alive for the request body. Ollama
// accepts either a duration string carrying a unit ("30m") or a bare number of
// seconds, where -1 pins the model indefinitely. A bare number sent as a JSON
// string is rejected with `time: missing unit in duration "-1"`, so integral
// values must be emitted as numbers.
func (c *OllamaClient) keepAlive() any {
	if c.opts.KeepAlive == "" {
		return nil
	}
	if n, err := strconv.ParseInt(c.opts.KeepAlive, 10, 64); err == nil {
		return n
	}
	return c.opts.KeepAlive
}

// Generate sends a prompt to the Ollama server and returns the full response.
// Uses stream=false mode for batch inference (DD-W-1).
func (c *OllamaClient) Generate(ctx context.Context, model, prompt string) (string, error) {
	reqBody := GenerateRequest{
		Model:     model,
		Prompt:    prompt,
		Stream:    false,
		KeepAlive: c.keepAlive(),
		Options:   c.requestOptions(),
		Think:     c.opts.Think,
		Format:    c.opts.Format,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal generate request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/generate", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create generate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama generate request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read generate response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama generate returned status %d: %s", resp.StatusCode, string(body))
	}

	var genResp GenerateResponse
	if err := json.Unmarshal(body, &genResp); err != nil {
		return "", fmt.Errorf("unmarshal generate response: %w", err)
	}
	if err := errEmptyGeneration(len(genResp.Response), 0); err != nil {
		return "", err
	}

	return genResp.Response, nil
}

// Chat sends a multi-turn conversation to the Ollama server and returns the assistant's response.
// Uses POST /api/chat with stream=false.
func (c *OllamaClient) Chat(ctx context.Context, model string, messages []ChatMessage) (string, error) {
	reqBody := ChatRequest{
		Model:     model,
		Messages:  messages,
		Stream:    false,
		KeepAlive: c.keepAlive(),
		Options:   c.requestOptions(),
		Think:     c.opts.Think,
		Format:    c.opts.Format,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal chat request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama chat request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read chat response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama chat returned status %d: %s", resp.StatusCode, string(body))
	}

	var chatResp ChatResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return "", fmt.Errorf("unmarshal chat response: %w", err)
	}
	if err := errEmptyGeneration(len(chatResp.Message.Content), len(chatResp.Message.Thinking)); err != nil {
		return "", err
	}

	return chatResp.Message.Content, nil
}

// GenerateStream is the stream=true form of Generate: onToken fires once per
// token delta as Ollama emits it, and the accumulated full response is
// returned when the stream terminates. The returned string is byte-identical
// to the concatenation of every delta handed to onToken, so a caller can
// stream deltas to the user while still encrypting the whole response once
// for settlement.
func (c *OllamaClient) GenerateStream(
	ctx context.Context,
	model, prompt string,
	images []string,
	h StreamHandlers,
) (string, StreamStats, error) {
	reqBody := GenerateRequest{
		Model:     model,
		Prompt:    prompt,
		Stream:    true,
		KeepAlive: c.keepAlive(),
		Options:   c.requestOptions(),
		Think:     c.opts.Think,
		Format:    c.opts.Format,
		Images:    images,
	}

	var full strings.Builder
	var thinking int
	var last timings
	err := c.stream(ctx, "generate", reqBody, func(line []byte) (bool, error) {
		var chunk generateStreamLine
		if err := json.Unmarshal(line, &chunk); err != nil {
			return false, fmt.Errorf("unmarshal generate stream line: %w", err)
		}
		if chunk.Error != "" {
			return false, fmt.Errorf("ollama generate stream error: %s", chunk.Error)
		}
		if chunk.Thinking != "" {
			thinking += len(chunk.Thinking)
			if h.OnThinking != nil {
				if err := h.OnThinking(chunk.Thinking); err != nil {
					return false, fmt.Errorf("generate thinking callback: %w", err)
				}
			}
		}
		if chunk.Response != "" {
			// Only the answer accumulates into `full`. That string becomes
			// the settlement ciphertext, so letting reasoning into it would
			// change the hash completeJob commits to.
			full.WriteString(chunk.Response)
			if h.OnToken != nil {
				if err := h.OnToken(chunk.Response); err != nil {
					return false, fmt.Errorf("generate token callback: %w", err)
				}
			}
		}
		if chunk.Done {
			last = chunk.timings
		}
		return chunk.Done, nil
	})
	if err != nil {
		return "", StreamStats{}, err
	}
	stats := last.stats(thinking)
	if err := errEmptyGeneration(full.Len(), thinking); err != nil {
		return "", stats, err
	}
	return full.String(), stats, nil
}

// errEmptyGeneration turns a generation that produced no answer into an
// error. Returning it empty would settle on chain as a zero-byte response:
// the consumer is charged, the relay publishes nothing, and the browser
// renders a blank message. Failing here instead lets the job retry.
//
// Every returned error wraps ErrEmptyGeneration so the pipeline can mark the
// failure no-retry: the request parameters are identical on every attempt,
// so a retry reproduces the same empty generation deterministically and only
// delays the keeper refund.
//
// thinkingBytes separates the two ways this happens, because they need
// different fixes: a model that spent its whole budget reasoning wants
// think=false or a larger num_predict, whereas a model that emitted nothing
// at all is a model or template problem.
func errEmptyGeneration(contentBytes, thinkingBytes int) error {
	if contentBytes > 0 {
		return nil
	}
	if thinkingBytes > 0 {
		return fmt.Errorf(
			"%w: the whole generation arrived as thinking (%d bytes), which is never transmitted",
			ErrEmptyGeneration,
			thinkingBytes,
		)
	}
	return ErrEmptyGeneration
}

// ErrEmptyGeneration marks a generation that produced zero answer bytes.
// Retrying with identical parameters reproduces it deterministically, so
// callers should treat it as no-retry (the pipeline maps it to
// asynq.SkipRetry).
var ErrEmptyGeneration = errors.New("ollama returned an empty response")

// ChatStream is the stream=true form of Chat. Deltas arrive in
// message.content rather than a top-level response field; otherwise the
// contract matches GenerateStream.
func (c *OllamaClient) ChatStream(
	ctx context.Context,
	model string,
	messages []ChatMessage,
	h StreamHandlers,
) (string, StreamStats, error) {
	reqBody := ChatRequest{
		Model:     model,
		Messages:  messages,
		Stream:    true,
		KeepAlive: c.keepAlive(),
		Options:   c.requestOptions(),
		Think:     c.opts.Think,
		Format:    c.opts.Format,
	}

	var full strings.Builder
	var thinking int
	var last timings
	err := c.stream(ctx, "chat", reqBody, func(line []byte) (bool, error) {
		var chunk chatStreamLine
		if err := json.Unmarshal(line, &chunk); err != nil {
			return false, fmt.Errorf("unmarshal chat stream line: %w", err)
		}
		if chunk.Error != "" {
			return false, fmt.Errorf("ollama chat stream error: %s", chunk.Error)
		}
		if chunk.Message.Thinking != "" {
			thinking += len(chunk.Message.Thinking)
			if h.OnThinking != nil {
				if err := h.OnThinking(chunk.Message.Thinking); err != nil {
					return false, fmt.Errorf("chat thinking callback: %w", err)
				}
			}
		}
		if chunk.Message.Content != "" {
			full.WriteString(chunk.Message.Content)
			if h.OnToken != nil {
				if err := h.OnToken(chunk.Message.Content); err != nil {
					return false, fmt.Errorf("chat token callback: %w", err)
				}
			}
		}
		if chunk.Done {
			last = chunk.timings
		}
		return chunk.Done, nil
	})
	if err != nil {
		return "", StreamStats{}, err
	}
	stats := last.stats(thinking)
	if err := errEmptyGeneration(full.Len(), thinking); err != nil {
		return "", stats, err
	}
	return full.String(), stats, nil
}

// stream POSTs reqBody to /api/{name} and feeds each NDJSON line to onLine
// until onLine reports the terminal done=true object.
//
// A body that ends before done=true is reported as an error rather than
// silently returning a partial answer: the caller anchors the full response
// on-chain, so a truncated generation must fail the job and retry instead of
// committing half an answer.
func (c *OllamaClient) stream(ctx context.Context, name string, reqBody any, onLine func([]byte) (bool, error)) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", name, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/"+name, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create %s request: %w", name, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama %s request: %w", name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama %s returned status %d: %s", name, resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, streamLineInitialBytes), streamLineMaxBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		done, err := onLine(line)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s stream: %w", name, err)
	}
	return fmt.Errorf("ollama %s stream ended before done", name)
}

// VerifyModels checks that all required models are loaded on the Ollama server.
// Returns an error listing any missing models.
func (c *OllamaClient) VerifyModels(ctx context.Context, models []string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("create tags request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama tags request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read tags response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama tags returned status %d: %s", resp.StatusCode, string(body))
	}

	var tagsResp TagsResponse
	if err := json.Unmarshal(body, &tagsResp); err != nil {
		return fmt.Errorf("unmarshal tags response: %w", err)
	}

	loaded := make(map[string]bool, len(tagsResp.Models))
	for _, m := range tagsResp.Models {
		loaded[m.Name] = true
		// Also register without the ":latest" tag so "llama3-8b" matches "llama3-8b:latest".
		if base, ok := strings.CutSuffix(m.Name, ":latest"); ok {
			loaded[base] = true
		}
	}

	var missing []string
	for _, required := range models {
		if !loaded[required] {
			missing = append(missing, required)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing models on Ollama server: %v", missing)
	}

	return nil
}

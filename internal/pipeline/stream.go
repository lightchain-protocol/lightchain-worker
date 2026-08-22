package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"strings"
	"time"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/ollama"
)

// tokenCoalescer batches token deltas from a streaming inference call into
// chunk-sized payloads. A batch is emitted when either maxTokens deltas have
// accumulated or maxInterval has elapsed since the batch's first delta,
// whichever comes first.
//
// Coalescing exists for two reasons: the relay enforces a per-consumer rate
// limit (~1000 messages/minute by default) that a per-token frame would
// approach at high generation rates, and every frame costs one AES-GCM
// encryption plus one Redis PUBLISH.
//
// The clock is only consulted when a delta arrives, so a batch that is
// waiting on the interval is emitted with the next delta rather than by a
// background timer. At the measured ~10 tok/s that adds at most one
// inter-token gap (~100ms) to the deadline; the tradeoff buys a coalescer
// with no goroutine, no lock, and no chance of interleaving a timer-driven
// flush with the read loop's flush (which would reorder Sequence numbers).
//
// Not safe for concurrent use â€” drive it from a single reader goroutine.
type tokenCoalescer struct {
	maxTokens   int
	maxInterval time.Duration
	// now is injectable so tests can drive the interval deterministically.
	now   func() time.Time
	emit  func(batch string) error
	buf   strings.Builder
	count int
	// batchStart is the arrival time of the first delta in the current
	// batch. Meaningless while count == 0.
	batchStart time.Time
}

// newTokenCoalescer builds a coalescer that calls emit once per batch.
// Non-positive maxTokens or maxInterval fall back to the documented
// defaults so a misconfigured knob degrades to sane batching rather than
// emitting one frame per token.
func newTokenCoalescer(maxTokens int, maxInterval time.Duration, emit func(batch string) error) *tokenCoalescer {
	if maxTokens <= 0 {
		maxTokens = defaultStreamChunkTokens
	}
	if maxInterval <= 0 {
		maxInterval = defaultStreamChunkInterval
	}
	return &tokenCoalescer{
		maxTokens:   maxTokens,
		maxInterval: maxInterval,
		now:         time.Now,
		emit:        emit,
	}
}

// Defaults mirror config.StreamChunkTokens / config.StreamChunkInterval.
// They exist here so a JobHandler built without stream config (tests, the
// CLI) still batches sensibly.
const (
	defaultStreamChunkTokens   = 8
	defaultStreamChunkInterval = 250 * time.Millisecond
)

// Add appends a delta and emits the batch if either threshold is now met.
// Empty deltas are ignored so they cannot start a batch whose interval then
// expires with nothing to send.
func (c *tokenCoalescer) Add(delta string) error {
	if delta == "" {
		return nil
	}
	if c.count == 0 {
		c.batchStart = c.now()
	}
	c.buf.WriteString(delta)
	c.count++

	if c.count >= c.maxTokens || c.now().Sub(c.batchStart) >= c.maxInterval {
		return c.Flush()
	}
	return nil
}

// Flush emits any buffered deltas and resets the batch. A no-op when the
// buffer is empty, so it is safe to call unconditionally at end of stream.
func (c *tokenCoalescer) Flush() error {
	if c.count == 0 {
		return nil
	}
	batch := c.buf.String()
	c.buf.Reset()
	c.count = 0
	return c.emit(batch)
}

// chunkStreamer encrypts each coalesced batch of deltas under the session
// key and publishes it as an in-flight `chunk` frame. One instance per job;
// it owns that job's Sequence counter, which starts at 1.
//
// The deltas it encrypts are throwaway from the protocol's point of view:
// each pkgcrypto.Encrypt mints a fresh 12-byte nonce and the wire format is
// nonce||ciphertext||tag, so per-chunk ciphertexts cannot be concatenated
// and decrypted as one message. Settlement therefore continues to use the
// single full-response ciphertext built in stage 6, unchanged.
//
// A response can carry several content kinds - visible answer, reasoning,
// artifact descriptors - so the streamer keeps one coalescer per kind but a
// single Sequence counter across all of them. Sharing the counter keeps the
// relay's gap detection and the terminal frame's totalFrames accounting
// intact; the consumer demultiplexes on Kind instead.
type chunkStreamer struct {
	client     StreamingInferenceClient
	publisher  ResponsePublisher
	ctx        context.Context
	logger     *slog.Logger
	jobID      uint64
	sessionID  uint64
	corrID     string
	sessionKey []byte
	seq        uint32

	// One coalescer per kind, built lazily. Batching text and reasoning
	// separately is required for correctness, not just tidiness: a single
	// buffer would concatenate a thinking delta onto an answer delta and
	// ship them under one kind.
	coalescers map[pkgtypes.FrameKind]*tokenCoalescer
	// lastKind is the kind of the most recent delta. A change flushes the
	// previous kind's buffer so frames reach the consumer in the order the
	// model produced them.
	lastKind pkgtypes.FrameKind

	maxTokens   int
	maxInterval time.Duration

	// started is when generation began, so the first published chunk can
	// report time to first token.
	started time.Time
	// sawText records whether any visible-answer frame has gone out, so
	// TTFT is reported against the answer rather than against reasoning
	// the user may have collapsed.
	sawText bool
}

// newChunkStreamer returns a streamer for this job, or nil when incremental
// delivery is not possible. Nil means "run the batch inference path", which
// is the exact pre-streaming behaviour. Streaming is skipped when it is
// disabled by config, when the caller forbids it (a retry that already
// delivered a terminal frame), when there is no publisher, when the
// publisher cannot emit distinct chunk frames, or when the inference client
// has no streaming API.
func (h *JobHandler) newChunkStreamer(
	ctx context.Context,
	logger *slog.Logger,
	p JobPayload,
	sessionKey []byte,
	allowed bool,
) *chunkStreamer {
	if !allowed || !h.cfg.StreamEnabled {
		return nil
	}
	if h.responsePublisher == nil || !h.responsePublisher.SupportsChunks() {
		return nil
	}
	client, ok := h.ollamaClient.(StreamingInferenceClient)
	if !ok {
		return nil
	}
	return &chunkStreamer{
		client:      client,
		publisher:   h.responsePublisher,
		ctx:         ctx,
		logger:      logger,
		jobID:       p.JobID,
		sessionID:   p.SessionID,
		corrID:      p.CorrelationID,
		sessionKey:  sessionKey,
		coalescers:  make(map[pkgtypes.FrameKind]*tokenCoalescer, 2),
		maxTokens:   h.cfg.StreamChunkTokens,
		maxInterval: h.cfg.StreamChunkInterval,
		started:     time.Now(),
	}
}

// coalescerFor returns the batcher for a kind, building it on first use.
func (s *chunkStreamer) coalescerFor(kind pkgtypes.FrameKind) *tokenCoalescer {
	kind = kind.Normalize()
	if c, ok := s.coalescers[kind]; ok {
		return c
	}
	c := newTokenCoalescer(s.maxTokens, s.maxInterval, func(batch string) error {
		return s.publish(kind, batch)
	})
	s.coalescers[kind] = c
	return c
}

// Add buffers a delta of the given kind. Switching kinds flushes whatever the
// previous kind had buffered first, so the consumer sees frames in generation
// order rather than having a half-finished reasoning batch arrive after the
// answer has started.
func (s *chunkStreamer) Add(kind pkgtypes.FrameKind, delta string) error {
	if s == nil || delta == "" {
		return nil
	}
	kind = kind.Normalize()
	if s.lastKind != "" && s.lastKind != kind {
		if err := s.coalescerFor(s.lastKind).Flush(); err != nil {
			return err
		}
	}
	s.lastKind = kind
	return s.coalescerFor(kind).Add(delta)
}

// FlushAll drains every kind's buffer at end of stream. Text goes last so the
// final visible token is the last thing published before the terminal frame.
func (s *chunkStreamer) FlushAll() error {
	if s == nil {
		return nil
	}
	for _, kind := range []pkgtypes.FrameKind{pkgtypes.FrameKindReasoning, pkgtypes.FrameKindArtifact, pkgtypes.FrameKindAudio, pkgtypes.FrameKindText} {
		if c, ok := s.coalescers[kind]; ok {
			if err := c.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// generationStats is the wire shape of a FrameKindStats payload. Durations
// are milliseconds so the consumer does not have to know Ollama reports
// nanoseconds.
type generationStats struct {
	PromptTokens    int     `json:"promptTokens"`
	EvalTokens      int     `json:"evalTokens"`
	ThinkingBytes   int     `json:"thinkingBytes,omitempty"`
	TokensPerSecond float64 `json:"tokensPerSecond"`
	LoadMs          int64   `json:"loadMs,omitempty"`
	PromptEvalMs    int64   `json:"promptEvalMs"`
	EvalMs          int64   `json:"evalMs"`
	TotalMs         int64   `json:"totalMs"`
}

// recordStats publishes the model's own measurements on their own frame kind.
// Best-effort and nil-safe: a response that reached the user without its
// throughput badge is a cosmetic loss, not a failed job.
func (s *chunkStreamer) recordStats(st ollama.StreamStats) {
	if s == nil || st.EvalTokens == 0 {
		return
	}
	payload, err := json.Marshal(generationStats{
		PromptTokens:    st.PromptTokens,
		EvalTokens:      st.EvalTokens,
		ThinkingBytes:   st.ThinkingBytes,
		TokensPerSecond: math.Round(st.TokensPerSecond()*10) / 10,
		LoadMs:          st.LoadDuration.Milliseconds(),
		PromptEvalMs:    st.PromptEvalDuration.Milliseconds(),
		EvalMs:          st.EvalDuration.Milliseconds(),
		TotalMs:         st.TotalDuration.Milliseconds(),
	})
	if err != nil {
		return
	}
	if err := s.PublishBytes(pkgtypes.FrameKindStats, payload); err != nil {
		s.logger.Warn("failed to publish generation stats",
			"stage", "inference",
			"jobID", s.jobID,
			"error", err,
		)
	}
}

// PublishBytes emits a single frame of an already-complete payload, bypassing
// coalescing. Used for structured payloads such as artifact descriptors, where
// batching would corrupt the JSON rather than merely delay it.
func (s *chunkStreamer) PublishBytes(kind pkgtypes.FrameKind, payload []byte) error {
	if s == nil || len(payload) == 0 {
		return nil
	}
	if s.lastKind != "" {
		if err := s.coalescerFor(s.lastKind).Flush(); err != nil {
			return err
		}
	}
	return s.publish(kind, string(payload))
}

// frames reports how many chunk frames have been published. Nil-safe so the
// caller can read it without branching on whether streaming was active.
func (s *chunkStreamer) frames() uint32 {
	if s == nil {
		return 0
	}
	return s.seq
}

// publish encrypts one coalesced batch and emits it as the next chunk
// frame. Always returns nil: incremental delivery is best-effort in exactly
// the same way stage 7 is, and a publish problem must never abort a
// generation whose full response is still headed on-chain.
func (s *chunkStreamer) publish(kind pkgtypes.FrameKind, batch string) error {
	if batch == "" {
		return nil
	}
	kind = kind.Normalize()
	ciphertext, err := pkgcrypto.Encrypt(s.sessionKey, []byte(batch))
	if err != nil {
		s.logger.Warn("failed to encrypt token chunk; dropping frame",
			"stage", "inference",
			"jobID", s.jobID,
			"kind", kind,
			"error", err,
		)
		return nil
	}
	s.seq++
	if s.seq == 1 {
		// First frame of any kind - the moment the spinner stops and the
		// user sees something happening.
		s.logger.Info("stage 5: first chunk published",
			"stage", "inference",
			"path", "ttft",
			"kind", kind,
			"ttftMs", time.Since(s.started).Milliseconds(),
		)
	}
	if kind == pkgtypes.FrameKindText && !s.sawText {
		s.sawText = true
		// Time to first *answer* token, reported separately because a
		// reasoning model can stream thought for many seconds before the
		// answer begins and the two latencies mean different things.
		s.logger.Info("stage 5: first text chunk published",
			"stage", "inference",
			"path", "ttft_text",
			"ttftTextMs", time.Since(s.started).Milliseconds(),
		)
	}
	s.publisher.PublishChunk(s.ctx, s.jobID, s.sessionID, s.corrID, s.seq, kind, ciphertext)
	return nil
}

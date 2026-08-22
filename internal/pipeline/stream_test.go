package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/ollama"
)

// fakeClock lets the coalescer tests drive the interval deterministically
// instead of sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestCoalescer builds a coalescer wired to a fake clock and returns the
// clock plus a pointer to the slice that collects emitted batches.
func newTestCoalescer(maxTokens int, maxInterval time.Duration) (*tokenCoalescer, *fakeClock, *[]string) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	batches := &[]string{}
	c := newTokenCoalescer(maxTokens, maxInterval, func(batch string) error {
		*batches = append(*batches, batch)
		return nil
	})
	c.now = clock.now
	return c, clock, batches
}

func TestTokenCoalescer_FlushesOnTokenCount(t *testing.T) {
	t.Parallel()

	// A one-hour window guarantees only the token threshold can fire.
	c, _, batches := newTestCoalescer(3, time.Hour)

	for _, tok := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		require.NoError(t, c.Add(tok))
	}
	assert.Equal(t, []string{"abc", "def"}, *batches,
		"a batch must be emitted every 3 tokens")

	require.NoError(t, c.Flush())
	assert.Equal(t, []string{"abc", "def", "g"}, *batches,
		"Flush must emit the partial trailing batch")
}

func TestTokenCoalescer_FlushesOnInterval(t *testing.T) {
	t.Parallel()

	// A very high token threshold guarantees only the interval can fire.
	c, clock, batches := newTestCoalescer(1000, 250*time.Millisecond)

	require.NoError(t, c.Add("a"))
	require.NoError(t, c.Add("b"))
	assert.Empty(t, *batches, "nothing due yet: 2 tokens, no time elapsed")

	// The deadline is measured from the first token of the batch, so
	// crossing it on the next token's arrival flushes everything buffered.
	clock.advance(250 * time.Millisecond)
	require.NoError(t, c.Add("c"))
	assert.Equal(t, []string{"abc"}, *batches)

	// The window restarts with the next batch's first token.
	require.NoError(t, c.Add("d"))
	assert.Equal(t, []string{"abc"}, *batches)
	clock.advance(300 * time.Millisecond)
	require.NoError(t, c.Add("e"))
	assert.Equal(t, []string{"abc", "de"}, *batches)
}

func TestTokenCoalescer_WhicheverThresholdComesFirst(t *testing.T) {
	t.Parallel()

	c, clock, batches := newTestCoalescer(4, 250*time.Millisecond)

	// Fast tokens: the count threshold wins.
	for _, tok := range []string{"1", "2", "3", "4"} {
		clock.advance(10 * time.Millisecond)
		require.NoError(t, c.Add(tok))
	}
	require.Equal(t, []string{"1234"}, *batches)

	// Slow tokens: the interval wins before 4 accumulate.
	require.NoError(t, c.Add("5"))
	clock.advance(400 * time.Millisecond)
	require.NoError(t, c.Add("6"))
	assert.Equal(t, []string{"1234", "56"}, *batches)
}

func TestTokenCoalescer_FlushIsNoOpWhenEmpty(t *testing.T) {
	t.Parallel()

	c, _, batches := newTestCoalescer(8, 250*time.Millisecond)

	require.NoError(t, c.Flush())
	require.NoError(t, c.Flush())
	assert.Empty(t, *batches, "an empty buffer must never produce a frame")
}

func TestTokenCoalescer_IgnoresEmptyDeltas(t *testing.T) {
	t.Parallel()

	// An empty delta must not open a batch — otherwise the interval could
	// expire on a batch with no bytes and emit a zero-length frame.
	c, clock, batches := newTestCoalescer(1000, 250*time.Millisecond)

	require.NoError(t, c.Add(""))
	clock.advance(time.Second)
	require.NoError(t, c.Add(""))
	assert.Empty(t, *batches)

	require.NoError(t, c.Add("x"))
	require.NoError(t, c.Flush())
	assert.Equal(t, []string{"x"}, *batches)
}

func TestTokenCoalescer_PreservesTokenOrderAndContent(t *testing.T) {
	t.Parallel()

	c, _, batches := newTestCoalescer(8, time.Hour)

	var want string
	for i := 0; i < 50; i++ {
		tok := fmt.Sprintf("tok%d ", i)
		want += tok
		require.NoError(t, c.Add(tok))
	}
	require.NoError(t, c.Flush())

	var got string
	for _, b := range *batches {
		got += b
	}
	assert.Equal(t, want, got,
		"concatenated batches must reproduce the token stream exactly")
}

func TestNewTokenCoalescer_NonPositiveConfigFallsBackToDefaults(t *testing.T) {
	t.Parallel()

	c := newTokenCoalescer(0, 0, func(string) error { return nil })
	assert.Equal(t, defaultStreamChunkTokens, c.maxTokens)
	assert.Equal(t, defaultStreamChunkInterval, c.maxInterval)
}


// --- multi-kind streaming ---

// capturingPublisher records every chunk frame the streamer emits, in order,
// so a test can assert both the kind routing and the shared sequence.
type capturingPublisher struct {
	frames []capturedFrame
}

type capturedFrame struct {
	seq  uint32
	kind pkgtypes.FrameKind
	body string
}

func (p *capturingPublisher) PublishResponse(context.Context, uint64, uint64, string, string, []byte, uint32) {
}

func (p *capturingPublisher) PublishChunk(
	_ context.Context,
	_, _ uint64,
	_ string,
	sequence uint32,
	kind pkgtypes.FrameKind,
	ciphertext []byte,
) {
	plain, err := pkgcrypto.Decrypt(testStreamKey, ciphertext)
	if err != nil {
		panic(err)
	}
	p.frames = append(p.frames, capturedFrame{seq: sequence, kind: kind, body: string(plain)})
}

func (p *capturingPublisher) SupportsChunks() bool { return true }

var testStreamKey = bytes.Repeat([]byte{7}, 32)

func newTestStreamer(t *testing.T) (*chunkStreamer, *capturingPublisher) {
	t.Helper()
	pub := &capturingPublisher{}
	return &chunkStreamer{
		publisher:  pub,
		ctx:        context.Background(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobID:      1,
		sessionID:  2,
		sessionKey: testStreamKey,
		coalescers: make(map[pkgtypes.FrameKind]*tokenCoalescer, 2),
		// One token per batch keeps the assertions about ordering free of
		// coalescing effects.
		maxTokens:   1,
		maxInterval: time.Hour,
		started:     time.Now(),
	}, pub
}

// Reasoning and answer deltas must land on separate frames with distinct
// kinds. Concatenating them would ship a model's chain of thought as if it
// were the answer.
func TestChunkStreamer_SeparatesKinds(t *testing.T) {
	t.Parallel()

	s, pub := newTestStreamer(t)
	require.NoError(t, s.Add(pkgtypes.FrameKindReasoning, "thinking"))
	require.NoError(t, s.Add(pkgtypes.FrameKindText, "answer"))
	require.NoError(t, s.FlushAll())

	require.Len(t, pub.frames, 2)
	assert.Equal(t, pkgtypes.FrameKindReasoning, pub.frames[0].kind)
	assert.Equal(t, "thinking", pub.frames[0].body)
	assert.Equal(t, pkgtypes.FrameKindText, pub.frames[1].kind)
	assert.Equal(t, "answer", pub.frames[1].body)
}

// Sequence is global across kinds so the relay's gap detection and the
// terminal frame's totalFrames accounting keep working.
func TestChunkStreamer_SequenceIsSharedAcrossKinds(t *testing.T) {
	t.Parallel()

	s, pub := newTestStreamer(t)
	require.NoError(t, s.Add(pkgtypes.FrameKindReasoning, "a"))
	require.NoError(t, s.Add(pkgtypes.FrameKindText, "b"))
	require.NoError(t, s.Add(pkgtypes.FrameKindReasoning, "c"))
	require.NoError(t, s.FlushAll())

	var seqs []uint32
	for _, f := range pub.frames {
		seqs = append(seqs, f.seq)
	}
	assert.Equal(t, []uint32{1, 2, 3}, seqs)
	assert.Equal(t, uint32(3), s.frames())
}

// Switching kinds mid-batch must flush the previous kind first, or a partially
// buffered reasoning batch would arrive after the answer it preceded.
func TestChunkStreamer_KindSwitchFlushesInOrder(t *testing.T) {
	t.Parallel()

	s, pub := newTestStreamer(t)
	s.maxTokens = 8 // wide enough that only the kind switch can flush
	require.NoError(t, s.Add(pkgtypes.FrameKindReasoning, "think1"))
	require.NoError(t, s.Add(pkgtypes.FrameKindReasoning, "think2"))
	require.NoError(t, s.Add(pkgtypes.FrameKindText, "ans"))
	require.NoError(t, s.FlushAll())

	require.Len(t, pub.frames, 2)
	assert.Equal(t, pkgtypes.FrameKindReasoning, pub.frames[0].kind)
	assert.Equal(t, "think1think2", pub.frames[0].body)
	assert.Equal(t, pkgtypes.FrameKindText, pub.frames[1].kind)
}

// Stats ride their own frame kind rather than being folded into the answer.
func TestChunkStreamer_RecordStatsEmitsItsOwnFrame(t *testing.T) {
	t.Parallel()

	s, pub := newTestStreamer(t)
	s.recordStats(ollama.StreamStats{
		PromptTokens: 7,
		EvalTokens:   40,
		EvalDuration: time.Second,
	})

	require.Len(t, pub.frames, 1)
	assert.Equal(t, pkgtypes.FrameKindStats, pub.frames[0].kind)

	var got generationStats
	require.NoError(t, json.Unmarshal([]byte(pub.frames[0].body), &got))
	assert.Equal(t, 40, got.EvalTokens)
	assert.Equal(t, 7, got.PromptTokens)
	assert.InDelta(t, 40.0, got.TokensPerSecond, 0.01)
}

// A generation that produced nothing has no throughput worth reporting.
func TestChunkStreamer_RecordStatsSkipsEmptyGeneration(t *testing.T) {
	t.Parallel()

	s, pub := newTestStreamer(t)
	s.recordStats(ollama.StreamStats{})
	assert.Empty(t, pub.frames)
}

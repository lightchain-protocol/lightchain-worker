package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	"github.com/lightchain/pkg/searchaug"
	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
	"github.com/lightchain/worker/internal/search"
)

// recordingPublisher implements ResponsePublisher and records the wall-clock
// time at which PublishMetadata is called. This lets ordering tests assert
// that metadata is published after inference completes. It also records chunk
// sequences for TestStage5_StreamsChunks.
type recordingPublisher struct {
	mu             sync.Mutex
	metadataCalled bool
	metadataAt     time.Time
	responseCalled bool
	responseAt     time.Time
	chunkSeqs      []uint32
}

func (r *recordingPublisher) PublishMetadata(_ context.Context, _, _ uint64, _ string, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metadataCalled = true
	r.metadataAt = time.Now()
}

func (r *recordingPublisher) PublishResponse(_ context.Context, _, _ uint64, _ string, _ string, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.responseCalled = true
	r.responseAt = time.Now()
}

func (r *recordingPublisher) PublishChunk(_ context.Context, _, _ uint64, _ string, seq uint32, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunkSeqs = append(r.chunkSeqs, seq)
}

// trackingOllama wraps mockOllama and records when Generate/Chat complete.
type trackingOllama struct {
	inner      *mockOllama
	mu         sync.Mutex
	completedAt time.Time
}

func (t *trackingOllama) Generate(ctx context.Context, model, prompt string) (string, error) {
	resp, err := t.inner.Generate(ctx, model, prompt)
	t.mu.Lock()
	t.completedAt = time.Now()
	t.mu.Unlock()
	return resp, err
}

func (t *trackingOllama) Chat(ctx context.Context, model string, messages []ollama.ChatMessage) (string, error) {
	resp, err := t.inner.Chat(ctx, model, messages)
	t.mu.Lock()
	t.completedAt = time.Now()
	t.mu.Unlock()
	return resp, err
}

func (t *trackingOllama) GenerateStream(ctx context.Context, model, prompt string, onDelta func(string)) (string, error) {
	resp, err := t.inner.GenerateStream(ctx, model, prompt, onDelta)
	t.mu.Lock()
	t.completedAt = time.Now()
	t.mu.Unlock()
	return resp, err
}

func (t *trackingOllama) ChatStream(ctx context.Context, model string, messages []ollama.ChatMessage, onDelta func(string)) (string, error) {
	resp, err := t.inner.ChatStream(ctx, model, messages, onDelta)
	t.mu.Lock()
	t.completedAt = time.Now()
	t.mu.Unlock()
	return resp, err
}

// fakeSearcher satisfies search.Searcher and returns a fixed set of sources.
type fakeSearcher struct {
	sources []search.Source
	err     error
}

func (f *fakeSearcher) Search(_ context.Context, _ string, _ int) ([]search.Source, error) {
	return f.sources, f.err
}

// fakeStreamInference implements InferenceClient with configurable per-delta output
// so TestStage5_StreamsChunks can control exactly which deltas are emitted.
type fakeStreamInference struct {
	deltas []string
}

func (f *fakeStreamInference) Generate(_ context.Context, _, _ string) (string, error) {
	return strings.Join(f.deltas, ""), nil
}

func (f *fakeStreamInference) Chat(_ context.Context, _ string, _ []ollama.ChatMessage) (string, error) {
	return strings.Join(f.deltas, ""), nil
}

func (f *fakeStreamInference) GenerateStream(_ context.Context, _, _ string, onDelta func(string)) (string, error) {
	var full strings.Builder
	for _, d := range f.deltas {
		onDelta(d)
		full.WriteString(d)
	}
	return full.String(), nil
}

func (f *fakeStreamInference) ChatStream(_ context.Context, _ string, _ []ollama.ChatMessage, onDelta func(string)) (string, error) {
	return f.GenerateStream(context.Background(), "", "", onDelta)
}

func twoSources() []search.Source {
	return []search.Source{
		{Position: 1, Title: "Result One", URL: "https://example.com/1", Snippet: "first snippet"},
		{Position: 2, Title: "Result Two", URL: "https://example.com/2", Snippet: "second snippet"},
	}
}

// TestStage45_MetadataEmittedAfterInference asserts that the web-search
// citation frame (PublishMetadata) is sent AFTER inference completes, not
// before. This is the ordering the UI requires so Sources appear beneath
// the answer.
func TestStage45_MetadataEmittedAfterInference(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, "What is the capital of France?")

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	inner := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "Paris", nil
	}}
	tracker := &trackingOllama{inner: inner}
	rec := &recordingPublisher{}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), tracker,
		nil, // no real Redis — publisher injected directly
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:     5 * time.Second,
			BlobTxTimeout:    60 * time.Second,
			SearchMaxResults: 3,
		},
		rec,   // injected recording publisher
		nil,   // no checkpoints
		testMetrics(t),
		metrics.DeliveryAsynq,
		&fakeSearcher{sources: twoSources()},
	)

	payload := testPayload(t)
	payload.SearchEnabled = true

	// Encrypt the session key into the keystore so stage 3 succeeds without chain
	ks := newMockKeyStore()
	ks.keys[payload.SessionID] = sessionKey
	handler.keyStore = ks

	blobData, err := pkgcrypto.Encrypt(sessionKey, []byte("What is the capital of France?"))
	require.NoError(t, err)

	_, err = handler.runInferencePipeline(
		context.Background(),
		logger,
		payload,
		blobData,
		"llama3-8b",
		metrics.DeliveryAsynq,
	)
	require.NoError(t, err)

	require.True(t, rec.metadataCalled, "sources must be published")
	require.False(t, tracker.completedAt.IsZero(), "inference must have been called")

	assert.True(t,
		!rec.metadataAt.Before(tracker.completedAt),
		"PublishMetadata (at %v) must not happen before inference completed (at %v)",
		rec.metadataAt, tracker.completedAt,
	)
}

func TestSourcesMetadataPayload_ShapeMatchesFrontend(t *testing.T) {
	sources := []searchaug.Source{{Position: 1, Title: "T1", URL: "https://a", Snippet: "s1"}}
	payload := sourcesMetadataJSON(sources)
	var decoded struct {
		Type    string `json:"type"`
		Sources []struct {
			Position int    `json:"position"`
			Title    string `json:"title"`
			URL      string `json:"url"`
			Snippet  string `json:"snippet"`
		} `json:"sources"`
	}
	require.NoError(t, json.Unmarshal(payload, &decoded))
	assert.Equal(t, "webSearchSources", decoded.Type)
	require.Len(t, decoded.Sources, 1)
	assert.Equal(t, "https://a", decoded.Sources[0].URL)
}

// TestStage5_StreamsChunks asserts that stage 5 publishes chunk frames in monotonically
// increasing sequence order and that the returned ciphertext decrypts to the full text.
func TestStage5_StreamsChunks(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)

	// deltas chosen so exactly two flushes happen:
	//   delta 1: "Hello, streaming world!!" = 24 bytes → buffer reaches threshold (>=24) → mid-stream flush (seq=1)
	//   delta 2: "Done" = 4 bytes → buffer = 4, below threshold, not flushed mid-stream
	//   after loop: flushChunk() fires on the remaining 4-byte remainder → final flush (seq=2)
	// This exercises both the threshold flush path AND the final-remainder flush path.
	deltas := []string{"Hello, streaming world!!", "Done"}
	gen := &fakeStreamInference{deltas: deltas}
	rec := &recordingPublisher{}

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return encryptBlob(t, sessionKey, "test prompt"), nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), gen,
		nil,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:  5 * time.Second,
			BlobTxTimeout: 60 * time.Second,
		},
		rec,
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil,
	)

	payload := testPayload(t)
	ks := newMockKeyStore()
	ks.keys[payload.SessionID] = sessionKey
	handler.keyStore = ks

	blobData, err := pkgcrypto.Encrypt(sessionKey, []byte("test prompt"))
	require.NoError(t, err)

	ciphertext, err := handler.runInferencePipeline(
		context.Background(),
		logger,
		payload,
		blobData,
		"llama3-8b",
		metrics.DeliveryAsynq,
	)
	require.NoError(t, err)

	// Must produce exactly two chunk frames: seq=1 from the mid-stream threshold flush
	// (buffer reached 24 bytes after delta 1) and seq=2 from the final-remainder flush
	// (the leftover 4 bytes of delta 2 flushed after GenerateStream returns).
	rec.mu.Lock()
	seqs := rec.chunkSeqs
	rec.mu.Unlock()
	assert.Equal(t, []uint32{1, 2}, seqs, "expected exactly two monotonic chunk seqs: threshold flush then final-remainder flush")

	// Full response ciphertext must decrypt to the concatenated deltas.
	// Note: non-search job → EncodeResponse returns raw answer bytes → ciphertext
	// decrypts back to the plain text (legacy format unchanged).
	plaintext, decErr := pkgcrypto.Decrypt(sessionKey, ciphertext)
	require.NoError(t, decErr)
	assert.Equal(t, strings.Join(deltas, ""), string(plaintext))
}

// TestStage6_EmitsEnvelopeForSearchJob asserts that when web search returns
// sources, stage 6 encrypts a v2 JSON envelope (not raw answer bytes) so the
// disputer can replay the search context deterministically.
func TestStage6_EmitsEnvelopeForSearchJob(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)

	gen := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "the answer", nil
	}}
	rec := &recordingPublisher{}

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return encryptBlob(t, sessionKey, "what year is it?"), nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), gen,
		nil,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:     5 * time.Second,
			BlobTxTimeout:    60 * time.Second,
			SearchMaxResults: 3,
		},
		rec,
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		&fakeSearcher{sources: twoSources()},
	)

	payload := testPayload(t)
	payload.SearchEnabled = true
	ks := newMockKeyStore()
	ks.keys[payload.SessionID] = sessionKey
	handler.keyStore = ks

	blobData, err := pkgcrypto.Encrypt(sessionKey, []byte("what year is it?"))
	require.NoError(t, err)

	ct, err := handler.runInferencePipeline(
		context.Background(),
		logger,
		payload,
		blobData,
		"llama3-8b",
		metrics.DeliveryAsynq,
	)
	require.NoError(t, err)

	// Decrypt the ciphertext and decode the v2 envelope.
	plaintext, decErr := pkgcrypto.Decrypt(sessionKey, ct)
	require.NoError(t, decErr)
	env := searchaug.DecodeResponse(plaintext)

	assert.Equal(t, searchaug.ResponseEnvelopeVersion, env.V)
	assert.Equal(t, "the answer", env.Answer)
	assert.Len(t, env.SearchContext, 2)
}

// TestStage6_RawAnswerForNonSearchJob asserts that when no web search is
// performed, stage 6 encrypts the raw answer bytes (legacy format) — the
// v2 envelope is NOT emitted.
func TestStage6_RawAnswerForNonSearchJob(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)

	gen := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "plain", nil
	}}
	rec := &recordingPublisher{}

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return encryptBlob(t, sessionKey, "hello"), nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), gen,
		nil,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:  5 * time.Second,
			BlobTxTimeout: 60 * time.Second,
		},
		rec,
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil, // no searcher → SearchEnabled is ignored, searchSources stays nil
	)

	payload := testPayload(t)
	// SearchEnabled = false (default), no searcher wired
	ks := newMockKeyStore()
	ks.keys[payload.SessionID] = sessionKey
	handler.keyStore = ks

	blobData, err := pkgcrypto.Encrypt(sessionKey, []byte("hello"))
	require.NoError(t, err)

	ct, err := handler.runInferencePipeline(
		context.Background(),
		logger,
		payload,
		blobData,
		"llama3-8b",
		metrics.DeliveryAsynq,
	)
	require.NoError(t, err)

	// Decrypt and decode; for non-search jobs EncodeResponse returns raw bytes
	// so DecodeResponse falls back to treating the bytes as a raw answer.
	plaintext, decErr := pkgcrypto.Decrypt(sessionKey, ct)
	require.NoError(t, decErr)
	env := searchaug.DecodeResponse(plaintext)

	assert.Equal(t, "plain", env.Answer)
	assert.Nil(t, env.SearchContext, "non-search job must not emit a SearchContext")
}

// TestBuildConversationHistory_DecodesV2EnvelopeToAnswer asserts that when a
// prior job's response blob is a v2 search envelope, buildConversationHistory
// assembles an assistant message whose Content is the decoded answer — NOT the
// raw envelope JSON. For legacy/non-search blobs DecodeResponse falls back to
// treating the bytes as a raw answer, so behavior for those is unchanged.
func TestBuildConversationHistory_DecodesV2EnvelopeToAnswer(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)

	const priorAnswer = "prior answer from search"
	priorSources := []searchaug.Source{
		{Position: 1, Title: "T1", URL: "https://example.com", Snippet: "snip"},
	}

	// Build a v2 envelope and encrypt it as the worker would store it.
	envelope, err := searchaug.EncodeResponse(priorAnswer, priorSources)
	require.NoError(t, err)
	encEnvelope, err := pkgcrypto.Encrypt(sessionKey, envelope)
	require.NoError(t, err)

	// Build a plain prompt blob for the same prior job.
	encPrompt := encryptBlob(t, sessionKey, "prior user question")

	// Sentinel hashes to distinguish prompt from response fetches.
	promptHash := common.HexToHash("0x0101010101010101010101010101010101010101010101010101010101010101")
	responseHash := common.HexToHash("0x0202020202020202020202020202020202020202020202020202020202020202")

	chain := &mockChainClient{
		ackJobFn:      func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) {
			return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil
		},
		getJobBlobInfoFn: func(_ context.Context, _ uint64) (common.Hash, common.Hash, uint64, uint64, error) {
			return promptHash, responseHash, 10, 11, nil
		},
	}

	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
		switch hash {
		case promptHash:
			return encPrompt, nil
		case responseHash:
			return encEnvelope, nil
		default:
			return nil, fmt.Errorf("unexpected hash %s", hash)
		}
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x03}}, nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			return "", nil
		}},
		nil,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:  5 * time.Second,
			BlobTxTimeout: 60 * time.Second,
		},
		&recordingPublisher{},
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil,
	)

	msgs, err := handler.buildConversationHistory(context.Background(), []uint64{99}, sessionKey)
	require.NoError(t, err)
	require.Len(t, msgs, 2, "expected one user + one assistant message")

	userMsg := msgs[0]
	assert.Equal(t, "user", userMsg.Role)
	assert.Equal(t, "prior user question", userMsg.Content)

	assistantMsg := msgs[1]
	assert.Equal(t, "assistant", assistantMsg.Role)
	// Must be the decoded answer, not the raw envelope JSON.
	assert.Equal(t, priorAnswer, assistantMsg.Content,
		"assistant Content must be the decoded answer, not the raw v2 envelope JSON")
}

package pipeline

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgcrypto "github.com/lightchain/pkg/crypto"

	"github.com/lightchain/worker/internal/ollama"
	pkgtypes "github.com/lightchain/pkg/types"
	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/voice"
)

func newJobCounter() *atomic.Int32 { return &atomic.Int32{} }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockVoiceEngine records calls and replays canned results. Nil funcs
// panic, so a test that expects a path to stay silent gets a loud failure
// instead of a nil result.
type mockVoiceEngine struct {
	transcribeFn func(ctx context.Context, audio []byte, format string) (string, error)
	synthesizeFn func(ctx context.Context, text, voiceName string) (io.ReadCloser, error)
	settledFn    func(ctx context.Context, text, voiceName string, maxBytes int) ([]byte, bool, error)

	mu           sync.Mutex
	transcribes  int
	synthesizes  int
	lastText     string
	lastVoice    string
	lastFormat   string
	lastAudioLen int
	settleds     int
	lastMaxBytes int
}

// SynthesizeSettled backs the speech-model path, whose answer is the audio
// itself. A nil settledFn panics like the others: a test that reaches this
// unexpectedly should fail loudly rather than settle an empty recording.
func (m *mockVoiceEngine) SynthesizeSettled(
	ctx context.Context,
	text, voiceName string,
	maxBytes int,
) ([]byte, bool, error) {
	m.mu.Lock()
	m.settleds++
	m.lastText = text
	m.lastVoice = voiceName
	m.lastMaxBytes = maxBytes
	m.mu.Unlock()
	return m.settledFn(ctx, text, voiceName, maxBytes)
}

func (m *mockVoiceEngine) Transcribe(ctx context.Context, audio []byte, format string) (string, error) {
	m.mu.Lock()
	m.transcribes++
	m.lastAudioLen = len(audio)
	m.lastFormat = format
	m.mu.Unlock()
	return m.transcribeFn(ctx, audio, format)
}

func (m *mockVoiceEngine) Synthesize(ctx context.Context, text, voiceName string) (io.ReadCloser, error) {
	m.mu.Lock()
	m.synthesizes++
	m.lastText = text
	m.lastVoice = voiceName
	m.mu.Unlock()
	return m.synthesizeFn(ctx, text, voiceName)
}

// voiceTestRig assembles the common mocks for a full HandleTask run with a
// voice-capable handler.
type voiceTestRig struct {
	handler   *JobHandler
	engine    *mockVoiceEngine
	submitter *mockBlobSubmitter
	rc        *redis.Client
	session   []byte
	// settledBlob is the stage-8a payload: the exact bytes committed on
	// chain, which for a speech job is the audio answer.
	settledBlob []byte
	// settledBlobRef points at the closure variable the submitter writes,
	// so the answer can be read immediately after the job rather than only
	// at cleanup.
	settledBlobRef *[]byte
}

// decryptSettledAnswer returns the plaintext the job settled — what the
// consumer will decrypt from the terminal frame and the blob alike.
func (r *voiceTestRig) decryptSettledAnswer(t *testing.T) (string, error) {
	t.Helper()
	blob := r.settledBlob
	if r.settledBlobRef != nil && len(*r.settledBlobRef) > 0 {
		blob = *r.settledBlobRef
	}
	require.NotEmpty(t, blob, "no blob was submitted")
	plain, err := pkgcrypto.Decrypt(r.session, blob)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func newVoiceTestRig(t *testing.T, promptPlaintext string, cfg HandlerConfig, inf InferenceClient, engine *mockVoiceEngine) *voiceTestRig {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, promptPlaintext)

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	var settled []byte
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, data []byte) ([][32]byte, error) {
		settled = append([]byte(nil), data...)
		return [][32]byte{{0x77}}, nil
	}}

	if cfg.AckTxTimeout == 0 {
		cfg.AckTxTimeout = 5 * time.Second
	}
	if cfg.BlobTxTimeout == 0 {
		cfg.BlobTxTimeout = 60 * time.Second
	}
	if cfg.RedisPublishTimeout == 0 {
		cfg.RedisPublishTimeout = 5 * time.Second
	}
	// The terminal frame is only published once signing succeeds, and
	// signing needs the domain separators.
	if cfg.ChainID == nil {
		cfg.ChainID = big.NewInt(31337)
	}
	if cfg.JobRegistryAddr == (common.Address{}) {
		cfg.JobRegistryAddr = common.HexToAddress("0x0000000000000000000000000000000000001337")
	}

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), inf,
		rc, testSigningKey(t), ecdhKey, newJobCounter(), discardLogger(),
		cfg, nil, nil, testMetrics(t), metrics.DeliveryAsynq,
nil,
	)
	if engine != nil {
		handler.SetVoiceEngine(engine)
	}
	rig := &voiceTestRig{handler: handler, engine: engine, submitter: submitter, rc: rc, session: sessionKey}
	// settled is filled by the submitter closure during the run, so the rig
	// reads it back through a pointer rather than a value copied at build.
	t.Cleanup(func() { rig.settledBlob = settled })
	rig.settledBlobRef = &settled
	return rig
}

func runVoiceJob(t *testing.T, rig *voiceTestRig) error {
	t.Helper()
	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return rig.handler.HandleTask(ctx, asynq.NewTask(TaskTypeJobInference, data))
}

// The transcript must become the prompt text the model sees when the
// envelope carries audio and no text.
func TestHandleTask_STTTranscriptBecomesPrompt(t *testing.T) {
	t.Parallel()

	audioB64 := base64.StdEncoding.EncodeToString([]byte("fake wav bytes"))
	envelope := fmt.Sprintf(`{"v":2,"audio":%q,"audioFormat":"wav"}`, audioB64)

	var gotPrompt string
	ol := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		gotPrompt = prompt
		return "it is sunny", nil
	}}
	engine := &mockVoiceEngine{
		transcribeFn: func(_ context.Context, audio []byte, format string) (string, error) {
			assert.Equal(t, []byte("fake wav bytes"), audio)
			assert.Equal(t, "wav", format)
			return "what is the weather", nil
		},
	}

	rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: true}, ol, engine)
	require.NoError(t, runVoiceJob(t, rig))

	assert.Equal(t, 1, rig.engine.transcribes)
	assert.Equal(t, "what is the weather", gotPrompt,
		"model must see the transcript, not the audio payload")
}

// Typed text plus audio: the transcript appends to the text, preserving
// both the typed context and the spoken question.
func TestHandleTask_STTMergesTranscriptWithText(t *testing.T) {
	t.Parallel()

	audioB64 := base64.StdEncoding.EncodeToString([]byte("fake wav"))
	envelope := fmt.Sprintf(`{"v":2,"text":"context: paris office","audio":%q}`, audioB64)

	var gotPrompt string
	ol := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		gotPrompt = prompt
		return "answer", nil
	}}
	engine := &mockVoiceEngine{
		transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			return "spoken question", nil
		},
	}

	rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: true}, ol, engine)
	require.NoError(t, runVoiceJob(t, rig))
	assert.Equal(t, "context: paris office\nspoken question", gotPrompt)
}

// An audio-only prompt on a worker without STT is a deterministic failure:
// retrying this worker cannot help, so the error must carry SkipRetry.
func TestHandleTask_STTDisabledAudioOnlyPrompt_NoRetry(t *testing.T) {
	t.Parallel()

	audioB64 := base64.StdEncoding.EncodeToString([]byte("fake wav"))
	envelope := fmt.Sprintf(`{"v":2,"audio":%q}`, audioB64)

	ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		panic("model must not be called")
	}}
	engine := &mockVoiceEngine{
		transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			panic("sidecar must not be called when STT is disabled")
		},
	}

	rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: false}, ol, engine)
	err := runVoiceJob(t, rig)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
	assert.ErrorIs(t, err, errSTTUnavailable)
}

// STT failure classification: sidecar 4xx (ErrBadAudio) and undecodable
// base64 are deterministic (no-retry); transport errors stay retryable.
func TestHandleTask_STTErrorClassification(t *testing.T) {
	t.Parallel()

	audioB64 := base64.StdEncoding.EncodeToString([]byte("fake wav"))
	envelope := fmt.Sprintf(`{"v":2,"audio":%q}`, audioB64)

	t.Run("bad audio is no-retry", func(t *testing.T) {
		engine := &mockVoiceEngine{transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			return "", fmt.Errorf("%w: status 400", voice.ErrBadAudio)
		}}
		ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			panic("model must not be called after STT failure")
		}}
		rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: true}, ol, engine)
		err := runVoiceJob(t, rig)
		require.Error(t, err)
		assert.ErrorIs(t, err, asynq.SkipRetry)
	})

	t.Run("transport error is retryable", func(t *testing.T) {
		engine := &mockVoiceEngine{transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			return "", fmt.Errorf("dial tcp: connection refused")
		}}
		ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			panic("model must not be called after STT failure")
		}}
		rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: true}, ol, engine)
		err := runVoiceJob(t, rig)
		require.Error(t, err)
		assert.NotErrorIs(t, err, asynq.SkipRetry)
	})

	t.Run("empty transcript is no-retry", func(t *testing.T) {
		engine := &mockVoiceEngine{transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			return "   ", nil
		}}
		ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			panic("model must not be called for an empty transcript")
		}}
		rig := newVoiceTestRig(t, envelope, HandlerConfig{STTEnabled: true}, ol, engine)
		err := runVoiceJob(t, rig)
		require.Error(t, err)
		assert.ErrorIs(t, err, asynq.SkipRetry)
		assert.ErrorIs(t, err, errEmptyTranscript)
	})

	t.Run("undecodable base64 is no-retry", func(t *testing.T) {
		engine := &mockVoiceEngine{transcribeFn: func(_ context.Context, _ []byte, _ string) (string, error) {
			panic("sidecar must not be called for undecodable audio")
		}}
		ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			panic("model must not be called for undecodable audio")
		}}
		rig := newVoiceTestRig(t, `{"v":2,"audio":"!!!not-base64!!!"}`, HandlerConfig{STTEnabled: true}, ol, engine)
		err := runVoiceJob(t, rig)
		require.Error(t, err)
		assert.ErrorIs(t, err, asynq.SkipRetry)
	})
}

// Voice output: the response is synthesized and streamed as `audio` frames
// - a JSON header, raw PCM chunks, a JSON final descriptor with the content
// hash - while the settlement path stays text-only and single-blob.
func TestHandleTask_TTSStreamsAudioFrames(t *testing.T) {
	t.Parallel()

	tokens := []string{"hello", " voice", " world"}
	fullResponse := strings.Join(tokens, "")
	fakePCM := bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 10000) // 40000 bytes -> 2 frames

	ol := &mockStreamingOllama{
		mockOllama: mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			return "", fmt.Errorf("batch Generate must not be called when streaming is enabled")
		}},
		tokens: tokens,
	}
	engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, text, voiceName string) (io.ReadCloser, error) {
		assert.Equal(t, fullResponse, text)
		assert.Equal(t, "af_heart", voiceName)
		return io.NopCloser(bytes.NewReader(fakePCM)), nil
	}}

	rig := newVoiceTestRig(t, `{"v":2,"text":"say hi","audioResponse":true}`, HandlerConfig{
		StreamEnabled:       true,
		StreamChunkTokens:   1,
		StreamChunkInterval: time.Hour,
		TTSEnabled:          true,
		TTSVoice:            "af_heart",
		TTSMaxChars:         4000,
	}, ol, engine)

	var submitted [][]byte
	rig.submitter.submitFn = func(_ context.Context, data []byte) ([][32]byte, error) {
		submitted = append(submitted, append([]byte(nil), data...))
		return [][32]byte{{0x77}}, nil
	}

	payload := testPayload(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub := rig.rc.Subscribe(ctx, fmt.Sprintf("session:%d:responses", payload.SessionID))
	t.Cleanup(func() { _ = sub.Close() })
	_, err := sub.Receive(ctx)
	require.NoError(t, err)

	require.NoError(t, runVoiceJob(t, rig))

	// 3 text chunks + header + 2 PCM chunks + final descriptor + terminal.
	frames := readFrames(t, ctx, sub, 8)

	// Header frame: JSON descriptor declaring the wire format.
	header := decryptFrame(t, rig.session, frames[3])
	assert.Equal(t, pkgtypes.FrameKindAudio, frames[3].Kind)
	var hdr audioMetaFrame
	require.NoError(t, json.Unmarshal(header, &hdr))
	assert.Equal(t, "pcm-s16le", hdr.Meta.Format)
	assert.Equal(t, 24000, hdr.Meta.SampleRate)
	assert.Equal(t, 1, hdr.Meta.Channels)
	assert.Equal(t, voice.AudioMIME, hdr.Meta.Mime)
	assert.Equal(t, "af_heart", hdr.Meta.Voice)
	assert.Equal(t, "kokoro", hdr.Meta.Engine)
	assert.Equal(t, len(fullResponse), hdr.Meta.Chars)
	assert.True(t, hdr.Meta.Delivered)
	assert.False(t, hdr.Meta.Settled, "audio is delivered, not settled")
	assert.False(t, hdr.Meta.Final)

	// PCM frames: raw audio bytes, concatenation reproduces the stream.
	var pcmOut []byte
	for _, f := range frames[4:6] {
		assert.Equal(t, pkgtypes.FrameKindAudio, f.Kind)
		plain := decryptFrame(t, rig.session, f)
		require.False(t, json.Valid(plain), "PCM frames must not look like JSON descriptors")
		pcmOut = append(pcmOut, plain...)
	}
	assert.Equal(t, fakePCM, pcmOut)

	// Final descriptor: byte/chunk counts plus the content hash that a
	// Phase-2 manifestHash commitment could cover.
	final := decryptFrame(t, rig.session, frames[6])
	var fin audioMetaFrame
	require.NoError(t, json.Unmarshal(final, &fin))
	assert.True(t, fin.Meta.Final)
	assert.Equal(t, len(fakePCM), fin.Meta.Bytes)
	assert.Equal(t, 2, fin.Meta.Chunks)
	assert.Equal(t, fmt.Sprintf("0x%x", ethcrypto.Keccak256(fakePCM)), fin.Meta.ContentHash)
	assert.False(t, fin.Meta.Settled)

	// Settlement invariant: exactly one blob (the response), and the
	// terminal frame carries it - the text-only ciphertext the contract
	// commits to. Audio added frames but no blob.
	require.Len(t, submitted, 1, "audio must stream as frames, not a blob")
	terminal := frames[7]
	assert.Equal(t, pkgtypes.MessageTypeComplete, terminal.Type)
	assert.Equal(t, uint32(8), terminal.TotalChunks,
		"audio frames must count toward TotalChunks")
	assert.Equal(t, submitted[0], terminal.Payload,
		"settlement ciphertext must be the terminal frame payload")
	terminalPlain, err := pkgcrypto.Decrypt(rig.session, submitted[0])
	require.NoError(t, err)
	assert.Equal(t, fullResponse, string(terminalPlain),
		"settlement covers the response text only - no audio bytes")
}

// decryptFrame decrypts one chunk frame payload under the session key.
func decryptFrame(t *testing.T, sessionKey []byte, frame pkgtypes.PubSubMessage) []byte {
	t.Helper()
	plain, err := pkgcrypto.Decrypt(sessionKey, frame.Payload)
	require.NoError(t, err)
	return plain
}

// With TTS gated off (or the request not opting in) the sidecar must never
// be called and the blob layout is exactly the pre-voice one.
func TestHandleTask_TTSGatedOff(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		envelope string
		cfg      HandlerConfig
	}{
		{"opt-in but worker gate off", `{"v":2,"text":"hi","audioResponse":true}`, HandlerConfig{TTSEnabled: false}},
		{"worker gate on but no opt-in", `{"v":2,"text":"hi"}`, HandlerConfig{TTSEnabled: true, TTSVoice: "af_heart"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
				return "answer", nil
			}}
			engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
				panic("sidecar must not be called")
			}}
			rig := newVoiceTestRig(t, tc.envelope, tc.cfg, ol, engine)

			calls := 0
			rig.submitter.submitFn = func(_ context.Context, _ []byte) ([][32]byte, error) {
				calls++
				return [][32]byte{{0x01}}, nil
			}

			require.NoError(t, runVoiceJob(t, rig))
			assert.Equal(t, 1, calls, "only the response blob may be submitted")
			assert.Equal(t, 0, engine.synthesizes)
		})
	}
}

// A TTS failure is a skipped convenience, never a failed job: the text
// answer still settles.
func TestHandleTask_TTSFailureIsBestEffort(t *testing.T) {
	t.Parallel()

	ol := &mockStreamingOllama{
		mockOllama: mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
			return "", fmt.Errorf("batch path must not be used")
		}},
		tokens: []string{"answer"},
	}
	engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("kokoro sidecar down")
	}}

	rig := newVoiceTestRig(t, `{"v":2,"text":"hi","audioResponse":true}`, HandlerConfig{
		StreamEnabled: true,
		TTSEnabled:    true,
		TTSVoice:      "af_heart",
	}, ol, engine)

	calls := 0
	rig.submitter.submitFn = func(_ context.Context, _ []byte) ([][32]byte, error) {
		calls++
		return [][32]byte{{0x01}}, nil
	}

	require.NoError(t, runVoiceJob(t, rig))
	assert.Equal(t, 1, calls, "no audio blob, just the response blob")
}

// Without a streamer (batch mode) there is no channel for audio frames;
// delivery must skip rather than emit nothing playable.
func TestHandleTask_TTSSkippedWithoutStreamer(t *testing.T) {
	t.Parallel()

	ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "answer", nil
	}}
	engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
		panic("sidecar must not be called when there is no streamer")
	}}

	rig := newVoiceTestRig(t, `{"v":2,"text":"hi","audioResponse":true}`, HandlerConfig{
		StreamEnabled: false,
		TTSEnabled:    true,
		TTSVoice:      "af_heart",
	}, ol, engine)
	require.NoError(t, runVoiceJob(t, rig))
	assert.Equal(t, 0, engine.synthesizes)
}

// TTSMaxChars truncates the synthesized text while the settled response
// stays complete.
func TestHandleTask_TTSTruncatesLongResponse(t *testing.T) {
	t.Parallel()

	ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "unused", nil
	}}
	engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, text, _ string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("pcm"))), nil
	}}

	rig := newVoiceTestRig(t, `{"v":2,"text":"hi"}`, HandlerConfig{
		TTSEnabled:  true,
		TTSVoice:    "af_heart",
		TTSMaxChars: 10,
	}, ol, engine)
	// Drive maybeDeliverAudio directly with a streamer to exercise
	// truncation without the streaming machinery.
	streamer, _ := newTestStreamer(t)
	rig.handler.maybeDeliverAudio(context.Background(), discardLogger(), streamer,
		"a much longer answer than the tts budget allows",
		promptEnvelope{AudioResponse: true}, nil)

	assert.Equal(t, 1, engine.synthesizes)
	assert.Equal(t, "a much lon", engine.lastText, "synthesized text must be truncated to TTSMaxChars")
}

// A truncated PCM stream (sidecar dies mid-synthesis) must omit the final
// descriptor; the frames already delivered stay playable.
func TestDeliverAudio_TruncatedStreamOmitsFinal(t *testing.T) {
	t.Parallel()

	engine := &mockVoiceEngine{synthesizeFn: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
		// Three bytes then a read error, mimicking a dropped connection.
		return io.NopCloser(io.MultiReader(
			bytes.NewReader([]byte{1, 2, 3}),
			&errReader{},
		)), nil
	}}
	ol := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "unused", nil
	}}
	rig := newVoiceTestRig(t, `{"v":2,"text":"hi"}`, HandlerConfig{
		TTSEnabled: true,
		TTSVoice:   "af_heart",
	}, ol, engine)

	streamer, pub := newTestStreamer(t)
	rig.handler.maybeDeliverAudio(context.Background(), discardLogger(), streamer,
		"answer", promptEnvelope{AudioResponse: true}, nil)

	// header + 1 PCM frame; no final descriptor.
	require.Len(t, pub.frames, 2)
	var hdr audioMetaFrame
	require.NoError(t, json.Unmarshal([]byte(pub.frames[0].body), &hdr))
	assert.False(t, hdr.Meta.Final)
	assert.Equal(t, []byte{1, 2, 3}, []byte(pub.frames[1].body))
}

type errReader struct{}

func (e *errReader) Read([]byte) (int, error) { return 0, fmt.Errorf("connection reset") }

// --- voiceBudgetOK ---

type fakeDeadlineClient struct {
	deadline time.Time
	ack      bool
	err      error
}

func (f fakeDeadlineClient) GetJobDeadline(context.Context, uint64) (time.Time, bool, error) {
	return f.deadline, f.ack, f.err
}

func TestVoiceBudgetOK(t *testing.T) {
	t.Parallel()

	guard := &deadlineGuard{
		jobID:             1,
		completionReserve: 12 * time.Second,
	}

	t.Run("nil guard allows", func(t *testing.T) {
		var nilGuard *deadlineGuard
		assert.True(t, nilGuard.voiceBudgetOK(context.Background(), discardLogger(), time.Minute))
	})

	t.Run("ample window allows", func(t *testing.T) {
		g := *guard
		g.client = fakeDeadlineClient{deadline: time.Now().Add(90 * time.Second)}
		assert.True(t, g.voiceBudgetOK(context.Background(), discardLogger(), 25*time.Second))
	})

	t.Run("tight window refuses", func(t *testing.T) {
		g := *guard
		g.client = fakeDeadlineClient{deadline: time.Now().Add(30 * time.Second)}
		assert.False(t, g.voiceBudgetOK(context.Background(), discardLogger(), 25*time.Second),
			"30s remaining cannot cover 12s completion reserve + 25s voice work")
	})

	t.Run("read error fails open", func(t *testing.T) {
		g := *guard
		g.client = fakeDeadlineClient{err: errors.New("rpc flake")}
		assert.True(t, g.voiceBudgetOK(context.Background(), discardLogger(), time.Minute))
	})
}

// --- artifact emitter stub ---

func TestEmitArtifact_RoundTrip(t *testing.T) {
	t.Parallel()

	streamer, pub := newTestStreamer(t)
	require.NoError(t, emitArtifact(streamer, "genui", "lightchain.genui.v1",
		json.RawMessage(`{"component":"stat","props":{"label":"kpi"}}`)))

	require.Len(t, pub.frames, 1)
	assert.Equal(t, pkgtypes.FrameKindArtifact, pub.frames[0].kind)

	var desc artifactDescriptor
	require.NoError(t, json.Unmarshal([]byte(pub.frames[0].body), &desc))
	assert.Equal(t, "genui", desc.ArtifactType)
	assert.Equal(t, "lightchain.genui.v1", desc.Schema)
	assert.False(t, desc.Settled)
	assert.JSONEq(t, `{"component":"stat","props":{"label":"kpi"}}`, string(desc.Payload))
}

func TestEmitArtifact_RejectsInvalid(t *testing.T) {
	t.Parallel()

	streamer, pub := newTestStreamer(t)

	assert.Error(t, emitArtifact(streamer, "", "schema", json.RawMessage(`{}`)))
	assert.Error(t, emitArtifact(streamer, "genui", "", json.RawMessage(`{}`)))
	assert.Error(t, emitArtifact(streamer, "genui", "schema", nil))
	assert.Error(t, emitArtifact(streamer, "genui", "schema",
		json.RawMessage(strings.Repeat("x", maxArtifactDescriptorBytes))))
	assert.Empty(t, pub.frames, "rejected descriptors must not publish")

	// Nil streamer is a silent skip (batch mode), not an error.
	assert.NoError(t, emitArtifact(nil, "genui", "schema", json.RawMessage(`{}`)))
}

// --- speech model: the job whose settled answer IS the recording ----------

// fatalInference fails the test if the language model is reached at all. A
// speech job must never call one: doing so would settle a spoken sentence
// instead of the audio, and burn a generation the consumer did not ask for.
type fatalInference struct{ t *testing.T }

func (f fatalInference) Generate(context.Context, string, string) (string, error) {
	f.t.Fatal("speech job reached Generate; it must skip the language model")
	return "", nil
}

func (f fatalInference) Chat(context.Context, string, []ollama.ChatMessage) (string, error) {
	f.t.Fatal("speech job reached Chat; it must skip the language model")
	return "", nil
}

func TestSpeechModel_SettlesAudioAndSkipsInference(t *testing.T) {
	mp3 := []byte("\xff\xfbID3 fake mp3 payload")
	engine := &mockVoiceEngine{
		settledFn: func(_ context.Context, _, _ string, _ int) ([]byte, bool, error) {
			return mp3, false, nil
		},
	}

	rig := newVoiceTestRig(t, "read me aloud",
		HandlerConfig{SpeechModelName: "tts-piper", TTSVoice: "af_heart",
			ModelIDToName: map[string]string{"llama3-8b": "tts-piper"}},
		fatalInference{t}, engine)
	require.NoError(t, runVoiceJob(t, rig))

	assert.Equal(t, 1, rig.engine.settleds, "the speech path must be taken")
	assert.Equal(t, "read me aloud", rig.engine.lastText)
	assert.Equal(t, "af_heart", rig.engine.lastVoice)
	assert.Equal(t, SettledAudioMaxBytes, rig.engine.lastMaxBytes,
		"the blob budget must reach the sidecar, or an oversized answer fails at stage 8a")
}

func TestSpeechModel_SettledAnswerIsBase64OfTheAudio(t *testing.T) {
	mp3 := []byte{0xff, 0xfb, 0x10, 0x00, 0x42, 0x99}
	engine := &mockVoiceEngine{
		settledFn: func(_ context.Context, _, _ string, _ int) ([]byte, bool, error) {
			return mp3, false, nil
		},
	}

	rig := newVoiceTestRig(t, "hello",
		HandlerConfig{SpeechModelName: "tts-piper",
			ModelIDToName: map[string]string{"llama3-8b": "tts-piper"}}, fatalInference{t}, engine)
	require.NoError(t, runVoiceJob(t, rig))

	// The consumer decodes the settled answer straight into an <audio>
	// element, so the exact encoding is a wire contract, not an internal
	// detail.
	got, err := rig.decryptSettledAnswer(t)
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(got)
	require.NoError(t, err, "settled answer must be valid base64")
	assert.Equal(t, mp3, decoded)
}

func TestSpeechModel_FailsLoudlyWithoutAVoiceEngine(t *testing.T) {
	// The worker advertises this model on-chain. Settling silence would look
	// like a successful job to everyone including the consumer.
	rig := newVoiceTestRig(t, "hello",
		HandlerConfig{SpeechModelName: "tts-piper",
			ModelIDToName: map[string]string{"llama3-8b": "tts-piper"}}, fatalInference{t}, nil)

	err := runVoiceJob(t, rig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no voice engine configured")
}

func TestSpeechModel_OtherModelsAreUnaffected(t *testing.T) {
	engine := &mockVoiceEngine{}
	ol := &mockOllama{generateFn: func(context.Context, string, string) (string, error) {
		return "an ordinary answer", nil
	}}

	// Same worker, different model: the language model still runs and the
	// speech sidecar is never touched.
	rig := newVoiceTestRig(t, "hello",
		HandlerConfig{SpeechModelName: "some-other-speech-model"}, ol, engine)
	require.NoError(t, runVoiceJob(t, rig))

	assert.Equal(t, 0, rig.engine.settleds)
}

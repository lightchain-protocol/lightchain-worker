package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/pkg/publicresp"
	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
)

const publicSalt = "0x0102030405060708091011121314151617181920212223242526272829303132"

// publicEnvelope is a v4 public-output prompt as the blob carries it.
func publicEnvelope(format string, numCtx, numPredict int) string {
	f := ""
	if format != "" {
		f = `,"format":` + format
	}
	return `{"v":4,"text":"What is 2+2?","output":"public"` + f +
		`,"params":{"numCtx":` + itoa(numCtx) + `,"numPredict":` + itoa(numPredict) + `},"salt":"` + publicSalt + `"}`
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// ollamaGenerateServer answers POST /api/generate with one batch response
// and hands every request body to capture.
func ollamaGenerateServer(t *testing.T, response string, capture func(body map[string]any)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/generate", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		capture(body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"model": body["model"], "response": response, "done": true})
	}))
	t.Cleanup(srv.Close)
	return srv
}

type publicRun struct {
	plaintext      string
	blob           []byte
	completedHash  [32]byte
	completeCalled bool
	requests       []map[string]any
}

// runPublicJob drives HandleTask for a public envelope against a real
// *ollama.OllamaClient (the pinned-client path needs WithOverrides, which
// the handler mocks do not have) talking to an httptest Ollama.
func runPublicJob(t *testing.T, plaintext, engineText string, cfg func(*HandlerConfig)) (*publicRun, error) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	run := &publicRun{plaintext: plaintext}
	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, plaintext)
	modelID := common.Bytes2Hex(crypto.Keccak256Hash([]byte("llama3-8b")).Bytes())

	chain := &mockChainClient{
		ackJobFn: func(context.Context, uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, responseCiphertextHash [32]byte) error {
			run.completeCalled = true
			run.completedHash = responseCiphertextHash
			return nil
		},
		getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, data []byte) ([][32]byte, error) {
		run.blob = append([]byte(nil), data...)
		return [][32]byte{{0x01}}, nil
	}}
	srv := ollamaGenerateServer(t, engineText, func(body map[string]any) { run.requests = append(run.requests, body) })
	// Operator defaults that a public job must override.
	client := ollama.NewOllamaClientWithOptions(srv.URL, 5*time.Second, ollama.ClientOptions{
		NumPredict: 1024, NumCtx: 8192, Think: ollama.ThinkSetting(true), ThinkLevel: "high",
	})

	hc := HandlerConfig{
		AckTxTimeout:        5 * time.Second,
		BlobTxTimeout:       60 * time.Second,
		RedisPublishTimeout: 5 * time.Second,
		ModelIDToName:       map[string]string{modelID: "llama3-8b"},
		PublicOutput:        true,
		PublicMaxNumCtx:     8192,
		PublicMaxNumPredict: 1024,
	}
	if cfg != nil {
		cfg(&hc)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), client,
		rc, testSigningKey(t), ecdhKey, &atomic.Int32{}, logger,
		hc, nil, nil, testMetrics(t), metrics.DeliveryAsynq, nil,
	)

	payload := testPayload(t)
	payload.ModelID = modelID
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return run, handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
}

func TestHandleTask_PublicOutput_CommitsCanonicalR(t *testing.T) {
	t.Parallel()

	schema := `{"type":"object","properties":{"n":{"type":"integer"},"ok":{"type":"boolean"}}}`
	plaintext := publicEnvelope(schema, 2048, 64)
	run, err := runPublicJob(t, plaintext, "{\"ok\": true, \"n\": 4}", nil)
	require.NoError(t, err)

	// The engine ran under the envelope's profile, not the operator's.
	require.Len(t, run.requests, 1)
	req := run.requests[0]
	opts, _ := req["options"].(map[string]any)
	assert.Equal(t, float64(2048), opts["num_ctx"])
	assert.Equal(t, float64(64), opts["num_predict"])
	assert.Equal(t, float64(0), opts["temperature"])
	assert.Equal(t, float64(ollama.DeterministicSeed), opts["seed"])
	assert.Equal(t, false, req["think"], "thinking is forced off for a format job")
	gotFormat, _ := json.Marshal(req["format"])
	assert.JSONEq(t, schema, string(gotFormat), "the schema reaches the engine as format")

	// The blob is R in plaintext and completeJob committed keccak256(R).
	require.True(t, run.completeCalled)
	r, err := publicresp.Decode(run.blob)
	require.NoError(t, err, "blob is canonical R")
	assert.Equal(t, crypto.Keccak256Hash(run.blob), common.Hash(run.completedHash))
	assert.Equal(t, publicresp.Hash(run.blob), common.Hash(run.completedHash).Hex())

	assert.Equal(t, publicresp.Version, r.V)
	assert.Equal(t, publicresp.PromptHash([]byte(plaintext)), r.PromptHash, "promptHash is keccak of the decrypted envelope")
	wantFormat, err := publicresp.FormatHash(json.RawMessage(schema))
	require.NoError(t, err)
	assert.Equal(t, wantFormat, r.FormatHash)
	assert.Equal(t, publicresp.Params{Model: "llama3-8b", NumCtx: 2048, NumPredict: 64, Seed: 42, Temperature: 0}, r.Params)
	assert.True(t, r.Valid)
	assert.Nil(t, r.Answer)
	assert.Equal(t, `{"n":4,"ok":true}`, string(r.Output), "output is canonical inside R")
}

func TestHandleTask_PublicOutput_WithoutFormatIsRawAnswer(t *testing.T) {
	t.Parallel()

	plaintext := publicEnvelope("", 2048, 64)
	run, err := runPublicJob(t, plaintext, "4", nil)
	require.NoError(t, err)
	require.Len(t, run.requests, 1)
	_, hasFormat := run.requests[0]["format"]
	assert.False(t, hasFormat)

	r, err := publicresp.Decode(run.blob)
	require.NoError(t, err)
	assert.Equal(t, publicresp.ZeroHash, r.FormatHash)
	assert.False(t, r.Valid)
	require.NotNil(t, r.Answer)
	assert.Equal(t, "4", *r.Answer)
	assert.Nil(t, r.Output)
}

// A generation cut mid-object is committed as it came, valid false, so a
// replay reproduces it; the worker never retries with a new seed.
func TestHandleTask_PublicOutput_InvalidJSONIsCommittedNotRetried(t *testing.T) {
	t.Parallel()

	plaintext := publicEnvelope(`{"type":"object"}`, 2048, 8)
	run, err := runPublicJob(t, plaintext, `{"summary":"the mod`, nil)
	require.NoError(t, err)
	require.Len(t, run.requests, 1)
	r, err := publicresp.Decode(run.blob)
	require.NoError(t, err)
	assert.False(t, r.Valid)
	require.NotNil(t, r.Answer)
	assert.Equal(t, `{"summary":"the mod`, *r.Answer)
}

func TestHandleTask_PublicOutput_DisabledRefusesWithoutRetry(t *testing.T) {
	t.Parallel()

	run, err := runPublicJob(t, publicEnvelope("", 2048, 64), "4", func(c *HandlerConfig) { c.PublicOutput = false })
	require.Error(t, err)
	assert.True(t, errors.Is(err, asynq.SkipRetry), "no retry: %v", err)
	assert.True(t, errors.Is(err, ErrPublicOutputDisabled), "%v", err)
	assert.Empty(t, run.requests, "the engine is never called")
	assert.False(t, run.completeCalled)
}

func TestHandleTask_PublicOutput_ParamsOverLimitRefusedWithoutRetry(t *testing.T) {
	t.Parallel()

	run, err := runPublicJob(t, publicEnvelope("", 16384, 64), "4", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, asynq.SkipRetry), "no retry: %v", err)
	assert.Empty(t, run.requests)
	assert.False(t, run.completeCalled)
}

// A v4 envelope that is public but malformed (no salt) is refused at decode,
// without retry, before anything is sent to the engine.
func TestHandleTask_PublicOutput_MalformedEnvelopeRefused(t *testing.T) {
	t.Parallel()

	plaintext := `{"v":4,"text":"hi","output":"public","params":{"numCtx":2048,"numPredict":64}}`
	run, err := runPublicJob(t, plaintext, "4", nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, asynq.SkipRetry), "no retry: %v", err)
	assert.Empty(t, run.requests)
}

// A private job on the same handler still encrypts: the public branch does
// not change the default path.
func TestHandleTask_PrivateJobStillEncrypts(t *testing.T) {
	t.Parallel()

	run, err := runPublicJob(t, "What is 2+2?", "4", nil)
	require.NoError(t, err)
	_, decErr := publicresp.Decode(run.blob)
	assert.Error(t, decErr, "an encrypted blob is not R")
	assert.NotEqual(t, []byte("4"), run.blob)
}

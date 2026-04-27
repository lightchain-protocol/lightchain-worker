package pipeline

import (
	"context"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	pkgtypes "github.com/lightchain/pkg/types"
	"github.com/lightchain/worker/internal/ollama"
)

// --- Mocks ---

type mockChainClient struct {
	ackJobFn             func(ctx context.Context, jobID uint64) error
	completeJobFn        func(ctx context.Context, jobID uint64, hash [32]byte, responseCiphertextHash [32]byte) error
	hasJobAcknowledgedFn func(ctx context.Context, jobID uint64) (bool, error)
	hasJobCompletedFn    func(ctx context.Context, jobID uint64) (bool, error)
	getEncWorkerKeyFn    func(ctx context.Context, sessionID uint64) ([]byte, error)
}

func (m *mockChainClient) AcknowledgeJob(ctx context.Context, jobID uint64) error {
	return m.ackJobFn(ctx, jobID)
}
func (m *mockChainClient) CompleteJob(ctx context.Context, jobID uint64, h [32]byte, responseCiphertextHash [32]byte) error {
	return m.completeJobFn(ctx, jobID, h, responseCiphertextHash)
}
func (m *mockChainClient) HasJobAcknowledged(ctx context.Context, jobID uint64) (bool, error) {
	if m.hasJobAcknowledgedFn == nil {
		return false, nil
	}
	return m.hasJobAcknowledgedFn(ctx, jobID)
}
func (m *mockChainClient) HasJobCompleted(ctx context.Context, jobID uint64) (bool, error) {
	if m.hasJobCompletedFn == nil {
		return false, nil
	}
	return m.hasJobCompletedFn(ctx, jobID)
}
func (m *mockChainClient) GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error) {
	return m.getEncWorkerKeyFn(ctx, sessionID)
}
func (m *mockChainClient) GetJobBlobInfo(_ context.Context, _ uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	return common.Hash{}, common.Hash{}, 0, 0, nil
}

type mockBlobFetcher struct {
	fetchFn func(ctx context.Context, hash common.Hash, block uint64) ([]byte, error)
}

func (m *mockBlobFetcher) FetchBlob(ctx context.Context, hash common.Hash, block uint64) ([]byte, error) {
	return m.fetchFn(ctx, hash, block)
}

type mockBlobSubmitter struct {
	submitFn func(ctx context.Context, data []byte) ([][32]byte, error)
}

func (m *mockBlobSubmitter) SubmitBlobTx(ctx context.Context, data []byte) ([][32]byte, error) {
	return m.submitFn(ctx, data)
}

type mockKeyStore struct {
	mu   sync.Mutex
	keys map[uint64][]byte
}

func newMockKeyStore() *mockKeyStore {
	return &mockKeyStore{keys: make(map[uint64][]byte)}
}

func (m *mockKeyStore) GetKey(sessionID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[sessionID]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return k, nil
}

func (m *mockKeyStore) StoreKey(sessionID uint64, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[sessionID] = key
	return nil
}

type mockOllama struct {
	generateFn func(ctx context.Context, model, prompt string) (string, error)
	chatFn     func(ctx context.Context, model string, messages []ollama.ChatMessage) (string, error)
}

func (m *mockOllama) Generate(ctx context.Context, model, prompt string) (string, error) {
	return m.generateFn(ctx, model, prompt)
}

func (m *mockOllama) Chat(ctx context.Context, model string, messages []ollama.ChatMessage) (string, error) {
	if m.chatFn != nil {
		return m.chatFn(ctx, model, messages)
	}
	return m.generateFn(ctx, model, messages[len(messages)-1].Content)
}

// --- Helpers ---

func testSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return key
}

func testECDHKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	key, err := pkgcrypto.GenerateKeyPair()
	require.NoError(t, err)
	return key
}

func testSessionKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	return key
}

func testPayload(t *testing.T) JobPayload {
	t.Helper()
	return JobPayload{
		JobID:          42,
		SessionID:      1,
		ModelID:        "llama3-8b",
		PromptBlobHash: common.HexToHash("0xdeadbeef"),
		BlockNumber:    100,
		Timestamp:      time.Now().Unix(),
		CorrelationID:  "test-correlation",
	}
}

func encryptBlob(t *testing.T, sessionKey []byte, plaintext string) []byte {
	t.Helper()
	ct, err := pkgcrypto.Encrypt(sessionKey, []byte(plaintext))
	require.NoError(t, err)
	return ct
}

func encryptSessionKeyForWorker(t *testing.T, sessionKey []byte, workerECDH *ecdh.PrivateKey) []byte {
	t.Helper()
	enc, err := pkgcrypto.EncryptSessionKey(sessionKey, workerECDH.PublicKey())
	require.NoError(t, err)
	return enc
}

func newTestHandler(
	t *testing.T,
	chain *mockChainClient,
	fetcher *mockBlobFetcher,
	submitter *mockBlobSubmitter,
	keyStore *mockKeyStore,
	ollama *mockOllama,
	redisClient *redis.Client,
) *JobHandler {
	t.Helper()
	return newTestHandlerWithConfig(t, chain, fetcher, submitter, keyStore, ollama, redisClient, HandlerConfig{
		AckTxTimeout:        5 * time.Second,
		BlobTxTimeout:       60 * time.Second,
		RedisPublishTimeout: 5 * time.Second,
	})
}

func newTestHandlerWithConfig(
	t *testing.T,
	chain *mockChainClient,
	fetcher *mockBlobFetcher,
	submitter *mockBlobSubmitter,
	keyStore *mockKeyStore,
	ollama *mockOllama,
	redisClient *redis.Client,
	cfg HandlerConfig,
) *JobHandler {
	t.Helper()
	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	return NewJobHandler(
		chain, fetcher, submitter, keyStore, ollama,
		redisClient, testSigningKey(t), testECDHKey(t), counter, logger,
		cfg,
		nil, // checkpoints — disabled in most handler tests; see TestHandleTask_Checkpoint* for coverage
	)
}

// --- Tests ---

func TestHandleTask_FullPipelineSuccess(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, "What is 2+2?")

	stagesExecuted := make([]string, 0)
	expectedModelID := common.Bytes2Hex(crypto.Keccak256Hash([]byte("llama3-8b")).Bytes())

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, jobID uint64) error {
			stagesExecuted = append(stagesExecuted, "ack")
			assert.Equal(t, uint64(42), jobID)
			return nil
		},
		completeJobFn: func(_ context.Context, jobID uint64, hash [32]byte, responseCiphertextHash [32]byte) error {
			stagesExecuted = append(stagesExecuted, "complete")
			assert.Equal(t, uint64(42), jobID)
			assert.NotEqual(t, [32]byte{}, hash)
			assert.NotEqual(t, [32]byte{}, responseCiphertextHash)
			return nil
		},
		getEncWorkerKeyFn: func(_ context.Context, sessionID uint64) ([]byte, error) {
			stagesExecuted = append(stagesExecuted, "getEncKey")
			return encSessionKey, nil
		},
	}

	fetcher := &mockBlobFetcher{
		fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
			stagesExecuted = append(stagesExecuted, "fetchBlob")
			return promptCiphertext, nil
		},
	}

	submitter := &mockBlobSubmitter{
		submitFn: func(_ context.Context, data []byte) ([][32]byte, error) {
			stagesExecuted = append(stagesExecuted, "submitBlob")
			return [][32]byte{{0x01}}, nil
		},
	}

	ollama := &mockOllama{
		generateFn: func(_ context.Context, model, prompt string) (string, error) {
			stagesExecuted = append(stagesExecuted, "infer")
			assert.Equal(t, "llama3-8b", model)
			assert.Equal(t, "What is 2+2?", prompt)
			return "4", nil
		},
	}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:        5 * time.Second,
			BlobTxTimeout:       60 * time.Second,
			RedisPublishTimeout: 5 * time.Second,
			ModelIDToName: map[string]string{
				expectedModelID: "llama3-8b",
			},
		},
		nil, // checkpoints disabled for this test
	)

	payload := testPayload(t)
	payload.ModelID = expectedModelID
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err = handler.HandleTask(context.Background(), task)
	require.NoError(t, err)

	// Verify all stages ran in order
	assert.Equal(t, []string{"ack", "fetchBlob", "getEncKey", "infer", "submitBlob", "complete"}, stagesExecuted)

	// Counter should be back to 0 after completion
	assert.Equal(t, int32(0), counter.Load())
}

func TestHandleTask_AckFailure_StopsEarly(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error {
			return fmt.Errorf("chain unavailable")
		},
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { panic("should not be called") },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { panic("should not be called") },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) { panic("should not be called") }}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) { panic("should not be called") }}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) { panic("should not be called") }}

	handler := newTestHandler(t, chain, fetcher, submitter, newMockKeyStore(), ollama, rc)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage 1 (ack)")
}

func TestHandleTask_BlobFetchFailure(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { panic("should not be called") },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { panic("should not be called") },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return nil, fmt.Errorf("blob pruned")
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) { panic("should not be called") }}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) { panic("should not be called") }}

	handler := newTestHandler(t, chain, fetcher, submitter, newMockKeyStore(), ollama, rc)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage 2")
	assert.Contains(t, err.Error(), "blob pruned")
}

func TestHandleTask_SessionKeyCacheHit(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	promptCiphertext := encryptBlob(t, sessionKey, "cached test")

	// Pre-populate cache
	ks := newMockKeyStore()
	ks.keys[1] = sessionKey

	chainCalled := false
	chain := &mockChainClient{
		ackJobFn:      func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) {
			chainCalled = true
			return nil, fmt.Errorf("should not be called — key cached")
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "response", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, ks, ollama, rc,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.NoError(t, err)
	assert.False(t, chainCalled, "chain should not be called when session key is cached")
}

func TestHandleTask_SessionKeyCacheMiss_FetchAndStore(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, "test")

	ks := newMockKeyStore()

	chain := &mockChainClient{
		ackJobFn:      func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, sid uint64) ([]byte, error) {
			assert.Equal(t, uint64(1), sid)
			return encSessionKey, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "resp", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, ks, ollama, rc,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.NoError(t, err)

	// Key should now be in the store
	stored, err := ks.GetKey(1)
	require.NoError(t, err)
	assert.Equal(t, sessionKey, stored)
}

// TestHandleTask_SessionKeyRotated_Refreshes guards the updateSessionKey flow.
// When the cached session key fails to decrypt the prompt
// blob — which happens after the consumer rotates the key via updateSessionKey
// on-chain — the handler must refresh from the chain once and retry. The
// mocked chain returns an outdated encrypted key on the first call and the
// rotated one on the second, mirroring how GetSessionEncWorkerKey picks up
// the latest SessionKeyUpdated event.
func TestHandleTask_SessionKeyRotated_Refreshes(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	oldSessionKey := testSessionKey(t)
	newSessionKey := testSessionKey(t)
	require.NotEqual(t, oldSessionKey, newSessionKey)

	ecdhKey := testECDHKey(t)
	encNewSessionKey := encryptSessionKeyForWorker(t, newSessionKey, ecdhKey)

	// Blob is encrypted with the NEW key — the old cached key cannot decrypt it.
	promptCiphertext := encryptBlob(t, newSessionKey, "rotated prompt")

	// Pre-populate cache with the OLD session key — this is the stale state
	// that exists at the start of the first job after a rotation.
	ks := newMockKeyStore()
	ks.keys[1] = oldSessionKey

	var getEncKeyCalls int
	chain := &mockChainClient{
		ackJobFn:      func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, sid uint64) ([]byte, error) {
			getEncKeyCalls++
			assert.Equal(t, uint64(1), sid)
			return encNewSessionKey, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		assert.Equal(t, "rotated prompt", prompt)
		return "rotated resp", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, ks, ollama, rc,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.NoError(t, err)

	// Chain must have been queried exactly once (only the refresh path — the
	// first lookup was a cache hit on the stale key).
	assert.Equal(t, 1, getEncKeyCalls)

	// After refresh, the keystore should hold the new key — subsequent jobs
	// for this session will get a clean cache hit.
	stored, err := ks.GetKey(1)
	require.NoError(t, err)
	assert.Equal(t, newSessionKey, stored)
}

func TestHandleTask_CompleteJobFailure(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	promptCiphertext := encryptBlob(t, sessionKey, "test")

	ks := newMockKeyStore()
	ks.keys[1] = sessionKey

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error {
			return fmt.Errorf("TX reverted")
		},
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return nil, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "resp", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, ks, ollama, rc,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	err := handler.HandleTask(context.Background(), task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage 8 (complete job)")
}

func TestHandleTask_JobCounterIncrementDecrement(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	counter := &atomic.Int32{}

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error {
			// During processing, counter should be 1
			assert.Equal(t, int32(1), counter.Load())
			return fmt.Errorf("fail after checking counter")
		},
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return nil, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) { return nil, nil }}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) { return nil, nil }}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) { return "", nil }}

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, newMockKeyStore(), ollama, rc,
		testSigningKey(t), testECDHKey(t), counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	_ = handler.HandleTask(context.Background(), task)

	// After error, counter should be back to 0
	assert.Equal(t, int32(0), counter.Load())
}

func TestHandleTask_RedisPublishFailure_NonFatal(t *testing.T) {
	t.Parallel()

	// Use a closed Redis to simulate publish failure
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	mr.Close() // close redis so publish fails

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	promptCiphertext := encryptBlob(t, sessionKey, "test")

	ks := newMockKeyStore()
	ks.keys[1] = sessionKey

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return nil, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		return "resp", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(chain, fetcher, submitter, ks, ollama, rc,
		testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second, RedisPublishTimeout: 5 * time.Second}, nil)

	payload := testPayload(t)
	data, _ := json.Marshal(payload)
	task := asynq.NewTask(TaskTypeJobInference, data)

	// Should succeed despite Redis publish failure
	err := handler.HandleTask(context.Background(), task)
	require.NoError(t, err)

	rc.Close()
}

func TestPublishToRedis_SignsContractCompatibleDigest(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	signingKey := testSigningKey(t)
	chainID := big.NewInt(31337)
	jobRegistryAddr := common.HexToAddress("0x0000000000000000000000000000000000001337")
	handler := &JobHandler{
		redisClient: rc,
		signingKey:  signingKey,
		logger:      slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		cfg: HandlerConfig{
			ChainID:             chainID,
			JobRegistryAddr:     jobRegistryAddr,
			RedisPublishTimeout: 5 * time.Second,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := "session:9:responses"
	sub := rc.Subscribe(ctx, channel)
	t.Cleanup(func() { _ = sub.Close() })
	_, err := sub.Receive(ctx)
	require.NoError(t, err)

	ciphertext := []byte("ciphertext-for-redis")
	handler.publishToRedis(ctx, handler.logger, 42, 9, "corr-42", ciphertext)

	msg, err := sub.ReceiveMessage(ctx)
	require.NoError(t, err)

	var payload pkgtypes.PubSubMessage
	require.NoError(t, json.Unmarshal([]byte(msg.Payload), &payload))
	assert.Equal(t, pkgtypes.MessageTypeComplete, payload.Type)
	assert.Equal(t, uint64(42), uint64(payload.JobID))
	assert.Equal(t, uint64(9), uint64(payload.SessionID))
	assert.Equal(t, uint32(0), payload.Sequence)
	assert.Equal(t, uint32(1), payload.TotalChunks)
	assert.Equal(t, "corr-42", payload.CorrelationID)
	assert.Equal(t, ciphertext, payload.Payload)

	// Reconstruct the same digest the contract's disputeResponseMismatch
	// verification would compute and assert we recover the worker's signing
	// address.
	expectedDigest, err := responseMismatchDigest(chainID, jobRegistryAddr, 42, 9, ciphertext)
	require.NoError(t, err)
	sig, err := hex.DecodeString(strings.TrimPrefix(payload.Signature, "0x"))
	require.NoError(t, err)

	pubKey, err := crypto.SigToPub(accounts.TextHash(expectedDigest), sig)
	require.NoError(t, err)
	assert.Equal(t, crypto.PubkeyToAddress(signingKey.PublicKey), crypto.PubkeyToAddress(*pubKey))
}

// TestPublishToRedis_RespectsRedisPublishTimeout asserts that a slow/
// unreachable Redis cannot block stage 7 indefinitely. Stage 7 is
// non-fatal but synchronous, so without the bound a hung Publish would
// eat into stage 8's BlobTxTimeout budget and stall job throughput.
func TestPublishToRedis_RespectsRedisPublishTimeout(t *testing.T) {
	t.Parallel()

	// 192.0.2.0/24 is RFC 5737 TEST-NET-1 — guaranteed unroutable, so
	// the dial blocks until our context cancels it. Use a short
	// RedisPublishTimeout to keep the test fast; go-redis's default
	// 5s DialTimeout would otherwise dominate.
	rc := redis.NewClient(&redis.Options{Addr: "192.0.2.1:6379"})
	t.Cleanup(func() { rc.Close() })

	handler := &JobHandler{
		redisClient: rc,
		signingKey:  testSigningKey(t),
		logger:      slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		cfg: HandlerConfig{
			ChainID:             big.NewInt(31337),
			JobRegistryAddr:     common.HexToAddress("0x0000000000000000000000000000000000001337"),
			RedisPublishTimeout: 50 * time.Millisecond,
		},
	}

	start := time.Now()
	handler.publishToRedis(context.Background(), handler.logger, 1, 1, "corr", []byte("ct"))
	elapsed := time.Since(start)

	// 50ms timeout + scheduling jitter; 1s is generous but well below
	// go-redis's 5s default DialTimeout, which would dominate if our
	// context cancellation didn't fire.
	require.Less(t, elapsed, 1*time.Second,
		"publishToRedis must return within RedisPublishTimeout, not wait for go-redis DialTimeout")
}

func TestHandleTask_AckAlreadyMined_SkipsRetryAck(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	promptCiphertext := encryptBlob(t, sessionKey, "retry-safe prompt")

	ks := newMockKeyStore()
	ks.keys[1] = sessionKey

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error {
			t.Fatal("ack should be skipped when already acknowledged")
			return nil
		},
		hasJobAcknowledgedFn: func(_ context.Context, jobID uint64) (bool, error) {
			assert.Equal(t, uint64(42), jobID)
			return true, nil
		},
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
	}

	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		assert.Equal(t, "retry-safe prompt", prompt)
		return "retry-safe answer", nil
	}}

	handler := newTestHandler(t, chain, fetcher, submitter, ks, ollama, rc)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	err = handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
	require.NoError(t, err)
}

func TestHandleTask_CompleteAlreadyMined_TreatsRetryAsSuccess(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	promptCiphertext := encryptBlob(t, sessionKey, "complete-retry prompt")

	ks := newMockKeyStore()
	ks.keys[1] = sessionKey

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error {
			return fmt.Errorf("already completed")
		},
		hasJobCompletedFn: func(_ context.Context, jobID uint64) (bool, error) {
			assert.Equal(t, uint64(42), jobID)
			return true, nil
		},
	}

	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		assert.Equal(t, "complete-retry prompt", prompt)
		return "complete-retry answer", nil
	}}

	handler := newTestHandler(t, chain, fetcher, submitter, ks, ollama, rc)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	err = handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
	require.NoError(t, err)
}

// -----------------------------------------------------------------------
// JobCheckpoint tests — PR 2
// -----------------------------------------------------------------------

// TestHandleTask_CheckpointHit_SkipsInference pre-populates a checkpoint
// with a canonical ciphertext, then runs the handler. Stages 2-6 must be
// skipped (fetchBlob / inference never called), but stages 7 and 8
// must still execute against the cached ciphertext. This is the core
// "skip re-inference on retry" behavior.
func TestHandleTask_CheckpointHit_SkipsInference(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	// The cached ciphertext is what stages 2-6 WOULD have produced; we
	// hand it in directly so we can prove inference never runs.
	cachedCiphertext, err := pkgcrypto.Encrypt(sessionKey, []byte("cached response"))
	require.NoError(t, err)

	store := NewCheckpointStore(rc, 2*time.Hour, 10*time.Minute, 256*1024)
	_, wasSet, err := store.SetCiphertextIfAbsent(context.Background(), 42, cachedCiphertext)
	require.NoError(t, err)
	require.True(t, wasSet)

	var fetchCalls, inferCalls atomic.Int32
	var submitted []byte

	chain := &mockChainClient{
		ackJobFn:      func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		// getEncWorkerKeyFn returns a not-used-by-this-test value; stage 3
		// should NOT be reached on checkpoint hit, so an unimplemented
		// callback here would surface as a nil-call panic if the skip logic
		// was wrong.
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) {
			return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		fetchCalls.Add(1)
		return nil, fmt.Errorf("fetch must NOT be called on checkpoint hit")
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, data []byte) ([][32]byte, error) {
		submitted = append([]byte(nil), data...)
		return [][32]byte{{0x07}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		inferCalls.Add(1)
		return "", fmt.Errorf("inference must NOT be called on checkpoint hit")
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second},
		store,
	)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	err = handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
	require.NoError(t, err)

	assert.Equal(t, int32(0), fetchCalls.Load(), "stage 2 must be skipped")
	assert.Equal(t, int32(0), inferCalls.Load(), "stage 5 must be skipped")
	assert.Equal(t, cachedCiphertext, submitted,
		"stage 8a must submit the CACHED ciphertext (not a fresh encryption)")
}

// TestHandleTask_CheckpointRaceLoserUsesCanonical starts two goroutines
// on the same jobID concurrently. Each would produce a different
// ciphertext (AES-GCM nonces are fresh each encrypt). The winner of
// SetCiphertextIfAbsent writes its bytes; the loser refreshes to the
// canonical ciphertext before stage 8a. The critical invariant: both
// SubmitBlobTx calls must see BYTE-IDENTICAL ciphertext, so the two
// on-chain hashes agree.
func TestHandleTask_CheckpointRaceLoserUsesCanonical(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext, err := pkgcrypto.Encrypt(sessionKey, []byte("hello"))
	require.NoError(t, err)

	store := NewCheckpointStore(rc, 2*time.Hour, 10*time.Minute, 256*1024)

	// Gate inference so both goroutines call Encrypt back-to-back, fighting
	// for the SETNX. Without this barrier the race would usually be
	// trivially won by the first goroutine before the second starts.
	barrier := make(chan struct{}, 2)
	release := make(chan struct{})
	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	var submittedMu sync.Mutex
	var submittedCiphertexts [][]byte
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, data []byte) ([][32]byte, error) {
		submittedMu.Lock()
		submittedCiphertexts = append(submittedCiphertexts, append([]byte(nil), data...))
		submittedMu.Unlock()
		return [][32]byte{{0x42}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) {
		// Both goroutines park here until released, so they're both poised
		// to encrypt + race on SETNX with minimal inter-goroutine skew.
		barrier <- struct{}{}
		<-release
		return "response", nil
	}}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second},
		store,
	)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
		}(i)
	}

	// Wait for both to reach the inference barrier, then release.
	for len(barrier) < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()

	for i, e := range errs {
		require.NoError(t, e, "goroutine %d", i)
	}

	submittedMu.Lock()
	defer submittedMu.Unlock()
	require.Len(t, submittedCiphertexts, 2)
	assert.Equal(t, submittedCiphertexts[0], submittedCiphertexts[1],
		"both SubmitBlobTx calls must observe BYTE-IDENTICAL canonical ciphertext — loser must refresh from SETNX")
}

// TestHandleTask_CheckpointTombstonedAfterCompletion asserts that a
// successful run shrinks the checkpoint TTL so a straggler retry can
// still see it for a short window while stale entries age out.
func TestHandleTask_CheckpointTombstonedAfterCompletion(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext, err := pkgcrypto.Encrypt(sessionKey, []byte("hello"))
	require.NoError(t, err)

	// Redis TTL resolution is whole seconds, so the tombstone TTL must be
	// >= 1s. Use 1s and FastForward past it to verify expiry.
	store := NewCheckpointStore(rc, 2*time.Hour, 1*time.Second, 256*1024)

	chain := &mockChainClient{
		ackJobFn:          func(_ context.Context, _ uint64) error { return nil },
		completeJobFn:     func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error { return nil },
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0x99}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) { return "r", nil }}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second},
		store,
	)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data)))

	// Immediately after completion the record is still present under the
	// short tombstone TTL.
	ckpt, err := store.Get(context.Background(), payload.JobID)
	require.NoError(t, err)
	assert.False(t, ckpt.IsEmpty(), "tombstoned checkpoint must still be readable briefly")

	// Fast-forward past the tombstone TTL; the key must be GC'd.
	mr.FastForward(2 * time.Second)
	ckpt, err = store.Get(context.Background(), payload.JobID)
	require.NoError(t, err)
	assert.True(t, ckpt.IsEmpty(), "tombstoned checkpoint must expire within tombstone TTL")
}

// TestHandleTask_CheckpointRetained_WhenStage8bFails asserts that a
// failure in stage 8b (completeJob) leaves the checkpoint INTACT so the
// asynq retry can fast-forward through stages 2-8a.
func TestHandleTask_CheckpointRetained_WhenStage8bFails(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext, err := pkgcrypto.Encrypt(sessionKey, []byte("hello"))
	require.NoError(t, err)

	store := NewCheckpointStore(rc, 2*time.Hour, 10*time.Minute, 256*1024)

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error {
			return fmt.Errorf("simulated stage 8b failure")
		},
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
		return [][32]byte{{0xab}}, nil
	}}
	ollama := &mockOllama{generateFn: func(_ context.Context, _, _ string) (string, error) { return "r", nil }}

	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{AckTxTimeout: 5 * time.Second, BlobTxTimeout: 60 * time.Second},
		store,
	)

	payload := testPayload(t)
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	err = handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
	require.Error(t, err, "stage 8b failure must propagate so asynq retries")

	// Checkpoint must still hold ciphertext + versionedHash so the retry
	// can skip stages 2-8a and go straight to stage 8b.
	ckpt, err := store.Get(context.Background(), payload.JobID)
	require.NoError(t, err)
	assert.True(t, ckpt.HasCiphertext(), "ciphertext must survive stage 8b failure")
	assert.True(t, ckpt.HasVersionedHash(), "versionedHash must survive stage 8b failure")
	assert.True(t, ckpt.Delivered, "stage 7 ran successfully before 8b, so delivered must be set")
}

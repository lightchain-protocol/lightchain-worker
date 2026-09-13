package pipeline

import (
	"context"
	"crypto/ecdh"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
)

// mockDeadlineChainClient adds GetJobDeadline to mockChainClient so the
// deadline guard engages. The embedded mock's function fields still govern
// the lifecycle methods.
type mockDeadlineChainClient struct {
	mockChainClient
	getJobDeadlineFn func(ctx context.Context, jobID uint64) (time.Time, bool, error)
}

func (m *mockDeadlineChainClient) GetJobDeadline(ctx context.Context, jobID uint64) (time.Time, bool, error) {
	return m.getJobDeadlineFn(ctx, jobID)
}

// mockHistoryChainClient overrides GetJobBlobInfo for conversation-history
// tests. Everything else falls through to the embedded mock.
type mockHistoryChainClient struct {
	mockChainClient
	getJobBlobInfoFn func(ctx context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error)
}

func (m *mockHistoryChainClient) GetJobBlobInfo(ctx context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	return m.getJobBlobInfoFn(ctx, jobID)
}

// newGuardTestHandler builds a handler with the deadline guard enabled and
// explicit reserves, so tests can reason about exact thresholds.
func newGuardTestHandler(
	t *testing.T,
	chain JobExecutionClient,
	fetcher *mockBlobFetcher,
	submitter *mockBlobSubmitter,
	ollamaClient *mockOllama,
	redisClient *redis.Client,
	ecdhKey *ecdh.PrivateKey,
) *JobHandler {
	t.Helper()
	counter := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollamaClient,
		redisClient, testSigningKey(t), ecdhKey, counter, logger,
		HandlerConfig{
			AckTxTimeout:         5 * time.Second,
			BlobTxTimeout:        60 * time.Second,
			RedisPublishTimeout:  5 * time.Second,
			DeadlineGuardEnabled: true,
			SettleReserve:        25 * time.Second,
			CompletionReserve:    12 * time.Second,
			MinInferenceBudget:   10 * time.Second,
		},
		nil, // publisher — fallback wires RedisResponsePublisher
		nil, // checkpoints
		testMetrics(t),
		metrics.DeliveryAsynq,
nil,
	)
}

// guardFixtures returns the crypto material a full-pipeline run needs:
// a session key, the worker's ECDH key, the on-chain encrypted session key,
// and the encrypted prompt blob.
func guardFixtures(t *testing.T) (sessionKey []byte, ecdhKey *ecdh.PrivateKey, encSessionKey, promptCiphertext []byte) {
	t.Helper()
	sessionKey = testSessionKey(t)
	ecdhKey = testECDHKey(t)
	encSessionKey = encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext = encryptBlob(t, sessionKey, "What is 2+2?")
	return sessionKey, ecdhKey, encSessionKey, promptCiphertext
}

func runTestJob(t *testing.T, handler *JobHandler) error {
	t.Helper()
	data, err := json.Marshal(testPayload(t))
	require.NoError(t, err)
	return handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
}

func TestHandleTask_DeadlineExpiredAtPickup_NoRetry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:          func(context.Context, uint64) error { panic("ack must not be sent for an expired job") },
			completeJobFn:     func(context.Context, uint64, [32]byte, [32]byte) error { panic("completeJob must not run") },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { panic("session key must not be fetched") },
		},
		getJobDeadlineFn: func(_ context.Context, jobID uint64) (time.Time, bool, error) {
			assert.Equal(t, uint64(42), jobID)
			return time.Now().Add(-time.Minute), false, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		panic("blob fetch must not run")
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		panic("blob tx must not be burned for an expired job")
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) {
		panic("inference must not run")
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, testECDHKey(t))

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry, "an expired job must never be retried")
	assert.ErrorIs(t, err, ErrJobDoomed)
}

func TestHandleTask_DeadlineReadFailure_ProceedsWithoutGuard(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:      func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			return time.Time{}, false, fmt.Errorf("rpc flake")
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	oll := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		assert.Equal(t, "What is 2+2?", prompt)
		return "4", nil
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	// The guard fails open: a flaky RPC must never break a healthy job.
	require.NoError(t, runTestJob(t, handler))
}

func TestHandleTask_DeadlineGuardAbortsBeforeInference(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	var deadlineCalls int
	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn: func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
				panic("completeJob must not run")
			},
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			deadlineCalls++
			if deadlineCalls == 1 {
				// Pickup: not yet acknowledged, ack deadline well ahead.
				return time.Now().Add(60 * time.Second), false, nil
			}
			// Stage-5 gate: acknowledged, but only 20s of the completion
			// window remain - less than settleReserve(25s)+minInference(10s).
			return time.Now().Add(20 * time.Second), true, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		panic("blob tx must not be burned for a doomed job")
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) {
		panic("inference must not start with no viable settlement window")
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
	assert.ErrorIs(t, err, ErrJobDoomed)
	assert.Equal(t, 2, deadlineCalls, "expected exactly the pickup and pre-inference reads")
}

func TestHandleTask_DeadlineGuardAbortsBeforeBlobSubmit(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	var deadlineCalls int
	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn: func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
				panic("completeJob must not run after the blob gate aborts")
			},
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			deadlineCalls++
			switch deadlineCalls {
			case 1:
				// Pickup: not yet acknowledged, ack deadline fine.
				return time.Now().Add(60 * time.Second), false, nil
			case 2:
				// Stage-5 gate: 60s remain, comfortably over 35s - inference
				// runs (and is instant in the mock).
				return time.Now().Add(60 * time.Second), true, nil
			default:
				// Pre-8a gate: only 5s left, under the 12s completion
				// reserve - the blob tx must not be burned.
				return time.Now().Add(5 * time.Second), true, nil
			}
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		panic("blob tx must not be burned inside the completion reserve")
	}}
	oll := &mockOllama{generateFn: func(_ context.Context, _, prompt string) (string, error) {
		assert.Equal(t, "What is 2+2?", prompt)
		return "4", nil
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
	assert.ErrorIs(t, err, ErrJobDoomed)
	assert.Equal(t, 3, deadlineCalls, "expected pickup, pre-inference and pre-blob reads")
}

func TestHandleTask_CompleteJobFailurePastDeadline_NoRetry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	var deadlineCalls int
	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn: func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
				return fmt.Errorf("execution reverted: DeadlineExceeded")
			},
			hasJobCompletedFn: func(context.Context, uint64) (bool, error) { return false, nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			deadlineCalls++
			switch deadlineCalls {
			case 1:
				return time.Now().Add(60 * time.Second), false, nil
			case 2:
				return time.Now().Add(60 * time.Second), true, nil
			case 3:
				// Pre-8a: 30s left, over the 12s completion reserve.
				return time.Now().Add(30 * time.Second), true, nil
			default:
				// Post-completeJob classification: the deadline has passed.
				return time.Now().Add(-5 * time.Second), true, nil
			}
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) { return "4", nil }}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry,
		"a completeJob failure with the deadline already passed is permanent DeadlineExceeded")
	assert.ErrorIs(t, err, ErrJobDoomed)
}

func TestHandleTask_CompleteJobFailureBeforeDeadline_StaysRetryable(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	var deadlineCalls int
	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn: func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
				return fmt.Errorf("rpc: transaction underpriced")
			},
			hasJobCompletedFn: func(context.Context, uint64) (bool, error) { return false, nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			deadlineCalls++
			if deadlineCalls == 1 {
				return time.Now().Add(60 * time.Second), false, nil
			}
			// Everything comfortably inside the window; the completeJob
			// failure is transient and must stay retryable.
			return time.Now().Add(60 * time.Second), true, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) { return "4", nil }}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.NotErrorIs(t, err, asynq.SkipRetry,
		"a completeJob failure with time left on the deadline is transient and retryable")
	assert.NotErrorIs(t, err, ErrJobDoomed)
}

func TestHandleTask_EmptyGeneration_NoRetry(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	// Plain mockChainClient: no GetJobDeadline, so the guard is disabled
	// even though the config enables it - the empty-generation no-retry
	// mapping must not depend on the deadline guard.
	chain := &mockChainClient{
		ackJobFn: func(context.Context, uint64) error { return nil },
		completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
			panic("completeJob must not run after an empty generation")
		},
		getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
			return encSessionKey, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		panic("blob tx must not be burned after an empty generation")
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) {
		return "", fmt.Errorf("%w: the whole generation arrived as thinking (512 bytes), which is never transmitted", ollama.ErrEmptyGeneration)
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	err := runTestJob(t, handler)
	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry,
		"parameter-identical retries of an empty generation are deterministic waste")
	assert.ErrorIs(t, err, ollama.ErrEmptyGeneration)
}

func TestHandleTask_DeadlineGuardClampsInferenceContext(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)

	deadline := time.Now().Add(90 * time.Second)
	var deadlineCalls int
	chain := &mockDeadlineChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:      func(context.Context, uint64) error { return nil },
			completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) {
				return encSessionKey, nil
			},
		},
		getJobDeadlineFn: func(context.Context, uint64) (time.Time, bool, error) {
			deadlineCalls++
			if deadlineCalls == 1 {
				return deadline, false, nil
			}
			return deadline, true, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	oll := &mockOllama{generateFn: func(ctx context.Context, _, _ string) (string, error) {
		dl, ok := ctx.Deadline()
		require.True(t, ok, "inference context must carry the clamped deadline")
		// Clamp = on-chain deadline minus the 25s settle reserve.
		remaining := time.Until(dl)
		assert.InDelta(t, (90 - 25), remaining.Seconds(), 2.0,
			"inference must be cut off settleReserve before the on-chain deadline")
		return "4", nil
	}}

	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)

	require.NoError(t, runTestJob(t, handler))
}

// --- N3: conversation history must decode prompt envelopes ---

func TestBuildConversationHistory_DecodesPromptEnvelopes(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)
	promptHash := common.HexToHash("0xaa01")
	responseHash := common.HexToHash("0xbb02")
	envelopeJSON := `{"v":1,"text":"describe this image","images":["aW1hZ2U="]}`
	promptBlob := encryptBlob(t, sessionKey, envelopeJSON)
	responseBlob := encryptBlob(t, sessionKey, "a previous answer")

	chain := &mockHistoryChainClient{
		getJobBlobInfoFn: func(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
			assert.Equal(t, uint64(7), jobID)
			return promptHash, responseHash, 100, 200, nil
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
		switch hash {
		case promptHash:
			return promptBlob, nil
		case responseHash:
			return responseBlob, nil
		default:
			return nil, fmt.Errorf("unexpected blob hash %s", hash)
		}
	}}

	handler := NewJobHandler(
		chain, fetcher, nil, newMockKeyStore(), nil,
		nil, testSigningKey(t), testECDHKey(t), &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		HandlerConfig{}, nil, nil, testMetrics(t), metrics.DeliveryAsynq,
nil,
	)

	msgs, err := handler.buildConversationHistory(context.Background(), []uint64{7}, sessionKey)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	// The user turn carries the envelope's text and images - not the raw
	// JSON with its base64 payload.
	assert.Equal(t, "user", msgs[0].Role)
	assert.Equal(t, "describe this image", msgs[0].Content)
	assert.Equal(t, []string{"aW1hZ2U="}, msgs[0].Images)
	assert.NotContains(t, msgs[0].Content, "aW1hZ2U=")

	assert.Equal(t, "assistant", msgs[1].Role)
	assert.Equal(t, "a previous answer", msgs[1].Content)
}

func TestBuildConversationHistory_PlainPromptsUnchanged(t *testing.T) {
	t.Parallel()

	sessionKey := testSessionKey(t)

	cases := map[string]string{
		"plain text":                   "just a plain question",
		"JSON that is not an envelope": `{"question":"is this an envelope?"}`,
	}
	for name, plaintext := range cases {
		t.Run(name, func(t *testing.T) {
			promptHash := common.HexToHash("0xcc03")
			promptBlob := encryptBlob(t, sessionKey, plaintext)

			chain := &mockHistoryChainClient{
				getJobBlobInfoFn: func(context.Context, uint64) (common.Hash, common.Hash, uint64, uint64, error) {
					return promptHash, common.Hash{}, 100, 0, nil
				},
			}
			fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
				require.Equal(t, promptHash, hash)
				return promptBlob, nil
			}}

			handler := NewJobHandler(
				chain, fetcher, nil, newMockKeyStore(), nil,
				nil, testSigningKey(t), testECDHKey(t), &atomic.Int32{},
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				HandlerConfig{}, nil, nil, testMetrics(t), metrics.DeliveryAsynq,
nil,
			)

			msgs, err := handler.buildConversationHistory(context.Background(), []uint64{9}, sessionKey)
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			assert.Equal(t, plaintext, msgs[0].Content)
			assert.Nil(t, msgs[0].Images)
		})
	}
}

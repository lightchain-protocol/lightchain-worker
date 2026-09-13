package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
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

	"github.com/lightchain/worker/internal/metrics"
)

// trackerCall captures a single MarkEligible invocation.
type trackerCall struct {
	jobID       uint64
	completedAt int64
}

// mockTracker implements ReleaseTracker. The optional `failWith` field
// makes the tracker return an error so the non-fatal property can be
// verified.
type mockTracker struct {
	mu       sync.Mutex
	calls    []trackerCall
	failWith error
}

func (m *mockTracker) MarkEligible(_ context.Context, jobID uint64, completedAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, trackerCall{jobID: jobID, completedAt: completedAt})
	return m.failWith
}

func (m *mockTracker) snapshot() []trackerCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]trackerCall, len(m.calls))
	copy(out, m.calls)
	return out
}

// runFullPipelineWithTracker mirrors TestHandleTask_FullPipelineSuccess but
// installs a release tracker so we can assert the post-stage-8b call. The
// `tracker` argument is bound via SetReleaseTracker.
func runFullPipelineWithTracker(t *testing.T, tracker ReleaseTracker) (handlerErr error, setupTime time.Time) {
	t.Helper()

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	promptCiphertext := encryptBlob(t, sessionKey, "What is 2+2?")

	expectedModelID := common.Bytes2Hex(crypto.Keccak256Hash([]byte("llama3-8b")).Bytes())

	chain := &mockChainClient{
		ackJobFn: func(_ context.Context, _ uint64) error { return nil },
		completeJobFn: func(_ context.Context, _ uint64, _ [32]byte, _ [32]byte) error {
			return nil
		},
		getEncWorkerKeyFn: func(_ context.Context, _ uint64) ([]byte, error) {
			return encSessionKey, nil
		},
	}
	fetcher := &mockBlobFetcher{
		fetchFn: func(_ context.Context, _ common.Hash, _ uint64) ([]byte, error) {
			return promptCiphertext, nil
		},
	}
	submitter := &mockBlobSubmitter{
		submitFn: func(_ context.Context, _ []byte) ([][32]byte, error) {
			return [][32]byte{{0x01}}, nil
		},
	}
	ollama := &mockOllama{
		generateFn: func(_ context.Context, _, _ string) (string, error) { return "4", nil },
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), ollama,
		rc, testSigningKey(t), ecdhKey, &atomic.Int32{}, logger,
		HandlerConfig{
			AckTxTimeout:        5 * time.Second,
			BlobTxTimeout:       60 * time.Second,
			RedisPublishTimeout: 5 * time.Second,
			ModelIDToName:       map[string]string{expectedModelID: "llama3-8b"},
		},
		nil, nil, testMetrics(t), metrics.DeliveryAsynq, nil,
	)
	handler.SetReleaseTracker(tracker)

	payload := testPayload(t)
	payload.ModelID = expectedModelID
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	task := asynq.NewTask(TaskTypeJobInference, data)

	setupTime = time.Now()
	handlerErr = handler.HandleTask(context.Background(), task)
	return
}

func TestHandleTask_MarkEligibleAfterStage8b(t *testing.T) {
	t.Parallel()
	tracker := &mockTracker{}

	err, before := runFullPipelineWithTracker(t, tracker)
	after := time.Now()
	require.NoError(t, err)

	calls := tracker.snapshot()
	require.Len(t, calls, 1, "MarkEligible must fire exactly once after a successful stage 8b")
	assert.Equal(t, uint64(42), calls[0].jobID, "jobID from payload")
	assert.GreaterOrEqual(t, calls[0].completedAt, before.Unix())
	assert.LessOrEqual(t, calls[0].completedAt, after.Unix())
}

func TestHandleTask_MarkEligibleFailureIsNonFatal(t *testing.T) {
	t.Parallel()
	tracker := &mockTracker{failWith: errors.New("disk full")}

	err, _ := runFullPipelineWithTracker(t, tracker)
	require.NoError(t, err, "tracker error must NOT propagate; reconciler will backfill")

	calls := tracker.snapshot()
	require.Len(t, calls, 1, "tracker still called even though it errors")
}

func TestHandleTask_NilTrackerSkipped(t *testing.T) {
	t.Parallel()
	// Default behavior — handler with no tracker installed.
	err, _ := runFullPipelineWithTracker(t, nil)
	require.NoError(t, err, "missing tracker is the no-tracker default; not an error")
}

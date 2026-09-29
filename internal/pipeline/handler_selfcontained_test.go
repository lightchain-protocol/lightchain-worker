package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/pkg/promptenv"
	"github.com/lightchain/pkg/promptenv/promptenvtest"

	"github.com/lightchain/worker/internal/metrics"
	"github.com/lightchain/worker/internal/ollama"
)

// chatOnlyInference records every chat call, batch or streaming, and fails
// the test on any generate call.
type chatOnlyInference struct {
	t     *testing.T
	chats [][]ollama.ChatMessage
}

func (c *chatOnlyInference) Generate(context.Context, string, string) (string, error) {
	c.t.Error("a self-contained job must never use the generate call")
	return "", errors.New("generate called")
}

func (c *chatOnlyInference) Chat(_ context.Context, _ string, msgs []ollama.ChatMessage) (string, error) {
	c.chats = append(c.chats, msgs)
	return "4", nil
}

func (c *chatOnlyInference) GenerateStream(context.Context, string, string, []string, ollama.StreamHandlers) (string, ollama.StreamStats, error) {
	c.t.Error("a self-contained job must never use the generate call")
	return "", ollama.StreamStats{}, errors.New("generate called")
}

func (c *chatOnlyInference) ChatStream(_ context.Context, _ string, msgs []ollama.ChatMessage, h ollama.StreamHandlers) (string, ollama.StreamStats, error) {
	c.chats = append(c.chats, msgs)
	if err := h.OnToken("4"); err != nil {
		return "", ollama.StreamStats{}, err
	}
	return "4", ollama.StreamStats{}, nil
}

// runSelfContainedJob serves one job whose prompt blob decrypts to envelope.
// The payload names prior jobs in the session, as the job watcher supplies
// them; any lookup of their blobs fails the test.
func runSelfContainedJob(t *testing.T, envelope string, inference *chatOnlyInference, stream bool) error {
	t.Helper()
	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	payload := testPayload(t)
	payload.PriorJobIDs = []uint64{40, 41}

	chain := &mockChainClient{
		ackJobFn:          func(context.Context, uint64) error { return nil },
		completeJobFn:     func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
		getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
		getJobBlobInfoFn: func(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
			t.Errorf("a self-contained job must not look up prior job %d", jobID)
			return common.Hash{}, common.Hash{}, 0, 0, errors.New("prior job lookup")
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
		if hash != payload.PromptBlobHash {
			t.Errorf("a self-contained job must fetch only its own prompt blob, got %s", hash)
			return nil, fmt.Errorf("unexpected blob %s", hash)
		}
		return encryptBlob(t, sessionKey, envelope), nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	handler := NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), inference,
		nil, testSigningKey(t), ecdhKey, &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		HandlerConfig{
			AckTxTimeout:      5 * time.Second,
			BlobTxTimeout:     60 * time.Second,
			StreamEnabled:     stream,
			StreamChunkTokens: 1,
		},
		&recordingPublisher{},
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil,
	)

	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
}

func TestHandleTask_SelfContainedJobChatsWithExactlyItsMessages(t *testing.T) {
	t.Parallel()

	for _, f := range promptenvtest.SelfContained {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", f.Name, stream), func(t *testing.T) {
				t.Parallel()
				inference := &chatOnlyInference{t: t}

				require.NoError(t, runSelfContainedJob(t, f.Envelope, inference, stream))

				require.Len(t, inference.chats, 1)
				assert.Equal(t, toChatMessages(f.Messages), inference.chats[0])
			})
		}
	}
}

func TestHandleTask_MalformedSelfContainedJobIsRefusedWithoutRetry(t *testing.T) {
	t.Parallel()

	for name, envelope := range promptenvtest.Refused {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inference := &chatOnlyInference{t: t}

			err := runSelfContainedJob(t, envelope, inference, false)

			require.Error(t, err)
			assert.ErrorIs(t, err, asynq.SkipRetry)
			assert.Empty(t, inference.chats, "a refused job never reaches the model")
		})
	}
}

// toChatMessages spells out what the model is expected to receive for a
// fixture, independently of the handler's own conversion.
func toChatMessages(msgs []promptenv.Message) []ollama.ChatMessage {
	out := make([]ollama.ChatMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, ollama.ChatMessage{Role: m.Role, Content: m.Content})
	}
	return out
}

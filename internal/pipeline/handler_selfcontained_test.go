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

// listingChainClient adds the prior-job listing sortition mode uses to find
// a session's earlier jobs.
type listingChainClient struct {
	mockChainClient
	listFn func(ctx context.Context, sessionID, currentJobID, fromBlock, toBlock uint64) ([]uint64, error)
}

func (c *listingChainClient) GetPriorSessionJobIDs(ctx context.Context, sessionID, currentJobID, fromBlock, toBlock uint64) ([]uint64, error) {
	return c.listFn(ctx, sessionID, currentJobID, fromBlock, toBlock)
}

// runSelfContainedJob serves one job whose prompt blob decrypts to envelope.
// prior is the payload's PriorJobIDs: named by the dispatcher, or empty as in
// sortition mode, where the handler would list them from the chain. Listing
// them, or looking up any of their blobs, fails the test.
func runSelfContainedJob(t *testing.T, envelope string, prior []uint64, inference *chatOnlyInference, stream bool) error {
	t.Helper()
	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	payload := testPayload(t)
	payload.PriorJobIDs = prior

	chain := &listingChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:          func(context.Context, uint64) error { return nil },
			completeJobFn:     func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
			getJobBlobInfoFn: func(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
				t.Errorf("a self-contained job must not look up prior job %d", jobID)
				return common.Hash{}, common.Hash{}, 0, 0, errors.New("prior job lookup")
			},
		},
		listFn: func(context.Context, uint64, uint64, uint64, uint64) ([]uint64, error) {
			t.Error("a self-contained job must not list the session's prior jobs")
			return []uint64{40, 41}, nil
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
			AckTxTimeout:          5 * time.Second,
			BlobTxTimeout:         60 * time.Second,
			StreamEnabled:         stream,
			StreamChunkTokens:     1,
			HistoryLookbackBlocks: 1000,
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

				require.NoError(t, runSelfContainedJob(t, f.Envelope, nil, inference, stream))

				require.Len(t, inference.chats, 1)
				assert.Equal(t, toChatMessages(f.Messages), inference.chats[0])
			})
		}
	}
}

// On the dispatcher path the payload names the session's earlier jobs; a
// self-contained job still never fetches them.
func TestHandleTask_SelfContainedJobIgnoresPriorJobsNamedInThePayload(t *testing.T) {
	t.Parallel()
	f := promptenvtest.SelfContained[0]
	inference := &chatOnlyInference{t: t}

	require.NoError(t, runSelfContainedJob(t, f.Envelope, []uint64{40, 41}, inference, false))

	require.Len(t, inference.chats, 1)
	assert.Equal(t, toChatMessages(f.Messages), inference.chats[0])
}

func TestHandleTask_MalformedSelfContainedJobIsRefusedWithoutRetry(t *testing.T) {
	t.Parallel()

	for name, envelope := range promptenvtest.Refused {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inference := &chatOnlyInference{t: t}

			err := runSelfContainedJob(t, envelope, nil, inference, false)

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

// newListingHandler builds a handler in sortition shape: the payload names no
// prior jobs and the handler lists them from the chain, looking back 1000
// blocks. The prompt is "second question"; prior job 7 is a completed turn.
func newListingHandler(t *testing.T, listFn func(ctx context.Context, sessionID, currentJobID, fromBlock, toBlock uint64) ([]uint64, error), oll *mockOllama) *JobHandler {
	t.Helper()
	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	priorPrompt, priorResponse := common.HexToHash("0xaa07"), common.HexToHash("0xbb07")

	chain := &listingChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:          func(context.Context, uint64) error { return nil },
			completeJobFn:     func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
			getJobBlobInfoFn: func(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
				require.Equal(t, uint64(7), jobID)
				return priorPrompt, priorResponse, 60, 61, nil
			},
		},
		listFn: listFn,
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
		switch hash {
		case priorPrompt:
			return encryptBlob(t, sessionKey, "first question"), nil
		case priorResponse:
			return encryptBlob(t, sessionKey, "first answer"), nil
		default:
			return encryptBlob(t, sessionKey, "second question"), nil
		}
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	return NewJobHandler(
		chain, fetcher, submitter, newMockKeyStore(), oll,
		nil, testSigningKey(t), ecdhKey, &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		HandlerConfig{
			AckTxTimeout:          5 * time.Second,
			BlobTxTimeout:         60 * time.Second,
			HistoryLookbackBlocks: 1000,
		},
		&recordingPublisher{},
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil,
	)
}

// Sortition mode has no dispatcher to name a chat job's earlier turns, so the
// handler lists them from the chain and rebuilds the conversation.
func TestHandleJobPayload_ChatJobListsPriorJobsForHistory(t *testing.T) {
	t.Parallel()
	var listed []uint64
	var chat []ollama.ChatMessage
	oll := &mockOllama{chatFn: func(_ context.Context, _ string, msgs []ollama.ChatMessage) (string, error) {
		chat = msgs
		return "second answer", nil
	}}
	handler := newListingHandler(t, func(_ context.Context, sessionID, currentJobID, fromBlock, toBlock uint64) ([]uint64, error) {
		listed = []uint64{sessionID, currentJobID, fromBlock, toBlock}
		return []uint64{7}, nil
	}, oll)

	payload := testPayload(t)
	payload.BlockNumber = 1500
	require.NoError(t, handler.HandleJobPayload(context.Background(), payload))

	assert.Equal(t, []uint64{payload.SessionID, payload.JobID, 500, 1500}, listed,
		"the listing covers the look-back window up to the job's own block")
	assert.Equal(t, []ollama.ChatMessage{
		{Role: "user", Content: "first question"},
		{Role: "assistant", Content: "first answer"},
		{Role: "user", Content: "second question"},
	}, chat)
}

// The listing is best-effort: when it fails the job is still served,
// single-turn.
func TestHandleJobPayload_PriorJobListingFailureServesSingleTurn(t *testing.T) {
	t.Parallel()
	var prompt string
	oll := &mockOllama{generateFn: func(_ context.Context, _, p string) (string, error) {
		prompt = p
		return "an answer", nil
	}}
	handler := newListingHandler(t, func(context.Context, uint64, uint64, uint64, uint64) ([]uint64, error) {
		return nil, errors.New("rpc boom")
	}, oll)

	require.NoError(t, handler.HandleJobPayload(context.Background(), testPayload(t)))

	assert.Equal(t, "second question", prompt)
}

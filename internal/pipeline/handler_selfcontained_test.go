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

// listingChainClient adds the prior-job listing sortition mode wires in.
type listingChainClient struct {
	mockChainClient
	listFn func(ctx context.Context, sessionID, fromBlock, toBlock uint64) ([]uint64, error)
}

func (c *listingChainClient) SessionJobIDs(ctx context.Context, sessionID, fromBlock, toBlock uint64) ([]uint64, error) {
	return c.listFn(ctx, sessionID, fromBlock, toBlock)
}

// jobRig serves one job over mocks: blobs maps a blob hash to the plaintext
// it decrypts to, and priors the session's earlier jobs to their prompt and
// answer hashes. Looking up a prior job not added, fetching an unknown blob,
// or listing prior jobs without a listFn fails the test.
type jobRig struct {
	handler *JobHandler
	chain   *listingChainClient
	pub     *recordingPublisher
	payload JobPayload
	blobs   map[common.Hash]string
	priors  map[uint64][2]common.Hash
}

func newJobRig(t *testing.T, prompt string, inference InferenceClient, stream bool) *jobRig {
	t.Helper()
	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	encSessionKey := encryptSessionKeyForWorker(t, sessionKey, ecdhKey)
	r := &jobRig{
		pub:     &recordingPublisher{},
		payload: testPayload(t),
		blobs:   map[common.Hash]string{},
		priors:  map[uint64][2]common.Hash{},
	}
	r.blobs[r.payload.PromptBlobHash] = prompt

	r.chain = &listingChainClient{
		mockChainClient: mockChainClient{
			ackJobFn:          func(context.Context, uint64) error { return nil },
			completeJobFn:     func(context.Context, uint64, [32]byte, [32]byte) error { return nil },
			getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
			getJobBlobInfoFn: func(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
				h, ok := r.priors[jobID]
				if !ok {
					t.Errorf("unexpected lookup of prior job %d", jobID)
					return common.Hash{}, common.Hash{}, 0, 0, errors.New("prior job lookup")
				}
				return h[0], h[1], 10, 11, nil
			},
		},
		listFn: func(context.Context, uint64, uint64, uint64) ([]uint64, error) {
			t.Error("unexpected listing of the session's prior jobs")
			return nil, errors.New("prior job listing")
		},
	}
	fetcher := &mockBlobFetcher{fetchFn: func(_ context.Context, hash common.Hash, _ uint64) ([]byte, error) {
		plain, ok := r.blobs[hash]
		if !ok {
			t.Errorf("unexpected fetch of blob %s", hash)
			return nil, fmt.Errorf("unexpected blob %s", hash)
		}
		return encryptBlob(t, sessionKey, plain), nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}

	r.handler = NewJobHandler(
		r.chain, fetcher, submitter, newMockKeyStore(), inference,
		nil, testSigningKey(t), ecdhKey, &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		HandlerConfig{
			AckTxTimeout:      5 * time.Second,
			BlobTxTimeout:     60 * time.Second,
			StreamEnabled:     stream,
			StreamChunkTokens: 1,
		},
		r.pub,
		nil,
		testMetrics(t),
		metrics.DeliveryAsynq,
		nil,
	)
	return r
}

func (r *jobRig) addPriorJob(id uint64, prompt, answer string) {
	p, a := common.Hash{0xaa, byte(id)}, common.Hash{0xbb, byte(id)}
	r.priors[id] = [2]common.Hash{p, a}
	r.blobs[p], r.blobs[a] = prompt, answer
}

func (r *jobRig) run(t *testing.T) error {
	t.Helper()
	data, err := json.Marshal(r.payload)
	require.NoError(t, err)
	return r.handler.HandleTask(context.Background(), asynq.NewTask(TaskTypeJobInference, data))
}

// In sortition shape: the lister is wired and the payload names no prior
// jobs, yet a self-contained job neither lists nor fetches any.
func TestHandleTask_SelfContainedJobChatsWithExactlyItsMessages(t *testing.T) {
	t.Parallel()

	for _, f := range promptenvtest.SelfContained {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", f.Name, stream), func(t *testing.T) {
				t.Parallel()
				inference := &chatOnlyInference{t: t}
				r := newJobRig(t, f.Envelope, inference, stream)
				r.handler.SetPriorJobLister(r.chain)

				require.NoError(t, r.run(t))

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
	r := newJobRig(t, f.Envelope, inference, false)
	r.payload.PriorJobIDs = []uint64{40, 41}

	require.NoError(t, r.run(t))

	require.Len(t, inference.chats, 1)
	assert.Equal(t, toChatMessages(f.Messages), inference.chats[0])
}

func TestHandleTask_MalformedSelfContainedJobIsRefusedWithoutRetry(t *testing.T) {
	t.Parallel()

	for name, envelope := range promptenvtest.Refused {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inference := &chatOnlyInference{t: t}

			err := newJobRig(t, envelope, inference, false).run(t)

			require.Error(t, err)
			assert.ErrorIs(t, err, asynq.SkipRetry)
			assert.Empty(t, inference.chats, "a refused job never reaches the model")
		})
	}
}

// The speech model reads one text aloud; it has no chat call for a
// conversation, so a self-contained job for it is refused, not voiced empty.
func TestHandleTask_SelfContainedJobForTheSpeechModelIsRefused(t *testing.T) {
	engine := &mockVoiceEngine{}
	rig := newVoiceTestRig(t, promptenvtest.SelfContained[0].Envelope,
		HandlerConfig{
			SpeechModelName: "tts-piper",
			ModelIDToName:   map[string]string{"llama3-8b": "tts-piper"},
		},
		fatalInference{t}, engine)

	err := runVoiceJob(t, rig)

	require.Error(t, err)
	assert.ErrorIs(t, err, asynq.SkipRetry)
	assert.Zero(t, engine.settleds, "nothing may be synthesized")
}

// A chat job's history replays each earlier prompt the way the disputer
// does, and leaves out a self-contained job and its answer: it was another
// conversation in the same session.
func TestHandleTask_HistoryReplaysPriorPromptsLikeTheDisputer(t *testing.T) {
	t.Parallel()
	var chat []ollama.ChatMessage
	oll := &mockOllama{chatFn: func(_ context.Context, _ string, msgs []ollama.ChatMessage) (string, error) {
		chat = msgs
		return "next answer", nil
	}}
	r := newJobRig(t, "next question", oll, false)

	var want []ollama.ChatMessage
	for i, f := range promptenvtest.PriorPrompts {
		id := uint64(i + 1)
		answer := fmt.Sprintf("answer %d", id)
		r.addPriorJob(id, f.Prompt, answer)
		r.payload.PriorJobIDs = append(r.payload.PriorJobIDs, id)
		if !f.Skipped {
			want = append(want,
				ollama.ChatMessage{Role: "user", Content: f.Turn},
				ollama.ChatMessage{Role: "assistant", Content: answer})
		}
	}
	want = append(want, ollama.ChatMessage{Role: "user", Content: "next question"})

	require.NoError(t, r.run(t))

	assert.Equal(t, want, chat)
}

// On a busy chain a session's earlier jobs sit far more than 50 job ids behind
// the one being served. Sortition mode has no dispatcher to name them, so the
// handler finds them on chain through the shared lookup and serves the job on
// their history: exactly the history the disputer rebuilds for the same
// fixture.
func TestHandleJobPayload_ChatJobOnABusyChainServesTheSessionsHistory(t *testing.T) {
	t.Parallel()
	s := promptenvtest.BusySession
	var chat []ollama.ChatMessage
	oll := &mockOllama{chatFn: func(_ context.Context, _ string, msgs []ollama.ChatMessage) (string, error) {
		chat = msgs
		return "next answer", nil
	}}
	r := newJobRig(t, s.Current.Prompt, oll, false)
	r.payload.JobID, r.payload.SessionID, r.payload.BlockNumber = s.Current.ID, s.Current.SessionID, s.Current.SubmitBlock
	for _, j := range s.Chain {
		r.addPriorJob(j.ID, j.Prompt, j.Answer)
	}
	r.chain.listFn = func(_ context.Context, sessionID, fromBlock, toBlock uint64) ([]uint64, error) {
		return s.JobSubmitted(sessionID, fromBlock, toBlock), nil
	}
	r.handler.SetPriorJobLister(r.chain)

	require.NoError(t, r.handler.HandleJobPayload(context.Background(), r.payload))

	want := append(toChatMessages(s.History), ollama.ChatMessage{Role: "user", Content: s.Current.Prompt})
	assert.Equal(t, want, chat)
}

// A history the worker cannot rebuild fails the job: the consumer gets an
// error frame, and the job is refunded or times out. Serving it on no history
// instead would leave an answer the disputer cannot reproduce: it rebuilds
// the whole history to re-run the job.
func TestHandleJobPayload_HistoryFailureFailsTheJob(t *testing.T) {
	t.Parallel()

	for name, breakChain := range map[string]func(r *jobRig){
		"listing the session's jobs fails": func(r *jobRig) {
			r.chain.listFn = func(context.Context, uint64, uint64, uint64) ([]uint64, error) {
				return nil, errors.New("rpc boom")
			}
		},
		"reading an earlier job fails": func(r *jobRig) {
			r.chain.listFn = func(context.Context, uint64, uint64, uint64) ([]uint64, error) {
				return []uint64{7}, nil
			}
			r.chain.getJobBlobInfoFn = func(context.Context, uint64) (common.Hash, common.Hash, uint64, uint64, error) {
				return common.Hash{}, common.Hash{}, 0, 0, errors.New("rpc boom")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			model := &mockOllama{
				generateFn: func(context.Context, string, string) (string, error) {
					t.Error("the job must not be served without its history")
					return "", nil
				},
			}
			r := newJobRig(t, "second question", model, false)
			breakChain(r)
			r.handler.SetPriorJobLister(r.chain)

			require.Error(t, r.handler.HandleJobPayload(context.Background(), r.payload))

			assert.Equal(t, 1, r.pub.errorFrames, "the consumer must be told the job failed")
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

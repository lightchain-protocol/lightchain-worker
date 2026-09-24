package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgtypes "github.com/lightchain/pkg/types"
	"github.com/lightchain/worker/internal/ollama"
)

// A job the worker gives up on must tell the consumer at once. Without an
// error frame the chat keeps waiting until the on-chain timeout, then blames
// the worker, even though the worker knew within seconds.

// sessionFrames subscribes to the session's response channel before the job
// runs and returns a func that collects everything published to it.
func sessionFrames(t *testing.T, mr *miniredis.Miniredis, sessionID uint64) func() []pkgtypes.PubSubMessage {
	t.Helper()
	sub := mr.NewSubscriber()
	t.Cleanup(sub.Close)
	sub.Subscribe(fmt.Sprintf("session:%d:responses", sessionID))
	return func() []pkgtypes.PubSubMessage {
		var out []pkgtypes.PubSubMessage
		for {
			select {
			case m := <-sub.Messages():
				var msg pkgtypes.PubSubMessage
				require.NoError(t, json.Unmarshal([]byte(m.Message), &msg))
				out = append(out, msg)
			case <-time.After(200 * time.Millisecond):
				return out
			}
		}
	}
}

func framesOfType(frames []pkgtypes.PubSubMessage, typ pkgtypes.MessageType) []pkgtypes.PubSubMessage {
	var out []pkgtypes.PubSubMessage
	for _, f := range frames {
		if f.Type == typ {
			out = append(out, f)
		}
	}
	return out
}

// emptyGenerationHandler fails every job at stage 5 the way qwen3-vl did on
// mainnet job 3498: the whole budget spent on reasoning, no answer.
func emptyGenerationHandler(t *testing.T, rc *redis.Client) *JobHandler {
	t.Helper()
	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)
	chain := &mockChainClient{
		ackJobFn: func(context.Context, uint64) error { return nil },
		completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
			panic("completeJob must not run after an empty generation")
		},
		getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		panic("blob tx must not be burned after an empty generation")
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) {
		return "", fmt.Errorf("%w: the whole generation arrived as thinking (5242 bytes), which is never transmitted", ollama.ErrEmptyGeneration)
	}}
	return newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)
}

func TestHandleJobPayload_TerminalFailurePublishesErrorFrame(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })
	collect := sessionFrames(t, mr, 1)

	// Sortition serves each job exactly once, so any failure is final.
	err := emptyGenerationHandler(t, rc).HandleJobPayload(context.Background(), testPayload(t))
	require.Error(t, err)

	errs := framesOfType(collect(), pkgtypes.MessageTypeError)
	require.Len(t, errs, 1, "the consumer must be told the job failed")
	assert.Equal(t, pkgtypes.JobID(42), errs[0].JobID)
	assert.Equal(t, pkgtypes.SessionID(1), errs[0].SessionID)
	assert.Equal(t, "test-correlation", errs[0].CorrelationID)
}

func TestHandleTask_NoRetryFailurePublishesErrorFrame(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })
	collect := sessionFrames(t, mr, 1)

	err := runTestJob(t, emptyGenerationHandler(t, rc))
	require.Error(t, err)

	assert.Len(t, framesOfType(collect(), pkgtypes.MessageTypeError), 1,
		"a no-retry failure is final on the queue path too")
}

func TestHandleJobPayload_FailureAfterDeliveryPublishesNoErrorFrame(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rc.Close() })
	collect := sessionFrames(t, mr, 1)

	_, ecdhKey, encSessionKey, promptCiphertext := guardFixtures(t)
	chain := &mockChainClient{
		ackJobFn: func(context.Context, uint64) error { return nil },
		completeJobFn: func(context.Context, uint64, [32]byte, [32]byte) error {
			return errors.New("completeJob reverted")
		},
		getEncWorkerKeyFn: func(context.Context, uint64) ([]byte, error) { return encSessionKey, nil },
	}
	fetcher := &mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
		return promptCiphertext, nil
	}}
	submitter := &mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
		return [][32]byte{{0x01}}, nil
	}}
	oll := &mockOllama{generateFn: func(context.Context, string, string) (string, error) { return "4", nil }}
	handler := newGuardTestHandler(t, chain, fetcher, submitter, oll, rc, ecdhKey)
	handler.cfg.ChainID = big.NewInt(9200) // stage 7 signs the complete frame

	require.Error(t, handler.HandleJobPayload(context.Background(), testPayload(t)))

	frames := collect()
	require.Len(t, framesOfType(frames, pkgtypes.MessageTypeComplete), 1, "stage 7 delivered the answer")
	assert.Empty(t, framesOfType(frames, pkgtypes.MessageTypeError),
		"an error frame after the answer would replace a delivered answer with an error")
}

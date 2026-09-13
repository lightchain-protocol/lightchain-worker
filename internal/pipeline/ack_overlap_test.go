package pipeline

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/metrics"
)

// ackTxHash is the hash the async mock hands back at broadcast. Tests assert
// it reaches the failure messages so an operator can find the tx.
var ackTxHash = common.HexToHash("0x1234abcd")

// mockAsyncAckChain additionally satisfies AsyncAckClient by handing back a
// join function the test drives explicitly. It embeds mockChainClient so the
// blocking AcknowledgeJob stays available — a test can leave ackJobFn fatal
// to prove the handler took the broadcast path, or leave the async fields
// untouched and prove the opposite.
type mockAsyncAckChain struct {
	mockChainClient

	// broadcastErr, when set, fails the broadcast itself.
	broadcastErr error
	// confirm gates the join function. Nil confirms immediately; otherwise
	// the join blocks until the test sends the result it wants (or the
	// confirmation budget expires).
	confirm chan error

	broadcasts atomic.Int32
	joins      atomic.Int32
	joinReturn atomic.Int32
}

func (m *mockAsyncAckChain) AcknowledgeJobAsync(
	_ context.Context,
	_ uint64,
) (common.Hash, func(context.Context) error, error) {
	m.broadcasts.Add(1)
	if m.broadcastErr != nil {
		return common.Hash{}, nil, m.broadcastErr
	}
	return ackTxHash, func(waitCtx context.Context) error {
		m.joins.Add(1)
		defer m.joinReturn.Add(1)
		if m.confirm == nil {
			return nil
		}
		select {
		case err := <-m.confirm:
			return err
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}, nil
}

// ackFixture wires a handler around a chain client that can be either
// async-capable or blocking-only, and records how far the pipeline got.
type ackFixture struct {
	handler   *JobHandler
	payload   JobPayload
	collector *metrics.Metrics

	// inferenceStarted closes when stage 5 is entered, so a test can catch
	// the pipeline mid-flight while the acknowledgement is still pending.
	inferenceStarted chan struct{}
	// inferenceReturned closes when stage 5 hands back a response, i.e.
	// immediately before the stage 1b join.
	inferenceReturned chan struct{}

	submits   atomic.Int32
	completes atomic.Int32
}

// newAckFixture builds a handler whose stages 2-6 all succeed, so the only
// thing under test is the acknowledgement path. chainClient is taken as the
// interface so both the async and blocking-only mocks fit.
func newAckFixture(t *testing.T, chainClient *mockAsyncAckChain, overlapEnabled bool) *ackFixture {
	t.Helper()

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	fix := &ackFixture{
		payload:           testPayload(t),
		collector:         testMetrics(t),
		inferenceStarted:  make(chan struct{}),
		inferenceReturned: make(chan struct{}),
	}

	chainClient.getEncWorkerKeyFn = func(context.Context, uint64) ([]byte, error) {
		return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil
	}
	if chainClient.completeJobFn == nil {
		chainClient.completeJobFn = func(context.Context, uint64, [32]byte, [32]byte) error {
			fix.completes.Add(1)
			return nil
		}
	}

	fetcher := &mockBlobFetcher{
		fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
			return encryptBlob(t, sessionKey, "what is 2+2?"), nil
		},
	}
	submitter := &mockBlobSubmitter{
		submitFn: func(context.Context, []byte) ([][32]byte, error) {
			fix.submits.Add(1)
			return [][32]byte{common.HexToHash("0xfeed")}, nil
		},
	}
	inference := &mockOllama{
		generateFn: func(context.Context, string, string) (string, error) {
			close(fix.inferenceStarted)
			defer close(fix.inferenceReturned)
			return "4", nil
		},
	}

	fix.handler = NewJobHandler(
		chainClient, fetcher, submitter, newMockKeyStore(), inference,
		nil, // redis — no publisher, stage 7 is a no-op
		testSigningKey(t), ecdhKey, &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		HandlerConfig{
			AckTxTimeout:      2 * time.Second,
			BlobTxTimeout:     2 * time.Second,
			AckOverlapEnabled: overlapEnabled,
		},
		nil, // publisher
		nil, // checkpoints
		fix.collector,
		metrics.DeliveryAsynq,
		nil,
	)
	return fix
}

// run executes the pipeline on a goroutine and returns a channel carrying
// its result, so tests can inspect intermediate state while it is parked.
func (f *ackFixture) run(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.handler.processJob(ctx, f.payload) }()
	return done
}

// stageOutcomes returns the {outcome} label values recorded against a stage.
func stageOutcomes(t *testing.T, m *metrics.Metrics, stage string) []string {
	t.Helper()
	families, err := m.Registry.Gather()
	require.NoError(t, err)

	var outcomes []string
	for _, mf := range families {
		if mf.GetName() != "worker_pipeline_stage_duration_seconds" {
			continue
		}
		for _, series := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range series.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["stage"] == stage {
				outcomes = append(outcomes, labels["outcome"])
			}
		}
	}
	return outcomes
}

// TestAckOverlap_InferenceRunsBeforeAckConfirms is the point of the whole
// change: stage 1 returns at broadcast, so stages 2-6 run inside the block
// the acknowledgement is mining in rather than after it.
func TestAckOverlap_InferenceRunsBeforeAckConfirms(t *testing.T) {
	t.Parallel()

	chain := &mockAsyncAckChain{confirm: make(chan error)}
	chain.ackJobFn = func(context.Context, uint64) error {
		t.Error("blocking AcknowledgeJob must not be used when overlap is enabled")
		return nil
	}
	fix := newAckFixture(t, chain, true)

	done := fix.run(context.Background())

	select {
	case <-fix.inferenceStarted:
	case err := <-done:
		t.Fatalf("pipeline finished before reaching inference: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("inference did not start — stage 1 is still blocking on the ack")
	}

	assert.Equal(t, int32(1), chain.broadcasts.Load(), "the ack must have been broadcast")
	assert.Equal(t, int32(0), chain.joinReturn.Load(),
		"inference must be running while the ack is still unconfirmed")
	// The confirmation is watched concurrently rather than deferred to the
	// join point, so it makes progress during inference. Polled because
	// this only asserts that the watcher goroutine gets scheduled, which
	// carries no ordering guarantee against the pipeline goroutine.
	require.Eventually(t, func() bool { return chain.joins.Load() == 1 },
		2*time.Second, 5*time.Millisecond,
		"the confirmation must be watched in the background, not at the join")

	chain.confirm <- nil
	require.NoError(t, <-done)
	assert.Equal(t, int32(1), fix.completes.Load())
}

// TestAckOverlap_JoinGatesSettlement covers MBA-2's hard condition: however
// far ahead the compute runs, nothing is published or settled until the
// acknowledgement is confirmed mined.
func TestAckOverlap_JoinGatesSettlement(t *testing.T) {
	t.Parallel()

	chain := &mockAsyncAckChain{confirm: make(chan error)}
	fix := newAckFixture(t, chain, true)

	done := fix.run(context.Background())

	select {
	case <-fix.inferenceReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("inference never completed")
	}

	// Inference is done and the ack is still unconfirmed. The pipeline must
	// be parked at the stage 1b join, short of the blob submit.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), fix.submits.Load(),
		"stage 8a must not run before the ack is confirmed")
	assert.Equal(t, int32(0), fix.completes.Load(),
		"stage 8b must not run before the ack is confirmed")
	select {
	case err := <-done:
		t.Fatalf("pipeline completed without joining the ack: %v", err)
	default:
	}

	chain.confirm <- nil
	require.NoError(t, <-done)
	assert.Equal(t, int32(1), fix.submits.Load())
	assert.Equal(t, int32(1), fix.completes.Load())
	assert.Equal(t, []string{metrics.OutcomeOK}, stageOutcomes(t, fix.collector, metrics.StageAckConfirm),
		"the confirmation wait must be observable as its own stage")
}

// TestAckOverlap_ConfirmationFailureFailsJob is the loud-failure condition.
// The acknowledgement reverts only after ~77s of inference has already been
// spent, and the job must still be abandoned before settlement with an
// error that names the acknowledgement.
func TestAckOverlap_ConfirmationFailureFailsJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		confirmed error
	}{
		{
			name:      "ack reverted",
			confirmed: assert.AnError,
		},
		{
			name:      "ack dropped and never mined",
			confirmed: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			chain := &mockAsyncAckChain{confirm: make(chan error, 1)}
			// The job is not acknowledged on-chain: neither the initial
			// stage 1 check nor the stage 1b recheck may claim otherwise.
			chain.hasJobAcknowledgedFn = func(context.Context, uint64) (bool, error) {
				return false, nil
			}
			fix := newAckFixture(t, chain, true)

			chain.confirm <- tt.confirmed
			err := <-fix.run(context.Background())

			require.Error(t, err)
			assert.Contains(t, err.Error(), "acknowledgement",
				"the failure must name the acknowledgement, not read as an inference failure")
			assert.Contains(t, err.Error(), "refusing to settle")
			assert.NotContains(t, strings.ToLower(err.Error()), "inference")

			assert.Equal(t, int32(0), fix.submits.Load(), "a failed ack must not submit a blob")
			assert.Equal(t, int32(0), fix.completes.Load(), "a failed ack must not call completeJob")
			assert.Equal(t, []string{metrics.OutcomeError}, stageOutcomes(t, fix.collector, metrics.StageAckConfirm))
		})
	}
}

// TestAckOverlap_LateMineIsToleratedViaRecheck: a confirmation that expires
// moments before the tx lands is not an acknowledgement failure. Stage 1b
// re-reads chain state before condemning the job, mirroring what stage 1
// already did for a failed broadcast.
func TestAckOverlap_LateMineIsToleratedViaRecheck(t *testing.T) {
	t.Parallel()

	var ackChecks atomic.Int32
	chain := &mockAsyncAckChain{confirm: make(chan error, 1)}
	chain.hasJobAcknowledgedFn = func(context.Context, uint64) (bool, error) {
		// False at stage 1 (so the ack is sent), true at the stage 1b
		// recheck (the tx landed just after the wait gave up).
		return ackChecks.Add(1) > 1, nil
	}
	fix := newAckFixture(t, chain, true)

	chain.confirm <- context.DeadlineExceeded
	require.NoError(t, <-fix.run(context.Background()))

	assert.Equal(t, int32(2), ackChecks.Load(), "stage 1b must recheck before failing the job")
	assert.Equal(t, int32(1), fix.completes.Load(), "a confirmed-late ack must still settle")
	assert.Equal(t, []string{metrics.OutcomeOK}, stageOutcomes(t, fix.collector, metrics.StageAckConfirm))
}

// TestAckOverlap_BroadcastFailureFailsAtStage1 keeps the pre-existing
// stage-1 contract: a send that never reaches the wire fails immediately,
// before any inference is spent on the job.
func TestAckOverlap_BroadcastFailureFailsAtStage1(t *testing.T) {
	t.Parallel()

	chain := &mockAsyncAckChain{broadcastErr: assert.AnError}
	chain.hasJobAcknowledgedFn = func(context.Context, uint64) (bool, error) { return false, nil }
	fix := newAckFixture(t, chain, true)

	err := <-fix.run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stage 1 (ack)")
	assert.Contains(t, err.Error(), "acknowledge job")

	select {
	case <-fix.inferenceStarted:
		t.Fatal("inference must not run when the ack was never broadcast")
	default:
	}
	assert.Equal(t, int32(0), fix.completes.Load())
}

// TestAckOverlap_AlreadyAcknowledgedSkipsTx preserves the idempotency
// short-circuit: a redelivered job that is already acknowledged sends no
// second ack and has nothing to join.
func TestAckOverlap_AlreadyAcknowledgedSkipsTx(t *testing.T) {
	t.Parallel()

	chain := &mockAsyncAckChain{}
	chain.hasJobAcknowledgedFn = func(context.Context, uint64) (bool, error) { return true, nil }
	chain.ackJobFn = func(context.Context, uint64) error {
		t.Error("an already-acknowledged job must not be acknowledged again")
		return nil
	}
	fix := newAckFixture(t, chain, true)

	require.NoError(t, <-fix.run(context.Background()))
	assert.Equal(t, int32(0), chain.broadcasts.Load(),
		"an already-acknowledged job must not broadcast")
	assert.Equal(t, int32(1), fix.completes.Load())
	assert.Empty(t, stageOutcomes(t, fix.collector, metrics.StageAckConfirm),
		"there is nothing to confirm, so no confirmation stage should be recorded")
}

// TestAckOverlap_DisabledBlocksLikeBefore pins the kill-switch: with
// ACK_OVERLAP_ENABLED off the handler ignores the async capability entirely
// and stage 1 blocks on the receipt, exactly as it did before this change.
func TestAckOverlap_DisabledBlocksLikeBefore(t *testing.T) {
	t.Parallel()

	ackMined := make(chan struct{})
	chain := &mockAsyncAckChain{}
	chain.ackJobFn = func(ctx context.Context, _ uint64) error {
		select {
		case <-ackMined:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fix := newAckFixture(t, chain, false)

	done := fix.run(context.Background())

	// Stage 1 owns the pipeline until the ack mines.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-fix.inferenceStarted:
		t.Fatal("inference must not start until the ack has mined when overlap is disabled")
	default:
	}

	close(ackMined)
	require.NoError(t, <-done)

	assert.Equal(t, int32(0), chain.broadcasts.Load(),
		"the async path must not be taken when overlap is disabled")
	assert.Equal(t, int32(1), fix.completes.Load())
	assert.Empty(t, stageOutcomes(t, fix.collector, metrics.StageAckConfirm),
		"the blocking path has no separate confirmation stage")
}

// TestAckOverlap_FallsBackWhenClientCannotBroadcast: overlap enabled but the
// configured chain client predates AsyncAckClient. The handler must degrade
// to the blocking path rather than fail.
func TestAckOverlap_FallsBackWhenClientCannotBroadcast(t *testing.T) {
	t.Parallel()

	var acks atomic.Int32
	chain := &mockChainClient{
		ackJobFn: func(context.Context, uint64) error {
			acks.Add(1)
			return nil
		},
	}
	require.NotImplements(t, (*AsyncAckClient)(nil), chain,
		"this test is only meaningful while mockChainClient lacks the async method")

	sessionKey := testSessionKey(t)
	ecdhKey := testECDHKey(t)
	chain.getEncWorkerKeyFn = func(context.Context, uint64) ([]byte, error) {
		return encryptSessionKeyForWorker(t, sessionKey, ecdhKey), nil
	}
	var completes atomic.Int32
	chain.completeJobFn = func(context.Context, uint64, [32]byte, [32]byte) error {
		completes.Add(1)
		return nil
	}

	handler := NewJobHandler(
		chain,
		&mockBlobFetcher{fetchFn: func(context.Context, common.Hash, uint64) ([]byte, error) {
			return encryptBlob(t, sessionKey, "hi"), nil
		}},
		&mockBlobSubmitter{submitFn: func(context.Context, []byte) ([][32]byte, error) {
			return [][32]byte{common.HexToHash("0xfeed")}, nil
		}},
		newMockKeyStore(),
		&mockOllama{generateFn: func(context.Context, string, string) (string, error) { return "ok", nil }},
		nil, testSigningKey(t), ecdhKey, &atomic.Int32{},
		slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		HandlerConfig{AckTxTimeout: 2 * time.Second, BlobTxTimeout: 2 * time.Second, AckOverlapEnabled: true},
		nil, nil, testMetrics(t), metrics.DeliveryAsynq,
nil,
	)

	require.NoError(t, handler.processJob(context.Background(), testPayload(t)))
	assert.Equal(t, int32(1), acks.Load(), "the blocking ack must still be used")
	assert.Equal(t, int32(1), completes.Load())
}

// TestAckConfirmation_AwaitIsBoundedByTimeout guards the backstop on the
// join itself: even if the watcher goroutine stops making progress, the job
// fails on a bound rather than parking forever.
func TestAckConfirmation_AwaitIsBoundedByTimeout(t *testing.T) {
	t.Parallel()

	ack := &ackConfirmation{txHash: ackTxHash, done: make(chan struct{})}
	start := time.Now()
	err := ack.awaitConfirmed(context.Background(), 50*time.Millisecond)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Contains(t, err.Error(), ackTxHash.Hex())
	assert.Less(t, time.Since(start), time.Second)
}

// TestAckConfirmation_NilJoinIsNoop keeps the "nothing to join" path free of
// nil checks at every call site.
func TestAckConfirmation_NilJoinIsNoop(t *testing.T) {
	t.Parallel()

	var ack *ackConfirmation
	require.NoError(t, ack.awaitConfirmed(context.Background(), time.Millisecond))
}

package sortition

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/pipeline"
)

// ── test doubles ─────────────────────────────────────────────────────────────

// mockServeClient is the test double for ServeClient.
type mockServeClient struct {
	head          chain.HeadInfo
	jobSubmitted  []chain.JobSubmittedEvent
	encWorkerKeys map[uint64][]byte // sessionID → encWorkerKey
	blobInfos     map[uint64]mockBlobInfo
	sessionInfos  map[uint64]chain.SessionInfo
	headErr       error
	filterErr     error
	priorJobIDs   []uint64 // returned by GetPriorSessionJobIDs
	priorJobErr   error    // returned by GetPriorSessionJobIDs
	// encWorkerKeyErrOverride, keyed by sessionID, forces GetSessionEncWorkerKey
	// to always return the given error (takes priority over encWorkerKeys).
	// Used to model a session that never becomes Active again.
	encWorkerKeyErrOverride map[uint64]error
}

type mockBlobInfo struct {
	promptHash  common.Hash
	submitBlock uint64
}

func (m *mockServeClient) Head(_ context.Context) (chain.HeadInfo, error) {
	return m.head, m.headErr
}

func (m *mockServeClient) FilterJobSubmitted(_ context.Context, _, _ uint64) ([]chain.JobSubmittedEvent, error) {
	return m.jobSubmitted, m.filterErr
}

func (m *mockServeClient) GetJobBlobInfo(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	if r, ok := m.blobInfos[jobID]; ok {
		return r.promptHash, common.Hash{}, r.submitBlock, 0, nil
	}
	return common.Hash{}, common.Hash{}, 0, 0, errors.New("no blob info")
}

func (m *mockServeClient) GetSessionInfo(_ context.Context, sessionID uint64) (chain.SessionInfo, error) {
	if si, ok := m.sessionInfos[sessionID]; ok {
		return si, nil
	}
	return chain.SessionInfo{}, errors.New("no session info")
}

func (m *mockServeClient) GetSessionEncWorkerKey(_ context.Context, sessionID uint64) ([]byte, error) {
	if err, ok := m.encWorkerKeyErrOverride[sessionID]; ok {
		return nil, err
	}
	if key, ok := m.encWorkerKeys[sessionID]; ok {
		return key, nil
	}
	return nil, errors.New("no enc worker key")
}

func (m *mockServeClient) GetPriorSessionJobIDs(_ context.Context, _, _, _, _ uint64) ([]uint64, error) {
	if m.priorJobErr != nil {
		return nil, m.priorJobErr
	}
	return m.priorJobIDs, nil
}

// mockKeyChecker is the test double for KeyChecker.
type mockKeyChecker struct {
	canDecrypt bool
}

func (m *mockKeyChecker) CanDecryptSessionKey(_ []byte) bool {
	return m.canDecrypt
}

// mockJobSink is the test double for JobSink.
// Access captured payloads via received().
type mockJobSink struct {
	mu       sync.Mutex
	payloads []pipeline.JobPayload
}

func (m *mockJobSink) HandleJobPayload(_ context.Context, payload pipeline.JobPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.payloads = append(m.payloads, payload)
	return nil
}

func (m *mockJobSink) received() []pipeline.JobPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]pipeline.JobPayload, len(m.payloads))
	copy(out, m.payloads)
	return out
}

// ── helpers ───────────────────────────────────────────────────────────────────

var (
	testMyWorker    = common.HexToAddress("0x0000000000000000000000000000000000000002")
	testOtherWorker = common.HexToAddress("0x0000000000000000000000000000000000000003")
)

// newJW builds a JobWatcher backed by a fresh CursorStore with syncServe=true
// for deterministic tests (HandleJobPayload is called synchronously, not in a
// goroutine). Returns the watcher and its CursorStore for cursor assertions.
func newJW(t *testing.T, mc *mockServeClient, kc *mockKeyChecker, sink *mockJobSink, counter *atomic.Int32) (*JobWatcher, *CursorStore) {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	return NewJobWatcher(JobWatcherOpts{
		Client:        mc,
		KeyChecker:    kc,
		Sink:          sink,
		Cursor:        cs,
		Worker:        testMyWorker,
		JobCounter:    counter,
		MaxConcurrent: 4,
		ChunkSize:     5000,
		Confirmations: 0,
		Logger:        testLogger(t),
		syncServe:     true,
	}), cs
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestJobWatcher_ServesOnlyMyJobs verifies that RunOnce serves exactly the
// mine event (Worker == me) with all payload fields correctly populated, and
// silently skips the other-worker event.
func TestJobWatcher_ServesOnlyMyJobs(t *testing.T) {
	var counter atomic.Int32

	var modelBytes [32]byte
	copy(modelBytes[:], []byte("test-model"))
	consumer := common.HexToAddress("0x0000000000000000000000000000000000000004")
	promptHash := common.HexToHash("0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef")

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 50},
			{JobID: 2, SessionID: 11, Worker: testOtherWorker, BlockNumber: 51},
		},
		encWorkerKeys: map[uint64][]byte{
			10: []byte("encKey"),
		},
		blobInfos: map[uint64]mockBlobInfo{
			1: {promptHash: promptHash, submitBlock: 50},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: consumer, ModelID: modelBytes, Worker: testMyWorker, Status: 1},
		},
	}
	sink := &mockJobSink{}
	jw, _ := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	payloads := sink.received()
	require.Len(t, payloads, 1, "should serve exactly one job (mine only, not other-worker)")

	p := payloads[0]
	require.Equal(t, uint64(1), p.JobID)
	require.Equal(t, uint64(10), p.SessionID)
	require.Equal(t, consumer, p.Consumer)
	require.Equal(t, testMyWorker, p.Worker)
	require.Equal(t, promptHash, p.PromptBlobHash)
	require.Equal(t, uint64(50), p.BlockNumber)
	require.Equal(t, "10-1", p.CorrelationID)

	// ModelID must be bare lowercase hex without "0x" prefix — matching the
	// dispatcher's common.Bytes2Hex(sess.ModelID[:]) format.
	wantModelID := hex.EncodeToString(modelBytes[:])
	require.Equal(t, wantModelID, p.ModelID, "ModelID must be bare lowercase hex (no 0x prefix)")
	require.False(t, strings.HasPrefix(p.ModelID, "0x"), "ModelID must not carry a 0x prefix")
}

// TestJobWatcher_SkipsUndecryptableKey verifies that when CanDecryptSessionKey
// returns false, the sink is NOT called and the cursor still advances to
// safeHead (the job is skipped, not retried forever).
func TestJobWatcher_SkipsUndecryptableKey(t *testing.T) {
	var counter atomic.Int32

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 50},
		},
		encWorkerKeys: map[uint64][]byte{
			10: []byte("encKey"),
		},
		blobInfos: map[uint64]mockBlobInfo{
			1: {promptHash: common.HexToHash("0xdead"), submitBlock: 50},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: common.HexToAddress("0x5"), Worker: testMyWorker, Status: 1},
		},
	}
	sink := &mockJobSink{}
	// canDecrypt=false → key belongs to a different worker, skip the job.
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: false}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	require.Empty(t, sink.received(), "sink must not be called when session key is undecryptable")

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got, "cursor must advance to safeHead even when job is skipped")
}

// TestJobWatcher_CapacityStopsAndRetries verifies that when jobCounter is at
// MaxConcurrent, RunOnce stops WITHOUT advancing the cursor past the un-served
// event, so the next pass rescans and retries the same event.
func TestJobWatcher_CapacityStopsAndRetries(t *testing.T) {
	var counter atomic.Int32
	counter.Store(4) // at cap (== MaxConcurrent=4)

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 20},
		},
		encWorkerKeys: map[uint64][]byte{10: []byte("encKey")},
		blobInfos: map[uint64]mockBlobInfo{
			1: {promptHash: common.HexToHash("0xdead"), submitBlock: 20},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: common.HexToAddress("0x5"), Worker: testMyWorker, Status: 1},
		},
	}
	sink := &mockJobSink{}
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	require.Empty(t, sink.received(), "sink must not be called when at capacity")

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	// cursor must be set to ev.BlockNumber-1 = 19; NOT advanced to safeHead (100).
	// Next pass rescans from block 20 and retries the event.
	require.Equal(t, uint64(19), got, "cursor must be just before the un-served event's block")
}

// TestJobWatcher_TransientLookupError_StopsAndRetries verifies that a transient
// chain-read error on a mine event (here GetSessionEncWorkerKey failing) does
// NOT drop the already-assigned job: RunOnce stops WITHOUT advancing the cursor
// past the event, so the next pass rescans and retries it. This is the opposite
// of the undecryptable-key case (permanent), which advances the cursor.
func TestJobWatcher_TransientLookupError_StopsAndRetries(t *testing.T) {
	var counter atomic.Int32

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 20},
		},
		// encWorkerKeys deliberately EMPTY → GetSessionEncWorkerKey returns an
		// error (models a transient RPC failure reading data the JobSubmitted
		// event proves exists on-chain), which must trigger stop-and-retry.
		encWorkerKeys: map[uint64][]byte{},
		blobInfos: map[uint64]mockBlobInfo{
			1: {promptHash: common.HexToHash("0xdead"), submitBlock: 20},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: common.HexToAddress("0x5"), Worker: testMyWorker, Status: 1},
		},
	}
	sink := &mockJobSink{}
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	require.Empty(t, sink.received(), "sink must not be called on a transient lookup error")

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	// Transient error → cursor set to ev.BlockNumber-1 = 19 (NOT advanced to
	// safeHead 100), so the next pass rescans block 20 and retries the job.
	require.Equal(t, uint64(19), got, "cursor must not advance past the un-served event on a transient error")
}

// TestJobWatcher_AdvancesCursorWhenNoneMine verifies that when there are no
// mine events in the range, the cursor still advances to safeHead.
func TestJobWatcher_AdvancesCursorWhenNoneMine(t *testing.T) {
	var counter atomic.Int32

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 50},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testOtherWorker, BlockNumber: 25},
			{JobID: 2, SessionID: 11, Worker: testOtherWorker, BlockNumber: 30},
		},
	}
	sink := &mockJobSink{}
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	require.Empty(t, sink.received(), "no jobs served when none are mine")

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(50), got, "cursor must advance to safeHead even with no mine events")
}

// TestJobWatcher_PopulatesPriorJobIDs verifies that when GetPriorSessionJobIDs
// returns earlier jobs for the session, the served payload carries them as
// PriorJobIDs so the pipeline can reconstruct conversation history (sortition
// mode has no dispatcher to supply them).
func TestJobWatcher_PopulatesPriorJobIDs(t *testing.T) {
	var counter atomic.Int32

	var modelBytes [32]byte
	copy(modelBytes[:], []byte("test-model"))
	consumer := common.HexToAddress("0x0000000000000000000000000000000000000004")
	promptHash := common.HexToHash("0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef")

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 3, SessionID: 10, Worker: testMyWorker, BlockNumber: 50},
		},
		encWorkerKeys: map[uint64][]byte{10: []byte("encKey")},
		blobInfos: map[uint64]mockBlobInfo{
			3: {promptHash: promptHash, submitBlock: 50},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: consumer, ModelID: modelBytes, Worker: testMyWorker, Status: 1},
		},
		priorJobIDs: []uint64{1, 2},
	}
	sink := &mockJobSink{}
	jw, _ := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	payloads := sink.received()
	require.Len(t, payloads, 1)
	require.Equal(t, []uint64{1, 2}, payloads[0].PriorJobIDs,
		"PriorJobIDs must be populated from GetPriorSessionJobIDs")
}

// TestJobWatcher_PriorJobIDsLookupError_ServesSingleTurn verifies that when
// GetPriorSessionJobIDs fails, the job is still served (best-effort) with empty
// PriorJobIDs — the error must never fail the job.
func TestJobWatcher_PriorJobIDsLookupError_ServesSingleTurn(t *testing.T) {
	var counter atomic.Int32

	var modelBytes [32]byte
	copy(modelBytes[:], []byte("test-model"))
	consumer := common.HexToAddress("0x0000000000000000000000000000000000000004")
	promptHash := common.HexToHash("0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef")

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 3, SessionID: 10, Worker: testMyWorker, BlockNumber: 50},
		},
		encWorkerKeys: map[uint64][]byte{10: []byte("encKey")},
		blobInfos: map[uint64]mockBlobInfo{
			3: {promptHash: promptHash, submitBlock: 50},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			10: {User: consumer, ModelID: modelBytes, Worker: testMyWorker, Status: 1},
		},
		priorJobErr: errors.New("rpc boom"),
	}
	sink := &mockJobSink{}
	jw, _ := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))

	payloads := sink.received()
	require.Len(t, payloads, 1, "job must still be served despite prior-jobs lookup error")
	require.Empty(t, payloads[0].PriorJobIDs, "PriorJobIDs must be empty on lookup error (single-turn)")
}

// TestJobWatcher_SessionNotActive_BoundedRetryThenSkip verifies that a job
// whose session key read keeps returning chain.ErrSessionNotActive is
// retried at most SessionRetryLimit passes (stop-and-retry, cursor parked
// before the event) and then skipped (cursor advances past it) on the final
// pass — and that a later job reachable in that same pass is then served.
// This is the wedge fix: one session that never returns to Active must not
// starve every job behind it forever.
func TestJobWatcher_SessionNotActive_BoundedRetryThenSkip(t *testing.T) {
	var counter atomic.Int32
	const retryLimit = 3

	consumer := common.HexToAddress("0x0000000000000000000000000000000000000004")
	promptHash := common.HexToHash("0xbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef")

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			// job 1's session never comes back Active.
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 20},
			// job 2 is healthy and reachable once job 1 is given up on.
			{JobID: 2, SessionID: 11, Worker: testMyWorker, BlockNumber: 30},
		},
		encWorkerKeyErrOverride: map[uint64]error{
			10: fmt.Errorf("session %d: %w (status=2)", 10, chain.ErrSessionNotActive),
		},
		encWorkerKeys: map[uint64][]byte{
			11: []byte("encKey"),
		},
		blobInfos: map[uint64]mockBlobInfo{
			2: {promptHash: promptHash, submitBlock: 30},
		},
		sessionInfos: map[uint64]chain.SessionInfo{
			11: {User: consumer, Worker: testMyWorker, Status: 1},
		},
	}
	sink := &mockJobSink{}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	jw := NewJobWatcher(JobWatcherOpts{
		Client:            mc,
		KeyChecker:        &mockKeyChecker{canDecrypt: true},
		Sink:              sink,
		Cursor:            cs,
		Worker:            testMyWorker,
		JobCounter:        &counter,
		MaxConcurrent:     4,
		ChunkSize:         5000,
		Confirmations:     0,
		SessionRetryLimit: retryLimit,
		Logger:            testLogger(t),
		syncServe:         true,
	})

	// Passes 1..retryLimit-1: still under the bound, so job 1 stop-and-retries
	// and RunOnce returns before ever reaching job 2.
	for i := 1; i < retryLimit; i++ {
		require.NoError(t, jw.RunOnce(context.Background()))
		require.Empty(t, sink.received(), "pass %d: nothing must be served while session 10 is stuck", i)
		got, gErr := cs.Get(cursorJobSubmitted)
		require.NoError(t, gErr)
		require.Equal(t, uint64(19), got, "pass %d: cursor must stay parked before job 1's block", i)
	}

	// Final pass: the bound is hit, job 1 is given up on (continue, not
	// stop-and-retry), so the loop proceeds to job 2 in the same pass.
	require.NoError(t, jw.RunOnce(context.Background()))

	payloads := sink.received()
	require.Len(t, payloads, 1, "job 2 must be served once job 1 is given up on")
	require.Equal(t, uint64(2), payloads[0].JobID)

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got, "cursor must advance to safeHead after giving up on job 1")
}

// TestJobWatcher_PlainRPCError_NeverGivesUp verifies that the bounded-retry
// skip is scoped to chain.ErrSessionNotActive only: an ordinary transient RPC
// error on GetSessionEncWorkerKey keeps stop-and-retrying forever, even past
// SessionRetryLimit passes, because it never satisfies errors.Is(err,
// chain.ErrSessionNotActive).
func TestJobWatcher_PlainRPCError_NeverGivesUp(t *testing.T) {
	var counter atomic.Int32
	const retryLimit = 2 // deliberately small to prove RPC errors ignore it

	mc := &mockServeClient{
		head: chain.HeadInfo{Number: 100},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 20},
		},
		// encWorkerKeys deliberately empty → GetSessionEncWorkerKey returns the
		// mock's generic "no enc worker key" error, which does NOT satisfy
		// errors.Is(_, chain.ErrSessionNotActive).
	}
	sink := &mockJobSink{}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	jw := NewJobWatcher(JobWatcherOpts{
		Client:            mc,
		KeyChecker:        &mockKeyChecker{canDecrypt: true},
		Sink:              sink,
		Cursor:            cs,
		Worker:            testMyWorker,
		JobCounter:        &counter,
		MaxConcurrent:     4,
		ChunkSize:         5000,
		Confirmations:     0,
		SessionRetryLimit: retryLimit,
		Logger:            testLogger(t),
		syncServe:         true,
	})

	for i := 1; i <= retryLimit+3; i++ {
		require.NoError(t, jw.RunOnce(context.Background()))
		require.Empty(t, sink.received(), "pass %d: job must never be served on a plain RPC error", i)
		got, gErr := cs.Get(cursorJobSubmitted)
		require.NoError(t, gErr)
		require.Equal(t, uint64(19), got, "pass %d: cursor must stay parked (no skip) past retryLimit", i)
	}
}

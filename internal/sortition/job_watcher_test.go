package sortition

import (
	"context"
	"encoding/hex"
	"errors"
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
	if key, ok := m.encWorkerKeys[sessionID]; ok {
		return key, nil
	}
	return nil, errors.New("no enc worker key")
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

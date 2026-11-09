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
	filterRanges  [][2]uint64 // every [from, to] FilterJobSubmitted was asked for
	// encWorkerKeyErrOverride, keyed by sessionID, forces GetSessionEncWorkerKey
	// to always return the given error (takes priority over encWorkerKeys).
	// Used to model a session that never becomes Active again.
	encWorkerKeyErrOverride map[uint64]error
}

type mockBlobInfo struct {
	promptHash    common.Hash
	submitBlock   uint64
	completeBlock uint64 // non-zero once the job is completed on chain
}

func (m *mockServeClient) Head(_ context.Context) (chain.HeadInfo, error) {
	return m.head, m.headErr
}

func (m *mockServeClient) FilterJobSubmitted(_ context.Context, from, to uint64) ([]chain.JobSubmittedEvent, error) {
	m.filterRanges = append(m.filterRanges, [2]uint64{from, to})
	if m.filterErr != nil {
		return nil, m.filterErr
	}
	var result []chain.JobSubmittedEvent
	for _, ev := range m.jobSubmitted {
		if ev.BlockNumber >= from && ev.BlockNumber <= to {
			result = append(result, ev)
		}
	}
	return result, nil
}

func (m *mockServeClient) GetJobBlobInfo(_ context.Context, jobID uint64) (common.Hash, common.Hash, uint64, uint64, error) {
	if r, ok := m.blobInfos[jobID]; ok {
		return r.promptHash, common.Hash{}, r.submitBlock, r.completeBlock, nil
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
// goroutine). The cursor is stored at 0: an absent one would start the watcher
// at the safe head instead. Returns the watcher and its CursorStore for cursor
// assertions.
func newJW(t *testing.T, mc *mockServeClient, kc *mockKeyChecker, sink *mockJobSink, counter *atomic.Int32) (*JobWatcher, *CursorStore) {
	t.Helper()
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, cs.Set(cursorJobSubmitted, 0))
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
	require.NoError(t, cs.Set(cursorJobSubmitted, 0))
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
	require.NoError(t, cs.Set(cursorJobSubmitted, 0))
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

// ── first pass of a fresh worker (no cursor on disk) ─────────────────────────

// freshJobChain is a long chain with one job of ours months back and one in
// the last few blocks, both servable.
func freshJobChain(head uint64) *mockServeClient {
	return &mockServeClient{
		head: chain.HeadInfo{Number: head},
		jobSubmitted: []chain.JobSubmittedEvent{
			{JobID: 1, SessionID: 10, Worker: testMyWorker, BlockNumber: 50},
			{JobID: 2, SessionID: 10, Worker: testMyWorker, BlockNumber: 9_990},
		},
		encWorkerKeys: map[uint64][]byte{10: []byte("encKey")},
		blobInfos:     map[uint64]mockBlobInfo{1: {submitBlock: 50}, 2: {submitBlock: 9_990}},
		sessionInfos:  map[uint64]chain.SessionInfo{10: {Worker: testMyWorker, Status: 1}},
	}
}

// A worker with no job cursor must not scan the chain from the first block: it
// starts LookbackBlocks behind the safe head.
func TestJobWatcher_NoCursorStartsWithinLookbackOfSafeHead(t *testing.T) {
	var counter atomic.Int32
	mc := freshJobChain(10_005) // safe head 10_000 at 5 confirmations
	sink := &mockJobSink{}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	jw := NewJobWatcher(JobWatcherOpts{
		Client:         mc,
		KeyChecker:     &mockKeyChecker{canDecrypt: true},
		Sink:           sink,
		Cursor:         cs,
		Worker:         testMyWorker,
		JobCounter:     &counter,
		MaxConcurrent:  4,
		ChunkSize:      5000,
		Confirmations:  5,
		LookbackBlocks: 60,
		Logger:         testLogger(t),
		syncServe:      true,
	})

	require.NoError(t, jw.RunOnce(context.Background()))
	require.Equal(t, [][2]uint64{{9_941, 10_000}}, mc.filterRanges,
		"only the look-back window behind the safe head is scanned")
	payloads := sink.received()
	require.Len(t, payloads, 1)
	require.Equal(t, uint64(2), payloads[0].JobID)
	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000), got)
}

// A stored cursor stays authoritative however far behind the head it is, even a
// stored 0: the scan resumes right after it.
func TestJobWatcher_StoredCursorIsAuthoritative(t *testing.T) {
	var counter atomic.Int32
	mc := freshJobChain(10_000)
	sink := &mockJobSink{}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, cs.Set(cursorJobSubmitted, 0))
	jw := NewJobWatcher(JobWatcherOpts{
		Client:         mc,
		KeyChecker:     &mockKeyChecker{canDecrypt: true},
		Sink:           sink,
		Cursor:         cs,
		Worker:         testMyWorker,
		JobCounter:     &counter,
		MaxConcurrent:  4,
		ChunkSize:      5000,
		LookbackBlocks: 60,
		Logger:         testLogger(t),
		syncServe:      true,
	})

	require.NoError(t, jw.RunOnce(context.Background()))
	require.Equal(t, [][2]uint64{{1, 5000}, {5001, 10_000}}, mc.filterRanges)
	require.Len(t, sink.received(), 2)
}

// ── no re-dispatch of jobs already served ────────────────────────────────────

// slotSink models the pipeline's slot accounting: every HandleJobPayload call
// takes a job slot (counter) and keeps it until the test completes the job,
// which also marks it completed on chain. It records every call.
type slotSink struct {
	counter  *atomic.Int32
	chain    *mockServeClient
	calls    map[uint64]int
	inFlight []uint64
}

func (s *slotSink) HandleJobPayload(_ context.Context, p pipeline.JobPayload) error {
	s.calls[p.JobID]++
	s.counter.Add(1)
	s.inFlight = append(s.inFlight, p.JobID)
	return nil
}

// completeAll finishes every in-flight job: frees its slot and records the
// completion on chain at block completeAt.
func (s *slotSink) completeAll(completeAt uint64) {
	for _, id := range s.inFlight {
		bi := s.chain.blobInfos[id]
		bi.completeBlock = completeAt
		s.chain.blobInfos[id] = bi
		s.counter.Add(-1)
	}
	s.inFlight = nil
}

// sameBlockChain returns a chain with n jobs for this worker (IDs 1..n), all
// submitted in block 20, plus one job for another worker in the same block.
func sameBlockChain(n int) *mockServeClient {
	mc := &mockServeClient{
		head:          chain.HeadInfo{Number: 100},
		encWorkerKeys: map[uint64][]byte{10: []byte("encKey")},
		blobInfos:     map[uint64]mockBlobInfo{},
		sessionInfos:  map[uint64]chain.SessionInfo{10: {Worker: testMyWorker, Status: 1}},
	}
	for id := uint64(1); id <= uint64(n); id++ {
		mc.jobSubmitted = append(mc.jobSubmitted, chain.JobSubmittedEvent{
			JobID: id, SessionID: 10, Worker: testMyWorker, BlockNumber: 20,
		})
		if id == 2 {
			mc.jobSubmitted = append(mc.jobSubmitted, chain.JobSubmittedEvent{
				JobID: 100, SessionID: 11, Worker: testOtherWorker, BlockNumber: 20,
			})
		}
		mc.blobInfos[id] = mockBlobInfo{submitBlock: 20}
	}
	return mc
}

// TestJobWatcher_SameBlockAtCapacity_DispatchesEachJobOnce is the 2026-11-09
// devnet starvation: six jobs for this worker land in one block and the
// worker runs two at a time. Capacity parks the cursor before that block, so
// every pass rescans it. Jobs already handed to the pipeline must not be
// handed over again: a re-dispatch takes a slot, the watcher hits capacity at
// the same event again, and the jobs behind it starve.
func TestJobWatcher_SameBlockAtCapacity_DispatchesEachJobOnce(t *testing.T) {
	const nJobs = 6
	var counter atomic.Int32
	mc := sameBlockChain(nJobs)
	sink := &slotSink{counter: &counter, chain: mc, calls: map[uint64]int{}}
	cs, err := NewCursorStore(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, cs.Set(cursorJobSubmitted, 0))
	jw := NewJobWatcher(JobWatcherOpts{
		Client:        mc,
		KeyChecker:    &mockKeyChecker{canDecrypt: true},
		Sink:          sink,
		Cursor:        cs,
		Worker:        testMyWorker,
		JobCounter:    &counter,
		MaxConcurrent: 2,
		ChunkSize:     5000,
		Logger:        testLogger(t),
		syncServe:     true,
	})

	// Each pass, the jobs started in the previous pass finish. Ten passes is
	// far more than the three a correct watcher needs for six jobs at two.
	for pass := 1; pass <= 10; pass++ {
		require.NoError(t, jw.RunOnce(context.Background()))
		sink.completeAll(30 + uint64(pass))
	}

	total := 0
	for _, n := range sink.calls {
		total += n
	}
	t.Logf("HandleJobPayload calls: %d total for %d jobs, per job %v", total, nJobs, sink.calls)
	for id := uint64(1); id <= nJobs; id++ {
		require.Equal(t, 1, sink.calls[id], "job %d must be dispatched exactly once", id)
	}
	require.Equal(t, nJobs, total, "no job dispatched more than once, none missed")
	require.NotContains(t, sink.calls, uint64(100), "other worker's job must not be dispatched")

	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got, "cursor must reach the safe head once every job is served")
	require.Empty(t, jw.dispatched, "dispatched set must be pruned once the cursor passes the block")
}

// TestJobWatcher_Restart_SkipsJobCompletedOnChain covers a restart: the
// in-memory dispatched set is empty and the cursor still sits before the
// block, so the rescan must recognise the job completed on chain and skip it
// rather than hand it to the pipeline again.
func TestJobWatcher_Restart_SkipsJobCompletedOnChain(t *testing.T) {
	var counter atomic.Int32
	mc := sameBlockChain(3)
	bi := mc.blobInfos[1]
	bi.completeBlock = 25 // job 1 finished before the restart
	mc.blobInfos[1] = bi
	sink := &mockJobSink{}
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)
	require.NoError(t, cs.Set(cursorJobSubmitted, 19)) // parked before block 20

	require.NoError(t, jw.RunOnce(context.Background()))

	var ids []uint64
	for _, p := range sink.received() {
		ids = append(ids, p.JobID)
	}
	require.Equal(t, []uint64{2, 3}, ids, "completed job 1 is skipped, the rest are served")
	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got)
}

// TestJobWatcher_RetryableFailureBeforeDispatch_StillRetried verifies that a
// job whose lookup failed before it reached HandleJobPayload is not treated
// as dispatched: the next pass serves it.
func TestJobWatcher_RetryableFailureBeforeDispatch_StillRetried(t *testing.T) {
	var counter atomic.Int32
	mc := sameBlockChain(1)
	sessions := mc.sessionInfos
	mc.sessionInfos = map[uint64]chain.SessionInfo{} // GetSessionInfo fails
	sink := &mockJobSink{}
	jw, cs := newJW(t, mc, &mockKeyChecker{canDecrypt: true}, sink, &counter)

	require.NoError(t, jw.RunOnce(context.Background()))
	require.Empty(t, sink.received())
	got, err := cs.Get(cursorJobSubmitted)
	require.NoError(t, err)
	require.Equal(t, uint64(19), got)

	mc.sessionInfos = sessions
	require.NoError(t, jw.RunOnce(context.Background()))
	require.Len(t, sink.received(), 1, "job must be served once the lookup succeeds")
}

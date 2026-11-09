package sortition

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/pipeline"
)

const cursorJobSubmitted = "job_submitted"

// ServeClient is the narrow chain interface the JobWatcher needs.
// Head returns chain.HeadInfo so the watcher can use head.Number with the
// same confirmations-guard pattern as SessionWatcher (session_watcher.go).
type ServeClient interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	FilterJobSubmitted(ctx context.Context, fromBlock, toBlock uint64) ([]chain.JobSubmittedEvent, error)
	GetJobBlobInfo(ctx context.Context, jobID uint64) (promptHash, respHash common.Hash, submitBlock, completeBlock uint64, err error)
	GetSessionInfo(ctx context.Context, sessionID uint64) (chain.SessionInfo, error)
	GetSessionEncWorkerKey(ctx context.Context, sessionID uint64) ([]byte, error)
}

// KeyChecker validates that the session key is decryptable before serving a
// job. Avoids wasting inference on a session whose key was wrapped for a
// different worker key.
type KeyChecker interface {
	CanDecryptSessionKey(encWorkerKey []byte) bool
}

// JobSink is the existing pipeline entrypoint.
type JobSink interface {
	HandleJobPayload(ctx context.Context, payload pipeline.JobPayload) error
}

// JobWatcherOpts carries all configuration for a JobWatcher.
type JobWatcherOpts struct {
	Client        ServeClient
	KeyChecker    KeyChecker
	Sink          JobSink
	Cursor        *CursorStore
	Worker        common.Address
	JobCounter    *atomic.Int32
	MaxConcurrent int
	ChunkSize     uint64
	Confirmations uint64
	// SessionRetryLimit bounds how many passes a job may stop-and-retry on
	// chain.ErrSessionNotActive before it is given up on (cursor advances,
	// skipping the job) so one session that never returns to Active cannot
	// wedge the watcher and starve every job behind it. Defaults to 10 when
	// zero.
	SessionRetryLimit int
	// LookbackBlocks is how far behind the safe head a worker with no stored
	// cursor starts scanning, instead of at the first block. Jobs submitted
	// before that are not served. It has no effect once a cursor is stored.
	LookbackBlocks uint64
	PollInterval   time.Duration
	Logger         *slog.Logger
	// syncServe is an internal test hook. When true, HandleJobPayload is called
	// synchronously in RunOnce rather than in a goroutine, making unit tests
	// fully deterministic without WaitGroups or polling. Must not be set in
	// production.
	syncServe bool
}

// JobWatcher polls the chain for JobSubmitted events and serves jobs destined
// for this worker into the inference pipeline.
type JobWatcher struct {
	c                 ServeClient
	keys              KeyChecker
	sink              JobSink
	cursor            *CursorStore
	worker            common.Address
	counter           *atomic.Int32
	maxJobs           int
	chunk             uint64
	confs             uint64
	sessionRetryLimit int
	lookback          uint64
	// notActiveRetries counts consecutive chain.ErrSessionNotActive passes per
	// jobID. Only ever touched from runOnce, which Start() calls sequentially
	// off a single ticker goroutine, so no mutex is needed. Cleared on success
	// or on give-up.
	notActiveRetries map[uint64]int
	// dispatched maps each job this process has handed to HandleJobPayload to
	// the block of its JobSubmitted event. Capacity and transient-error stops
	// park the cursor before an event's block, so the next pass rescans the
	// whole block; jobs in it that were already handed over (in flight or
	// finished) are skipped instead of dispatched again. Entries are pruned
	// once the cursor has moved past their block, which bounds the set by the
	// jobs in the blocks still ahead of the cursor. Only touched from runOnce
	// (sequential), so no mutex is needed. Empty after a restart: the
	// on-chain completion check covers that case.
	dispatched map[uint64]uint64
	interval   time.Duration
	log        *slog.Logger
	syncServe  bool
	// jobs counts the serve goroutines still running, so Start can wait for
	// them before it returns.
	jobs sync.WaitGroup
}

// NewJobWatcher constructs a JobWatcher from the given options.
// ChunkSize defaults to 5000 when zero. PollInterval defaults to 30s when zero.
func NewJobWatcher(o JobWatcherOpts) *JobWatcher {
	chunk := o.ChunkSize
	if chunk == 0 {
		chunk = 5000
	}
	interval := o.PollInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	retryLimit := o.SessionRetryLimit
	if retryLimit == 0 {
		retryLimit = 10
	}
	return &JobWatcher{
		c:                 o.Client,
		keys:              o.KeyChecker,
		sink:              o.Sink,
		cursor:            o.Cursor,
		worker:            o.Worker,
		counter:           o.JobCounter,
		maxJobs:           o.MaxConcurrent,
		chunk:             chunk,
		confs:             o.Confirmations,
		sessionRetryLimit: retryLimit,
		lookback:          o.LookbackBlocks,
		notActiveRetries:  make(map[uint64]int),
		dispatched:        make(map[uint64]uint64),
		interval:          interval,
		log:               o.Logger,
		syncServe:         o.syncServe,
	}
}

// stopAndRetry persists the cursor to one block before ev.BlockNumber so
// the next poll pass rescans from ev.BlockNumber. lo is the chunk lower bound,
// used as the underflow-safe fallback when ev.BlockNumber is zero.
//
// Use for TRANSIENT failures (RPC errors on GetSessionEncWorkerKey,
// GetJobBlobInfo, GetSessionInfo) where the data provably exists on-chain but
// the call failed — retrying next pass is correct.
//
// Do NOT use for PERMANENT conditions (CanDecryptSessionKey == false): a key
// wrapped for a different worker never becomes decryptable; stop-and-retry
// would wedge the watcher indefinitely. Those must use continue to advance.
func (w *JobWatcher) stopAndRetry(ev chain.JobSubmittedEvent, lo uint64) error {
	stopAt := lo - 1
	if ev.BlockNumber > 0 {
		stopAt = ev.BlockNumber - 1
	}
	return w.setCursor(stopAt)
}

// setCursor persists the cursor and drops dispatched entries at or below it:
// the cursor never moves back past a persisted position, so those blocks are
// never rescanned.
func (w *JobWatcher) setCursor(block uint64) error {
	if err := w.cursor.Set(cursorJobSubmitted, block); err != nil {
		return err
	}
	for id, b := range w.dispatched {
		if b <= block {
			delete(w.dispatched, id)
		}
	}
	return nil
}

// Start polls on each tick until ctx is cancelled, then returns once the jobs
// it has served have returned. The jobs run on jobCtx rather than ctx, so
// cancelling ctx stops intake and leaves running jobs alone; cancel jobCtx to
// end them.
func (w *JobWatcher) Start(ctx, jobCtx context.Context) {
	defer w.jobs.Wait()
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.runOnce(ctx, jobCtx); err != nil {
				w.log.Warn("job watcher pass failed", "error", err)
			}
		}
	}
}

// RunOnce performs one poll pass: read cursor → safeHead → for each chunk in
// [cursor+1..safeHead], filter JobSubmitted events, skip non-mine events,
// stop if at capacity (without advancing cursor past the un-served event),
// skip jobs whose session key cannot be decrypted, and serve the rest into
// the pipeline via HandleJobPayload. Cursor is advanced to each chunk's hi
// after the chunk is processed, unless capacity forced an early return.
//
// A stop parks the cursor before the event's block, so the next pass rescans
// that whole block. Re-serving a job is idempotent in the pipeline but not
// free: each re-dispatch takes a job slot and chain RPC, and at capacity that
// makes the watcher stop at the same event on every pass while the jobs
// behind it miss their deadlines. So a job is served at most once per
// process (the dispatched set), and after a restart a job already completed
// on chain is skipped.
//
// A serve error (e.g. transient pipeline failure) is logged at warn and is
// not fatal to the pass — the cursor still advances. The pipeline's own
// on-chain ack/completion deadline handles missed completions via claimTimeout.
//
// Jobs served by RunOnce run on ctx; Start serves them on its own job context.
func (w *JobWatcher) RunOnce(ctx context.Context) error {
	return w.runOnce(ctx, ctx)
}

func (w *JobWatcher) runOnce(ctx, jobCtx context.Context) error {
	head, err := w.c.Head(ctx)
	if err != nil {
		return err
	}

	// Underflow guard: head.Number < confs wraps on uint64 subtraction.
	// Mirror the reconciler/SessionWatcher pattern exactly (session_watcher.go).
	var safeHead uint64
	if head.Number > w.confs {
		safeHead = head.Number - w.confs
	}

	cursor, fresh, err := w.cursor.GetOrSeed(cursorJobSubmitted, safeHead, w.lookback)
	if err != nil {
		return err
	}
	if fresh {
		w.log.Info("no job cursor stored; starting behind the safe head",
			"cursor", cursor, "safeHead", safeHead)
	}

	if safeHead <= cursor {
		return nil
	}

	for lo := cursor + 1; lo <= safeHead; lo += w.chunk {
		hi := lo + w.chunk - 1
		if hi > safeHead {
			hi = safeHead
		}

		evts, err := w.c.FilterJobSubmitted(ctx, lo, hi)
		if err != nil {
			return err
		}

		for _, ev := range evts {
			if ev.Worker != w.worker {
				// Not our job; cursor still advances to hi at end of chunk.
				continue
			}

			// Already handed to the pipeline by this process (in flight or
			// finished): a rescan of its block must not serve it again.
			if _, done := w.dispatched[ev.JobID]; done {
				continue
			}

			// Capacity check: if already at max concurrent jobs, stop this pass
			// WITHOUT advancing the cursor past this event. Persist cursor to one
			// block before this event so the next poll pass rescans from
			// ev.BlockNumber; the events in that block that were already served
			// are skipped above.
			if int(w.counter.Load()) >= w.maxJobs {
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
			}

			enc, encErr := w.c.GetSessionEncWorkerKey(ctx, ev.SessionID)
			if encErr != nil {
				if errors.Is(encErr, chain.ErrSessionNotActive) {
					// Bounded PERMANENT-ish condition: the session may still
					// return to Active (on-chain failover reassigns it back to
					// us), so retry a few passes — but not forever, or a
					// session that's actually Closed for good wedges the
					// watcher and starves every job behind it.
					w.notActiveRetries[ev.JobID]++
					attempts := w.notActiveRetries[ev.JobID]
					if attempts < w.sessionRetryLimit {
						w.log.Warn("session not active, retrying next pass",
							"jobId", ev.JobID, "sessionId", ev.SessionID, "attempts", attempts, "error", encErr)
						if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
							return stopErr
						}
						return nil
					}
					w.log.Warn("session_not_active_giving_up",
						"jobId", ev.JobID, "sessionId", ev.SessionID, "attempts", attempts)
					delete(w.notActiveRetries, ev.JobID)
					continue
				}
				// TRANSIENT: RPC error reading on-chain data that provably exists
				// (JobSubmitted fires after the session is Active with a key).
				// Stop-and-retry so the assigned job is not silently dropped.
				w.log.Warn("get session enc worker key failed, retrying next pass",
					"jobId", ev.JobID, "sessionId", ev.SessionID, "error", encErr)
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
			}
			delete(w.notActiveRetries, ev.JobID) // session is Active again; clear any prior bounded-retry count
			if !w.keys.CanDecryptSessionKey(enc) {
				// PERMANENT: the session key is wrapped for a different worker key.
				// Retrying would never fix this — the consumer must call
				// updateSessionKey for this worker. Advance cursor (continue) so
				// the watcher is not wedged by this event forever.
				w.log.Warn("session_key_undecryptable, skipping job",
					"jobId", ev.JobID, "sessionId", ev.SessionID)
				continue
			}

			promptHash, _, submitBlock, completeBlock, blobErr := w.c.GetJobBlobInfo(ctx, ev.JobID)
			if blobErr != nil {
				// TRANSIENT: stop-and-retry (see stopAndRetry doc).
				w.log.Warn("get job blob info failed, retrying next pass",
					"jobId", ev.JobID, "error", blobErr)
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
			}
			if completeBlock != 0 {
				// Completed on chain (completeJob sets completionBlockNumber).
				// Reached after a restart, when the dispatched set is empty
				// and the cursor still sits before this block. Serving it
				// again would only send a completeJob that reverts.
				w.log.Info("job already completed on chain, skipping",
					"jobId", ev.JobID, "completionBlock", completeBlock)
				continue
			}
			si, siErr := w.c.GetSessionInfo(ctx, ev.SessionID)
			if siErr != nil {
				// TRANSIENT: stop-and-retry (see stopAndRetry doc).
				w.log.Warn("get session info failed, retrying next pass",
					"jobId", ev.JobID, "sessionId", ev.SessionID, "error", siErr)
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
			}

			payload := pipeline.JobPayload{
				JobID:          ev.JobID,
				SessionID:      ev.SessionID,
				Consumer:       si.User,
				Worker:         w.worker,
				ModelID:        modelIDString(si.ModelID),
				PromptBlobHash: promptHash,
				BlockNumber:    submitBlock,
				Timestamp:      time.Now().Unix(),
				CorrelationID:  fmt.Sprintf("%d-%d", ev.SessionID, ev.JobID),
			}

			// PriorJobIDs stays empty: the handler lists the session's earlier
			// jobs itself once it has decoded the prompt, and only for a job
			// that is not self-contained.

			w.dispatched[ev.JobID] = ev.BlockNumber
			if w.syncServe {
				// Test hook: synchronous serve makes tests deterministic without
				// goroutine synchronisation primitives or polling.
				if serveErr := w.sink.HandleJobPayload(jobCtx, payload); serveErr != nil {
					w.log.Warn("serve job failed", "jobId", payload.JobID, "error", serveErr)
				}
			} else {
				w.jobs.Add(1)
				go func() {
					defer w.jobs.Done()
					if serveErr := w.sink.HandleJobPayload(jobCtx, payload); serveErr != nil {
						w.log.Warn("serve job failed", "jobId", payload.JobID, "error", serveErr)
					}
				}()
			}
		}

		if setErr := w.setCursor(hi); setErr != nil {
			return setErr
		}
	}

	return nil
}

// modelIDString converts the on-chain bytes32 model ID to the string format
// used by the dispatcher and pipeline: lowercase hex without a "0x" prefix.
//
// Evidence: dispatcher/internal/router/router.go builds job payloads with
//
//	ModelID: common.Bytes2Hex(sess.ModelID[:])
//
// and common.Bytes2Hex is defined as hex.EncodeToString (no prefix). Pipeline
// tests (handler_test.go) use the same form:
//
//	expectedModelID := common.Bytes2Hex(crypto.Keccak256Hash([]byte("llama3-8b")).Bytes())
//
// pipeline.handler.normalizeModelLookupKey strips a "0x" prefix if present, so
// both forms pass the model-name lookup — but we match the canonical format here.
func modelIDString(id [32]byte) string {
	return hex.EncodeToString(id[:])
}

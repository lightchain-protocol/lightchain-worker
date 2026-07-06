package sortition

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
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
	PollInterval  time.Duration
	Logger        *slog.Logger
	// syncServe is an internal test hook. When true, HandleJobPayload is called
	// synchronously in RunOnce rather than in a goroutine, making unit tests
	// fully deterministic without WaitGroups or polling. Must not be set in
	// production.
	syncServe bool
}

// JobWatcher polls the chain for JobSubmitted events and serves jobs destined
// for this worker into the inference pipeline.
type JobWatcher struct {
	c         ServeClient
	keys      KeyChecker
	sink      JobSink
	cursor    *CursorStore
	worker    common.Address
	counter   *atomic.Int32
	maxJobs   int
	chunk     uint64
	confs     uint64
	interval  time.Duration
	log       *slog.Logger
	syncServe bool
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
	return &JobWatcher{
		c:         o.Client,
		keys:      o.KeyChecker,
		sink:      o.Sink,
		cursor:    o.Cursor,
		worker:    o.Worker,
		counter:   o.JobCounter,
		maxJobs:   o.MaxConcurrent,
		chunk:     chunk,
		confs:     o.Confirmations,
		interval:  interval,
		log:       o.Logger,
		syncServe: o.syncServe,
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
	return w.cursor.Set(cursorJobSubmitted, stopAt)
}

// Start runs a ticker loop calling RunOnce on each tick until ctx is cancelled.
func (w *JobWatcher) Start(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.RunOnce(ctx); err != nil {
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
// Replay-on-restart is safe: the pipeline checks HasJobAcknowledged and
// HasJobCompleted on-chain before re-doing work, so re-serving an already-done
// job is idempotent. No separate served-set is maintained.
//
// A serve error (e.g. transient pipeline failure) is logged at warn and is
// not fatal to the pass — the cursor still advances. The pipeline's own
// on-chain ack/completion deadline handles missed completions via claimTimeout.
func (w *JobWatcher) RunOnce(ctx context.Context) error {
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

	cursor, err := w.cursor.Get(cursorJobSubmitted)
	if err != nil {
		return err
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

			// Capacity check: if already at max concurrent jobs, stop this pass
			// WITHOUT advancing the cursor past this event. Persist cursor to one
			// block before this event so the next poll pass rescans from
			// ev.BlockNumber. Replay is safe because HandleJobPayload is
			// idempotent — the pipeline checks HasJobAcknowledged/HasJobCompleted
			// on-chain before re-running any stage.
			if int(w.counter.Load()) >= w.maxJobs {
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
			}

			enc, encErr := w.c.GetSessionEncWorkerKey(ctx, ev.SessionID)
			if encErr != nil {
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
			if !w.keys.CanDecryptSessionKey(enc) {
				// PERMANENT: the session key is wrapped for a different worker key.
				// Retrying would never fix this — the consumer must call
				// updateSessionKey for this worker. Advance cursor (continue) so
				// the watcher is not wedged by this event forever.
				w.log.Warn("session_key_undecryptable, skipping job",
					"jobId", ev.JobID, "sessionId", ev.SessionID)
				continue
			}

			promptHash, _, submitBlock, _, blobErr := w.c.GetJobBlobInfo(ctx, ev.JobID)
			if blobErr != nil {
				// TRANSIENT: stop-and-retry (see stopAndRetry doc).
				w.log.Warn("get job blob info failed, retrying next pass",
					"jobId", ev.JobID, "error", blobErr)
				if stopErr := w.stopAndRetry(ev, lo); stopErr != nil {
					return stopErr
				}
				return nil
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

			if w.syncServe {
				// Test hook: synchronous serve makes tests deterministic without
				// goroutine synchronisation primitives or polling.
				if serveErr := w.sink.HandleJobPayload(ctx, payload); serveErr != nil {
					w.log.Warn("serve job failed", "jobId", payload.JobID, "error", serveErr)
				}
			} else {
				go func() {
					if serveErr := w.sink.HandleJobPayload(ctx, payload); serveErr != nil {
						w.log.Warn("serve job failed", "jobId", payload.JobID, "error", serveErr)
					}
				}()
			}
		}

		if setErr := w.cursor.Set(cursorJobSubmitted, hi); setErr != nil {
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

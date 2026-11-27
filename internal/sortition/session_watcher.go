package sortition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

const cursorSessionRequested = "session_requested"

// reseedMaxAttempts bounds how many passes retry a failing look-back before the
// watcher gives it up (a pruned-history RPC would otherwise fail it forever).
const reseedMaxAttempts = 10

// releaseMaxAttempts bounds how many reverted releaseClaim calls the watcher
// makes before it forgets the claim (a SessionManager without releaseClaim
// would otherwise be asked forever). Other failures (RPC, gas, a paused
// contract) never count: giving up on them would hold the slot for good.
const releaseMaxAttempts = 3

// refusedByContract reports whether err is the contract itself reverting,
// as opposed to a transient failure worth retrying. A pause also reverts but
// lifts, so it is retried.
func refusedByContract(err error) bool {
	return strings.Contains(err.Error(), "reverted") && !errors.Is(err, chain.ErrContractPaused)
}

// ClaimClient is the narrow chain interface the SessionWatcher needs.
// Head returns chain.HeadInfo (not a bare uint64) so the watcher can use
// head.Number with the same confirmations-guard pattern as the release Reconciler.
type ClaimClient interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	FilterSessionRequested(ctx context.Context, fromBlock, toBlock uint64) ([]chain.SessionRequestedEvent, error)
	GetRequestInfo(ctx context.Context, reqID uint64) (chain.RequestInfo, error)
	// GetRequiredCapabilities reads a request's required-capability mask (zero = unconstrained)..
	GetRequiredCapabilities(ctx context.Context, reqID uint64) (*big.Int, error)
	EligibleNow(ctx context.Context, reqID uint64, worker common.Address) (bool, error)
	ClaimSession(ctx context.Context, reqID uint64) error
}

// TermsReader is the optional part of a ClaimClient that reads a request's
// terms and this worker's quote. eligibleNow is false for a request that
// excludes a capability the worker declares, or caps the fee below its
// price, exactly as it is while the sortition draw has not reached the
// worker yet; with a TermsReader the watcher tells the two apart, says why
// it will never claim the request, and stops re-checking it.
type TermsReader interface {
	GetRequestTerms(ctx context.Context, reqID uint64) (chain.RequestTerms, error)
	// QuoteWorkerFee with the zero address is the model's fee.
	QuoteWorkerFee(ctx context.Context, worker common.Address, modelID [32]byte) (*big.Int, error)
}

// LifecycleClient is the optional part of a ClaimClient for a SessionManager
// with chosen workers and claim slots: a request can name the worker it
// wants, that worker can turn it down, and a claim the requester never adopts
// can be given back. Without it the watcher claims exactly as before.
type LifecycleClient interface {
	// GetChosenWorker is the worker a request named (zero = any worker).
	GetChosenWorker(ctx context.Context, reqID uint64) (common.Address, error)
	DeclineSession(ctx context.Context, reqID uint64) error
	ReleaseClaim(ctx context.Context, reqID uint64) error
	// AtCapacity is whether the chain counts the worker as full: jobs in
	// flight plus unadopted claims, which the local job counter does not see.
	AtCapacity(ctx context.Context, worker common.Address) (bool, error)
}

// The production client provides both optional parts; without these checks a
// signature drift would silently turn decline, release and the terms skip off.
var (
	_ TermsReader     = (*chain.ChainClient)(nil)
	_ LifecycleClient = (*chain.ChainClient)(nil)
)

// SessionWatcherOpts carries all configuration for a SessionWatcher.
type SessionWatcherOpts struct {
	Client        ClaimClient
	Cursor        *CursorStore
	Worker        common.Address
	JobCounter    *atomic.Int32
	MaxConcurrent int
	ChunkSize     uint64
	Confirmations uint64
	PollInterval  time.Duration
	Logger        *slog.Logger
	// OwnCapabilities is this worker's declared on-chain capability mask.
	// Requests requiring bits outside it are skipped instead of burning a doomed
	// claim tx. Nil is treated as zero (no capabilities).
	OwnCapabilities *big.Int
	// LookbackBlocks is how far behind the persisted cursor the first pass
	// re-scans for SessionRequested events. The pending set lives only in memory,
	// so without this a restart forgets every request discovered before the
	// cursor — including ones still waiting out the sortition threshold. Size it
	// to how long requests stay Open: the consumer-api's default expiry is 1 h,
	// but callers may request longer, so raise it where they do. Zero disables
	// the look-back. It is also how far behind the safe head a worker with no
	// stored cursor starts discovery (see CursorStore.GetOrSeed).
	LookbackBlocks uint64
	// MaxRequestAgeBlocks drops a request once it is this many blocks behind the
	// head without a claim. The consumer-api gives up on a request after its
	// claim timeout (60 s) and never provides session keys for it, but the
	// request stays Open on chain until its expiry (an hour), so a worker that
	// frees up late used to claim dozens of abandoned requests at once: one tx
	// each, for sessions nobody is waiting on. Zero disables the check.
	MaxRequestAgeBlocks uint64
	// AdoptTimeoutBlocks is how many blocks a claim may wait for its requester
	// to adopt it before the worker gives it back (releaseClaim). An unadopted
	// claim holds one of the worker's capacity slots on chain, so a requester
	// that never adopts — or spams requests — would otherwise block it. Zero
	// disables the release.
	AdoptTimeoutBlocks uint64
	// DeclineWhenFull makes the worker turn down (declineSession) a request
	// that named it while it is at capacity, or that it can never serve, so the
	// requester is re-routed at once instead of waiting.
	DeclineWhenFull bool
}

// SessionWatcher polls the chain for SessionRequested events and claims
// sessions this worker is eligible for (subject to local capacity).
//
// Discovered requests are held in an in-memory pending set and re-evaluated
// every pass so that the decaying sortition threshold is respected: a worker
// becomes eligible only after the claim window widens over blocks, which may
// happen on a later pass than discovery.
type SessionWatcher struct {
	c        ClaimClient
	cursor   *CursorStore
	worker   common.Address
	counter  *atomic.Int32
	maxJobs  int
	chunk    uint64
	confs    uint64
	interval time.Duration
	log      *slog.Logger
	ownCaps  *big.Int
	lookback uint64
	// seeded is set once the look-back behind the persisted cursor has run, has
	// been given up after reseedMaxAttempts failures, or is moot because there
	// was no cursor to look behind (see RunOnce, phase 0).
	seeded      bool
	reseedTries int
	// pending maps reqID → its required-capability mask, fetched lazily on first
	// evaluation (nil = not yet fetched). The mask is immutable per request, so a
	// successful fetch is cached for every later pass.
	pending map[uint64]*big.Int
	// requestBlock remembers each pending request's block so the age check in
	// the evaluation phase needs no extra chain read.
	requestBlock map[uint64]uint64
	maxAge       uint64
	// termsChecked holds the pending requests whose terms were already read
	// (or could not be: a SessionManager without terms), so each is read once.
	termsChecked map[uint64]bool

	adoptTimeout    uint64
	declineWhenFull bool
	// chosen caches each pending request's chosen worker (immutable once set).
	chosen map[uint64]common.Address
	// claims maps a request this worker claimed to the head block it claimed
	// at (or found the claim at, after a restart), until the requester adopts
	// it or the worker releases it.
	claims map[uint64]uint64
	// releaseFails counts the failed releaseClaim calls per claim.
	releaseFails map[uint64]int
	// full caches AtCapacity for one pass (nil = not read yet this pass).
	full *bool
}

// NewSessionWatcher constructs a SessionWatcher from the given options.
// ChunkSize defaults to 5000 when zero. PollInterval defaults to 30s when zero.
func NewSessionWatcher(o SessionWatcherOpts) *SessionWatcher {
	chunk := o.ChunkSize
	if chunk == 0 {
		chunk = 5000
	}
	interval := o.PollInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	ownCaps := o.OwnCapabilities
	if ownCaps == nil {
		ownCaps = big.NewInt(0)
	}
	return &SessionWatcher{
		c:        o.Client,
		cursor:   o.Cursor,
		worker:   o.Worker,
		counter:  o.JobCounter,
		maxJobs:  o.MaxConcurrent,
		chunk:    chunk,
		confs:    o.Confirmations,
		interval: interval,
		log:      o.Logger,
		ownCaps:  ownCaps,
		lookback: o.LookbackBlocks,
		pending:  make(map[uint64]*big.Int),

		requestBlock: make(map[uint64]uint64),
		maxAge:       o.MaxRequestAgeBlocks,
		termsChecked: make(map[uint64]bool),

		adoptTimeout:    o.AdoptTimeoutBlocks,
		declineWhenFull: o.DeclineWhenFull,
		chosen:          make(map[uint64]common.Address),
		claims:          make(map[uint64]uint64),
		releaseFails:    make(map[uint64]int),
	}
}

// discover adds a request to the pending set, remembering its block for the age check.
func (w *SessionWatcher) discover(r chain.SessionRequestedEvent) {
	if _, ok := w.pending[r.ReqID]; !ok {
		w.pending[r.ReqID] = nil // capability mask fetched lazily at evaluation
	}
	w.requestBlock[r.ReqID] = r.RequestBlock
}

// forget drops a request from the pending set and its bookkeeping.
func (w *SessionWatcher) forget(reqID uint64) {
	delete(w.pending, reqID)
	delete(w.requestBlock, reqID)
	delete(w.termsChecked, reqID)
	delete(w.chosen, reqID)
}

// tooOld reports whether a pending request is older than MaxRequestAgeBlocks:
// its consumer gave up on it long ago, so claiming it would only spend a tx.
func (w *SessionWatcher) tooOld(reqID, head uint64) bool {
	if w.maxAge == 0 {
		return false
	}
	rb, ok := w.requestBlock[reqID]
	return ok && rb > 0 && head > rb && head-rb > w.maxAge
}

// Start runs a ticker loop calling RunOnce on each tick until ctx is cancelled.
func (w *SessionWatcher) Start(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.RunOnce(ctx); err != nil {
				w.log.Warn("session watcher pass failed", "error", err)
			}
		}
	}
}

// RunOnce performs one poll pass in four phases. A worker with no stored
// cursor starts discovery LookbackBlocks behind the safe head, so it never
// scans further back than that, and has nothing behind its cursor for phase 0.
//
// Phase 0 — Reseed (first pass only): re-scan LookbackBlocks behind the
// persisted cursor and add whatever SessionRequested events are there to the
// pending set, without moving the cursor. The evaluation phase then prunes the
// ones that are no longer Open. This is what lets a restarted worker keep
// competing for requests it had discovered but was not yet eligible for.
//
// Phase 1 — Discovery: scan cursor+1..safeHead in chunks, add discovered
// SessionRequested events to the in-memory pending set, and advance the cursor
// per chunk (resumable on error). If safeHead <= cursor, discovery is skipped
// but the evaluation phase still runs.
//
// Phase 2 — Evaluation: iterate the pending set. For each request, fetch its
// on-chain status (GetRequestInfo); drop it if it is no longer Open or has
// expired. If this worker is now eligible (EligibleNow), claim it. Requests
// that are not yet eligible are kept in the set for re-evaluation next pass —
// this is the fix for the decaying sortition threshold: a worker that was
// ineligible at discovery time will be re-checked here on every subsequent
// pass until the claim window widens enough.
//
// With a LifecycleClient and DeclineWhenFull, a request that named this
// worker is turned down (declineSession) while the worker is full, locally or
// on chain, or when its terms rule this worker out (TermsReader).
//
// Phase 3 — Release: with a LifecycleClient and AdoptTimeoutBlocks, every
// claim of this worker still unadopted that many blocks after it was claimed
// is given back (releaseClaim), freeing the capacity slot it holds on chain.
//
// A claim error (e.g. AlreadyClaimed revert, racing loser) is logged at debug
// and is not fatal — the request is removed optimistically (if our tx lost the
// race, the status will be non-Open on the next pass anyway).
func (w *SessionWatcher) RunOnce(ctx context.Context) error {
	head, err := w.c.Head(ctx)
	if err != nil {
		return err
	}
	w.full = nil

	// Underflow guard: head.Number < confs wraps on uint64 subtraction.
	// Mirror the reconciler pattern exactly (reconciler.go:97-102).
	var safeHead uint64
	if head.Number > w.confs {
		safeHead = head.Number - w.confs
	}

	cursor, fresh, err := w.cursor.GetOrSeed(cursorSessionRequested, safeHead, w.lookback)
	if err != nil {
		return err
	}
	if fresh {
		// Discovery below scans the look-back window itself.
		w.seeded = true
		w.log.Info("no session cursor stored; starting behind the safe head",
			"cursor", cursor, "safeHead", safeHead)
	}

	// Phase 0: Reseed — once per process, look behind the cursor. A failure
	// must not wedge the pass: discovery and evaluation go on as before the
	// look-back existed, and the look-back is retried on later passes up to
	// reseedMaxAttempts (the window drifts forward with the cursor meanwhile).
	if !w.seeded {
		if err := w.reseed(ctx, cursor); err != nil {
			w.reseedTries++
			w.seeded = w.reseedTries >= reseedMaxAttempts
			w.log.Warn("look-back behind the cursor failed; pass continues without it",
				"attempt", w.reseedTries, "maxAttempts", reseedMaxAttempts, "givingUp", w.seeded, "error", err)
		} else {
			w.seeded = true
		}
	}

	// Phase 1: Discovery — scan new blocks, populate pending.
	if safeHead > cursor {
		for lo := cursor + 1; lo <= safeHead; lo += w.chunk {
			hi := lo + w.chunk - 1
			if hi > safeHead {
				hi = safeHead
			}

			reqs, err := w.c.FilterSessionRequested(ctx, lo, hi)
			if err != nil {
				return err
			}

			for _, r := range reqs {
				w.discover(r)
			}

			if err := w.cursor.Set(cursorSessionRequested, hi); err != nil {
				return err
			}
		}
	}

	// Phase 2: Evaluation — re-check all pending requests regardless of
	// whether discovery ran. This is the core fix: a request discovered in a
	// prior pass when the worker was ineligible is re-evaluated here every
	// pass until the sortition claim window widens.
	for reqID := range w.pending {
		ri, err := w.c.GetRequestInfo(ctx, reqID)
		if err != nil {
			w.log.Warn("getRequestInfo failed", "reqId", reqID, "error", err)
			continue // transient; keep in pending
		}

		if ri.Status != 0 { // not Open (Claimed=1, Ready=2, Expired=3)
			if ri.Status == 1 && ri.Worker == w.worker {
				// Our claim, not adopted yet: one made before a restart, or
				// one whose claim call failed but landed.
				w.track(reqID, head.Number)
			}
			w.forget(reqID)
			continue
		}

		if uint64(time.Now().Unix()) > ri.Expiry {
			w.forget(reqID)
			continue
		}

		// Abandoned by its consumer: the consumer-api stops waiting after its
		// claim timeout and never provides keys, so a late claim is a wasted tx.
		// Checked after the status so an old claim of ours is still tracked.
		if w.tooOld(reqID, head.Number) {
			w.log.Debug("dropping stale session request", "reqId", reqID, "requestBlock", w.requestBlock[reqID], "head", head.Number, "maxAgeBlocks", w.maxAge)
			w.forget(reqID)
			continue
		}

		// Skip requests whose required capabilities this worker does not
		// cover — the claim would revert MissingCapabilities anyway. Fail-open on
		// read errors (the on-chain check is the guarantee, this is gas politeness).
		caps := w.pending[reqID]
		if caps == nil {
			fetched, err := w.c.GetRequiredCapabilities(ctx, reqID)
			if err != nil {
				w.log.Warn("required-capabilities read failed; proceeding unconstrained",
					"reqId", reqID, "error", err)
				fetched = big.NewInt(0) // fail-open; not cached so the next pass retries
			} else {
				w.pending[reqID] = fetched
			}
			caps = fetched
		}
		if caps.Sign() != 0 && new(big.Int).And(caps, w.ownCaps).Cmp(caps) != 0 {
			w.log.Debug("skipping request requiring capabilities this worker lacks",
				"reqId", reqID, "requiredMask", caps.String())
			w.forget(reqID)
			continue
		}

		if tr, ok := w.c.(TermsReader); ok && !w.termsChecked[reqID] {
			w.termsChecked[reqID] = true
			reason, err := w.neverClaimable(ctx, tr, reqID)
			switch {
			case err != nil:
				// Fail-open: eligibleNow and the claim tx still decide.
				w.log.Debug("request terms not read; eligibility decides", "reqId", reqID, "error", err)
			case reason != "":
				w.log.Info("skipping session request: "+reason, "reqId", reqID)
				// A request that named this worker waits on it alone (or until
				// its fallback): turn it down so the requester is re-routed.
				w.declineIfMine(ctx, reqID, "cannot serve it: "+reason)
				w.forget(reqID)
				continue
			}
		}

		if int(w.counter.Load()) >= w.maxJobs || w.namedAndFullOnChain(ctx, reqID) {
			// At capacity (locally, or on chain where unadopted claims count):
			// a request that named this worker is turned down so it does not
			// wait for us; every other request stays pending and is re-tried
			// next pass.
			if w.declineIfMine(ctx, reqID, "at capacity") {
				w.forget(reqID)
			}
			continue
		}

		ok, err := w.c.EligibleNow(ctx, reqID, w.worker)
		if err != nil {
			w.log.Warn("eligibleNow check failed", "reqId", reqID, "error", err)
			continue // keep in pending
		}

		if ok {
			if err := w.c.ClaimSession(ctx, reqID); err != nil {
				// Losing a claim race (AlreadyClaimed) or a transient tx error
				// is not fatal to the pass — log at debug and move on.
				w.log.Debug("claim not landed", "reqId", reqID, "error", err)
			} else {
				w.log.Info("claimed session request", "reqId", reqID)
			}
			// Tracked either way: a claim whose receipt wait failed may have
			// landed. One that did not is forgotten at release time.
			w.track(reqID, head.Number)
			// Remove optimistically: if our tx lost the race the request's
			// status will be non-Open on the next pass and it would be pruned
			// from pending then anyway.
			w.forget(reqID)
		}
		// else: not yet eligible — keep in pending for re-evaluation next pass.
	}

	// Phase 3: give back claims the requester never adopted.
	w.releaseStaleClaims(ctx, head.Number)
	return nil
}

// chosenFor returns the worker a request named (zero = any), cached. ok is
// false when the chain cannot say (no LifecycleClient, or a read error).
func (w *SessionWatcher) chosenFor(ctx context.Context, lc LifecycleClient, reqID uint64) (common.Address, bool) {
	if c, cached := w.chosen[reqID]; cached {
		return c, true
	}
	c, err := lc.GetChosenWorker(ctx, reqID)
	if err != nil && strings.Contains(err.Error(), "reverted") {
		// A SessionManager without chosen workers: the request names nobody.
		// Cached, so an older contract costs one call per request, not one
		// per pass.
		w.chosen[reqID] = common.Address{}
		return common.Address{}, true
	}
	if err != nil {
		w.log.Debug("chosen worker not read", "reqId", reqID, "error", err)
		return common.Address{}, false
	}
	w.chosen[reqID] = c
	return c, true
}

// namedAndFullOnChain reports whether reqID named this worker while the chain
// counts it as full. The chain is read at most once per pass, and only when
// declining is on; a read error counts as not full.
func (w *SessionWatcher) namedAndFullOnChain(ctx context.Context, reqID uint64) bool {
	lc, ok := w.c.(LifecycleClient)
	if !ok || !w.declineWhenFull {
		return false
	}
	if c, ok := w.chosenFor(ctx, lc, reqID); !ok || c != w.worker {
		return false
	}
	if w.full == nil {
		full, err := lc.AtCapacity(ctx, w.worker)
		if err != nil {
			w.log.Debug("on-chain capacity not read", "error", err)
		}
		w.full = &full
	}
	return *w.full
}

// track starts the adopt timeout of a claim this worker holds, unless it is
// already running. Only a watcher that releases claims keeps them.
func (w *SessionWatcher) track(reqID, at uint64) {
	if _, ok := w.c.(LifecycleClient); !ok || w.adoptTimeout == 0 {
		return
	}
	if _, ok := w.claims[reqID]; !ok {
		w.claims[reqID] = at
	}
}

// declineIfMine turns down an open request that named this worker, when
// DeclineWhenFull is on. It reports whether it declined.
func (w *SessionWatcher) declineIfMine(ctx context.Context, reqID uint64, why string) bool {
	lc, ok := w.c.(LifecycleClient)
	if !ok || !w.declineWhenFull {
		return false
	}
	c, ok := w.chosenFor(ctx, lc, reqID)
	if !ok || c != w.worker {
		return false
	}
	if err := lc.DeclineSession(ctx, reqID); err != nil {
		w.log.Warn("declineSession failed", "reqId", reqID, "reason", why, "error", err)
		return false
	}
	w.log.Info("declined a session request that named this worker", "reqId", reqID, "reason", why)
	return true
}

// releaseStaleClaims gives back (releaseClaim) every claim of this worker that
// is still waiting for its requester AdoptTimeoutBlocks after it was claimed,
// freeing the capacity slot it holds on chain. Claims that were adopted, or
// closed some other way, are forgotten.
func (w *SessionWatcher) releaseStaleClaims(ctx context.Context, head uint64) {
	lc, ok := w.c.(LifecycleClient)
	if !ok || w.adoptTimeout == 0 {
		return
	}
	for reqID, at := range w.claims {
		if head < at+w.adoptTimeout {
			continue
		}
		ri, err := w.c.GetRequestInfo(ctx, reqID)
		if err != nil {
			w.log.Warn("getRequestInfo failed for a claim", "reqId", reqID, "error", err)
			continue
		}
		if ri.Status != 1 || ri.Worker != w.worker { // adopted, closed, or not ours
			w.untrack(reqID)
			continue
		}
		if err := lc.ReleaseClaim(ctx, reqID); err != nil {
			if refusedByContract(err) {
				w.releaseFails[reqID]++
			}
			giveUp := w.releaseFails[reqID] >= releaseMaxAttempts
			w.log.Warn("releaseClaim failed", "reqId", reqID, "reverts", w.releaseFails[reqID],
				"givingUp", giveUp, "error", err)
			if giveUp {
				w.untrack(reqID)
			}
			continue
		}
		w.log.Info("released a claim its requester never adopted", "reqId", reqID,
			"claimedAtBlock", at, "waitedBlocks", head-at)
		w.untrack(reqID)
	}
}

// untrack stops tracking a claim.
func (w *SessionWatcher) untrack(reqID uint64) {
	delete(w.claims, reqID)
	delete(w.releaseFails, reqID)
}

// neverClaimable returns why this worker can never claim the request, or ""
// when the sortition draw alone decides. It mirrors eligibleNow's terms
// checks: a declared capability the request excludes, and a quote above its
// fee cap (the model's fee when it set none).
func (w *SessionWatcher) neverClaimable(ctx context.Context, tr TermsReader, reqID uint64) (string, error) {
	t, err := tr.GetRequestTerms(ctx, reqID)
	if err != nil {
		return "", err
	}
	if t.Worker != (common.Address{}) && t.Worker != w.worker && t.FallbackBlocks == 0 {
		return fmt.Sprintf("it named worker %s and nobody else", t.Worker.Hex()), nil
	}
	if t.Excluded != nil {
		if clash := new(big.Int).And(t.Excluded, w.ownCaps); clash.Sign() != 0 {
			return fmt.Sprintf("it excludes capabilities this worker declares (mask %s)", clash), nil
		}
	}
	quote, err := tr.QuoteWorkerFee(ctx, w.worker, t.ModelID)
	if err != nil {
		return "", err
	}
	feeCap := t.MaxFee
	if feeCap == nil || feeCap.Sign() == 0 {
		if feeCap, err = tr.QuoteWorkerFee(ctx, common.Address{}, t.ModelID); err != nil {
			return "", err
		}
	}
	if quote.Cmp(feeCap) > 0 {
		return fmt.Sprintf("its fee cap %s wei is below this worker's price %s wei", feeCap, quote), nil
	}
	return "", nil
}

// reseed adds every SessionRequested event in the last w.lookback blocks up to
// and including cursor to the pending set. It never touches the cursor; the
// evaluation phase drops anything that is no longer Open or has expired.
func (w *SessionWatcher) reseed(ctx context.Context, cursor uint64) error {
	if w.lookback == 0 || cursor == 0 {
		return nil
	}
	lo := uint64(1)
	if cursor > w.lookback {
		lo = cursor - w.lookback + 1
	}
	found := 0
	for from := lo; from <= cursor; from += w.chunk {
		to := from + w.chunk - 1
		if to > cursor {
			to = cursor
		}
		reqs, err := w.c.FilterSessionRequested(ctx, from, to)
		if err != nil {
			return err
		}
		for _, r := range reqs {
			w.discover(r)
		}
		found += len(reqs)
	}
	w.log.Info("reseeded pending session requests from behind the cursor",
		"fromBlock", lo, "toBlock", cursor, "requests", found)
	return nil
}

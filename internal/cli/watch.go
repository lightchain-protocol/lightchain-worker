package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/redis/go-redis/v9"

	"github.com/lightchain/pkg/chain/bindings"
	pkgtypes "github.com/lightchain/pkg/types"

	"github.com/lightchain/worker/internal/chain"
)

// WatchChain is the read-only chain surface the `watch` daemon needs.
type WatchChain interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error)
	IsWorkerSuspended(ctx context.Context, worker common.Address) (bool, error)
	GetSuspendedUntil(ctx context.Context, worker common.Address) (*big.Int, error)
	GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error)
	GetMinWorkerStake(ctx context.Context) (*big.Int, error)
	// SessionClaims lists the SessionClaimed logs of every worker in [from, to].
	SessionClaims(ctx context.Context, from, to uint64) ([]SessionClaim, error)
	// EligibleAt reads SessionManager.eligibleNow(reqID, worker) as of block.
	EligibleAt(ctx context.Context, reqID uint64, worker common.Address, block uint64) (bool, error)
	RequiredCapabilities(ctx context.Context, reqID uint64) (*big.Int, error)
}

// SessionClaim is one SessionClaimed log.
type SessionClaim struct {
	ReqID  uint64
	Worker common.Address
	Block  uint64
}

// HeartbeatReader is the one Redis read `watch` makes: the worker's
// heartbeat hash. *redis.Client satisfies it.
type HeartbeatReader interface {
	HGet(ctx context.Context, key, field string) *redis.StringCmd
}

// WatchHandler runs the `watch` daemon: every Interval it checks the worker
// and posts a Discord-compatible webhook message when a check starts failing
// and when it recovers. It only observes — it holds no signing key, sends no
// transaction and writes no worker state.
type WatchHandler struct {
	Chain      WatchChain
	WorkerAddr common.Address
	ChainID    int64

	LivenessURL string // the worker's /healthz; "" = skip
	OllamaURL   string
	// Heartbeat is nil for the gateway profiles, which heartbeat through
	// the worker-gateway instead of Redis.
	Heartbeat       HeartbeatReader
	HeartbeatMaxAge time.Duration
	// MissedClaims alerts after this many sessions in a row went to other
	// workers while this one had been eligible for them. 0 = skip (no
	// SessionManager).
	MissedClaims int

	WebhookURL string
	// Interval between ticks; one tick's checks must finish within it.
	Interval time.Duration
	// Cooldown is the minimum gap between repeat alerts for a check that
	// stays failing. A new failure after a recovery always alerts.
	Cooldown time.Duration

	HTTP   *http.Client     // nil = 10 s default
	Now    func() time.Time // nil = time.Now
	Logger *slog.Logger

	checks map[string]*checkState
	claims claimTracker
}

// claimTracker follows SessionClaimed incrementally, one block range per
// tick, so each tick reads only the blocks since the last.
type claimTracker struct {
	scanned uint64 // last block scanned; 0 = not started
	missed  int    // sessions missed since the worker's last own claim
}

const (
	// claimGraceBlocks: a worker eligible this many blocks before another
	// worker claimed has had time to poll and claim itself (sortition polls
	// every few seconds), so losing that request is a miss, not bad luck.
	claimGraceBlocks = 5
	// maxClaimScan caps one tick's log range. The eligibility reads are
	// historical, and a full node keeps state for only ~128 recent blocks.
	maxClaimScan = 100
)

// checkState is what the operator was last told about one check.
type checkState struct {
	alerted   bool      // an alert went out and no recovery since
	lastAlert time.Time // when the last alert went out
	since     time.Time // when the current failure started
}

// Run checks immediately and then every Interval until ctx is cancelled.
func (h *WatchHandler) Run(ctx context.Context) {
	t := time.NewTicker(h.Interval)
	defer t.Stop()
	for {
		h.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick runs every check once and sends the messages their changes call for.
// The checks share a budget of one Interval so a hung endpoint cannot stall
// the daemon; messages go out on ctx itself.
func (h *WatchHandler) Tick(ctx context.Context) {
	checkCtx, cancel := context.WithTimeout(ctx, h.Interval)
	defer cancel()

	if h.LivenessURL != "" {
		h.observe(ctx, "liveness", h.probe(checkCtx, h.LivenessURL))
	}
	if h.Heartbeat != nil {
		h.observe(ctx, "heartbeat", h.checkHeartbeat(checkCtx))
	}
	h.observe(ctx, "ollama", h.probe(checkCtx, strings.TrimRight(h.OllamaURL, "/")+"/api/tags"))

	findings, err := h.chainFindings(checkCtx)
	if err != nil {
		// The chain checks keep their state: an unreadable chain is not a recovery.
		h.observe(ctx, "rpc", fmt.Sprintf("chain read failed: %v", err))
		return
	}
	for _, f := range findings {
		h.observe(ctx, f.check, f.problem)
	}
}

type finding struct{ check, problem string }

// chainFindings reads the worker's on-chain state. Checks that only make
// sense for a registered worker are left out (not passed) when it is not.
func (h *WatchHandler) chainFindings(ctx context.Context) ([]finding, error) {
	head, err := h.Chain.Head(ctx)
	if err != nil {
		return nil, err
	}
	rpc := ""
	if lag := h.now().Sub(time.Unix(head.Timestamp, 0)).Round(time.Second); lag > headStaleAfter {
		rpc = fmt.Sprintf("head %d is %s old — chain stalled or RPC lagging", head.Number, lag)
	}
	out := []finding{{"rpc", rpc}}

	registered, err := h.Chain.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil {
		return nil, err
	}
	if !registered {
		return append(out, finding{"registered", "worker is not registered on-chain"}), nil
	}
	out = append(out, finding{"registered", ""})

	suspended, err := h.Chain.IsWorkerSuspended(ctx, h.WorkerAddr)
	if err != nil {
		return nil, err
	}
	f := finding{check: "suspended"}
	if suspended {
		until, err := h.Chain.GetSuspendedUntil(ctx, h.WorkerAddr)
		if err != nil {
			return nil, err
		}
		end := time.Unix(until.Int64(), 0).UTC().Format(time.RFC3339)
		if until.Int64() > head.Timestamp {
			f.problem = fmt.Sprintf("worker is suspended by WorkerRegistry until %s; after that it must send reinstate() to claim again", end)
		} else {
			f.problem = fmt.Sprintf("suspension cooldown ended %s but the worker is still suspended — it must send reinstate() to claim again", end)
		}
	}
	out = append(out, f)

	stake, err := h.Chain.GetWorkerStake(ctx, h.WorkerAddr)
	if err != nil {
		return nil, err
	}
	minStake, err := h.Chain.GetMinWorkerStake(ctx)
	if err != nil {
		return nil, err
	}
	f = finding{check: "stake"}
	if stake.Cmp(minStake) < 0 {
		f.problem = fmt.Sprintf("%s below the on-chain minimum %s — top up the stake", lcai(stake), lcai(minStake))
	}
	out = append(out, f)

	if h.MissedClaims > 0 {
		problem, err := h.checkClaims(ctx, head.Number)
		if err != nil {
			return nil, err
		}
		out = append(out, finding{"claims", problem})
	}
	return out, nil
}

// checkClaims scans the SessionClaimed logs since the last tick. The
// worker's own claim clears the count; another worker's claim on a request
// this worker had long been eligible for adds a miss. The first tick
// starts at head.
func (h *WatchHandler) checkClaims(ctx context.Context, head uint64) (string, error) {
	c := &h.claims
	if c.scanned == 0 {
		c.scanned = head
	}
	if head > c.scanned {
		from := c.scanned + 1
		// ponytail: after a long RPC outage the oldest blocks are skipped, not
		// chunked; their historical state is gone anyway.
		if head-from >= maxClaimScan {
			from = head - maxClaimScan + 1
		}
		claims, err := h.Chain.SessionClaims(ctx, from, head)
		if err != nil {
			return "", err
		}
		for _, cl := range claims {
			switch {
			case cl.Worker == h.WorkerAddr:
				c.missed = 0
			case h.missedClaim(ctx, cl):
				c.missed++
			}
		}
		c.scanned = head
	}
	if c.missed >= h.MissedClaims {
		return fmt.Sprintf("%d session(s) in a row went to other workers although this worker was sortition-eligible "+
			"for each at least %d blocks earlier — it is not claiming; check the worker's logs", c.missed, claimGraceBlocks), nil
	}
	return "", nil
}

// missedClaim reports whether the worker was eligible for cl's request
// claimGraceBlocks before another worker claimed it. Requests that need a
// capability are not judged (eligibleNow does not check capabilities), and
// a failed read counts as no miss so pruned state cannot raise an alert.
func (h *WatchHandler) missedClaim(ctx context.Context, cl SessionClaim) bool {
	if cl.Block <= claimGraceBlocks {
		return false
	}
	caps, err := h.Chain.RequiredCapabilities(ctx, cl.ReqID)
	if err != nil {
		h.Logger.Warn("watch: read required capabilities", "req", cl.ReqID, "error", err)
		return false
	}
	if caps.Sign() != 0 {
		return false
	}
	eligible, err := h.Chain.EligibleAt(ctx, cl.ReqID, h.WorkerAddr, cl.Block-claimGraceBlocks)
	if err != nil {
		h.Logger.Warn("watch: read historical eligibility", "req", cl.ReqID, "error", err)
		return false
	}
	return eligible
}

func (h *WatchHandler) checkHeartbeat(ctx context.Context) string {
	key := pkgtypes.HeartbeatRedisKey(h.WorkerAddr.Hex())
	last, err := h.Heartbeat.HGet(ctx, key, pkgtypes.HBFieldLastHeartbeat).Int64()
	switch {
	case errors.Is(err, redis.Nil):
		return fmt.Sprintf("no heartbeat in Redis (%s expired) — the worker stopped heartbeating", key)
	case err != nil:
		return fmt.Sprintf("cannot read heartbeat %s: %v", key, err)
	}
	if age := h.now().Sub(time.Unix(last, 0)); age > h.HeartbeatMaxAge {
		return fmt.Sprintf("last heartbeat %s ago (limit %s)", age.Round(time.Second), h.HeartbeatMaxAge)
	}
	return ""
}

// probe returns "" when target answers 200, otherwise what went wrong.
func (h *WatchHandler) probe(ctx context.Context, target string) string {
	resp, err := httpGet(ctx, h.HTTP, target)
	if err != nil {
		return fmt.Sprintf("unreachable: %v", err) // the error names the URL
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("%s returned %d", target, resp.StatusCode)
	}
	return ""
}

// observe applies one check result: alert on the transition to failing,
// repeat at most once per Cooldown while it stays failing, and send one
// recovery after an alert. State only advances when the webhook accepted
// the message, so a failed post is retried on the next tick.
func (h *WatchHandler) observe(ctx context.Context, check, problem string) {
	if h.checks == nil {
		h.checks = map[string]*checkState{}
	}
	st := h.checks[check]
	if st == nil {
		st = &checkState{}
		h.checks[check] = st
	}
	now := h.now()
	switch {
	case problem != "":
		if st.since.IsZero() {
			st.since = now
			h.Logger.Warn("watch check failing", "check", check, "problem", problem)
		}
		if st.alerted && now.Sub(st.lastAlert) < h.Cooldown {
			return
		}
		title := check + " failing"
		if st.alerted {
			title = fmt.Sprintf("%s still failing (%s)", check, now.Sub(st.since).Round(time.Second))
		}
		if h.notify(ctx, check, title, problem, false) {
			st.alerted, st.lastAlert = true, now
		}
	case !st.since.IsZero():
		h.Logger.Info("watch check recovered", "check", check)
		if st.alerted {
			desc := fmt.Sprintf("back to normal after %s", now.Sub(st.since).Round(time.Second))
			if !h.notify(ctx, check, check+" recovered", desc, true) {
				return
			}
		}
		st.alerted, st.since = false, time.Time{}
	}
}

type webhookMessage struct {
	Content string         `json:"content"`
	Embeds  []webhookEmbed `json:"embeds"`
}

type webhookEmbed struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Color       int    `json:"color"`
	Timestamp   string `json:"timestamp"`
}

// notify posts one message and reports whether the webhook accepted it.
func (h *WatchHandler) notify(ctx context.Context, check, title, desc string, recovered bool) bool {
	kind, color := "ALERT", 0xE74C3C
	if recovered {
		kind, color = "RECOVERED", 0x2ECC71
	}
	msg := webhookMessage{
		Content: fmt.Sprintf("**%s** %s — worker %s on %s (chain %d)",
			kind, check, h.WorkerAddr.Hex(), networkName(h.ChainID), h.ChainID),
		Embeds: []webhookEmbed{{
			Title:       title,
			Description: desc,
			Color:       color,
			Timestamp:   h.now().UTC().Format(time.RFC3339),
		}},
	}
	body, err := json.Marshal(msg)
	if err != nil {
		h.Logger.Error("encode webhook message", "error", err)
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.WebhookURL, bytes.NewReader(body))
	if err != nil {
		h.Logger.Error("build webhook request failed")
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient(h.HTTP).Do(req)
	if err != nil {
		// *url.Error carries the URL, and a webhook URL's path is its secret.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		h.Logger.Error("webhook post failed; retrying next tick", "check", check, "error", err)
		return false
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		h.Logger.Error("webhook rejected message; retrying next tick", "check", check, "status", resp.StatusCode)
		return false
	}
	return true
}

func (h *WatchHandler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// KeystoreAddress reads the address field of a v3 keystore file without
// decrypting it, so `watch` never needs the keystore password.
func KeystoreAddress(path string) (common.Address, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return common.Address{}, err
	}
	var ks struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(raw, &ks); err != nil {
		return common.Address{}, fmt.Errorf("parse keystore %s: %w", path, err)
	}
	if !common.IsHexAddress(ks.Address) {
		return common.Address{}, fmt.Errorf("keystore %s has no address field — pass --worker", path)
	}
	return common.HexToAddress(ks.Address), nil
}

// WatchBackend is what ReadOnlyChain needs from an RPC client: contract
// calls, log filters and headers. It has no way to send a transaction.
// *ethclient.Client satisfies it.
type WatchBackend interface {
	bind.ContractCaller
	bind.ContractFilterer
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
}

// ReadOnlyChain implements WatchChain with the generated *Caller and
// *Filterer bindings only — no signing key and no transactor exist here.
type ReadOnlyChain struct {
	backend  WatchBackend
	registry *bindings.WorkerRegistryCaller
	aiConfig *bindings.AIConfigCaller
	// Both nil without a SessionManager.
	sessions    *bindings.SessionManagerCaller
	sessionLogs *bindings.SessionManagerFilterer
}

// NewReadOnlyChain binds the contracts watch reads. sessionManager may be
// the zero address, in which case the session methods must not be called.
func NewReadOnlyChain(backend WatchBackend, registry, aiConfig, sessionManager common.Address) (*ReadOnlyChain, error) {
	c := &ReadOnlyChain{backend: backend}
	var err error
	if c.registry, err = bindings.NewWorkerRegistryCaller(registry, backend); err != nil {
		return nil, err
	}
	if c.aiConfig, err = bindings.NewAIConfigCaller(aiConfig, backend); err != nil {
		return nil, err
	}
	if sessionManager != (common.Address{}) {
		if c.sessions, err = bindings.NewSessionManagerCaller(sessionManager, backend); err != nil {
			return nil, err
		}
		if c.sessionLogs, err = bindings.NewSessionManagerFilterer(sessionManager, backend); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Head returns the latest block.
func (c *ReadOnlyChain) Head(ctx context.Context) (chain.HeadInfo, error) {
	h, err := c.backend.HeaderByNumber(ctx, nil)
	if err != nil {
		return chain.HeadInfo{}, fmt.Errorf("latest header: %w", err)
	}
	if h.Number == nil {
		return chain.HeadInfo{}, errors.New("latest header has no block number")
	}
	return chain.HeadInfo{Number: h.Number.Uint64(), Timestamp: int64(h.Time)}, nil
}

// IsWorkerRegistered reads WorkerRegistry.isWorkerRegistered.
func (c *ReadOnlyChain) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	return c.registry.IsWorkerRegistered(&bind.CallOpts{Context: ctx}, worker)
}

// IsWorkerSuspended reads WorkerRegistry.isWorkerSuspended.
func (c *ReadOnlyChain) IsWorkerSuspended(ctx context.Context, worker common.Address) (bool, error) {
	return c.registry.IsWorkerSuspended(&bind.CallOpts{Context: ctx}, worker)
}

// GetSuspendedUntil reads WorkerRegistry.getSuspendedUntil.
func (c *ReadOnlyChain) GetSuspendedUntil(ctx context.Context, worker common.Address) (*big.Int, error) {
	return c.registry.GetSuspendedUntil(&bind.CallOpts{Context: ctx}, worker)
}

// GetWorkerStake reads WorkerRegistry.getWorkerStake.
func (c *ReadOnlyChain) GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error) {
	return c.registry.GetWorkerStake(&bind.CallOpts{Context: ctx}, worker)
}

// GetMinWorkerStake reads AIConfig.getMinWorkerStake.
func (c *ReadOnlyChain) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	return c.aiConfig.GetMinWorkerStake(&bind.CallOpts{Context: ctx})
}

// SessionClaims lists SessionClaimed logs in [from, to].
func (c *ReadOnlyChain) SessionClaims(ctx context.Context, from, to uint64) ([]SessionClaim, error) {
	it, err := c.sessionLogs.FilterSessionClaimed(&bind.FilterOpts{Context: ctx, Start: from, End: &to}, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("filter SessionClaimed [%d,%d]: %w", from, to, err)
	}
	defer func() { _ = it.Close() }()
	var out []SessionClaim
	for it.Next() {
		out = append(out, SessionClaim{ReqID: it.Event.ReqId.Uint64(), Worker: it.Event.Worker, Block: it.Event.Raw.BlockNumber})
	}
	return out, it.Error()
}

// EligibleAt reads SessionManager.eligibleNow against the state of block.
func (c *ReadOnlyChain) EligibleAt(ctx context.Context, reqID uint64, worker common.Address, block uint64) (bool, error) {
	opts := &bind.CallOpts{Context: ctx, BlockNumber: new(big.Int).SetUint64(block)}
	return c.sessions.EligibleNow(opts, new(big.Int).SetUint64(reqID), worker)
}

// RequiredCapabilities reads SessionManager.getRequiredCapabilities.
func (c *ReadOnlyChain) RequiredCapabilities(ctx context.Context, reqID uint64) (*big.Int, error) {
	return c.sessions.GetRequiredCapabilities(&bind.CallOpts{Context: ctx}, new(big.Int).SetUint64(reqID))
}

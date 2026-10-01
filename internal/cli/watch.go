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
	GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error)
	GetMinWorkerStake(ctx context.Context) (*big.Int, error)
	// CountSessionClaims counts SessionClaimed events for worker in [from, to].
	CountSessionClaims(ctx context.Context, worker common.Address, from, to uint64) (int, error)
	// CountSessionRequests counts SessionRequested events for any of models
	// (all models when empty) in [from, to].
	CountSessionRequests(ctx context.Context, models [][32]byte, from, to uint64) (int, error)
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
	ModelIDs   [][32]byte

	LivenessURL string // the worker's /healthz; "" = skip
	OllamaURL   string
	// Heartbeat is nil for the gateway profiles, which heartbeat through
	// the worker-gateway instead of Redis.
	Heartbeat       HeartbeatReader
	HeartbeatMaxAge time.Duration
	// NoClaimAfter alerts when session requests for the worker's models have
	// gone unclaimed by it this long. 0 = skip (no SessionManager).
	NoClaimAfter time.Duration

	WebhookURL string
	Interval   time.Duration
	// Cooldown is the minimum gap between two alerts for the same check.
	Cooldown time.Duration

	HTTP   *http.Client     // nil = 10 s default
	Now    func() time.Time // nil = time.Now
	Logger *slog.Logger

	checks map[string]*checkState
	claims claimTracker
}

// claimTracker follows SessionRequested / SessionClaimed incrementally, one
// block range per tick, so each tick reads only the blocks since the last.
type claimTracker struct {
	scanned      uint64    // last block scanned; 0 = not started
	pending      int       // requests for the worker's models since its last claim
	pendingSince time.Time // when the first of those was seen
}

// maxClaimScan caps one tick's log range, like the sortition watcher's
// default chunk size.
const maxClaimScan = 5000

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
func (h *WatchHandler) Tick(ctx context.Context) {
	if h.LivenessURL != "" {
		h.report(ctx, "liveness", h.probe(ctx, h.LivenessURL))
	}
	if h.Heartbeat != nil {
		h.report(ctx, "heartbeat", h.checkHeartbeat(ctx))
	}
	h.report(ctx, "ollama", h.probe(ctx, strings.TrimRight(h.OllamaURL, "/")+"/api/tags"))

	findings, err := h.chainFindings(ctx)
	if err != nil {
		// The chain checks keep their state: an unreadable chain is not a recovery.
		h.report(ctx, "rpc", fmt.Sprintf("chain read failed: %v", err))
		return
	}
	for _, f := range findings {
		h.report(ctx, f.check, f.problem)
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
		f.problem = "worker is suspended by WorkerRegistry — it cannot claim until the cooldown ends (getSuspendedUntil)"
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

	if h.NoClaimAfter > 0 {
		problem, err := h.checkClaims(ctx, head.Number)
		if err != nil {
			return nil, err
		}
		out = append(out, finding{"claims", problem})
	}
	return out, nil
}

// checkClaims scans the blocks since the last tick for the worker's claims
// and for requests it could have claimed. The first tick starts at head.
func (h *WatchHandler) checkClaims(ctx context.Context, head uint64) (string, error) {
	c := &h.claims
	if c.scanned == 0 {
		c.scanned = head
	}
	if head > c.scanned {
		from := c.scanned + 1
		// ponytail: after a long RPC outage the oldest blocks are skipped, not
		// chunked; the rpc check has already alerted for that gap.
		if head-from >= maxClaimScan {
			from = head - maxClaimScan + 1
		}
		claimed, err := h.Chain.CountSessionClaims(ctx, h.WorkerAddr, from, head)
		if err != nil {
			return "", err
		}
		requested, err := h.Chain.CountSessionRequests(ctx, h.ModelIDs, from, head)
		if err != nil {
			return "", err
		}
		c.scanned = head
		switch {
		case claimed > 0:
			c.pending, c.pendingSince = 0, time.Time{}
		case requested > 0:
			if c.pending == 0 {
				c.pendingSince = h.now()
			}
			c.pending += requested
		}
	}
	if c.pending > 0 && h.now().Sub(c.pendingSince) >= h.NoClaimAfter {
		return fmt.Sprintf("no session claimed in %s despite %d session request(s) for its models — check the worker's logs for claim errors",
			h.now().Sub(c.pendingSince).Round(time.Second), c.pending), nil
	}
	return "", nil
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

// report applies one check result: alert on failure unless this check
// alerted within the cooldown, recover once after an alert. State only
// advances when the webhook accepted the message, so a failed post is
// retried on the next tick.
func (h *WatchHandler) report(ctx context.Context, check, problem string) {
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
		if now.Sub(st.lastAlert) < h.Cooldown {
			return
		}
		title := check + " failing"
		if st.alerted {
			title = fmt.Sprintf("%s still failing (%s)", check, now.Sub(st.since).Round(time.Second))
		}
		if h.notify(ctx, "ALERT", check, title, problem, colorAlert) {
			st.alerted, st.lastAlert = true, now
		}
	case !st.since.IsZero():
		h.Logger.Info("watch check recovered", "check", check)
		if st.alerted {
			desc := fmt.Sprintf("back to normal after %s", now.Sub(st.since).Round(time.Second))
			if !h.notify(ctx, "RECOVERED", check, check+" recovered", desc, colorRecovered) {
				return
			}
		}
		st.alerted, st.since = false, time.Time{}
	}
}

const (
	colorAlert     = 0xE74C3C
	colorRecovered = 0x2ECC71
)

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
func (h *WatchHandler) notify(ctx context.Context, kind, check, title, desc string, color int) bool {
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
	c := h.HTTP
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := c.Do(req)
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
	sessions *bindings.SessionManagerFilterer // nil without a SessionManager
}

// NewReadOnlyChain binds the contracts watch reads. sessionManager may be
// the zero address, in which case the Count* methods must not be called.
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
		if c.sessions, err = bindings.NewSessionManagerFilterer(sessionManager, backend); err != nil {
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

// GetWorkerStake reads WorkerRegistry.getWorkerStake.
func (c *ReadOnlyChain) GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error) {
	return c.registry.GetWorkerStake(&bind.CallOpts{Context: ctx}, worker)
}

// GetMinWorkerStake reads AIConfig.getMinWorkerStake.
func (c *ReadOnlyChain) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	return c.aiConfig.GetMinWorkerStake(&bind.CallOpts{Context: ctx})
}

// CountSessionClaims counts the worker's SessionClaimed logs, filtered on
// the indexed worker topic by the node.
func (c *ReadOnlyChain) CountSessionClaims(ctx context.Context, worker common.Address, from, to uint64) (int, error) {
	it, err := c.sessions.FilterSessionClaimed(&bind.FilterOpts{Context: ctx, Start: from, End: &to}, nil, []common.Address{worker})
	if err != nil {
		return 0, fmt.Errorf("filter SessionClaimed [%d,%d]: %w", from, to, err)
	}
	return countLogs(it)
}

// CountSessionRequests counts SessionRequested logs for models (indexed
// topic; every model when empty).
func (c *ReadOnlyChain) CountSessionRequests(ctx context.Context, models [][32]byte, from, to uint64) (int, error) {
	it, err := c.sessions.FilterSessionRequested(&bind.FilterOpts{Context: ctx, Start: from, End: &to}, nil, nil, models)
	if err != nil {
		return 0, fmt.Errorf("filter SessionRequested [%d,%d]: %w", from, to, err)
	}
	return countLogs(it)
}

func countLogs(it interface {
	Next() bool
	Error() error
	Close() error
},
) (int, error) {
	defer func() { _ = it.Close() }()
	n := 0
	for it.Next() {
		n++
	}
	return n, it.Error()
}

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// PreflightChain is the read-only chain surface the preflight needs.
type PreflightChain interface {
	ChainID(ctx context.Context) (*big.Int, error)
	Head(ctx context.Context) (chain.HeadInfo, error)
	Balance(ctx context.Context, addr common.Address) (*big.Int, error)
	IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error)
	IsWorkerSuspended(ctx context.Context, worker common.Address) (bool, error)
	GetOffenseCount(ctx context.Context, worker common.Address) (*big.Int, error)
	GetWorkerStake(ctx context.Context, worker common.Address) (*big.Int, error)
	GetMinWorkerStake(ctx context.Context) (*big.Int, error)
	GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error)
	IsModelWhitelisted(ctx context.Context, modelID [32]byte) (bool, error)
	IsModelEnabled(ctx context.Context, modelID [32]byte) (bool, error)
	WorkerSupportsModel(ctx context.Context, worker common.Address, modelID [32]byte) (bool, error)
}

// GatewayProbe is satisfied by the authenticated gateway client (full
// challenge/response) and by GatewayPing (reachability only, for read-only runs).
type GatewayProbe interface {
	Authenticate(ctx context.Context) error
}

// GatewayPing checks that the worker-gateway answers its unauthenticated
// challenge endpoint. Used when no signing key is loaded.
type GatewayPing struct {
	URL  string
	HTTP *http.Client
}

// Authenticate implements GatewayProbe without a signing key.
func (g *GatewayPing) Authenticate(ctx context.Context) error {
	resp, err := httpGet(ctx, g.HTTP, strings.TrimRight(g.URL, "/")+"/api/auth/challenge")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("challenge endpoint returned %d", resp.StatusCode)
	}
	return nil
}

// Published per-network minimums (LCAI) from the operator docs. The on-chain
// AIConfig.getMinWorkerStake() is what is enforced; a mismatch is reported so
// stale docs get noticed, not treated as a failure.
var publishedMinStakeLCAI = map[int64]int64{
	8200: 5_000,
	9200: 50_000,
}

var networkNames = map[int64]string{
	8200:  "testnet",
	9200:  "mainnet",
	48221: "devnet",
}

// gasBufferLCAI is the headroom the operator docs recommend above stake.
const gasBufferLCAI = 50

// headStaleAfter is how far behind wall-clock the head may be before the RPC
// is reported as lagging (2 s slots on testnet, 6 s on mainnet).
const headStaleAfter = 60 * time.Second

// PreflightHandler runs the read-only `preflight` subcommand: it checks the
// pieces an operator must have right before going live and prints one line
// per check. Optional probes (ECDH key, gateway, Ollama, beacon) are skipped
// when not configured.
type PreflightHandler struct {
	Chain      PreflightChain
	ChainID    int64 // expected chain id from CHAIN_ID
	WorkerAddr common.Address
	ModelIDs   [][32]byte
	ModelNames []string // parallel to ModelIDs

	ECDHKeyPath string
	ECDHPass    string
	LoadECDHKey ECDHKeyLoader // nil = no local keystore (read-only mode)

	Gateway   GatewayProbe // nil = skip
	OllamaURL string       // "" = skip
	BeaconURL string       // "" = skip
	HTTP      *http.Client // nil = 10 s default

	Now func() time.Time // nil = time.Now
	Out io.Writer
}

type report struct {
	out                  io.Writer
	passed, warned, fail int
}

func (r *report) line(status, name, format string, args ...any) {
	fmt.Fprintf(r.out, "[%s] %-10s %s\n", status, name, fmt.Sprintf(format, args...))
}
func (r *report) pass(name, f string, a ...any) { r.passed++; r.line("PASS", name, f, a...) }
func (r *report) warn(name, f string, a ...any) { r.warned++; r.line("WARN", name, f, a...) }
func (r *report) failf(name, f string, a ...any) {
	r.fail++
	r.line("FAIL", name, f, a...)
}
func (r *report) skip(name, f string, a ...any) { r.line("SKIP", name, f, a...) }

// Run executes every check and returns true when none failed.
func (h *PreflightHandler) Run(ctx context.Context) bool {
	r := &report{out: h.Out}
	now := h.Now
	if now == nil {
		now = time.Now
	}
	fmt.Fprintf(h.Out, "preflight: worker %s on chain %d (%s)\n", h.WorkerAddr.Hex(), h.ChainID, networkName(h.ChainID))

	if h.checkRPC(ctx, r, now()) {
		h.checkChain(ctx, r)
	} else {
		r.skip("chain", "registration, stake, model checks need a working RPC")
	}
	h.checkGateway(ctx, r)
	h.checkOllama(ctx, r)
	h.checkBeacon(ctx, r)

	fmt.Fprintf(h.Out, "preflight: %d passed, %d warnings, %d failed\n", r.passed, r.warned, r.fail)
	return r.fail == 0
}

// checkRPC reports false when the chain checks cannot meaningfully run.
func (h *PreflightHandler) checkRPC(ctx context.Context, r *report, now time.Time) bool {
	id, err := h.Chain.ChainID(ctx)
	if err != nil {
		r.failf("rpc", "unreachable: %v", err)
		return false
	}
	if id.Int64() != h.ChainID {
		r.failf("rpc", "chain id %s does not match CHAIN_ID=%d — wrong RPC_URL?", id, h.ChainID)
		return false
	}
	head, err := h.Chain.Head(ctx)
	if err != nil {
		r.failf("rpc", "chain id %d but head read failed: %v", h.ChainID, err)
		return false
	}
	lag := now.Sub(time.Unix(head.Timestamp, 0)).Round(time.Second)
	if lag > headStaleAfter {
		r.warn("rpc", "chain id %d, head %d is %s old — chain stalled or RPC lagging", h.ChainID, head.Number, lag)
	} else {
		r.pass("rpc", "chain id %d, head %d (%s old)", h.ChainID, head.Number, lag)
	}
	return true
}

func (h *PreflightHandler) checkChain(ctx context.Context, r *report) {
	addr := h.WorkerAddr
	registered, err := h.Chain.IsWorkerRegistered(ctx, addr)
	if err != nil {
		r.failf("registered", "%v", err)
		return
	}
	if registered {
		r.pass("registered", "REGISTERED")
		h.checkSuspension(ctx, r)
	} else {
		r.failf("registered", "NOT REGISTERED — run `lightchain-worker register` once funded")
	}

	minStake, err := h.Chain.GetMinWorkerStake(ctx)
	if err != nil {
		r.failf("stake", "read minimum: %v", err)
		minStake = nil
	}
	if minStake != nil {
		if pub, ok := publishedMinStakeLCAI[h.ChainID]; ok && minStake.Cmp(lcaiToWei(pub)) != 0 {
			r.warn("stake", "on-chain minimum %s differs from the published %d LCAI for %s — docs stale?",
				lcai(minStake), pub, networkName(h.ChainID))
		}
		if registered {
			h.checkStake(ctx, r, minStake)
		} else {
			r.skip("stake", "not registered; minimum is %s", lcai(minStake))
		}
	}
	h.checkBalance(ctx, r, registered, minStake)
	h.checkECDH(ctx, r, registered)
	h.checkModels(ctx, r, registered)
}

func (h *PreflightHandler) checkSuspension(ctx context.Context, r *report) {
	suspended, err := h.Chain.IsWorkerSuspended(ctx, h.WorkerAddr)
	if err != nil {
		r.failf("suspended", "%v", err)
		return
	}
	offenses := "?"
	if n, err := h.Chain.GetOffenseCount(ctx, h.WorkerAddr); err == nil {
		offenses = n.String()
	}
	if suspended {
		r.failf("suspended", "yes — %s offense(s); wait out the cooldown (getSuspendedUntil) before serving", offenses)
	} else {
		r.pass("suspended", "no (%s offense(s) on record)", offenses)
	}
}

func (h *PreflightHandler) checkStake(ctx context.Context, r *report, minStake *big.Int) {
	stake, err := h.Chain.GetWorkerStake(ctx, h.WorkerAddr)
	if err != nil {
		r.failf("stake", "%v", err)
		return
	}
	if stake.Cmp(minStake) < 0 {
		short := new(big.Int).Sub(minStake, stake)
		r.failf("stake", "%s below minimum %s — topUpStake with at least %s", lcai(stake), lcai(minStake), lcai(short))
		return
	}
	r.pass("stake", "%s (minimum %s)", lcai(stake), lcai(minStake))
}

func (h *PreflightHandler) checkBalance(ctx context.Context, r *report, registered bool, minStake *big.Int) {
	bal, err := h.Chain.Balance(ctx, h.WorkerAddr)
	if err != nil {
		r.failf("balance", "%v", err)
		return
	}
	buffer := lcaiToWei(gasBufferLCAI)
	if registered {
		switch {
		case bal.Sign() == 0:
			r.failf("balance", "0 LCAI — cannot pay gas for claim/ack/blob transactions")
		case bal.Cmp(buffer) < 0:
			r.warn("balance", "%s — below the ~%d LCAI gas buffer the docs recommend", lcai(bal), gasBufferLCAI)
		default:
			r.pass("balance", "%s", lcai(bal))
		}
		return
	}
	if minStake == nil {
		r.skip("balance", "%s — cannot judge without the on-chain minimum", lcai(bal))
		return
	}
	need := new(big.Int).Add(minStake, buffer)
	switch {
	case bal.Cmp(minStake) < 0:
		r.failf("balance", "%s — registering needs %s stake plus a gas buffer (~%s total)", lcai(bal), lcai(minStake), lcai(need))
	case bal.Cmp(need) < 0:
		r.warn("balance", "%s covers the %s stake but leaves little for gas (~%s recommended)", lcai(bal), lcai(minStake), lcai(need))
	default:
		r.pass("balance", "%s — enough to register (%s stake + gas)", lcai(bal), lcai(minStake))
	}
}

func (h *PreflightHandler) checkECDH(ctx context.Context, r *report, registered bool) {
	if h.LoadECDHKey == nil {
		r.skip("ecdh", "no local keystore (read-only mode)")
		return
	}
	key, err := h.LoadECDHKey(h.ECDHKeyPath, h.ECDHPass)
	switch {
	case err == nil:
	case errors.Is(err, fs.ErrNotExist) && !registered:
		r.warn("ecdh", "not generated yet at %s — register creates it", h.ECDHKeyPath)
		return
	case errors.Is(err, fs.ErrNotExist):
		r.failf("ecdh", "local key missing at %s but a key is registered on-chain — restore it from backup", h.ECDHKeyPath)
		return
	default:
		// Wrong WORKER_KEYSTORE_PASSWORD or a corrupt file: register would die on it too.
		r.failf("ecdh", "local key unreadable: %v", err)
		return
	}
	if !registered {
		r.pass("ecdh", "local key ready; register publishes it on-chain")
		return
	}
	onChain, err := h.Chain.GetWorkerEncryptionKey(ctx, h.WorkerAddr)
	if err != nil {
		r.failf("ecdh", "read on-chain key: %v", err)
		return
	}
	if !bytes.Equal(onChain, key.PublicKey().Bytes()) {
		r.failf("ecdh", "on-chain key differs from local keystore %s — sessions encrypted to the on-chain key cannot be served", h.ECDHKeyPath)
		return
	}
	r.pass("ecdh", "on-chain key matches local keystore")
}

func (h *PreflightHandler) checkModels(ctx context.Context, r *report, registered bool) {
	if len(h.ModelIDs) == 0 {
		r.skip("model", "SUPPORTED_MODELS not set")
		return
	}
	if len(h.ModelNames) != len(h.ModelIDs) {
		r.failf("model", "internal: %d model ids but %d names from SUPPORTED_MODELS", len(h.ModelIDs), len(h.ModelNames))
		return
	}
	for i, id := range h.ModelIDs {
		name := h.ModelNames[i]
		whitelisted, err := h.Chain.IsModelWhitelisted(ctx, id)
		if err != nil {
			r.failf("model", "%s: %v", name, err)
			continue
		}
		if !whitelisted {
			r.failf("model", "%s not whitelisted on-chain (keccak256 of the name must equal a whitelisted id)", name)
			continue
		}
		enabled, err := h.Chain.IsModelEnabled(ctx, id)
		if err != nil {
			r.failf("model", "%s: %v", name, err)
			continue
		}
		if !enabled {
			r.failf("model", "%s whitelisted but disabled in AIConfig", name)
			continue
		}
		if !registered {
			r.pass("model", "%s whitelisted, enabled", name)
			continue
		}
		supported, err := h.Chain.WorkerSupportsModel(ctx, h.WorkerAddr, id)
		if err != nil {
			r.failf("model", "%s: %v", name, err)
			continue
		}
		if !supported {
			r.failf("model", "%s not added for this worker — run `lightchain-worker add-models`", name)
			continue
		}
		r.pass("model", "%s whitelisted, enabled, added", name)
	}
}

func (h *PreflightHandler) checkGateway(ctx context.Context, r *report) {
	if h.Gateway == nil {
		r.skip("gateway", "WORKER_GATEWAY_URL not set")
		return
	}
	if err := h.Gateway.Authenticate(ctx); err != nil {
		r.failf("gateway", "%v", err)
		return
	}
	r.pass("gateway", "ok")
}

func (h *PreflightHandler) checkOllama(ctx context.Context, r *report) {
	if h.OllamaURL == "" {
		r.skip("ollama", "OLLAMA_URL not set")
		return
	}
	resp, err := httpGet(ctx, h.HTTP, strings.TrimRight(h.OllamaURL, "/")+"/api/tags")
	if err != nil {
		r.failf("ollama", "unreachable at %s: %v", h.OllamaURL, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		r.failf("ollama", "%s/api/tags returned %d", h.OllamaURL, resp.StatusCode)
		return
	}
	var tags struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		r.failf("ollama", "bad /api/tags response from %s: %v", h.OllamaURL, err)
		return
	}
	present := make(map[string]bool, len(tags.Models))
	for _, m := range tags.Models {
		present[m.Name] = true
	}
	missing := 0
	for _, name := range h.ModelNames {
		if !present[name] && !present[name+":latest"] {
			r.failf("ollama", "%s not pulled — run: ollama pull %s", name, name)
			missing++
		}
	}
	if missing == 0 {
		r.pass("ollama", "%d model(s) present at %s", len(h.ModelNames), h.OllamaURL)
	}
}

func (h *PreflightHandler) checkBeacon(ctx context.Context, r *report) {
	if h.BeaconURL == "" {
		r.skip("beacon", "BEACON_API_URL not set")
		return
	}
	resp, err := httpGet(ctx, h.HTTP, strings.TrimRight(h.BeaconURL, "/")+"/eth/v1/node/version")
	if err != nil {
		r.failf("beacon", "unreachable at %s: %v", h.BeaconURL, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	var v struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if resp.StatusCode != http.StatusOK {
		r.failf("beacon", "%s returned %d", h.BeaconURL, resp.StatusCode)
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		r.failf("beacon", "%s is not a beacon API (bad /eth/v1/node/version body: %v)", h.BeaconURL, err)
		return
	}
	r.pass("beacon", "%s", v.Data.Version)
}

func httpGet(ctx context.Context, c *http.Client, url string) (*http.Response, error) {
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func networkName(chainID int64) string {
	if n, ok := networkNames[chainID]; ok {
		return n
	}
	return "unknown network"
}

func lcaiToWei(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(n), big.NewInt(1_000_000_000_000_000_000))
}

// lcai renders wei as a short LCAI amount ("5000 LCAI", "60.25 LCAI").
func lcai(wei *big.Int) string {
	f := new(big.Float).SetInt(wei)
	f.Quo(f, weiPerEther)
	s := strings.TrimRight(strings.TrimRight(f.Text('f', 4), "0"), ".")
	return s + " LCAI"
}

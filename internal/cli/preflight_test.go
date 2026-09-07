package cli

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

// fakeChain is a PreflightChain with green defaults; tests override fields.
type fakeChain struct {
	chainID     int64
	head        chain.HeadInfo
	balance     *big.Int
	registered  bool
	suspended   bool
	offenses    int64
	stake       *big.Int
	minStake    *big.Int
	encKey      []byte
	whitelisted map[[32]byte]bool
	enabled     map[[32]byte]bool
	supported   map[[32]byte]bool
	err         error // when set, every call fails with it
	minStakeErr error // when set, only GetMinWorkerStake fails
}

func lcaiWei(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(n), big.NewInt(1_000_000_000_000_000_000))
}

func greenChain(now time.Time, encKey []byte) *fakeChain {
	return &fakeChain{
		chainID:     8200,
		head:        chain.HeadInfo{Number: 2259177, Timestamp: now.Unix() - 2},
		balance:     lcaiWei(60),
		registered:  true,
		stake:       lcaiWei(5000),
		minStake:    lcaiWei(5000),
		encKey:      encKey,
		whitelisted: map[[32]byte]bool{model1: true},
		enabled:     map[[32]byte]bool{model1: true},
		supported:   map[[32]byte]bool{model1: true},
	}
}

func (f *fakeChain) ChainID(context.Context) (*big.Int, error) {
	return big.NewInt(f.chainID), f.err
}
func (f *fakeChain) Head(context.Context) (chain.HeadInfo, error) { return f.head, f.err }
func (f *fakeChain) Balance(context.Context, common.Address) (*big.Int, error) {
	return f.balance, f.err
}

func (f *fakeChain) IsWorkerRegistered(context.Context, common.Address) (bool, error) {
	return f.registered, f.err
}

func (f *fakeChain) IsWorkerSuspended(context.Context, common.Address) (bool, error) {
	return f.suspended, f.err
}

func (f *fakeChain) GetOffenseCount(context.Context, common.Address) (*big.Int, error) {
	return big.NewInt(f.offenses), f.err
}

func (f *fakeChain) GetWorkerStake(context.Context, common.Address) (*big.Int, error) {
	return f.stake, f.err
}

func (f *fakeChain) GetMinWorkerStake(context.Context) (*big.Int, error) {
	if f.minStakeErr != nil {
		return nil, f.minStakeErr
	}
	return f.minStake, f.err
}

func (f *fakeChain) GetWorkerEncryptionKey(context.Context, common.Address) ([]byte, error) {
	return f.encKey, f.err
}

func (f *fakeChain) IsModelWhitelisted(_ context.Context, id [32]byte) (bool, error) {
	return f.whitelisted[id], f.err
}

func (f *fakeChain) IsModelEnabled(_ context.Context, id [32]byte) (bool, error) {
	return f.enabled[id], f.err
}

func (f *fakeChain) WorkerSupportsModel(_ context.Context, _ common.Address, id [32]byte) (bool, error) {
	return f.supported[id], f.err
}

type fakeGateway struct{ err error }

func (g fakeGateway) Authenticate(context.Context) error { return g.err }

// sidecarServer serves the Ollama tags and beacon version endpoints the
// preflight probes, plus the gateway challenge used in read-only mode.
func sidecarServer(t *testing.T, ollamaModels ...string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tags", func(w http.ResponseWriter, _ *http.Request) {
		var names []string
		for _, m := range ollamaModels {
			names = append(names, `{"name":"`+m+`"}`)
		}
		_, _ = w.Write([]byte(`{"models":[` + strings.Join(names, ",") + `]}`))
	})
	mux.HandleFunc("GET /eth/v1/node/version", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"version":"Prysm/test"}}`))
	})
	mux.HandleFunc("GET /api/auth/challenge", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"message":"m","expiresAt":"2030-01-01T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newPreflight(t *testing.T, fc *fakeChain, key *ecdh.PrivateKey, srv *httptest.Server) (*PreflightHandler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	h := &PreflightHandler{
		Chain:      fc,
		ChainID:    8200,
		WorkerAddr: testAddr,
		ModelIDs:   [][32]byte{model1},
		ModelNames: []string{"llama3:8b"},
		Gateway:    fakeGateway{},
		Now:        func() time.Time { return time.Unix(fc.head.Timestamp+2, 0) },
		Out:        &buf,
	}
	if key != nil {
		h.LoadECDHKey = func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil }
	}
	if srv != nil {
		h.OllamaURL = srv.URL
		h.BeaconURL = srv.URL
	}
	return h, &buf
}

func TestPreflight_AllGreen(t *testing.T) {
	t.Parallel()
	key := mustGenerateECDHKey(t)
	fc := greenChain(time.Now(), key.PublicKey().Bytes())
	h, buf := newPreflight(t, fc, key, sidecarServer(t, "llama3:8b"))

	ok := h.Run(context.Background())

	require.True(t, ok, buf.String())
	out := buf.String()
	assert.NotContains(t, out, "[FAIL]")
	assert.NotContains(t, out, "[WARN]")
	for _, want := range []string{"testnet", "chain id 8200", "REGISTERED", "5000 LCAI", "llama3:8b", "Prysm/test"} {
		assert.Contains(t, out, want)
	}
	assert.Contains(t, out, "0 failed")
}

func TestPreflight_NotRegistered_ReportsFundingShortfall(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	fc.stake = big.NewInt(0)
	fc.balance = lcaiWei(100)
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	out := buf.String()
	assert.Contains(t, out, "NOT REGISTERED")
	assert.Contains(t, out, "[FAIL] balance")
	assert.Contains(t, out, "5000 LCAI")
	// Whitelist/enabled state is still worth reporting before registering.
	assert.Contains(t, out, "[PASS] model")
}

func TestPreflight_ChainIDMismatch(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.chainID = 31337
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] rpc")
	assert.Contains(t, buf.String(), "31337")
}

func TestPreflight_RPCUnreachable_SkipsChainChecks(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.err = errors.New("dial tcp: connection refused")
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	out := buf.String()
	assert.Contains(t, out, "[FAIL] rpc")
	assert.Contains(t, out, "connection refused")
	assert.Equal(t, 1, strings.Count(out, "[FAIL]"), "only the rpc check should fail:\n%s", out)
}

func TestPreflight_StakeBelowMinimum(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.stake = lcaiWei(4000)
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] stake")
	assert.Contains(t, buf.String(), "4000 LCAI")
	assert.Contains(t, buf.String(), "topUpStake")
}

func TestPreflight_MinimumDiffersFromPublished_Warns(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.minStake = lcaiWei(4000)
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.True(t, ok)
	assert.Contains(t, buf.String(), "[WARN] stake")
	assert.Contains(t, buf.String(), "published")
}

func TestPreflight_Suspended(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.suspended = true
	fc.offenses = 3
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] suspended")
	assert.Contains(t, buf.String(), "3 offense")
}

func TestPreflight_ModelNotWhitelisted(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.whitelisted = nil
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] model")
	assert.Contains(t, buf.String(), "not whitelisted")
}

func TestPreflight_ModelNotAddedForWorker(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.supported = nil
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] model")
	assert.Contains(t, buf.String(), "add-models")
}

func TestPreflight_ModelMissingFromOllama(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, sidecarServer(t, "gemma4:e2b"))

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] ollama")
	assert.Contains(t, buf.String(), "ollama pull llama3:8b")
}

func TestPreflight_OllamaLatestTagMatchesBareName(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, sidecarServer(t, "llama3:latest"))
	h.ModelNames = []string{"llama3"}

	ok := h.Run(context.Background())

	require.True(t, ok, buf.String())
	assert.Contains(t, buf.String(), "[PASS] ollama")
}

func TestPreflight_ECDHKeyMismatch(t *testing.T) {
	t.Parallel()
	local := mustGenerateECDHKey(t)
	other := mustGenerateECDHKey(t)
	fc := greenChain(time.Now(), other.PublicKey().Bytes())
	h, buf := newPreflight(t, fc, local, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] ecdh")
}

func TestPreflight_GatewayAuthRejected(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.Gateway = fakeGateway{err: errors.New("auth verify failed (status 403): worker not registered on-chain")}

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] gateway")
}

func TestPreflight_StaleHead_Warns(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.Now = func() time.Time { return time.Unix(fc.head.Timestamp+600, 0) }

	ok := h.Run(context.Background())

	require.True(t, ok)
	assert.Contains(t, buf.String(), "[WARN] rpc")
	assert.Contains(t, buf.String(), "10m0s")
}

func TestPreflight_OptionalProbesSkipped(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.Gateway = nil

	ok := h.Run(context.Background())

	require.True(t, ok)
	out := buf.String()
	assert.Contains(t, out, "[SKIP] ecdh")
	assert.Contains(t, out, "[SKIP] gateway")
	assert.Contains(t, out, "[SKIP] ollama")
	assert.Contains(t, out, "[SKIP] beacon")
}

func TestGatewayPing_ChallengeReachable(t *testing.T) {
	t.Parallel()
	srv := sidecarServer(t)

	require.NoError(t, (&GatewayPing{URL: srv.URL}).Authenticate(context.Background()))
	err := (&GatewayPing{URL: srv.URL + "/nope"}).Authenticate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestPreflight_UnregisteredBalanceCoversStakeButNotGas_Warns(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	fc.balance = lcaiWei(5010)
	h, buf := newPreflight(t, fc, nil, nil)

	h.Run(context.Background())

	assert.Contains(t, buf.String(), "[WARN] balance")
	assert.NotContains(t, buf.String(), "[FAIL] balance")
}

func TestPreflight_RegisteredZeroBalance_Fails(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.balance = big.NewInt(0)
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] balance")
}

func TestPreflight_MinimumUnknown_Unregistered_SkipsBalanceVerdict(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	fc.balance = big.NewInt(0)
	fc.minStakeErr = errors.New("execution reverted")
	h, buf := newPreflight(t, fc, nil, nil)

	h.Run(context.Background())

	assert.Contains(t, buf.String(), "[FAIL] stake")
	assert.Contains(t, buf.String(), "[SKIP] balance")
	assert.NotContains(t, buf.String(), "[PASS] balance")
}

func TestPreflight_ECDHMissing_Registered_Fails(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.LoadECDHKey = func(p, _ string) (*ecdh.PrivateKey, error) {
		return nil, fmt.Errorf("read keystore %s: %w", p, fs.ErrNotExist)
	}

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] ecdh")
}

func TestPreflight_ECDHMissing_Unregistered_Warns(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	h, buf := newPreflight(t, fc, nil, nil)
	h.LoadECDHKey = func(p, _ string) (*ecdh.PrivateKey, error) {
		return nil, fmt.Errorf("read keystore %s: %w", p, fs.ErrNotExist)
	}

	h.Run(context.Background())

	assert.Contains(t, buf.String(), "[WARN] ecdh")
	assert.Contains(t, buf.String(), "not generated yet")
}

func TestPreflight_ECDHUnreadable_Unregistered_Fails(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	h, buf := newPreflight(t, fc, nil, nil)
	h.LoadECDHKey = func(_, _ string) (*ecdh.PrivateKey, error) {
		return nil, errors.New("decrypt ECDH private key: cipher: message authentication failed")
	}

	h.Run(context.Background())

	assert.Contains(t, buf.String(), "[FAIL] ecdh")
	assert.NotContains(t, buf.String(), "not generated yet")
}

func TestPreflight_ModelDisabled(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.enabled = nil
	h, buf := newPreflight(t, fc, nil, nil)

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] model")
	assert.Contains(t, buf.String(), "disabled")
}

func TestPreflight_ModelNamesMismatch_Fails(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.ModelNames = nil

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] model")
}

func TestPreflight_UnknownNetwork_NoPublishedMinimum(t *testing.T) {
	t.Parallel()
	fc := greenChain(time.Now(), nil)
	fc.chainID = 48221
	fc.minStake = lcaiWei(32)
	fc.stake = lcaiWei(32)
	h, buf := newPreflight(t, fc, nil, nil)
	h.ChainID = 48221

	ok := h.Run(context.Background())

	require.True(t, ok, buf.String())
	assert.Contains(t, buf.String(), "devnet")
	assert.NotContains(t, buf.String(), "[WARN] stake")
}

func TestPreflight_OllamaErrorStatus(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	t.Cleanup(srv.Close)
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.OllamaURL = srv.URL

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] ollama")
	assert.Contains(t, buf.String(), "500")
	assert.NotContains(t, buf.String(), "ollama pull")
}

func TestPreflight_BeaconMalformedBody(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html>not a beacon</html>`))
	}))
	t.Cleanup(srv.Close)
	fc := greenChain(time.Now(), nil)
	h, buf := newPreflight(t, fc, nil, nil)
	h.BeaconURL = srv.URL

	ok := h.Run(context.Background())

	require.False(t, ok)
	assert.Contains(t, buf.String(), "[FAIL] beacon")
	assert.NotContains(t, buf.String(), "returned 200")
}

package cli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testKeystorePass = "correct horse"

func newInitKeyStep(t *testing.T, answers string) (*InitHandler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return &InitHandler{
		KeystorePath: filepath.Join(t.TempDir(), "keys", "keystore.json"),
		KeystorePass: testKeystorePass,
		In:           bufio.NewReader(strings.NewReader(answers)),
		Out:          &buf,
	}, &buf
}

func TestInitKey_GeneratesWhenMissing(t *testing.T) {
	t.Parallel()
	h, buf := newInitKeyStep(t, "g\n")

	key, err := h.EnsureKey()

	require.NoError(t, err)
	info, err := os.Stat(h.KeystorePath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	data, err := os.ReadFile(h.KeystorePath)
	require.NoError(t, err)
	stored, err := ethkeystore.DecryptKey(data, testKeystorePass)
	require.NoError(t, err)
	assert.Equal(t, crypto.FromECDSA(key), crypto.FromECDSA(stored.PrivateKey))
	assert.Equal(t, crypto.PubkeyToAddress(key.PublicKey), h.WorkerAddr)
	assert.Contains(t, buf.String(), h.WorkerAddr.Hex())
	assert.NotContains(t, buf.String(), hex.EncodeToString(crypto.FromECDSA(key)))
}

func TestInitKey_ReRunLoadsExistingKeyWithoutPrompting(t *testing.T) {
	t.Parallel()
	first, _ := newInitKeyStep(t, "g\n")
	created, err := first.EnsureKey()
	require.NoError(t, err)

	again, buf := newInitKeyStep(t, "") // no answers: a prompt would fail
	again.KeystorePath = first.KeystorePath
	loaded, err := again.EnsureKey()

	require.NoError(t, err)
	assert.Equal(t, crypto.FromECDSA(created), crypto.FromECDSA(loaded))
	assert.Equal(t, first.WorkerAddr, again.WorkerAddr)
	assert.Contains(t, buf.String(), "already")
}

func TestInitKey_WrongPasswordExplained(t *testing.T) {
	t.Parallel()
	first, _ := newInitKeyStep(t, "g\n")
	_, err := first.EnsureKey()
	require.NoError(t, err)

	again, _ := newInitKeyStep(t, "")
	again.KeystorePath = first.KeystorePath
	again.KeystorePass = "wrong"
	_, err = again.EnsureKey()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_KEYSTORE_PASSWORD")
}

func writeHexKeyFile(t *testing.T, contents string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key.hex")
	require.NoError(t, os.WriteFile(p, []byte(contents), 0o600))
	return p
}

func TestInitKey_ImportsFromKeyFileWhenAsked(t *testing.T) {
	t.Parallel()
	want, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyHex := hex.EncodeToString(crypto.FromECDSA(want))
	keyFile := writeHexKeyFile(t, "0x"+keyHex+"\n")
	h, buf := newInitKeyStep(t, "i\n"+keyFile+"\n")

	got, err := h.EnsureKey()

	require.NoError(t, err)
	assert.Equal(t, crypto.PubkeyToAddress(want.PublicKey), h.WorkerAddr)
	assert.Equal(t, crypto.FromECDSA(want), crypto.FromECDSA(got))
	assert.NotContains(t, buf.String(), keyHex)
	info, err := os.Stat(h.KeystorePath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestInitKey_ImportFlagSkipsPrompt(t *testing.T) {
	t.Parallel()
	want, err := crypto.GenerateKey()
	require.NoError(t, err)
	h, _ := newInitKeyStep(t, "")
	h.ImportKeyFile = writeHexKeyFile(t, hex.EncodeToString(crypto.FromECDSA(want)))

	_, err = h.EnsureKey()

	require.NoError(t, err)
	assert.Equal(t, crypto.PubkeyToAddress(want.PublicKey), h.WorkerAddr)
}

func TestInitKey_ImportRejectsFileWithoutHexKey(t *testing.T) {
	t.Parallel()
	h, _ := newInitKeyStep(t, "")
	h.ImportKeyFile = writeHexKeyFile(t, "not a key\n")

	_, err := h.EnsureKey()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "hex private key")
	assert.NotContains(t, err.Error(), "not a key", "file contents must not be echoed")
	_, statErr := os.Stat(h.KeystorePath)
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestInitKey_NoAnswerPointsToFlags(t *testing.T) {
	t.Parallel()
	h, _ := newInitKeyStep(t, "")

	_, err := h.EnsureKey()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--generate")
	_, statErr := os.Stat(h.KeystorePath)
	assert.ErrorIs(t, statErr, os.ErrNotExist)
}

// initChain is the stateful chain the wizard drives: the preflight's
// fakeChain plus the registration writes, which update the state the
// preflight then reads back.
type initChain struct {
	*fakeChain
	registerCalls int
	addCalls      [][32]byte
}

func (c *initChain) RegisterWorker(_ context.Context, encKey []byte, stake *big.Int) error {
	c.registerCalls++
	c.registered = true
	c.stake = stake
	c.encKey = encKey
	c.balance = new(big.Int).Sub(c.balance, stake)
	return nil
}

func (c *initChain) AddSupportedModel(_ context.Context, id [32]byte) error {
	c.addCalls = append(c.addCalls, id)
	if !c.whitelisted[id] {
		return errors.New("execution reverted")
	}
	c.supported[id] = true
	return nil
}

func (c *initChain) DeregisterWorker(context.Context) error {
	c.registered = false
	return nil
}

func (c *initChain) GetCapabilityMask(context.Context, string) (*big.Int, error) {
	return new(big.Int), nil
}

func (c *initChain) GetWorkerCapabilities(context.Context, common.Address) (*big.Int, error) {
	return new(big.Int), nil
}
func (c *initChain) SetCapabilities(context.Context, *big.Int) error { return nil }

// freshWorker is a funded, unregistered worker whose model is whitelisted
// and pulled into Ollama: everything init has to do is still ahead.
func freshWorker(t *testing.T, answers string) (*InitHandler, *initChain, *bytes.Buffer) {
	t.Helper()
	key := mustGenerateECDHKey(t)
	fc := greenChain(time.Now(), nil)
	fc.registered = false
	fc.stake = big.NewInt(0)
	fc.balance = lcaiWei(5100)
	fc.supported = map[[32]byte]bool{}
	ic := &initChain{fakeChain: fc}

	pf, buf := newPreflight(t, fc, key, sidecarServer(t, "llama3:8b"))
	h := &InitHandler{
		Chain:       ic,
		ChainID:     8200,
		WorkerAddr:  testAddr,
		ModelIDs:    [][32]byte{model1},
		ModelNames:  []string{"llama3:8b"},
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Preflight:   pf,
		In:          bufio.NewReader(strings.NewReader(answers)),
		Out:         buf,
		Logger:      testLogger(),
	}
	return h, ic, buf
}

func TestInit_FreshWorker_RegistersAddsModelsAndPassesPreflight(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")

	err := h.Run(context.Background())

	require.NoError(t, err, buf.String())
	assert.Equal(t, 1, ic.registerCalls)
	assert.Equal(t, lcaiWei(5000), ic.stake)
	assert.True(t, ic.supported[model1])
	out := buf.String()
	assert.Contains(t, out, "stake 5000 LCAI")
	assert.Contains(t, out, "[PASS] ollama")
	assert.Contains(t, out, "0 failed")
}

func TestInit_ReRunSkipsFinishedSteps(t *testing.T) {
	t.Parallel()
	first, ic, _ := freshWorker(t, "y\n")
	require.NoError(t, first.Run(context.Background()))

	var buf bytes.Buffer
	again := *first
	again.In = bufio.NewReader(strings.NewReader("")) // a prompt would fail
	again.Out = &buf
	again.Preflight.Out = &buf

	require.NoError(t, again.Run(context.Background()), buf.String())
	assert.Equal(t, 1, ic.registerCalls, "registration must not repeat")
	assert.Len(t, ic.addCalls, 1, "models must not be re-added")
	assert.Contains(t, buf.String(), "already registered")
	assert.Contains(t, buf.String(), "all 1 added")
}

func TestInit_ReRunAddsOnlyNewModels(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "")
	ecdhKey, err := h.LoadECDHKey("", "")
	require.NoError(t, err)
	ic.registered = true
	ic.stake = lcaiWei(5000)
	ic.encKey = ecdhKey.PublicKey().Bytes()
	ic.supported[model1] = true
	ic.whitelisted[model2] = true
	ic.enabled[model2] = true
	h.ModelIDs = [][32]byte{model1, model2}
	h.ModelNames = []string{"llama3:8b", "gemma4:e2b"}
	h.Preflight.ModelIDs, h.Preflight.ModelNames = h.ModelIDs, h.ModelNames
	h.Preflight.OllamaURL = sidecarServer(t, "llama3:8b", "gemma4:e2b").URL

	require.NoError(t, h.Run(context.Background()), buf.String())
	assert.Equal(t, [][32]byte{model2}, ic.addCalls)
	assert.Zero(t, ic.registerCalls)
}

func TestInit_Unattended_YesSkipsConfirmation(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "")
	h.Yes = true

	require.NoError(t, h.Run(context.Background()), buf.String())
	assert.Equal(t, 1, ic.registerCalls)
}

func TestInit_StopsBeforeSpending(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		answers string
		setup   func(*initChain)
		want    []string
	}{
		{
			name:  "unfunded",
			setup: func(c *initChain) { c.balance = lcaiWei(100) },
			want:  []string{"holds 100 LCAI", "send at least 4950 LCAI", testAddr.Hex()},
		},
		{
			name:  "rpc unreachable",
			setup: func(c *initChain) { c.err = errors.New("dial tcp: connection refused") },
			want:  []string{"cannot reach the chain RPC", "RPC_URL"},
		},
		{
			name:  "wrong network",
			setup: func(c *initChain) { c.chainID = 9200 },
			want:  []string{"chain 9200", "CHAIN_ID is 8200"},
		},
		{
			name:  "model not whitelisted",
			setup: func(c *initChain) { c.whitelisted = map[[32]byte]bool{} },
			want:  []string{`"llama3:8b" is not on this network's model list`},
		},
		{
			name:  "model disabled",
			setup: func(c *initChain) { c.enabled = map[[32]byte]bool{} },
			want:  []string{"disabled"},
		},
		{
			name:    "stake declined",
			answers: "n\n",
			want:    []string{"nothing was staked"},
		},
		{
			name: "no answer to the stake prompt",
			want: []string{"nothing was staked", "--yes"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ic, buf := freshWorker(t, tc.answers)
			if tc.setup != nil {
				tc.setup(ic)
			}

			err := h.Run(context.Background())

			require.Error(t, err)
			for _, w := range tc.want {
				assert.Contains(t, err.Error(), w)
			}
			assert.Zero(t, ic.registerCalls)
			assert.Empty(t, ic.addCalls)
			assert.NotContains(t, buf.String(), "preflight:", "preflight runs only once the chain steps are done")
		})
	}
}

func TestInit_PreflightFailureIsReported(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*InitHandler)
		want  string
	}{
		{
			name:  "model missing from ollama",
			setup: func(h *InitHandler) { h.Preflight.OllamaURL = sidecarServer(t).URL },
			want:  "ollama pull llama3:8b",
		},
		{
			name: "gateway rejects the worker",
			setup: func(h *InitHandler) {
				h.Preflight.Gateway = fakeGateway{err: errors.New("auth verify failed (status 403)")}
			},
			want: "[FAIL] gateway",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ic, buf := freshWorker(t, "y\n")
			tc.setup(h)

			err := h.Run(context.Background())

			require.Error(t, err)
			assert.Contains(t, err.Error(), "re-run `lightchain-worker init`")
			assert.Contains(t, buf.String(), tc.want)
			assert.Equal(t, 1, ic.registerCalls, "registration still went through")
		})
	}
}

func TestInit_NoModelsConfigured(t *testing.T) {
	t.Parallel()
	h, ic, _ := freshWorker(t, "y\n")
	h.ModelIDs, h.ModelNames = nil, nil

	err := h.Run(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUPPORTED_MODELS")
	assert.Zero(t, ic.registerCalls)
}

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
	"syscall"
	"testing"
	"time"

	ethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
	"github.com/lightchain/worker/internal/release"
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
	registerErr   error // when set, the registration transaction fails
	addErr        error // when set, every add-model transaction fails

	txBlocks []uint64 // the block of each transaction the worker key has sent
	nonceErr error    // when set, the worker key's nonce cannot be read
}

// mine includes one transaction from the worker key in the next block.
func (c *initChain) mine() {
	c.head.Number++
	c.txBlocks = append(c.txBlocks, c.head.Number)
}

// NonceAt counts the worker key's transactions up to and including block.
func (c *initChain) NonceAt(_ context.Context, _ common.Address, block uint64) (uint64, error) {
	var nonce uint64
	for _, b := range c.txBlocks {
		if b <= block {
			nonce++
		}
	}
	return nonce, errors.Join(c.err, c.nonceErr)
}

func (c *initChain) RegisterWorker(_ context.Context, encKey []byte, stake *big.Int) error {
	c.registerCalls++
	if c.registerErr != nil {
		return c.registerErr
	}
	c.mine()
	c.registered = true
	c.stake = stake
	c.encKey = encKey
	c.balance = new(big.Int).Sub(c.balance, stake)
	return nil
}

func (c *initChain) AddSupportedModel(_ context.Context, id [32]byte) error {
	c.addCalls = append(c.addCalls, id)
	if c.addErr != nil {
		return c.addErr
	}
	if !c.whitelisted[id] {
		return errors.New("execution reverted")
	}
	c.mine()
	c.supported[id] = true
	return nil
}

func (c *initChain) DeregisterWorker(context.Context) error {
	c.mine()
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

func TestInit_RegistrationTxFailureIsReported(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")
	ic.registerErr = errors.New("insufficient funds for gas * price + value")

	err := h.Run(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "registration failed")
	assert.Contains(t, err.Error(), "insufficient funds")
	assert.False(t, ic.registered)
	assert.NotContains(t, buf.String(), "preflight:")
}

func TestInit_AddModelTxFailureIsReported(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "")
	ecdhKey, err := h.LoadECDHKey("", "")
	require.NoError(t, err)
	ic.registered = true
	ic.stake = lcaiWei(5000)
	ic.encKey = ecdhKey.PublicKey().Bytes()
	ic.addErr = errors.New("execution reverted")

	err = h.Run(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "llama3:8b")
	assert.Contains(t, buf.String(), "Added 0 of 1 models")
	assert.NotContains(t, buf.String(), "preflight:")
}

// withReconcileSeed points init at an empty release state file, on a chain
// whose head is block 100.
func withReconcileSeed(t *testing.T, h *InitHandler, ic *initChain) {
	t.Helper()
	ic.head.Number = 100
	h.ReconcileSeed = &ReconcileSeed{
		Chain:       ic,
		ChainID:     8200,
		JobRegistry: common.HexToAddress("0xaaaa000000000000000000000000000000000001"),
		StatePath:   filepath.Join(t.TempDir(), "release_state.json"),
	}
}

// openReleaseStore opens the release state file the way the sidecar and
// `release` do.
func openReleaseStore(t *testing.T, s *ReconcileSeed) *release.FileStore {
	t.Helper()
	store, err := release.NewFileStore(s.StatePath, release.StoreIdentity{
		ChainID:       s.ChainID,
		JobRegistry:   s.JobRegistry,
		WorkerAddress: testAddr,
	}, testLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func reconcileBlock(t *testing.T, s *ReconcileSeed) uint64 {
	t.Helper()
	block, err := openReleaseStore(t, s).GetReconcileBlock(context.Background())
	require.NoError(t, err)
	return block
}

// The first reconcile of a worker registered with a never-used key starts
// right after the safe head read at its registration: at the registration
// block itself with no confirmations, that many blocks earlier otherwise.
func TestInit_UnusedKey_FirstReconcileStartsAtRegistration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		confirmations uint64
		wantScan      [2]uint64
	}{
		{name: "no confirmations: at the registration block", confirmations: 0, wantScan: [2]uint64{101, 150}},
		{name: "five confirmations: five blocks before it", confirmations: 5, wantScan: [2]uint64{96, 145}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ic, buf := freshWorker(t, "y\n")
			withReconcileSeed(t, h, ic)
			h.ReconcileSeed.Confirmations = tc.confirmations

			require.NoError(t, h.Run(context.Background()), buf.String())
			require.Equal(t, uint64(101), ic.txBlocks[0], "the registration is mined in block 101")

			// The worker's first reconcile, as `release` and the sidecar's start run it.
			var scanned [][2]uint64
			cfg := release.DefaultConfig()
			cfg.Confirmations = tc.confirmations
			first := &Handler{
				Settlement: &mockSettlement{
					headFn: func(context.Context) (chain.HeadInfo, error) {
						return chain.HeadInfo{Number: 150}, nil
					},
					filterFn: func(_ context.Context, _ common.Address, lo, hi uint64) ([]chain.JobCompletedEvent, error) {
						scanned = append(scanned, [2]uint64{lo, hi})
						return nil, nil
					},
				},
				WorkerAddr:    testAddr,
				ReleaseStore:  openReleaseStore(t, h.ReconcileSeed),
				ReleaseConfig: cfg,
				Out:           &bytes.Buffer{},
				Logger:        testLogger(),
			}
			require.NoError(t, first.Release(context.Background(), true))

			assert.Equal(t, [][2]uint64{tc.wantScan}, scanned, "one query from the registration on, not a scan from block 1")
		})
	}
}

func TestInit_KeyWithEarlierTransactions_ReconcileBlockNotSeeded(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")
	withReconcileSeed(t, h, ic)
	ic.txBlocks = []uint64{40} // e.g. an earlier registration, since deregistered

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.Equal(t, 1, ic.registerCalls)
	assert.Zero(t, reconcileBlock(t, h.ReconcileSeed), "jobs completed before block 100 must still be found")
}

// The key's nonce is read at the safe head itself: a transaction in that
// block rules the seed out, one in the next block does not.
func TestInit_ReconcileBlock_NonceReadAtTheSafeHead(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		txBlock uint64
		want    uint64
	}{
		{name: "transaction in the safe head block", txBlock: 95, want: 0},
		{name: "transaction in the block after it", txBlock: 96, want: 95},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ic, buf := freshWorker(t, "y\n")
			withReconcileSeed(t, h, ic) // head 100
			h.ReconcileSeed.Confirmations = 5
			ic.txBlocks = []uint64{tc.txBlock}

			require.NoError(t, h.Run(context.Background()), buf.String())

			assert.Equal(t, tc.want, reconcileBlock(t, h.ReconcileSeed))
		})
	}
}

func TestInit_StoredReconcileBlock_LeftAlone(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")
	withReconcileSeed(t, h, ic)
	store := openReleaseStore(t, h.ReconcileSeed)
	require.NoError(t, store.SetReconcileBlock(context.Background(), 7))

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.Equal(t, 1, ic.registerCalls)
	assert.Equal(t, uint64(7), reconcileBlock(t, h.ReconcileSeed))
}

func TestInit_ReRunOnRegisteredWorker_ReconcileBlockNotSeeded(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "")
	withReconcileSeed(t, h, ic)
	ecdhKey, err := h.LoadECDHKey("", "")
	require.NoError(t, err)
	ic.registered = true
	ic.stake = lcaiWei(5000)
	ic.encKey = ecdhKey.PublicKey().Bytes()
	ic.supported[model1] = true

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.Zero(t, ic.registerCalls)
	assert.NoFileExists(t, h.ReconcileSeed.StatePath, "a re-run must not touch the release state")
}

func TestInit_ReleaseStateNotWritable_RegistrationStillSucceeds(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")
	withReconcileSeed(t, h, ic)
	// The state file's directory is a regular file, so it cannot be created.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	h.ReconcileSeed.StatePath = filepath.Join(blocker, "release_state.json")

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.True(t, ic.registered)
	assert.True(t, ic.supported[model1])
	out := buf.String()
	assert.Contains(t, out, "note: the release state")
	assert.Contains(t, out, "registration is not affected")
	assert.Contains(t, out, "init       done")
}

// The command prepares the state only in a directory that is already there
// and its own, so it never leaves files the sidecar's user cannot open.
func TestInit_ReleaseStateDirectoryMissing_NotCreated(t *testing.T) {
	t.Parallel()
	h, ic, buf := freshWorker(t, "y\n")
	withReconcileSeed(t, h, ic)
	dir := filepath.Join(t.TempDir(), "not-created-yet")
	h.ReconcileSeed.StatePath = filepath.Join(dir, "release_state.json")

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.True(t, ic.registered)
	assert.NoDirExists(t, dir)
	assert.Contains(t, buf.String(), "note: the release state was not prepared")
	assert.Contains(t, buf.String(), "no such file or directory")
}

func TestInit_ReleaseStateDirectoryOfAnotherUser_NothingWritten(t *testing.T) {
	t.Parallel()
	const dir = "/tmp" // root's on Linux and macOS, and writable by everyone
	info, err := os.Stat(dir)
	if err != nil {
		t.Skip("needs a directory owned by another user")
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || int(st.Uid) == os.Geteuid() {
		t.Skip("needs a directory owned by another user")
	}
	h, ic, buf := freshWorker(t, "y\n")
	withReconcileSeed(t, h, ic)
	h.ReconcileSeed.StatePath = filepath.Join(dir, "lightchain-init-test-"+filepath.Base(t.TempDir())+".json")
	t.Cleanup(func() {
		_ = os.Remove(h.ReconcileSeed.StatePath)
		_ = os.Remove(h.ReconcileSeed.StatePath + ".lock")
	})

	require.NoError(t, h.Run(context.Background()), buf.String())

	assert.True(t, ic.registered)
	assert.NoFileExists(t, h.ReconcileSeed.StatePath)
	assert.NoFileExists(t, h.ReconcileSeed.StatePath+".lock")
	assert.Contains(t, buf.String(), "note: the release state was not prepared (/tmp belongs to another user)")
}

func TestInit_ReconcileBlockNotSeeded_RegistrationUnaffected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		setup      func(*initChain, *ReconcileSeed)
		registered bool
		wantNote   string
	}{
		{
			name: "registration fails",
			setup: func(c *initChain, _ *ReconcileSeed) {
				c.registerErr = errors.New("insufficient funds for gas * price + value")
			},
			wantNote: "",
		},
		{
			name:       "the key's nonce cannot be read",
			setup:      func(c *initChain, _ *ReconcileSeed) { c.nonceErr = errors.New("missing trie node") },
			registered: true,
			wantNote:   "note: the release state was not prepared (missing trie node)",
		},
		{
			name:       "JOB_REGISTRY_ADDRESS is not set",
			setup:      func(_ *initChain, s *ReconcileSeed) { s.JobRegistry = common.Address{} },
			registered: true,
			wantNote:   "note: the release state was not prepared (JOB_REGISTRY_ADDRESS is not set)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, ic, buf := freshWorker(t, "y\n")
			withReconcileSeed(t, h, ic)
			tc.setup(ic, h.ReconcileSeed)

			err := h.Run(context.Background())

			assert.Equal(t, tc.registered, err == nil, buf.String())
			assert.Equal(t, tc.registered, ic.registered)
			assert.NoFileExists(t, h.ReconcileSeed.StatePath)
			if tc.wantNote == "" {
				assert.NotContains(t, buf.String(), "note:")
			} else {
				assert.Contains(t, buf.String(), tc.wantNote)
			}
		})
	}
}

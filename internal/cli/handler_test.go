package cli

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgcrypto "github.com/lightchain/pkg/crypto"
)

// mockRegClient is a hand-rolled mock for chain.RegistrationClient.
// Function-var fields allow each test to inject specific behaviour.
type mockRegClient struct {
	isWorkerRegisteredFn     func(ctx context.Context, worker common.Address) (bool, error)
	registerWorkerFn         func(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error
	addSupportedModelFn      func(ctx context.Context, modelID [32]byte) error
	deregisterWorkerFn       func(ctx context.Context) error
	getMinWorkerStakeFn      func(ctx context.Context) (*big.Int, error)
	getWorkerEncryptionKeyFn func(ctx context.Context, worker common.Address) ([]byte, error)
	getCapabilityMaskFn      func(ctx context.Context, name string) (*big.Int, error)
	getWorkerCapabilitiesFn  func(ctx context.Context, worker common.Address) (*big.Int, error)
	setCapabilitiesFn        func(ctx context.Context, mask *big.Int) error
}

func (m *mockRegClient) GetCapabilityMask(ctx context.Context, name string) (*big.Int, error) {
	return m.getCapabilityMaskFn(ctx, name)
}

func (m *mockRegClient) GetWorkerCapabilities(ctx context.Context, worker common.Address) (*big.Int, error) {
	return m.getWorkerCapabilitiesFn(ctx, worker)
}

func (m *mockRegClient) SetCapabilities(ctx context.Context, mask *big.Int) error {
	return m.setCapabilitiesFn(ctx, mask)
}

func (m *mockRegClient) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	return m.isWorkerRegisteredFn(ctx, worker)
}
func (m *mockRegClient) RegisterWorker(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error {
	return m.registerWorkerFn(ctx, encryptionPubKey, stake)
}
func (m *mockRegClient) AddSupportedModel(ctx context.Context, modelID [32]byte) error {
	return m.addSupportedModelFn(ctx, modelID)
}
func (m *mockRegClient) DeregisterWorker(ctx context.Context) error {
	return m.deregisterWorkerFn(ctx)
}
func (m *mockRegClient) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	return m.getMinWorkerStakeFn(ctx)
}
func (m *mockRegClient) GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error) {
	return m.getWorkerEncryptionKeyFn(ctx, worker)
}

var (
	testAddr = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	model1   = [32]byte{0x01}
	model2   = [32]byte{0x02}
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// mustGenerateECDHKey generates a real P-256 ECDH key for use in test stubs.
func mustGenerateECDHKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	key, err := pkgcrypto.GenerateKeyPair()
	require.NoError(t, err)
	return key
}

// --- Keygen tests ---

func TestKeygen_Success(t *testing.T) {
	t.Parallel()

	key := mustGenerateECDHKey(t)
	var buf bytes.Buffer

	h := &Handler{
		ECDHKeyPath: "/tmp/test.key",
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Out:         &buf,
		Logger:      testLogger(),
	}

	err := h.Keygen()
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "ECDH Public Key: 0x")
	assert.Contains(t, buf.String(), "/tmp/test.key")
}

func TestKeygen_LoadError(t *testing.T) {
	t.Parallel()

	h := &Handler{
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) {
			return nil, errors.New("disk error")
		},
		Out:    &bytes.Buffer{},
		Logger: testLogger(),
	}

	err := h.Keygen()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load/generate ECDH key")
}

// --- Register tests ---

func TestRegister_Success(t *testing.T) {
	t.Parallel()

	key := mustGenerateECDHKey(t)
	var buf bytes.Buffer

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error {
			return nil
		},
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error { return nil },
		getMinWorkerStakeFn: func(_ context.Context) (*big.Int, error) {
			return big.NewInt(1e18), nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		ModelIDs:   [][32]byte{model1},
		ModelNames: []string{"llama3-8b"},
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.Register(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), testAddr.Hex())
	assert.Contains(t, buf.String(), "min (auto-queried)")
}

func TestRegister_ExplicitStake(t *testing.T) {
	t.Parallel()

	key := mustGenerateECDHKey(t)
	var buf bytes.Buffer

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn:    func(_ context.Context, _ []byte, _ *big.Int) error { return nil },
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error { return nil },
	}

	stake := big.NewInt(5e18)
	h := &Handler{
		Client:      mock,
		WorkerAddr:  testAddr,
		ModelIDs:    [][32]byte{model1},
		ModelNames:  []string{"llama3-8b"},
		WorkerStake: stake,
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Out:         &buf,
		Logger:      testLogger(),
	}

	err := h.Register(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), stake.String())
	assert.Contains(t, buf.String(), "wei")
}

func TestRegister_NoModels(t *testing.T) {
	t.Parallel()

	h := &Handler{
		ModelIDs: nil,
		Out:      &bytes.Buffer{},
		Logger:   testLogger(),
	}

	err := h.Register(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUPPORTED_MODELS")
}

func TestRegister_ECDHKeyError(t *testing.T) {
	t.Parallel()

	h := &Handler{
		ModelIDs: [][32]byte{model1},
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) {
			return nil, errors.New("keystore corrupt")
		},
		Out:    &bytes.Buffer{},
		Logger: testLogger(),
	}

	err := h.Register(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load/generate ECDH key")
}

func TestRegister_ChainError(t *testing.T) {
	t.Parallel()

	key := mustGenerateECDHKey(t)
	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, errors.New("rpc timeout")
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		ModelIDs:   [][32]byte{model1},
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Out:        &bytes.Buffer{},
		Logger:     testLogger(),
	}

	err := h.Register(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "registration")
}

// --- AddModels tests ---

func TestAddModels_Success(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	addCount := 0

	mock := &mockRegClient{
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error {
			addCount++
			return nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		ModelIDs:   [][32]byte{model1, model2},
		ModelNames: []string{"llama3-8b", "mistral-7b"},
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.AddModels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, addCount)
	assert.Contains(t, buf.String(), "Added 2 models")
}

func TestAddModels_NoModels(t *testing.T) {
	t.Parallel()

	h := &Handler{
		ModelIDs: nil,
		Out:      &bytes.Buffer{},
		Logger:   testLogger(),
	}

	err := h.AddModels(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SUPPORTED_MODELS")
}

func TestAddModels_PartialFailure(t *testing.T) {
	t.Parallel()

	addCount := 0
	mock := &mockRegClient{
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error {
			addCount++
			if addCount == 2 {
				return errors.New("tx reverted")
			}
			return nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		ModelIDs:   [][32]byte{model1, model2},
		ModelNames: []string{"llama3-8b", "mistral-7b"},
		Out:        &bytes.Buffer{},
		Logger:     testLogger(),
	}

	err := h.AddModels(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mistral-7b")
	assert.Contains(t, err.Error(), "index 1")
}

// --- Deregister tests ---

func TestDeregister_Success(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	mock := &mockRegClient{
		deregisterWorkerFn: func(_ context.Context) error { return nil },
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.Deregister(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "deregistered, stake withdrawn")
}

func TestDeregister_Error(t *testing.T) {
	t.Parallel()

	mock := &mockRegClient{
		deregisterWorkerFn: func(_ context.Context) error {
			return errors.New("tx reverted")
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		Out:        &bytes.Buffer{},
		Logger:     testLogger(),
	}

	err := h.Deregister(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deregistration")
}

// --- Status tests ---

func TestStatus_Registered_KeyMatch(t *testing.T) {
	t.Parallel()

	key := mustGenerateECDHKey(t)
	pubBytes := key.PublicKey().Bytes()
	var buf bytes.Buffer

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return pubBytes, nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return key, nil },
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.Status(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "REGISTERED")
	assert.Contains(t, buf.String(), "Keys match:        yes")
}

func TestStatus_Registered_KeyMismatch(t *testing.T) {
	t.Parallel()

	localKey := mustGenerateECDHKey(t)
	otherKey := mustGenerateECDHKey(t)
	var buf bytes.Buffer

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return otherKey.PublicKey().Bytes(), nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) { return localKey, nil },
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.Status(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "REGISTERED")
	assert.Contains(t, buf.String(), "Keys match:        no")
}

func TestStatus_NotRegistered(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		Out:        &buf,
		Logger:     testLogger(),
	}

	err := h.Status(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "NOT REGISTERED")
}

func TestStatus_RPCError(t *testing.T) {
	t.Parallel()

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, errors.New("rpc down")
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		Out:        &bytes.Buffer{},
		Logger:     testLogger(),
	}

	err := h.Status(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check registration status")
}

func TestStatus_ECDHKeyNotFound(t *testing.T) {
	t.Parallel()

	otherKey := mustGenerateECDHKey(t)
	var buf bytes.Buffer

	mock := &mockRegClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return otherKey.PublicKey().Bytes(), nil
		},
	}

	h := &Handler{
		Client:     mock,
		WorkerAddr: testAddr,
		LoadECDHKey: func(_, _ string) (*ecdh.PrivateKey, error) {
			return nil, errors.New("file not found")
		},
		Out:    &buf,
		Logger: testLogger(),
	}

	err := h.Status(context.Background())
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "not found")
	assert.Contains(t, buf.String(), "unknown")
}

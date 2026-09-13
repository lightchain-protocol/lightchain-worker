package registration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient is a hand-rolled in-process mock for chain.RegistrationClient.
// Function-var fields allow each test to inject specific behaviour.
type mockClient struct {
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

func (m *mockClient) GetCapabilityMask(ctx context.Context, name string) (*big.Int, error) {
	return m.getCapabilityMaskFn(ctx, name)
}

func (m *mockClient) GetWorkerCapabilities(ctx context.Context, worker common.Address) (*big.Int, error) {
	return m.getWorkerCapabilitiesFn(ctx, worker)
}

func (m *mockClient) SetCapabilities(ctx context.Context, mask *big.Int) error {
	return m.setCapabilitiesFn(ctx, mask)
}

func (m *mockClient) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	return m.isWorkerRegisteredFn(ctx, worker)
}

func (m *mockClient) RegisterWorker(ctx context.Context, encryptionPubKey []byte, stake *big.Int) error {
	return m.registerWorkerFn(ctx, encryptionPubKey, stake)
}

func (m *mockClient) AddSupportedModel(ctx context.Context, modelID [32]byte) error {
	return m.addSupportedModelFn(ctx, modelID)
}

func (m *mockClient) DeregisterWorker(ctx context.Context) error {
	return m.deregisterWorkerFn(ctx)
}

func (m *mockClient) GetMinWorkerStake(ctx context.Context) (*big.Int, error) {
	return m.getMinWorkerStakeFn(ctx)
}

func (m *mockClient) GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error) {
	return m.getWorkerEncryptionKeyFn(ctx, worker)
}

var (
	testAddr = common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	// testPubKey is a synthetic 65-byte uncompressed P-256 pubkey — the first
	// byte is the required 0x04 prefix and the remaining 64 bytes are a
	// deterministic fill. WorkerRegistry.registerWorker only validates length
	// and prefix, not that the key lies on the P-256 curve,
	// so a fill value is sufficient for unit tests that don't exercise crypto.
	testPubKey = buildTestPubKey()
	model1     = [32]byte{0x01}
	model2     = [32]byte{0x02}
	minStake   = big.NewInt(1_000_000_000_000_000_000) // 1 ETH in wei
)

func buildTestPubKey() []byte {
	key := make([]byte, P256UncompressedPubKeyLength)
	key[0] = P256UncompressedPrefix
	for i := 1; i < P256UncompressedPubKeyLength; i++ {
		key[i] = byte(i)
	}
	return key
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestEnsureRegistered_AlreadyRegistered(t *testing.T) {
	t.Parallel()

	registerCalled := false
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return testPubKey, nil // matching key — no mismatch
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error {
			registerCalled = true
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.NoError(t, err)
	assert.False(t, registerCalled, "RegisterWorker must not be called when already registered")
}

func TestEnsureRegistered_AlreadyRegistered_KeyMismatch(t *testing.T) {
	t.Parallel()

	differentKey := []byte{0xFF, 0xFE, 0xFD} // differs from testPubKey
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return differentKey, nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestEnsureRegistered_AlreadyRegistered_KeyFetchError(t *testing.T) {
	t.Parallel()

	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return true, nil
		},
		getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
			return nil, errors.New("rpc timeout")
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch on-chain encryption key")
}

func TestEnsureRegistered_NotRegistered_StakeProvided(t *testing.T) {
	t.Parallel()

	var capturedStake *big.Int
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, stake *big.Int) error {
			capturedStake = new(big.Int).Set(stake)
			return nil
		},
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error { return nil },
	}

	provided := big.NewInt(5_000_000_000_000_000_000)
	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, provided)
	require.NoError(t, err)
	require.NotNil(t, capturedStake)
	assert.Equal(t, provided.String(), capturedStake.String(), "must use the provided stake")
}

func TestEnsureRegistered_NotRegistered_StakeNil_QueriesMin(t *testing.T) {
	t.Parallel()

	getMinCalled := false
	var capturedStake *big.Int
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		getMinWorkerStakeFn: func(_ context.Context) (*big.Int, error) {
			getMinCalled = true
			return new(big.Int).Set(minStake), nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, stake *big.Int) error {
			capturedStake = new(big.Int).Set(stake)
			return nil
		},
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error { return nil },
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, nil)
	require.NoError(t, err)
	assert.True(t, getMinCalled, "GetMinWorkerStake must be called when stake is nil")
	require.NotNil(t, capturedStake)
	assert.Equal(t, minStake.String(), capturedStake.String())
}

func TestEnsureRegistered_TwoModels_BothAdded(t *testing.T) {
	t.Parallel()

	var addedModels [][32]byte
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error { return nil },
		addSupportedModelFn: func(_ context.Context, modelID [32]byte) error {
			addedModels = append(addedModels, modelID)
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, [][32]byte{model1, model2}, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.NoError(t, err)
	require.Len(t, addedModels, 2)
	assert.Equal(t, model1, addedModels[0])
	assert.Equal(t, model2, addedModels[1])
}

func TestEnsureRegistered_RegisterWorkerError_AddModelNotCalled(t *testing.T) {
	t.Parallel()

	addCalled := false
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error {
			return errors.New("transaction failed")
		},
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error {
			addCalled = true
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, [][32]byte{model1}, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "register worker")
	assert.False(t, addCalled, "AddSupportedModel must not be called after RegisterWorker fails")
}

func TestEnsureRegistered_AddModelFails_RollsBack(t *testing.T) {
	t.Parallel()

	addCount := 0
	deregisterCalled := false
	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error { return nil },
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error {
			addCount++
			if addCount == 2 {
				return errors.New("second model failed")
			}
			return nil
		},
		deregisterWorkerFn: func(_ context.Context) error {
			deregisterCalled = true
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, [][32]byte{model1, model2}, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "add supported model at index 1")
	assert.Equal(t, 2, addCount, "first model should have been added before second failed")
	assert.True(t, deregisterCalled, "DeregisterWorker must be called to roll back")
}

func TestEnsureRegistered_AddModelFails_RollbackFails(t *testing.T) {
	t.Parallel()

	mock := &mockClient{
		isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
			return false, nil
		},
		registerWorkerFn: func(_ context.Context, _ []byte, _ *big.Int) error { return nil },
		addSupportedModelFn: func(_ context.Context, _ [32]byte) error {
			return errors.New("model add failed")
		},
		deregisterWorkerFn: func(_ context.Context) error {
			return errors.New("rollback tx reverted")
		},
	}

	mgr := NewManager(mock, testAddr, [][32]byte{model1}, newLogger())
	err := mgr.EnsureRegistered(context.Background(), testPubKey, minStake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "add supported model at index 0")
	assert.Contains(t, err.Error(), "rollback deregister")
}

func TestDeregister_CallsOnce(t *testing.T) {
	t.Parallel()

	deregisterCount := 0
	mock := &mockClient{
		deregisterWorkerFn: func(_ context.Context) error {
			deregisterCount++
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.Deregister(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, deregisterCount, "DeregisterWorker must be called exactly once")
}

func TestDeregister_Error_Propagated(t *testing.T) {
	t.Parallel()

	mock := &mockClient{
		deregisterWorkerFn: func(_ context.Context) error {
			return errors.New("tx reverted")
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	err := mgr.Deregister(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deregister worker")
}

// Post-audit guardrail: the manager must reject keys that
// don't meet the 65-byte / 0x04-prefix format BEFORE making any chain calls,
// so a misconfigured worker fails fast instead of burning a registerWorker tx
// on a contract revert.
func TestEnsureRegistered_InvalidPubKey_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         []byte
		errContains string
	}{
		{
			name:        "too short",
			key:         []byte{0x04, 0x01, 0x02},
			errContains: "65 bytes",
		},
		{
			name: "wrong prefix",
			key: func() []byte {
				k := make([]byte, P256UncompressedPubKeyLength)
				k[0] = 0x02 // compressed prefix, not uncompressed
				return k
			}(),
			errContains: "0x04 prefix",
		},
		{
			name:        "empty",
			key:         []byte{},
			errContains: "65 bytes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chainCalled := false
			mock := &mockClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					chainCalled = true
					return false, nil
				},
			}

			mgr := NewManager(mock, testAddr, nil, newLogger())
			err := mgr.EnsureRegistered(context.Background(), tc.key, minStake)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid encryption pubkey")
			assert.Contains(t, err.Error(), tc.errContains)
			assert.False(t, chainCalled, "no chain calls must be made when pubkey validation fails")
		})
	}
}

// ──────────────────────────────────────────────
// EnsureCapabilities
// ──────────────────────────────────────────────

func TestEnsureCapabilities_DeclaresWhenMissing(t *testing.T) {
	var setMask *big.Int
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, name string) (*big.Int, error) {
			require.Equal(t, "search", name)
			return big.NewInt(1), nil
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(0), nil
		},
		setCapabilitiesFn: func(_ context.Context, mask *big.Int) error {
			setCalls++
			setMask = mask
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), []string{"search"})
	require.Equal(t, 1, setCalls, "SetCapabilities must be called exactly once")
	assert.Zero(t, setMask.Cmp(big.NewInt(1)), "declared mask must be the search bit")
}

func TestEnsureCapabilities_IdempotentWhenAlreadyDeclared(t *testing.T) {
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, _ string) (*big.Int, error) {
			return big.NewInt(1), nil
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(1), nil // already declared
		},
		setCapabilitiesFn: func(_ context.Context, _ *big.Int) error {
			setCalls++
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), []string{"search"})
	assert.Zero(t, setCalls, "SetCapabilities must not be called when the mask already covers the wanted bits")
}

func TestEnsureCapabilities_SkipsUnregisteredCapability(t *testing.T) {
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, _ string) (*big.Int, error) {
			return big.NewInt(0), nil // capability not registered on-chain
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(0), nil
		},
		setCapabilitiesFn: func(_ context.Context, _ *big.Int) error {
			setCalls++
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), []string{"search"})
	assert.Zero(t, setCalls, "SetCapabilities must not be called for an unregistered capability")
}

func TestEnsureCapabilities_ClearsMaskWhenCapabilityDropped(t *testing.T) {
	var setMask *big.Int
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, _ string) (*big.Int, error) {
			t.Fatal("no mask lookups expected for an empty capability list")
			return nil, nil
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(1), nil // declared in a previous run
		},
		setCapabilitiesFn: func(_ context.Context, mask *big.Int) error {
			setCalls++
			setMask = mask
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), nil)
	require.Equal(t, 1, setCalls, "a dropped capability must be cleared on-chain")
	assert.Zero(t, setMask.Sign(), "the stale declaration must be overwritten with an empty mask")
}

func TestEnsureCapabilities_PartialResolutionNeverClears(t *testing.T) {
	var setMask *big.Int
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, name string) (*big.Int, error) {
			if name == "vision" {
				return nil, errors.New("rpc flake")
			}
			return big.NewInt(1), nil // "search" resolves to bit 0
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(2), nil // the vision bit is already declared
		},
		setCapabilitiesFn: func(_ context.Context, mask *big.Int) error {
			setCalls++
			setMask = mask
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), []string{"search", "vision"})
	require.Equal(t, 1, setCalls)
	assert.Zero(t, setMask.Cmp(big.NewInt(3)),
		"a failed lookup must degrade to add-only: keep the unresolved bit, add the resolved one")
}

func TestEnsureCapabilities_ReadFailureIsNonFatal(t *testing.T) {
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, _ string) (*big.Int, error) {
			return big.NewInt(1), nil
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return nil, errors.New("rpc down")
		},
		setCapabilitiesFn: func(_ context.Context, _ *big.Int) error {
			setCalls++
			return nil
		},
	}

	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), []string{"search"})
	assert.Zero(t, setCalls, "SetCapabilities must not be called when the current mask cannot be read")
}

func TestEnsureCapabilities_NoNamesNoDeclaration_NoOp(t *testing.T) {
	setCalls := 0
	mock := &mockClient{
		getCapabilityMaskFn: func(_ context.Context, _ string) (*big.Int, error) {
			t.Fatal("no mask lookups expected for an empty capability list")
			return nil, nil
		},
		getWorkerCapabilitiesFn: func(_ context.Context, _ common.Address) (*big.Int, error) {
			return big.NewInt(0), nil // nothing declared, nothing to clear
		},
		setCapabilitiesFn: func(_ context.Context, _ *big.Int) error {
			setCalls++
			return nil
		},
	}
	mgr := NewManager(mock, testAddr, nil, newLogger())
	mgr.EnsureCapabilities(context.Background(), nil)
	assert.Zero(t, setCalls, "no transaction when the mask is already empty")
}

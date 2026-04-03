package registration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockValidationClient is a hand-rolled mock for chain.ValidationClient.
// Only 2 function vars — matching the narrow read-only interface.
type mockValidationClient struct {
	isWorkerRegisteredFn     func(ctx context.Context, worker common.Address) (bool, error)
	getWorkerEncryptionKeyFn func(ctx context.Context, worker common.Address) ([]byte, error)
}

func (m *mockValidationClient) IsWorkerRegistered(ctx context.Context, worker common.Address) (bool, error) {
	return m.isWorkerRegisteredFn(ctx, worker)
}

func (m *mockValidationClient) GetWorkerEncryptionKey(ctx context.Context, worker common.Address) ([]byte, error) {
	return m.getWorkerEncryptionKeyFn(ctx, worker)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestValidate(t *testing.T) {
	t.Parallel()

	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	localKey := []byte{0x04, 0x01, 0x02, 0x03}

	tests := []struct {
		name       string
		mock       *mockValidationClient
		wantErr    bool
		errContain string
	}{
		{
			name: "registered with matching key",
			mock: &mockValidationClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					return true, nil
				},
				getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
					return []byte{0x04, 0x01, 0x02, 0x03}, nil
				},
			},
			wantErr: false,
		},
		{
			name: "not registered",
			mock: &mockValidationClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					return false, nil
				},
			},
			wantErr:    true,
			errContain: "not registered",
		},
		{
			name: "registered but key mismatch",
			mock: &mockValidationClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					return true, nil
				},
				getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
					return []byte{0xFF, 0xFE, 0xFD}, nil
				},
			},
			wantErr:    true,
			errContain: "does not match",
		},
		{
			name: "RPC error on IsWorkerRegistered",
			mock: &mockValidationClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					return false, errors.New("rpc timeout")
				},
			},
			wantErr:    true,
			errContain: "check registration status",
		},
		{
			name: "RPC error on GetWorkerEncryptionKey",
			mock: &mockValidationClient{
				isWorkerRegisteredFn: func(_ context.Context, _ common.Address) (bool, error) {
					return true, nil
				},
				getWorkerEncryptionKeyFn: func(_ context.Context, _ common.Address) ([]byte, error) {
					return nil, errors.New("rpc connection refused")
				},
			},
			wantErr:    true,
			errContain: "fetch on-chain encryption key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := NewValidator(tt.mock, addr, testLogger())
			err := v.Validate(context.Background(), localKey)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
				return
			}
			require.NoError(t, err)
		})
	}
}

package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reinstateChain is the preflight's fakeChain plus the reinstate transaction.
type reinstateChain struct {
	*fakeChain
	reinstateCalls int
	reinstateErr   error // when set, the reinstate transaction fails
}

func (c *reinstateChain) Reinstate(context.Context) error {
	c.reinstateCalls++
	return c.reinstateErr
}

// suspendedWorker is a registered, suspended worker whose stake is at the
// minimum: nothing stands between it and reinstating.
func suspendedWorker() (*Handler, *reinstateChain, *bytes.Buffer) {
	fc := greenChain(time.Now(), nil)
	fc.suspended = true
	rc := &reinstateChain{fakeChain: fc}
	var buf bytes.Buffer
	return &Handler{Reinstatement: rc, WorkerAddr: testAddr, Out: &buf}, rc, &buf
}

func TestReinstate_SuspendedWorkerIsReinstated(t *testing.T) {
	t.Parallel()
	h, rc, buf := suspendedWorker()

	err := h.Reinstate(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, rc.reinstateCalls)
	assert.Contains(t, buf.String(), testAddr.Hex())
	assert.Contains(t, buf.String(), "reinstated")
}

func TestReinstate_RefusesWithoutSending(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*reinstateChain)
		want  []string
	}{
		{
			name:  "not registered",
			setup: func(c *reinstateChain) { c.registered, c.suspended = false, false },
			want:  []string{"is not registered", testAddr.Hex()},
		},
		{
			name:  "not suspended",
			setup: func(c *reinstateChain) { c.suspended = false },
			want:  []string{"is not suspended", testAddr.Hex()},
		},
		{
			name:  "stake below the minimum",
			setup: func(c *reinstateChain) { c.stake = lcaiWei(4250) },
			want:  []string{"4250 LCAI", "minimum 5000 LCAI", "750 LCAI"},
		},
		{
			name:  "minimum stake unreadable",
			setup: func(c *reinstateChain) { c.minStakeErr = errors.New("GetMinWorkerStake: timeout") },
			want:  []string{"minimum stake", "timeout"},
		},
		{
			name:  "rpc unreachable",
			setup: func(c *reinstateChain) { c.err = errors.New("dial tcp: connection refused") },
			want:  []string{"connection refused"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, rc, buf := suspendedWorker()
			tc.setup(rc)

			err := h.Reinstate(context.Background())

			require.Error(t, err)
			for _, w := range tc.want {
				assert.Contains(t, err.Error(), w)
			}
			assert.Zero(t, rc.reinstateCalls, "no transaction may be sent")
			assert.NotContains(t, buf.String(), "reinstated")
		})
	}
}

func TestReinstate_TxFailureIsReported(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		txErr        string // as the chain client words it
		wantCooldown bool   // the error names the cooldown as the likely cause
	}{
		{
			name:         "reverted, as it is while the cooldown runs",
			txErr:        "Reinstate transaction: execution reverted",
			wantCooldown: true,
		},
		{
			name:  "not a revert",
			txErr: "send Reinstate tx: insufficient funds for gas * price + value",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, rc, buf := suspendedWorker()
			rc.reinstateErr = errors.New(tc.txErr)

			err := h.Reinstate(context.Background())

			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), tc.txErr), "the chain's error leads, said once: %s", err)
			assert.Equal(t, tc.wantCooldown, strings.Contains(err.Error(), "cooldown"), err.Error())
			assert.NotContains(t, buf.String(), "reinstated")
		})
	}
}

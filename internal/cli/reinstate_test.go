package cli

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reinstateChain is the preflight's fakeChain plus the reinstate
// transaction, which lifts the suspension the reads then report.
type reinstateChain struct {
	*fakeChain
	reinstateCalls int
	reinstateErr   error // when set, the reinstate transaction fails
}

func (c *reinstateChain) Reinstate(context.Context) error {
	c.reinstateCalls++
	if c.reinstateErr != nil {
		return c.reinstateErr
	}
	c.suspended = false
	return nil
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
	assert.False(t, rc.suspended)
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
	h, rc, buf := suspendedWorker()
	// What the RPC returns while the suspension cooldown is still running.
	rc.reinstateErr = errors.New("execution reverted")

	err := h.Reinstate(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "execution reverted")
	assert.Contains(t, err.Error(), "cooldown", "the usual cause must be named")
	assert.True(t, rc.suspended)
	assert.NotContains(t, buf.String(), "reinstated")
}

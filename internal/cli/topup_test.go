package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLCAI(t *testing.T) {
	t.Parallel()
	wei := func(s string) *big.Int {
		v, ok := new(big.Int).SetString(s, 10)
		require.True(t, ok)
		return v
	}
	valid := map[string]*big.Int{
		"750":                  wei("750000000000000000000"),
		"60.25":                wei("60250000000000000000"),
		"0.000000000000000001": wei("1"),
		"010":                  wei("10000000000000000000"),
		".5":                   wei("500000000000000000"),
		"5.":                   wei("5000000000000000000"),
	}
	for in, want := range valid {
		got, err := ParseLCAI(in)
		require.NoError(t, err, in)
		assert.Zero(t, want.Cmp(got), "%s parsed as %s wei", in, got)
	}
	for _, in := range []string{"", "0", "0.00", "-5", "+5", "abc", "1e3", "0x10", "1/2", "1.2.3", ".", "5 LCAI", "0.0000000000000000001"} {
		_, err := ParseLCAI(in)
		assert.Error(t, err, "%q must be refused", in)
	}
}

// slashedWorker is a registered worker whose stake a slash left 750 LCAI under
// the minimum, holding enough to top it up. Its handler runs unattended (--yes).
func slashedWorker() (*Handler, *reinstateChain, *bytes.Buffer) {
	fc := greenChain(time.Now(), nil)
	fc.stake = lcaiWei(4250)
	fc.balance = lcaiWei(1000)
	rc := &reinstateChain{fakeChain: fc}
	var buf bytes.Buffer
	return &Handler{Reinstatement: rc, WorkerAddr: testAddr, Yes: true, Out: &buf}, rc, &buf
}

func TestTopUpStake_SendsTheAmountAndPrintsTheNewStake(t *testing.T) {
	t.Parallel()
	h, rc, buf := slashedWorker()

	err := h.TopUpStake(context.Background(), lcaiWei(750))

	require.NoError(t, err)
	require.Len(t, rc.topUps, 1)
	assert.Zero(t, lcaiWei(750).Cmp(rc.topUps[0]), "sent %s wei", rc.topUps[0])
	out := buf.String()
	assert.Contains(t, out, testAddr.Hex())
	assert.Contains(t, out, "topped up by 750 LCAI")
	assert.Contains(t, out, "now 5000 LCAI")
	assert.Contains(t, out, "minimum 5000 LCAI")
	assert.NotContains(t, out, "reinstate", "a worker that is not suspended has nothing to reinstate")
	assert.NotContains(t, out, "gas", "250 LCAI is left, well over the gas buffer")
}

func TestTopUpStake_NotesABalanceLeftUnderTheGasBuffer(t *testing.T) {
	t.Parallel()
	h, rc, buf := slashedWorker()
	rc.balance = lcaiWei(760)
	h.Yes, h.In = false, bufio.NewReader(strings.NewReader("y\n"))

	err := h.TopUpStake(context.Background(), lcaiWei(750))

	require.NoError(t, err)
	assert.Len(t, rc.topUps, 1)
	note, question, _ := strings.Cut(buf.String(), "[y/N]")
	assert.Contains(t, note, "this top-up leaves under 50 LCAI for gas", "the operator reads it before answering")
	assert.Contains(t, question, "topped up by 750 LCAI")
}

func TestTopUpStake_RefusesWithoutSending(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(*reinstateChain)
		want  []string
	}{
		{
			name:  "not registered",
			setup: func(c *reinstateChain) { c.registered = false },
			want:  []string{"is not registered", testAddr.Hex(), "`lightchain-worker register`"},
		},
		{
			name:  "balance below the amount",
			setup: func(c *reinstateChain) { c.balance = lcaiWei(700) },
			want:  []string{"holds 700 LCAI", "750 LCAI", "send at least 100 LCAI", testAddr.Hex()},
		},
		{
			name:  "balance covers the amount but no gas",
			setup: func(c *reinstateChain) { c.balance = lcaiWei(750) },
			want:  []string{"holds 750 LCAI", "gas"},
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
			h, rc, buf := slashedWorker()
			h.Yes, h.In = false, bufio.NewReader(strings.NewReader("y\n"))
			tc.setup(rc)

			err := h.TopUpStake(context.Background(), lcaiWei(750))

			require.Error(t, err)
			for _, w := range tc.want {
				assert.Contains(t, err.Error(), w)
			}
			assert.Empty(t, rc.topUps, "no transaction may be sent")
			assert.NotContains(t, buf.String(), "[y/N]", "the question comes only after every check")
			assert.NotContains(t, buf.String(), "topped up")
		})
	}
}

func TestTopUpStake_SuspendedWorkerIsToldReinstateIsStillNeeded(t *testing.T) {
	t.Parallel()
	h, rc, buf := slashedWorker()
	rc.suspended = true

	err := h.TopUpStake(context.Background(), lcaiWei(750))

	require.NoError(t, err)
	assert.Len(t, rc.topUps, 1)
	out := buf.String()
	assert.Contains(t, out, "topped up by 750 LCAI")
	assert.Contains(t, out, "still suspended")
	assert.Contains(t, out, "`lightchain-worker reinstate`")
}

func TestTopUpStake_ShortOfTheMinimumSaysHowMuchIsMissing(t *testing.T) {
	t.Parallel()
	h, _, buf := slashedWorker()

	err := h.TopUpStake(context.Background(), lcaiWei(500))

	require.NoError(t, err)
	assert.Contains(t, buf.String(), "now 4750 LCAI, still 250 LCAI below the minimum 5000 LCAI")
}

func TestTopUpStake_TxFailureIsReported(t *testing.T) {
	t.Parallel()
	h, rc, buf := slashedWorker()
	rc.topUpErr = errors.New("send TopUpStake tx: insufficient funds for gas * price + value")

	err := h.TopUpStake(context.Background(), lcaiWei(750))

	require.ErrorIs(t, err, rc.topUpErr)
	assert.NotContains(t, buf.String(), "topped up")
}

func TestTopUpStake_DeclinedSendsNothing(t *testing.T) {
	t.Parallel()
	cases := map[string]*bufio.Reader{
		"no":              bufio.NewReader(strings.NewReader("n\n")),
		"empty line":      bufio.NewReader(strings.NewReader("\n")),
		"not a yes":       bufio.NewReader(strings.NewReader("yes please\n")),
		"end of input":    bufio.NewReader(strings.NewReader("")),
		"no input at all": nil,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, rc, buf := slashedWorker()
			h.Yes, h.In = false, in

			err := h.TopUpStake(context.Background(), lcaiWei(500))

			require.Error(t, err)
			assert.Contains(t, err.Error(), "nothing was sent")
			assert.Empty(t, rc.topUps, "no transaction may be sent")
			out := buf.String()
			assert.Contains(t, out, "Add 500 LCAI")
			assert.Contains(t, out, "will be 4750 LCAI, still 250 LCAI below the minimum 5000 LCAI")
			assert.Contains(t, out, "[y/N]")
			assert.NotContains(t, out, "topped up")
		})
	}
}

func TestTopUpStake_AcceptedSends(t *testing.T) {
	t.Parallel()
	for _, answer := range []string{"y\n", "YES\n"} {
		h, rc, buf := slashedWorker()
		h.Yes, h.In = false, bufio.NewReader(strings.NewReader(answer))

		err := h.TopUpStake(context.Background(), lcaiWei(750))

		require.NoError(t, err)
		require.Len(t, rc.topUps, 1)
		assert.Zero(t, lcaiWei(750).Cmp(rc.topUps[0]), "sent %s wei", rc.topUps[0])
		out := buf.String()
		assert.Contains(t, out, "will be 5000 LCAI (minimum 5000 LCAI)")
		assert.Contains(t, out, "[y/N]")
		assert.Contains(t, out, "topped up by 750 LCAI")
	}
}

func TestTopUpStake_YesSendsWithoutReadingInput(t *testing.T) {
	t.Parallel()
	h, rc, buf := slashedWorker()
	in := strings.NewReader("n\n")
	h.Yes, h.In = true, bufio.NewReader(in)

	err := h.TopUpStake(context.Background(), lcaiWei(750))

	require.NoError(t, err)
	assert.Len(t, rc.topUps, 1)
	assert.Equal(t, 2, in.Len(), "the input must not be read")
	assert.NotContains(t, buf.String(), "[y/N]")
	assert.Contains(t, buf.String(), "topped up by 750 LCAI")
}

// slowAnswer delays the operator's answer.
type slowAnswer struct {
	delay time.Duration
	io.Reader
}

func (s slowAnswer) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.Reader.Read(p)
}

func TestTopUpStake_WaitingForTheAnswerDoesNotUseUpTheSendBudget(t *testing.T) {
	t.Parallel()
	h, rc, _ := slashedWorker()
	h.Yes, h.In = false, bufio.NewReader(slowAnswer{300 * time.Millisecond, strings.NewReader("y\n")})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := h.TopUpStake(ctx, lcaiWei(750))

	require.NoError(t, err)
	require.Len(t, rc.topUps, 1)
	assert.NoError(t, rc.sendCtxErr, "the transaction must not be sent on an expired context")
}

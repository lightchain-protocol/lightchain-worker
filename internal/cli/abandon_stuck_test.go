package cli

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

type stuckChain struct {
	head      uint64
	events    []chain.JobSubmittedEvent
	states    map[uint64]chain.JobStateInfo
	abandoned []uint64
	failOn    uint64
	ranges    [][2]uint64
}

func (s *stuckChain) Head(context.Context) (chain.HeadInfo, error) {
	return chain.HeadInfo{Number: s.head}, nil
}

func (s *stuckChain) FilterJobSubmitted(_ context.Context, from, to uint64) ([]chain.JobSubmittedEvent, error) {
	s.ranges = append(s.ranges, [2]uint64{from, to})
	var out []chain.JobSubmittedEvent
	for _, ev := range s.events {
		if ev.BlockNumber >= from && ev.BlockNumber <= to {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (s *stuckChain) GetJobState(_ context.Context, id uint64) (chain.JobStateInfo, error) {
	st, ok := s.states[id]
	if !ok {
		return chain.JobStateInfo{}, errors.New("no such job")
	}
	return st, nil
}

func (s *stuckChain) AbandonJob(_ context.Context, id uint64) error {
	if id == s.failOn {
		return errors.New("execution reverted")
	}
	s.abandoned = append(s.abandoned, id)
	return nil
}

var (
	me    = common.HexToAddress("0x00000000000000000000000000000000000000a1")
	other = common.HexToAddress("0x00000000000000000000000000000000000000b2")
)

func newStuckChain() *stuckChain {
	return &stuckChain{
		head: 10_000,
		events: []chain.JobSubmittedEvent{
			{JobID: 1, Worker: me, BlockNumber: 100},   // outside the look-back
			{JobID: 2, Worker: me, BlockNumber: 6_000}, // acknowledged, mine
			{JobID: 3, Worker: other, BlockNumber: 6_100},
			{JobID: 4, Worker: me, BlockNumber: 7_000}, // completed
			{JobID: 5, Worker: me, BlockNumber: 9_900}, // submitted, mine
		},
		states: map[uint64]chain.JobStateInfo{
			1: {State: chain.JobStateAcknowledged, Worker: me},
			2: {State: chain.JobStateAcknowledged, Worker: me},
			3: {State: chain.JobStateAcknowledged, Worker: other},
			4: {State: chain.JobStateCompleted, Worker: me},
			5: {State: chain.JobStateSubmitted, Worker: me},
		},
	}
}

func TestAbandonStuck_AbandonsOnlyMyInFlightJobsInWindow(t *testing.T) {
	c := newStuckChain()
	var out bytes.Buffer
	n, err := AbandonStuck(context.Background(), AbandonStuckOpts{Client: c, Worker: me, LookbackBlocks: 5_000, ChunkSize: 2_000, Out: &out})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, []uint64{2, 5}, c.abandoned, "job 1 is outside the window, 3 belongs to another worker, 4 is completed")
	require.Equal(t, [2]uint64{5_000, 6_999}, c.ranges[0], "scan starts head - lookback and respects the chunk size")
	require.Contains(t, out.String(), "abandoned 2 of 2")
}

func TestAbandonStuck_DryRunSendsNothing(t *testing.T) {
	c := newStuckChain()
	var out bytes.Buffer
	n, err := AbandonStuck(context.Background(), AbandonStuckOpts{Client: c, Worker: me, LookbackBlocks: 5_000, DryRun: true, Out: &out})
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, c.abandoned)
	require.Contains(t, out.String(), "would abandon [2 5]")
}

func TestAbandonStuck_OneRevertDoesNotStopTheSweep(t *testing.T) {
	c := newStuckChain()
	c.failOn = 2
	var out bytes.Buffer
	n, err := AbandonStuck(context.Background(), AbandonStuckOpts{Client: c, Worker: me, LookbackBlocks: 5_000, Out: &out})
	require.Error(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []uint64{5}, c.abandoned)
	require.Contains(t, err.Error(), "[2]")
}

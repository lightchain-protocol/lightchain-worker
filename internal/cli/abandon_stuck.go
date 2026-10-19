package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"github.com/lightchain/worker/internal/chain"
)

// StuckChain is the narrow chain surface the abandon-stuck command needs.
type StuckChain interface {
	Head(ctx context.Context) (chain.HeadInfo, error)
	FilterJobSubmitted(ctx context.Context, fromBlock, toBlock uint64) ([]chain.JobSubmittedEvent, error)
	GetJobState(ctx context.Context, jobID uint64) (chain.JobStateInfo, error)
	AbandonJob(ctx context.Context, jobID uint64) error
}

// AbandonStuckOpts configures one backlog sweep.
type AbandonStuckOpts struct {
	Client         StuckChain
	Worker         common.Address
	LookbackBlocks uint64 // how far behind the head to look for this worker's jobs
	ChunkSize      uint64 // eth_getLogs range per request (RPC limits), default 5000
	DryRun         bool   // list, do not send
	Out            io.Writer
	Logger         *slog.Logger
}

// AbandonStuck hands back every job of this worker that is still Submitted or
// Acknowledged on chain (JobRegistry.abandonJob): the consumer is refunded and
// the job leaves the worker's in-flight count at once.
//
// Why a separate command: the worker abandons a job it fails from now on, but a
// backlog from before that change stays Acknowledged until the keeper's
// no-fault settle 48 h after each deadline. At maxConcurrentJobs of those the
// chain refuses the worker's session claims (2026-10-19, g1 worker-4/5), and
// WorkerRegistry.removeSupportedModel refuses while any job for the model is
// still counted active, so the backlog blocks both recovery paths.
//
// Jobs are found through JobSubmitted events in the look-back window; anything
// older has been swept by the keeper already. Each candidate's state is read
// before the transaction, and a failed abandon is reported and skipped, never
// fatal, so one odd job cannot stop the sweep.
func AbandonStuck(ctx context.Context, o AbandonStuckOpts) (abandoned int, err error) {
	if o.Client == nil {
		return 0, fmt.Errorf("abandon-stuck: no chain client")
	}
	chunk := o.ChunkSize
	if chunk == 0 {
		chunk = 5000
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}

	head, err := o.Client.Head(ctx)
	if err != nil {
		return 0, fmt.Errorf("abandon-stuck: head: %w", err)
	}
	from := uint64(1)
	if head.Number > o.LookbackBlocks {
		from = head.Number - o.LookbackBlocks
	}

	// Phase 1: this worker's jobs in the window.
	var mine []uint64
	for lo := from; lo <= head.Number; lo += chunk {
		hi := lo + chunk - 1
		if hi > head.Number {
			hi = head.Number
		}
		evs, err := o.Client.FilterJobSubmitted(ctx, lo, hi)
		if err != nil {
			return 0, fmt.Errorf("abandon-stuck: JobSubmitted %d-%d: %w", lo, hi, err)
		}
		for _, ev := range evs {
			if ev.Worker == o.Worker {
				mine = append(mine, ev.JobID)
			}
		}
	}
	fmt.Fprintf(o.Out, "worker %s: %d jobs submitted in blocks %d-%d\n", o.Worker.Hex(), len(mine), from, head.Number)

	// Phase 2: the ones still in flight.
	var stuck []uint64
	for _, id := range mine {
		st, err := o.Client.GetJobState(ctx, id)
		if err != nil {
			log.Warn("getJob failed; skipping", "jobID", id, "error", err)
			continue
		}
		if st.State == chain.JobStateSubmitted || st.State == chain.JobStateAcknowledged {
			stuck = append(stuck, id)
		}
	}
	fmt.Fprintf(o.Out, "in flight (Submitted/Acknowledged): %d\n", len(stuck))
	if len(stuck) == 0 {
		return 0, nil
	}
	if o.DryRun {
		fmt.Fprintf(o.Out, "dry run: would abandon %v\n", stuck)
		return 0, nil
	}

	// Phase 3: hand them back, one tx each.
	var failed []uint64
	for _, id := range stuck {
		if err := ctx.Err(); err != nil {
			return abandoned, fmt.Errorf("abandon-stuck: stopped after %d of %d: %w", abandoned, len(stuck), err)
		}
		if err := o.Client.AbandonJob(ctx, id); err != nil {
			log.Warn("abandonJob failed; skipping", "jobID", id, "error", err)
			failed = append(failed, id)
			continue
		}
		abandoned++
		log.Info("job abandoned: fee refunded, in-flight slot released", "jobID", id)
	}
	fmt.Fprintf(o.Out, "abandoned %d of %d\n", abandoned, len(stuck))
	if len(failed) > 0 {
		return abandoned, fmt.Errorf("%d job(s) could not be abandoned (already settled, or the tx reverted): %v", len(failed), failed)
	}
	return abandoned, nil
}

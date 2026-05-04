package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"

	pkgtypes "github.com/lightchain/pkg/types"
)

// DrainGateway is the subset of the gateway client used by the drain CLI.
type DrainGateway interface {
	SendDrain(ctx context.Context) error
	SendUndrain(ctx context.Context) error
}

// DisputeWindowReader returns the on-chain dispute window. Implementations
// should read AIConfig.getDisputeWindow().
type DisputeWindowReader interface {
	GetDisputeWindow(ctx context.Context) (time.Duration, error)
}

// DrainHandler runs the `drain` and `undrain` subcommands.
//
// Exactly one of RedisClient (direct mode) or Gateway (gateway mode) must be
// non-nil. ChainClient is consulted only in direct mode to derive the drain
// TTL from the on-chain dispute window; gateway mode delegates that to the
// worker-gateway service.
type DrainHandler struct {
	RedisClient *redis.Client
	Gateway     DrainGateway
	ChainClient DisputeWindowReader
	WorkerAddr  common.Address

	// DrainTTLOverride forces a specific TTL. When zero (the default),
	// direct-mode drain reads the dispute window from ChainClient and
	// adds DrainSlack.
	DrainTTLOverride time.Duration
	// DrainSlack is added to the on-chain dispute window when computing
	// TTL. Defaults to 2h if zero.
	DrainSlack time.Duration

	// SkipConfirm suppresses the undrain confirmation prompt.
	SkipConfirm bool
	// Stdin is read for the confirmation prompt; defaults to a closed
	// reader (which causes confirmation to fail) if unset.
	Stdin io.Reader

	Out    io.Writer
	Logger *slog.Logger
}

// DrainTTLFallback is the TTL applied when the dispute window cannot be
// read from chain at drain time. 24h matches the live testnet dispute
// window.
const DrainTTLFallback = 24 * time.Hour

// drainTTLLookupTimeout bounds the on-chain dispute-window read so it
// cannot consume the caller's full deadline. If the RPC takes longer
// than this, computeDrainTTL falls back to DrainTTLFallback while
// leaving the caller's context budget intact for the Redis write.
const drainTTLLookupTimeout = 5 * time.Second

// Drain marks the worker as ineligible for new sessions by writing the
// drain marker either directly to Redis (internal mode) or via the
// worker-gateway HTTP API (external mode).
func (h *DrainHandler) Drain(ctx context.Context) error {
	if h.Gateway != nil {
		if err := h.Gateway.SendDrain(ctx); err != nil {
			return fmt.Errorf("send drain via gateway: %w", err)
		}
		fmt.Fprintf(h.out(), "Worker %s drained via gateway.\n", h.WorkerAddr.Hex())
		fmt.Fprintln(h.out(),
			"The dispatcher will stop routing new sessions; in-flight jobs continue.")
		return nil
	}

	if h.RedisClient == nil {
		return fmt.Errorf("drain requires either a Redis client (direct mode) or a gateway client")
	}

	// computeDrainTTL bounds its chain RPC to its own sub-context so a
	// hung lookup does not eat the budget for SetDraining below.
	ttl := h.computeDrainTTL(ctx)
	if err := pkgtypes.SetDraining(ctx, h.RedisClient, h.WorkerAddr.Hex(), ttl); err != nil {
		return fmt.Errorf("set drain marker: %w", err)
	}
	fmt.Fprintf(h.out(), "Worker %s drained (TTL %s).\n", h.WorkerAddr.Hex(), ttl)
	fmt.Fprintln(h.out(),
		"The dispatcher will stop routing new sessions; in-flight jobs continue.")
	return nil
}

// Undrain removes the drain marker after operator confirmation.
//
// The CLI is selection-only — running drain does not stop the worker
// process from pulling reassignment jobs. Undrain just removes the
// dispatcher-level filter; it does not interact with the running process.
func (h *DrainHandler) Undrain(ctx context.Context) error {
	if !h.SkipConfirm {
		if err := h.confirmUndrain(ctx); err != nil {
			return err
		}
	}

	if h.Gateway != nil {
		if err := h.Gateway.SendUndrain(ctx); err != nil {
			return fmt.Errorf("send undrain via gateway: %w", err)
		}
		fmt.Fprintf(h.out(), "Worker %s undrained via gateway.\n", h.WorkerAddr.Hex())
		return nil
	}

	if h.RedisClient == nil {
		return fmt.Errorf("undrain requires either a Redis client (direct mode) or a gateway client")
	}

	if err := pkgtypes.Undrain(ctx, h.RedisClient, h.WorkerAddr.Hex()); err != nil {
		return fmt.Errorf("delete drain marker: %w", err)
	}
	fmt.Fprintf(h.out(), "Worker %s undrained.\n", h.WorkerAddr.Hex())
	return nil
}

// confirmUndrain prompts the operator and returns nil if they confirm.
// Surfaces the on-chain dispute window so the operator sees how much
// settlement time is still in flight when they reverse drain.
func (h *DrainHandler) confirmUndrain(ctx context.Context) error {
	fmt.Fprintf(h.out(),
		"This will mark worker %s as eligible for new sessions.\n", h.WorkerAddr.Hex())

	if h.ChainClient != nil {
		if dw, err := h.ChainClient.GetDisputeWindow(ctx); err == nil {
			fmt.Fprintf(h.out(), "On-chain dispute window: %s\n", dw)
		}
	}
	fmt.Fprint(h.out(), "Continue? [y/N]: ")

	if h.Stdin == nil {
		return fmt.Errorf("undrain not confirmed (no stdin available; pass --yes for unattended use)")
	}
	scanner := bufio.NewScanner(h.Stdin)
	if !scanner.Scan() {
		return fmt.Errorf("undrain not confirmed (no input)")
	}
	answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
	if answer != "y" && answer != "yes" {
		return fmt.Errorf("undrain aborted by operator")
	}
	return nil
}

func (h *DrainHandler) computeDrainTTL(ctx context.Context) time.Duration {
	if h.DrainTTLOverride > 0 {
		return h.DrainTTLOverride
	}
	slack := h.DrainSlack
	if slack <= 0 {
		slack = 2 * time.Hour
	}
	if h.ChainClient == nil {
		return DrainTTLFallback
	}

	// Bound the RPC to its own sub-context so a hung chain client cannot
	// burn the caller's deadline — the Redis write that follows must
	// still have time to run with the fallback TTL.
	lookupCtx, cancel := context.WithTimeout(ctx, drainTTLLookupTimeout)
	defer cancel()

	dw, err := h.ChainClient.GetDisputeWindow(lookupCtx)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Warn("get dispute window failed; using drain TTL fallback",
				"fallback", DrainTTLFallback, "error", err)
		}
		return DrainTTLFallback
	}
	return dw + slack
}

func (h *DrainHandler) out() io.Writer {
	if h.Out != nil {
		return h.Out
	}
	return io.Discard
}

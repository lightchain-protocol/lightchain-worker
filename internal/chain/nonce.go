package chain

import (
	"context"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// NonceFetcher is the narrow interface for fetching the pending nonce from the chain.
// Defined at the consumer to avoid coupling to ethclient.Client.
type NonceFetcher interface {
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
}

// NonceManager tracks the next usable nonce for a single account.
// It lazy-initializes from the chain on first use, then increments locally
// to support concurrent transaction submission without nonce collisions.
type NonceManager struct {
	mu           sync.Mutex
	fetcher      NonceFetcher
	account      common.Address
	pendingNonce uint64
	initialized  bool
	outstanding  int
	resetPending bool
}

// NewNonceManager creates a NonceManager for the given account.
func NewNonceManager(fetcher NonceFetcher, account common.Address) *NonceManager {
	return &NonceManager{
		fetcher: fetcher,
		account: account,
	}
}

// NextNonce returns the next available nonce for transaction submission.
// On the first call, it fetches the pending nonce from the chain.
// Subsequent calls increment the local counter without a chain call.
func (nm *NonceManager) NextNonce(ctx context.Context) (uint64, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if !nm.initialized {
		nonce, err := nm.fetcher.PendingNonceAt(ctx, nm.account)
		if err != nil {
			return 0, fmt.Errorf("fetch initial pending nonce for %s: %w", nm.account.Hex(), err)
		}
		nm.pendingNonce = nonce
		nm.initialized = true
	}

	nonce := nm.pendingNonce
	nm.pendingNonce++
	nm.outstanding++
	return nonce, nil
}

// ReleaseNonce returns a nonce obtained from NextNonce. consumed reports
// whether the transaction actually reached the network; an unsent nonce has
// to be accounted for or it leaves a hole the chain will stall behind.
func (nm *NonceManager) ReleaseNonce(nonce uint64, consumed bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if nm.outstanding > 0 {
		nm.outstanding--
	}

	if !consumed {
		if nm.initialized && nm.pendingNonce > 0 && nonce == nm.pendingNonce-1 {
			// Highest nonce handed out and never used, so hand it straight
			// back and the sequence stays contiguous.
			nm.pendingNonce--
		} else {
			// A successor already holds a higher nonce, so this leaves a gap.
			// Re-seed, but only once every lease has drained.
			nm.resetPending = true
		}
	}

	nm.applyPendingResetLocked()
}

// ResetNonce clears the local nonce state, forcing a re-fetch from the chain
// on the next NextNonce call. Use this after a transaction failure that may
// have left the local counter out of sync.
// A reset is deferred while any lease is outstanding, because re-seeding
// underneath a goroutine that already holds a nonce is what produces
// "nonce too low" on an account that is otherwise perfectly in sync:
// PendingNonceAt cannot see a nonce that has been allocated but not yet
// broadcast, so the re-seed lands behind it.
func (nm *NonceManager) ResetNonce() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.resetPending = true
	nm.applyPendingResetLocked()
}

func (nm *NonceManager) applyPendingResetLocked() {
	if !nm.resetPending || nm.outstanding > 0 {
		return
	}
	nm.initialized = false
	nm.pendingNonce = 0
	nm.resetPending = false
}

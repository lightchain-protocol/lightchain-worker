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
	return nonce, nil
}

// ResetNonce clears the local nonce state, forcing a re-fetch from the chain
// on the next NextNonce call. Use this after a transaction failure that may
// have left the local counter out of sync.
func (nm *NonceManager) ResetNonce() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.initialized = false
	nm.pendingNonce = 0
}

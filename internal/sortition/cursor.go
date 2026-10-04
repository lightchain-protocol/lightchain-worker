// Package sortition implements the worker's dispatcher-free intake: watching the
// chain to self-claim sessions (SessionWatcher) and to serve their jobs
// (JobWatcher) straight from JobSubmitted events.
package sortition

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// CursorStore persists per-watcher "last processed block" numbers to disk so a
// restart resumes without re-scanning the whole chain.
type CursorStore struct {
	mu  sync.Mutex
	dir string
}

func NewCursorStore(dir string) (*CursorStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create sortition state dir %s: %w", dir, err)
	}
	return &CursorStore{dir: dir}, nil
}

func (c *CursorStore) path(name string) string {
	return filepath.Join(c.dir, "cursor-"+name+".txt")
}

// Get returns the last processed block for `name`, or 0 if unset.
func (c *CursorStore) Get(name string) (uint64, error) {
	v, _, err := c.read(name)
	return v, err
}

// GetOrSeed returns the block a watcher's scan resumes after. A stored cursor
// is authoritative, a stored 0 included. With none stored (a fresh worker) the
// scan starts `lookback` blocks behind safeHead instead of at the first block,
// so the worker does not read the whole chain before it can do anything: that
// start is stored and returned with fresh set. A chain no longer than the
// look-back is scanned from its first block as before, and nothing is stored.
func (c *CursorStore) GetOrSeed(name string, safeHead, lookback uint64) (cursor uint64, fresh bool, err error) {
	cursor, stored, err := c.read(name)
	if err != nil || stored || safeHead <= lookback {
		return cursor, false, err
	}
	cursor = safeHead - lookback
	return cursor, true, c.Set(name, cursor)
}

// read is Get that also reports whether a cursor is stored for `name` at all,
// which tells a fresh worker (nothing stored) apart from a stored cursor of 0.
func (c *CursorStore) read(name string) (block uint64, stored bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := os.ReadFile(c.path(name))
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read cursor %s: %w", name, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cursor %s: %w", name, err)
	}
	return v, true, nil
}

// Set atomically writes the last processed block for `name`.
func (c *CursorStore) Set(name string, block uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tmp := c.path(name) + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(block, 10)), 0o644); err != nil {
		return fmt.Errorf("write cursor %s: %w", name, err)
	}
	if err := os.Rename(tmp, c.path(name)); err != nil {
		return fmt.Errorf("commit cursor %s: %w", name, err)
	}
	return nil
}

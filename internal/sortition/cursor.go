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
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := os.ReadFile(c.path(name))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read cursor %s: %w", name, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse cursor %s: %w", name, err)
	}
	return v, nil
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

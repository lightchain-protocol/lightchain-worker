package sortition

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursorStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir)
	require.NoError(t, err)

	got, err := cs.Get("session_requested")
	require.NoError(t, err)
	require.Equal(t, uint64(0), got, "unset cursor is 0")

	require.NoError(t, cs.Set("session_requested", 12345))
	got, err = cs.Get("session_requested")
	require.NoError(t, err)
	require.Equal(t, uint64(12345), got)

	// Independent names.
	require.NoError(t, cs.Set("job_submitted", 999))
	got, _ = cs.Get("session_requested")
	require.Equal(t, uint64(12345), got)
}

func TestCursorStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cs, _ := NewCursorStore(dir)
	require.NoError(t, cs.Set("job_submitted", 77))

	cs2, err := NewCursorStore(dir)
	require.NoError(t, err)
	got, err := cs2.Get("job_submitted")
	require.NoError(t, err)
	require.Equal(t, uint64(77), got, "cursor survives a fresh store on the same dir")
}

func TestCursorStore_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "state")
	cs, err := NewCursorStore(dir)
	require.NoError(t, err)
	require.NoError(t, cs.Set("x", 1))
}

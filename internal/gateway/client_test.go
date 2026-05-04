package gateway

import (
	"context"
	"crypto/ecdsa"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAuthedTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	c := NewClient(baseURL, key, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.token = "test-token"
	c.expiresAt = time.Now().Add(time.Hour)
	return c
}

func newKeyForTest(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := crypto.GenerateKey()
	require.NoError(t, err)
	return k
}

func TestSendDrain_postsToCorrectPathWithAuth(t *testing.T) {
	t.Parallel()

	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newAuthedTestClient(t, srv.URL)
	require.NoError(t, c.SendDrain(context.Background()))

	assert.Equal(t, "/api/worker/drain", gotPath)
	assert.Equal(t, "Bearer test-token", gotAuth)
}

func TestSendDrain_returnsErrOnNon204(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "redis offline", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := newAuthedTestClient(t, srv.URL)
	err := c.SendDrain(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "drain failed")
	assert.Contains(t, err.Error(), "500")
}

func TestSendDrain_returnsErrOnUnauthorized(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing worker context", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	c := newAuthedTestClient(t, srv.URL)
	err := c.SendDrain(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestSendDrain_returnsErrWhenAuthFails(t *testing.T) {
	t.Parallel()
	// Client with no cached token and a bad URL forces ensureAuth to run
	// and fail (the auth flow needs a working /api/auth/challenge endpoint
	// which we don't provide here).
	c := NewClient("http://127.0.0.1:0", newKeyForTest(t),
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := c.SendDrain(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth")
}

func TestSendUndrain_postsToCorrectPathWithAuth(t *testing.T) {
	t.Parallel()

	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newAuthedTestClient(t, srv.URL)
	require.NoError(t, c.SendUndrain(context.Background()))

	assert.Equal(t, "/api/worker/undrain", gotPath)
	assert.Equal(t, "Bearer test-token", gotAuth)
}

func TestSendUndrain_returnsErrOnNon204(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "redis offline", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := newAuthedTestClient(t, srv.URL)
	err := c.SendUndrain(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "undrain failed")
}

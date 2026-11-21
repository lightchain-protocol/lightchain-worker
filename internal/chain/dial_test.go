package chain

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rpcEndpoint is a JSON-RPC endpoint that counts its requests and answers
// eth_chainId with 0x1. A non-zero status makes it answer that HTTP status
// instead, the way a proxy in front of a dead node does. A non-nil hang makes
// it hold every request until hang is closed, the way a blackholed route does.
type rpcEndpoint struct {
	hits   atomic.Int32
	status int
	hang   chan struct{}
}

func (e *rpcEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.hits.Add(1)
	if e.hang != nil {
		select {
		case <-e.hang:
		case <-r.Context().Done():
		}
		return
	}
	if e.status != 0 {
		w.WriteHeader(e.status)
		return
	}
	var req struct {
		ID json.RawMessage `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":"0x1"}`))
}

func serve(t *testing.T, e *rpcEndpoint) string {
	t.Helper()
	srv := httptest.NewServer(e)
	t.Cleanup(func() {
		if e.hang != nil {
			close(e.hang)
		}
		srv.Close()
	})
	return srv.URL
}

func TestDialRPC_FailsOverWhenTheFirstEndpointIsDown(t *testing.T) {
	t.Parallel()

	down := &rpcEndpoint{status: http.StatusBadGateway}
	up := &rpcEndpoint{}
	client, err := DialRPC(serve(t, down)+", "+serve(t, up), time.Second)
	require.NoError(t, err)
	defer client.Close()

	for range 3 {
		id, err := client.ChainID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), id.Int64())
	}
	assert.Equal(t, int32(1), down.hits.Load(),
		"once an endpoint has failed, later calls must go straight to the one that answered")
	assert.Equal(t, int32(3), up.hits.Load())
}

// The node the worker is reading from stops mid-run: its port closes and
// the worker carries on with the next endpoint.
func TestDialRPC_KeepsWorkingWhenTheNodeInUseStops(t *testing.T) {
	t.Parallel()

	first := &rpcEndpoint{}
	node := httptest.NewServer(first)
	second := &rpcEndpoint{}
	client, err := DialRPC(node.URL+","+serve(t, second), time.Second)
	require.NoError(t, err)
	defer client.Close()

	_, err = client.ChainID(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(1), first.hits.Load())

	node.Close()
	for range 3 {
		id, err := client.ChainID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), id.Int64())
	}
	assert.Equal(t, int32(3), second.hits.Load())
}

func TestDialRPC_FailsOverWhenTheFirstEndpointHangs(t *testing.T) {
	t.Parallel()

	hung := &rpcEndpoint{hang: make(chan struct{})}
	up := &rpcEndpoint{}
	client, err := DialRPC(serve(t, hung)+","+serve(t, up), 100*time.Millisecond)
	require.NoError(t, err)
	defer client.Close()

	id, err := client.ChainID(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), id.Int64())
}

// A context without a deadline is what the watcher loops pass. With one
// endpoint and no route to it, the call has to fail instead of holding the
// loop for as long as the kernel keeps the connection.
func TestDialRPC_ACallToAHungEndpointFails(t *testing.T) {
	t.Parallel()

	hung := &rpcEndpoint{hang: make(chan struct{})}
	client, err := DialRPC(serve(t, hung), 100*time.Millisecond)
	require.NoError(t, err)
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		_, err := client.ChainID(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the call is still waiting on an endpoint that never answers")
	}
}

func TestDialRPC_RejectsAListItCannotFailOver(t *testing.T) {
	t.Parallel()

	for _, urls := range []string{"", " , ", "http://a:8545,ws://b:8546"} {
		_, err := DialRPC(urls, time.Second)
		assert.Error(t, err, "RPC_URL=%q", urls)
	}
}

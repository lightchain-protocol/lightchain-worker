package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	pkgtypes "github.com/lightchain/pkg/types"
)

// fakeGateway serves the auth endpoints plus either the WS stream ingress,
// the bulk response POST, or both — capturing what arrives.
type fakeGateway struct {
	t         *testing.T
	wsEnabled bool
	frames    chan pkgtypes.PubSubMessage
	bulk      chan map[string]interface{}
}

func newFakeGateway(t *testing.T, wsEnabled bool) (*fakeGateway, *httptest.Server) {
	f := &fakeGateway{
		t:         t,
		wsEnabled: wsEnabled,
		frames:    make(chan pkgtypes.PubSubMessage, 16),
		bulk:      make(chan map[string]interface{}, 16),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/auth/challenge", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"lightchain-worker-gateway-auth nonce=n1 issued=2026-01-01T00:00:00Z expires=2036-01-01T00:00:00Z","expiresAt":"2036-01-01T00:00:00Z"}`))
	})
	mux.HandleFunc("POST /api/auth/verify", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"test-token","wallet":"0xabc","expiresAt":"2036-01-01T00:00:00Z"}`))
	})
	mux.HandleFunc("GET /api/stream/responses", func(w http.ResponseWriter, r *http.Request) {
		if !f.wsEnabled {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
		for {
			_, data, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var msg pkgtypes.PubSubMessage
			if json.Unmarshal(data, &msg) == nil {
				f.frames <- msg
			}
		}
	})
	mux.HandleFunc("POST /api/jobs/{jobId}/response", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		body["jobId"] = r.PathValue("jobId")
		f.bulk <- body
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return f, ts
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStreamPublisherSendsChunkOverWS(t *testing.T) {
	fake, ts := newFakeGateway(t, true)
	client := NewClient(ts.URL, nil, discardLogger())
	// Pre-seed the token so Authenticate's EIP-191 signing (needs a real key)
	// is skipped — ensureAuth sees a fresh token and does nothing.
	client.token = "test-token"
	client.expiresAt = time.Now().Add(time.Hour)

	pub := NewStreamPublisher(client, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	deadline := time.After(5 * time.Second)
	for !pub.Connected() {
		select {
		case <-deadline:
			t.Fatal("stream never connected")
		case <-time.After(10 * time.Millisecond):
		}
	}

	pub.PublishChunk(ctx, 9, 42, "corr-1", 3, []byte("delta"))

	select {
	case got := <-fake.frames:
		if got.Type != pkgtypes.MessageTypeChunk || got.JobID != 9 || got.SessionID != 42 || got.Sequence != 3 {
			t.Fatalf("bad frame: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no frame received over WS")
	}
}

func TestStreamPublisherCompleteFallsBackToHTTP(t *testing.T) {
	fake, ts := newFakeGateway(t, false) // WS 404s — dial keeps failing
	client := NewClient(ts.URL, nil, discardLogger())
	client.token = "test-token"
	client.expiresAt = time.Now().Add(time.Hour)

	pub := NewStreamPublisher(client, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	pub.PublishResponse(ctx, 9, 42, "corr-1", "0xsig", []byte("ciphertext"))

	select {
	case got := <-fake.bulk:
		if got["jobId"] != "9" || got["sessionId"] != float64(42) || got["signature"] != "0xsig" {
			t.Fatalf("bad bulk body: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no HTTP fallback publish")
	}
}

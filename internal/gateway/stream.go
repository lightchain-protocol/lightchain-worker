package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	pkgtypes "github.com/lightchain/pkg/types"
)

// streamBufferSize bounds queued frames while the socket is down or slow.
// Chunks are tiny (≈24B ciphertext + envelope); at worker flush rates this
// is multiple seconds of backlog. Overflow drops frames — the stream plane
// is best-effort UX, the on-chain blob is the authoritative response.
const streamBufferSize = 256

// StreamPublisher implements pipeline.ResponsePublisher over a persistent
// WebSocket to the worker-gateway (external worker profile). Frames are
// enqueued non-blocking; a background loop dials, pumps, and re-dials with
// exponential backoff. The final `complete` frame falls back to the
// gateway's bulk HTTP endpoint whenever the socket is down, so consumers
// still get a terminal frame even if streaming never came up.
type StreamPublisher struct {
	client *Client
	logger *slog.Logger

	frames    chan pkgtypes.PubSubMessage
	connected atomic.Bool
}

// NewStreamPublisher constructs the publisher. Call Run (in a goroutine) to
// start the connection loop; Publish* methods are safe before Run starts —
// frames buffer (or fall back) until the stream connects.
func NewStreamPublisher(client *Client, logger *slog.Logger) *StreamPublisher {
	return &StreamPublisher{
		client: client,
		logger: logger,
		frames: make(chan pkgtypes.PubSubMessage, streamBufferSize),
	}
}

// Connected reports whether the stream socket is currently up.
func (p *StreamPublisher) Connected() bool { return p.connected.Load() }

// Run dials the gateway stream and pumps frames until ctx is cancelled.
// Blocking — run in a goroutine.
func (p *StreamPublisher) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		conn, err := p.dial(ctx)
		if err != nil {
			p.logger.Warn("gateway stream dial failed", "error", err, "retryIn", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		p.logger.Info("gateway stream connected")
		p.connected.Store(true)
		p.pump(ctx, conn)
		p.connected.Store(false)
	}
}

func (p *StreamPublisher) dial(ctx context.Context) (*websocket.Conn, error) {
	if err := p.client.ensureAuth(ctx); err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	// http→ws, https→wss.
	wsURL := strings.Replace(p.client.baseURL, "http", "ws", 1) + "/api/stream/responses"
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + p.client.getToken()}},
	})
	return conn, err
}

// pump writes queued frames until the connection breaks or ctx ends.
func (p *StreamPublisher) pump(ctx context.Context, conn *websocket.Conn) {
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "closing") }()

	// Reader goroutine: services pings and detects server close.
	readCtx, readCancel := context.WithCancel(ctx)
	defer readCancel()
	go func() {
		defer readCancel()
		for {
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-readCtx.Done():
			return
		case msg := <-p.frames:
			data, err := json.Marshal(msg)
			if err != nil {
				p.logger.Warn("stream frame marshal failed", "jobID", msg.JobID, "error", err)
				continue
			}
			writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err != nil {
				// The dequeued frame is lost (bounded damage: best-effort
				// plane; a lost complete frame is recovered by consumers via
				// the on-chain blob). Reconnect handles the rest.
				p.logger.Warn("stream write failed, reconnecting", "jobID", msg.JobID, "type", msg.Type, "error", err)
				return
			}
		}
	}
}

// enqueue adds a frame non-blocking; returns false when the buffer is full.
func (p *StreamPublisher) enqueue(msg *pkgtypes.PubSubMessage) bool {
	select {
	case p.frames <- *msg:
		return true
	default:
		return false
	}
}

// PublishChunk sends a streaming chunk frame (best-effort, dropped on overflow).
func (p *StreamPublisher) PublishChunk(
	_ context.Context,
	jobID, sessionID uint64,
	correlationID string,
	sequence uint32,
	payload []byte,
) {
	ok := p.enqueue(&pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeChunk,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		Sequence:      sequence,
		Payload:       payload,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	})
	if !ok {
		p.logger.Warn("stream buffer full, chunk dropped", "jobID", jobID, "seq", sequence)
	}
}

// PublishMetadata sends a metadata frame (e.g. web-search citations).
func (p *StreamPublisher) PublishMetadata(
	_ context.Context,
	jobID, sessionID uint64,
	correlationID string,
	payload []byte,
) {
	ok := p.enqueue(&pkgtypes.PubSubMessage{
		Type:          pkgtypes.MessageTypeMetadata,
		JobID:         pkgtypes.JobID(jobID),
		SessionID:     pkgtypes.SessionID(sessionID),
		TotalChunks:   1,
		Payload:       payload,
		CorrelationID: correlationID,
		Timestamp:     time.Now().Unix(),
	})
	if !ok {
		p.logger.Warn("stream buffer full, metadata dropped", "jobID", jobID)
	}
}

// PublishResponse sends the final complete frame. Ordering with in-flight
// chunks is preserved by using the same queue while connected; when the
// stream is down (or the queue is full) it falls back to the gateway's bulk
// HTTP endpoint so the consumer still receives a terminal frame.
func (p *StreamPublisher) PublishResponse(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	signature string,
	ciphertext []byte,
) {
	if p.connected.Load() {
		ok := p.enqueue(&pkgtypes.PubSubMessage{
			Type:          pkgtypes.MessageTypeComplete,
			JobID:         pkgtypes.JobID(jobID),
			SessionID:     pkgtypes.SessionID(sessionID),
			TotalChunks:   1,
			Payload:       ciphertext,
			Signature:     signature,
			CorrelationID: correlationID,
			Timestamp:     time.Now().Unix(),
		})
		if ok {
			return
		}
	}
	if err := p.client.PublishResponse(ctx, jobID, sessionID, correlationID, signature, ciphertext); err != nil {
		p.logger.Warn("gateway response fallback failed (non-fatal)", "jobID", jobID, "error", err)
	}
}

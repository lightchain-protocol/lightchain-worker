package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/lightchain/worker/internal/pipeline"
)

// WebSocket message types (must match worker-gateway/internal/api/ws_types.go).
const (
	wsTypeReady = "ready"
	wsTypeAck   = "ack"
	wsTypeDone  = "done"
	wsTypeJob   = "job"
)

type wsIncoming struct {
	Type    string          `json:"type"`
	JobID   uint64          `json:"jobId,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type wsOutgoing struct {
	Type  string `json:"type"`
	JobID uint64 `json:"jobId,omitempty"`
	Slots int    `json:"slots,omitempty"`
	Error string `json:"error,omitempty"`
}

// JobHandler is called for each job received via WebSocket.
// A non-nil error signals the gateway that the job failed and should be retried.
type JobHandler func(ctx context.Context, job pipeline.JobPayload) error

// StreamJobs connects to the worker-gateway via WebSocket and receives jobs.
// It handles reconnection with exponential backoff. Jobs are dispatched to
// the handler function. The handler is responsible for calling SendDone when
// processing completes.
func (c *Client) StreamJobs(ctx context.Context, maxConcurrentJobs int, handler JobHandler) {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		connected, err := c.streamOnce(ctx, maxConcurrentJobs, handler)
		if ctx.Err() != nil {
			return
		}

		// Reset backoff after a successful connection — the next disconnect
		// should start with the minimum delay, not continue from a previous one.
		if connected {
			backoff = time.Second
		}

		c.logger.Warn("websocket disconnected, reconnecting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// streamOnce runs a single WebSocket session: connect, send ready, receive jobs.
// Returns (connected, error) — connected is true if the WebSocket was established
// (used by the caller to reset backoff).
func (c *Client) streamOnce(ctx context.Context, maxConcurrentJobs int, handler JobHandler) (connected bool, err error) {
	if err := c.ensureAuth(ctx); err != nil {
		return false, fmt.Errorf("auth: %w", err)
	}

	// Build WebSocket URL from base HTTP URL.
	wsURL := strings.Replace(c.baseURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/api/jobs/stream"

	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+c.getToken())

	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: headers,
	})
	if err != nil {
		return false, fmt.Errorf("websocket dial: %w", err)
	}

	c.logger.Info("websocket connected to gateway")

	// Send ready message with capacity.
	readyMsg, _ := json.Marshal(wsOutgoing{
		Type:  wsTypeReady,
		Slots: maxConcurrentJobs,
	})
	if err := conn.Write(ctx, websocket.MessageText, readyMsg); err != nil {
		conn.CloseNow()
		return true, fmt.Errorf("send ready: %w", err)
	}

	// Track in-flight handler goroutines so we can drain them before closing.
	var wg sync.WaitGroup

	// Read loop: receive jobs, dispatch to handler.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			// Wait for in-flight handlers to finish before closing the connection.
			wg.Wait()
			conn.CloseNow()
			return true, fmt.Errorf("read: %w", err)
		}

		var msg wsIncoming
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warn("invalid websocket message", "error", err)
			continue
		}

		if msg.Type != wsTypeJob {
			continue
		}

		// Decode job payload.
		var job pipeline.JobPayload
		if err := json.Unmarshal(msg.Payload, &job); err != nil {
			c.logger.Warn("invalid job payload", "jobId", msg.JobID, "error", err)
			continue
		}

		// ACK immediately — we've received the job.
		ackMsg, _ := json.Marshal(wsOutgoing{Type: wsTypeAck, JobID: msg.JobID})
		if err := conn.Write(ctx, websocket.MessageText, ackMsg); err != nil {
			wg.Wait()
			conn.CloseNow()
			return true, fmt.Errorf("send ack for job %d: %w", msg.JobID, err)
		}

		c.logger.Info("ws_job_received", "jobId", msg.JobID)

		// Process in goroutine, send done when finished.
		wg.Add(1)
		go func(jobID uint64) {
			defer wg.Done()
			handlerErr := handler(ctx, job)

			done := wsOutgoing{Type: wsTypeDone, JobID: jobID}
			if handlerErr != nil {
				done.Error = handlerErr.Error()
			}
			doneMsg, _ := json.Marshal(done)
			if writeErr := conn.Write(ctx, websocket.MessageText, doneMsg); writeErr != nil {
				c.logger.Warn("send done failed", "jobId", jobID, "error", writeErr)
			}
		}(msg.JobID)
	}
}

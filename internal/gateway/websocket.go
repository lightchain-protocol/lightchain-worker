package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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
}

// JobHandler is called for each job received via WebSocket.
type JobHandler func(ctx context.Context, job pipeline.JobPayload)

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

		err := c.streamOnce(ctx, maxConcurrentJobs, handler)
		if ctx.Err() != nil {
			return
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
func (c *Client) streamOnce(ctx context.Context, maxConcurrentJobs int, handler JobHandler) error {
	if err := c.ensureAuth(ctx); err != nil {
		return fmt.Errorf("auth: %w", err)
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
		return fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.CloseNow()

	c.logger.Info("websocket connected to gateway")

	// Send ready message with capacity.
	readyMsg, _ := json.Marshal(wsOutgoing{
		Type:  wsTypeReady,
		Slots: maxConcurrentJobs,
	})
	if err := conn.Write(ctx, websocket.MessageText, readyMsg); err != nil {
		return fmt.Errorf("send ready: %w", err)
	}

	// Read loop: receive jobs, dispatch to handler.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
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
			return fmt.Errorf("send ack for job %d: %w", msg.JobID, err)
		}

		c.logger.Info("ws_job_received", "jobId", msg.JobID)

		// Process in goroutine, send done when finished.
		go func(jobID uint64) {
			handler(ctx, job)

			doneMsg, _ := json.Marshal(wsOutgoing{Type: wsTypeDone, JobID: jobID})
			if writeErr := conn.Write(ctx, websocket.MessageText, doneMsg); writeErr != nil {
				c.logger.Warn("send done failed", "jobId", jobID, "error", writeErr)
			}
		}(msg.JobID)
	}
}

// Package gateway provides an HTTP client for the worker-gateway service.
// When WORKER_GATEWAY_URL is configured, the worker communicates with the
// gateway instead of direct Redis for heartbeat, job polling, and response publishing.
package gateway

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/lightchain/worker/internal/pipeline"
)

// Client communicates with the worker-gateway HTTP API.
type Client struct {
	baseURL    string
	signingKey *ecdsa.PrivateKey
	httpClient *http.Client
	logger     *slog.Logger

	mu        sync.RWMutex
	token     string
	expiresAt time.Time
}

// NewClient creates a gateway client. The signingKey is used for
// EIP-191 challenge-response authentication.
func NewClient(baseURL string, signingKey *ecdsa.PrivateKey, logger *slog.Logger) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		signingKey: signingKey,
		httpClient: &http.Client{Timeout: 90 * time.Second},
		logger:     logger,
	}
}

// Authenticate performs EIP-191 challenge-response auth and caches the JWT.
func (c *Client) Authenticate(ctx context.Context) error {
	// Step 1: Get challenge.
	challengeResp, err := c.doGet(ctx, "/api/auth/challenge", "")
	if err != nil {
		return fmt.Errorf("get challenge: %w", err)
	}
	defer challengeResp.Body.Close()

	var challenge struct {
		Message string `json:"message"`
	}
	if err := json.NewDecoder(challengeResp.Body).Decode(&challenge); err != nil {
		return fmt.Errorf("decode challenge: %w", err)
	}

	// Step 2: Sign the challenge with EIP-191 personal_sign.
	prefix := "\x19Ethereum Signed Message:\n" + strconv.Itoa(len(challenge.Message))
	hash := crypto.Keccak256([]byte(prefix + challenge.Message))
	sig, err := crypto.Sign(hash, c.signingKey)
	if err != nil {
		return fmt.Errorf("sign challenge: %w", err)
	}
	// Normalize V: go-ethereum returns 0/1, EIP-191 expects 27/28.
	sig[64] += 27

	sigHex := "0x" + fmt.Sprintf("%x", sig)

	// Step 3: Verify signature → get JWT.
	body, _ := json.Marshal(map[string]string{
		"message":   challenge.Message,
		"signature": sigHex,
	})

	verifyResp, err := c.doPost(ctx, "/api/auth/verify", body, "")
	if err != nil {
		return fmt.Errorf("verify signature: %w", err)
	}
	defer verifyResp.Body.Close()

	if verifyResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(verifyResp.Body)
		return fmt.Errorf("auth verify failed (status %d): %s", verifyResp.StatusCode, string(respBody))
	}

	var result struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expiresAt"`
	}
	if err := json.NewDecoder(verifyResp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode auth response: %w", err)
	}

	expiry, _ := time.Parse(time.RFC3339, result.ExpiresAt)

	c.mu.Lock()
	c.token = result.Token
	c.expiresAt = expiry
	c.mu.Unlock()

	c.logger.Info("authenticated with worker-gateway", "expiresAt", result.ExpiresAt)
	return nil
}

// ensureAuth re-authenticates if the token is expired or about to expire (within 5 minutes).
func (c *Client) ensureAuth(ctx context.Context) error {
	c.mu.RLock()
	needsRefresh := c.token == "" || time.Until(c.expiresAt) < 5*time.Minute
	c.mu.RUnlock()

	if needsRefresh {
		return c.Authenticate(ctx)
	}
	return nil
}

func (c *Client) getToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

// SendHeartbeat posts heartbeat data to the gateway.
func (c *Client) SendHeartbeat(ctx context.Context, payload HeartbeatPayload) error {
	if err := c.ensureAuth(ctx); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}

	resp, err := c.doPost(ctx, "/api/heartbeat", body, c.getToken())
	if err != nil {
		return fmt.Errorf("post heartbeat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat failed (status %d): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// PollJob long-polls the gateway for a job. Returns nil payload if no job is available.
func (c *Client) PollJob(ctx context.Context, timeout time.Duration) (*pipeline.JobPayload, error) {
	if err := c.ensureAuth(ctx); err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}

	path := fmt.Sprintf("/api/jobs/poll?timeout=%s", timeout)
	resp, err := c.doGet(ctx, path, c.getToken())
	if err != nil {
		return nil, fmt.Errorf("poll job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	var job pipeline.JobPayload
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("decode job payload: %w", err)
	}
	return &job, nil
}

// PublishResponse posts an encrypted response to the gateway for relay delivery.
func (c *Client) PublishResponse(
	ctx context.Context,
	jobID, sessionID uint64,
	correlationID string,
	signature string,
	payload []byte,
) error {
	if err := c.ensureAuth(ctx); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	body, err := json.Marshal(map[string]interface{}{
		"sessionId":     sessionID,
		"seq":           0,
		"totalChunks":   1,
		"payload":       payload,
		"signature":     signature,
		"correlationId": correlationID,
	})
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	path := fmt.Sprintf("/api/jobs/%d/response", jobID)
	resp, err := c.doPost(ctx, path, body, c.getToken())
	if err != nil {
		return fmt.Errorf("post response: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("response publish failed (status %d): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// HeartbeatPayload matches the worker-gateway POST /api/heartbeat request body.
type HeartbeatPayload struct {
	ActiveJobs   int      `json:"activeJobs"`
	MaxJobs      int      `json:"maxJobs"`
	Models       []string `json:"models"`
	OllamaStatus string   `json:"ollamaStatus"`
	Uptime       int64    `json:"uptimeSeconds"`
}

// --- HTTP helpers ---

func (c *Client) doGet(ctx context.Context, path, token string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return c.httpClient.Do(req)
}

func (c *Client) doPost(ctx context.Context, path string, body []byte, token string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return c.httpClient.Do(req)
}

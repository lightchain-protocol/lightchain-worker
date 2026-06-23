// Package ollama provides an HTTP client for the Ollama inference API.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// GenerateRequest is the JSON body sent to POST /api/generate.
type GenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

// GenerateResponse is the JSON body returned from POST /api/generate (stream=false).
type GenerateResponse struct {
	Model    string `json:"model"`
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

// ChatMessage is a single message in a chat conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the JSON body sent to POST /api/chat.
type ChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

// ChatResponse is the JSON body returned from POST /api/chat (stream=false).
type ChatResponse struct {
	Model   string      `json:"model"`
	Message ChatMessage `json:"message"`
	Done    bool        `json:"done"`
}

// TagsResponse is the JSON body returned from GET /api/tags.
type TagsResponse struct {
	Models []ModelInfo `json:"models"`
}

// ModelInfo describes a single loaded model in the Ollama server.
type ModelInfo struct {
	Name string `json:"name"`
}

// OllamaClient talks to the Ollama HTTP API for AI inference.
type OllamaClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewOllamaClient creates a client configured for the given Ollama server.
func NewOllamaClient(baseURL string, timeout time.Duration) *OllamaClient {
	return &OllamaClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Generate sends a prompt to the Ollama server and returns the full response.
// Uses stream=false mode for batch inference (DD-W-1).
func (c *OllamaClient) Generate(ctx context.Context, model, prompt string) (string, error) {
	reqBody := GenerateRequest{
		Model:  model,
		Prompt: prompt,
		Stream: false,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal generate request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/generate", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create generate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama generate request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read generate response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama generate returned status %d: %s", resp.StatusCode, string(body))
	}

	var genResp GenerateResponse
	if err := json.Unmarshal(body, &genResp); err != nil {
		return "", fmt.Errorf("unmarshal generate response: %w", err)
	}

	return genResp.Response, nil
}

// Chat sends a multi-turn conversation to the Ollama server and returns the assistant's response.
// Uses POST /api/chat with stream=false.
func (c *OllamaClient) Chat(ctx context.Context, model string, messages []ChatMessage) (string, error) {
	reqBody := ChatRequest{
		Model:    model,
		Messages: messages,
		Stream:   false,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal chat request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/chat", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create chat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ollama chat request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read chat response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ollama chat returned status %d: %s", resp.StatusCode, string(body))
	}

	var chatResp ChatResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return "", fmt.Errorf("unmarshal chat response: %w", err)
	}

	return chatResp.Message.Content, nil
}

// VerifyModels checks that all required models are loaded on the Ollama server.
// Returns an error listing any missing models.
func (c *OllamaClient) VerifyModels(ctx context.Context, models []string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("create tags request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama tags request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read tags response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama tags returned status %d: %s", resp.StatusCode, string(body))
	}

	var tagsResp TagsResponse
	if err := json.Unmarshal(body, &tagsResp); err != nil {
		return fmt.Errorf("unmarshal tags response: %w", err)
	}

	loaded := make(map[string]bool, len(tagsResp.Models))
	for _, m := range tagsResp.Models {
		loaded[m.Name] = true
		// Also register without the ":latest" tag so "llama3-8b" matches "llama3-8b:latest".
		if base, ok := strings.CutSuffix(m.Name, ":latest"); ok {
			loaded[base] = true
		}
	}

	var missing []string
	for _, required := range models {
		if !loaded[required] {
			missing = append(missing, required)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing models on Ollama server: %v", missing)
	}

	return nil
}

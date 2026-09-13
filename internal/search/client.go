// Package search performs web-search augmentation for inference jobs.
// v1 uses Tavily with a single, non-streaming query (the raw prompt).
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Source is one search result, in display/citation order (Position is 1-based).
type Source struct {
	Position int    `json:"position"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	Snippet  string `json:"snippet"`
}

// Searcher turns a query into ranked web results. Implemented by TavilyClient;
// the interface is the seam for tests and future providers.
type Searcher interface {
	Search(ctx context.Context, query string, maxResults int) ([]Source, error)
}

// TavilyClient calls the Tavily Search API (POST {baseURL}/search).
type TavilyClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewTavilyClient(baseURL, apiKey string, timeout time.Duration) *TavilyClient {
	return &TavilyClient{
		baseURL:    baseURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: timeout},
	}
}

type tavilyRequest struct {
	APIKey      string `json:"api_key"`
	Query       string `json:"query"`
	MaxResults  int    `json:"max_results"`
	SearchDepth string `json:"search_depth"`
}

type tavilyResponse struct {
	Results []struct {
		Title   string `json:"title"`
		URL     string `json:"url"`
		Content string `json:"content"`
	} `json:"results"`
}

func (c *TavilyClient) Search(ctx context.Context, query string, maxResults int) ([]Source, error) {
	body, err := json.Marshal(tavilyRequest{
		APIKey:      c.apiKey,
		Query:       query,
		MaxResults:  maxResults,
		SearchDepth: "basic",
	})
	if err != nil {
		return nil, fmt.Errorf("marshal tavily request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/search", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build tavily request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tavily request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tavily returned status %d", resp.StatusCode)
	}

	var parsed tavilyResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode tavily response: %w", err)
	}

	sources := make([]Source, 0, len(parsed.Results))
	for i, r := range parsed.Results {
		sources = append(sources, Source{
			Position: i + 1,
			Title:    r.Title,
			URL:      r.URL,
			Snippet:  r.Content,
		})
	}
	return sources, nil
}

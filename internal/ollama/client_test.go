package ollama

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/generate", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var req GenerateRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "llama3-8b", req.Model)
		assert.Equal(t, "Hello, world!", req.Prompt)
		assert.False(t, req.Stream)

		resp := GenerateResponse{
			Model:    "llama3-8b",
			Response: "Hi there!",
			Done:     true,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	result, err := client.Generate(context.Background(), "llama3-8b", "Hello, world!")
	require.NoError(t, err)
	assert.Equal(t, "Hi there!", result)
}

func TestGenerate_NonOKStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("model not found"))
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, err := client.Generate(context.Background(), "nonexistent", "hello")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
	assert.Contains(t, err.Error(), "model not found")
}

func TestGenerate_ContextCancelled(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block forever — context should cancel
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	_, err := client.Generate(ctx, "llama3-8b", "hello")
	require.Error(t, err)
}

func TestVerifyModels_AllPresent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/tags", r.URL.Path)

		resp := TagsResponse{
			Models: []ModelInfo{
				{Name: "llama3-8b"},
				{Name: "mistral-7b"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	err := client.VerifyModels(context.Background(), []string{"llama3-8b", "mistral-7b"})
	require.NoError(t, err)
}

func TestVerifyModels_SomeMissing(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := TagsResponse{
			Models: []ModelInfo{
				{Name: "llama3-8b"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	err := client.VerifyModels(context.Background(), []string{"llama3-8b", "mistral-7b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mistral-7b")
}

func TestVerifyModels_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	err := client.VerifyModels(context.Background(), []string{"llama3-8b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 503")
}

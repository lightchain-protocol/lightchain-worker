package ollama

import (
	"context"
	"encoding/json"
	"io"
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

func TestGenerateStream(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/generate", r.URL.Path)

		var req GenerateRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.True(t, req.Stream)

		w.Header().Set("Content-Type", "application/x-ndjson")
		io.WriteString(w, `{"response":"Hel","done":false}`+"\n")
		io.WriteString(w, `{"response":"lo","done":false}`+"\n")
		io.WriteString(w, `{"response":"","done":true}`+"\n")
	}))
	defer srv.Close()

	c := NewOllamaClient(srv.URL, 5*time.Second)
	var deltas []string
	full, err := c.GenerateStream(context.Background(), "llama3-8b", "hi", func(d string) { deltas = append(deltas, d) })
	require.NoError(t, err)
	assert.Equal(t, []string{"Hel", "lo"}, deltas)
	assert.Equal(t, "Hello", full)
}

func TestChatStream(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/chat", r.URL.Path)

		var req ChatRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.True(t, req.Stream)

		w.Header().Set("Content-Type", "application/x-ndjson")
		io.WriteString(w, `{"message":{"role":"assistant","content":"Hel"},"done":false}`+"\n")
		io.WriteString(w, `{"message":{"role":"assistant","content":"lo"},"done":false}`+"\n")
		io.WriteString(w, `{"message":{"role":"assistant","content":""},"done":true}`+"\n")
	}))
	defer srv.Close()

	c := NewOllamaClient(srv.URL, 5*time.Second)
	var deltas []string
	full, err := c.ChatStream(context.Background(), "llama3-8b", []ChatMessage{{Role: "user", Content: "hi"}}, func(d string) { deltas = append(deltas, d) })
	require.NoError(t, err)
	assert.Equal(t, []string{"Hel", "lo"}, deltas)
	assert.Equal(t, "Hello", full)
}

// TestDeterministicOptions verifies that every request path serialises
// "temperature":0 (not omitted) and "seed":42 so Ollama uses greedy decoding
// and the worker's output is reproducible by the disputer.
func TestDeterministicOptions(t *testing.T) {
	t.Parallel()

	assertOptions := func(t *testing.T, body []byte) {
		t.Helper()
		bodyStr := string(body)
		assert.Contains(t, bodyStr, `"temperature":0`, "temperature must be serialised as 0, not omitted")
		assert.Contains(t, bodyStr, `"seed":42`, "seed must be 42")
		assert.Contains(t, bodyStr, `"options"`, "options object must be present")
	}

	t.Run("Generate", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assertOptions(t, body)
			json.NewEncoder(w).Encode(GenerateResponse{Response: "ok", Done: true})
		}))
		defer srv.Close()
		c := NewOllamaClient(srv.URL, 5*time.Second)
		_, err := c.Generate(context.Background(), "llama3-8b", "hi")
		require.NoError(t, err)
	})

	t.Run("Chat", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assertOptions(t, body)
			json.NewEncoder(w).Encode(ChatResponse{Message: ChatMessage{Role: "assistant", Content: "ok"}, Done: true})
		}))
		defer srv.Close()
		c := NewOllamaClient(srv.URL, 5*time.Second)
		_, err := c.Chat(context.Background(), "llama3-8b", []ChatMessage{{Role: "user", Content: "hi"}})
		require.NoError(t, err)
	})

	t.Run("GenerateStream", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assertOptions(t, body)
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, `{"response":"ok","done":true}`+"\n")
		}))
		defer srv.Close()
		c := NewOllamaClient(srv.URL, 5*time.Second)
		_, err := c.GenerateStream(context.Background(), "llama3-8b", "hi", func(string) {})
		require.NoError(t, err)
	})

	t.Run("ChatStream", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assertOptions(t, body)
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, `{"message":{"role":"assistant","content":"ok"},"done":true}`+"\n")
		}))
		defer srv.Close()
		c := NewOllamaClient(srv.URL, 5*time.Second)
		_, err := c.ChatStream(context.Background(), "llama3-8b", []ChatMessage{{Role: "user", Content: "hi"}}, func(string) {})
		require.NoError(t, err)
	})

	t.Run("OptionsStructMarshal", func(t *testing.T) {
		t.Parallel()
		req := GenerateRequest{
			Model:   "llama3-8b",
			Prompt:  "test",
			Stream:  false,
			Options: Options{Temperature: 0, Seed: deterministicSeed},
		}
		b, err := json.Marshal(req)
		require.NoError(t, err)
		bodyStr := string(b)
		assert.Contains(t, bodyStr, `"temperature":0`, "temperature 0 must not be omitted")
		assert.Contains(t, bodyStr, `"seed":42`)
	})
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

func TestVerifyModels_LatestSuffix(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := TagsResponse{
			Models: []ModelInfo{
				{Name: "llama3-8b:latest"},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	err := client.VerifyModels(context.Background(), []string{"llama3-8b"})
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

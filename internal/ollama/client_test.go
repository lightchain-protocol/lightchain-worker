package ollama

import (
	"context"
	"encoding/json"
	"errors"
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

func TestGenerate_OmitsKeepAliveAndOptionsByDefault(t *testing.T) {
	t.Parallel()

	// NewOllamaClient must reproduce the pre-streaming request body: no
	// keep_alive, no options object at all.
	var raw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&raw))
		json.NewEncoder(w).Encode(GenerateResponse{Response: "ok", Done: true})
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, err := client.Generate(context.Background(), "llama3-8b", "hi")
	require.NoError(t, err)

	assert.NotContains(t, raw, "keep_alive")
	assert.NotContains(t, raw, "options")
}

func TestGenerate_SendsKeepAliveAndOptions(t *testing.T) {
	t.Parallel()

	var req GenerateRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		json.NewEncoder(w).Encode(GenerateResponse{Response: "ok", Done: true})
	}))
	defer srv.Close()

	client := NewOllamaClientWithOptions(srv.URL, 5*time.Second, ClientOptions{
		KeepAlive:  "-1",
		NumPredict: 1024,
		NumCtx:     4096,
	})
	_, err := client.Generate(context.Background(), "llama3-8b", "hi")
	require.NoError(t, err)

	// Ollama rejects a bare integer sent as a JSON string with
	// `time: missing unit in duration "-1"`, so an integral keep_alive must
	// travel as a number. It decodes back into the `any` field as float64.
	assert.Equal(t, float64(-1), req.KeepAlive)
	require.NotNil(t, req.Options)
	assert.Equal(t, 1024, req.Options.NumPredict)
	assert.Equal(t, 4096, req.Options.NumCtx)
}

func TestGenerate_UnsetNumCtxIsOmitted(t *testing.T) {
	t.Parallel()

	var raw struct {
		Options map[string]any `json:"options"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&raw))
		json.NewEncoder(w).Encode(GenerateResponse{Response: "ok", Done: true})
	}))
	defer srv.Close()

	client := NewOllamaClientWithOptions(srv.URL, 5*time.Second, ClientOptions{NumPredict: 512})
	_, err := client.Generate(context.Background(), "llama3-8b", "hi")
	require.NoError(t, err)

	require.NotNil(t, raw.Options)
	assert.Contains(t, raw.Options, "num_predict")
	assert.NotContains(t, raw.Options, "num_ctx",
		"num_ctx=0 means unset and must not override the model default")
}

// ndjsonServer replays lines as an NDJSON stream, flushing after each so a
// client that reads incrementally observes them one at a time.
func ndjsonServer(t *testing.T, path string, lines []string, capture func(body []byte)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, path, r.URL.Path)
		if capture != nil {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			capture(body)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, line := range lines {
			_, _ = w.Write([]byte(line + "\n"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
}

func TestGenerateStream_DeltasAndFullResponse(t *testing.T) {
	t.Parallel()

	var body []byte
	srv := ndjsonServer(t, "/api/generate", []string{
		`{"model":"llama3-8b","response":"Hello","done":false}`,
		`{"model":"llama3-8b","response":", ","done":false}`,
		`{"model":"llama3-8b","response":"world","done":false}`,
		`{"model":"llama3-8b","response":"!","done":false}`,
		`{"model":"llama3-8b","response":"","done":true,"total_duration":123456}`,
	}, func(b []byte) { body = b })
	defer srv.Close()

	client := NewOllamaClientWithOptions(srv.URL, 5*time.Second, ClientOptions{KeepAlive: "-1", NumPredict: 1024})

	var deltas []string
	full, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{
		OnToken: func(d string) error {
			deltas = append(deltas, d)
			return nil
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"Hello", ", ", "world", "!"}, deltas)
	assert.Equal(t, "Hello, world!", full,
		"the returned text must equal the concatenation of every delta")

	var req GenerateRequest
	require.NoError(t, json.Unmarshal(body, &req))
	assert.True(t, req.Stream, "GenerateStream must request stream=true")
	assert.Equal(t, float64(-1), req.KeepAlive)
	require.NotNil(t, req.Options)
	assert.Equal(t, 1024, req.Options.NumPredict)
}

func TestChatStream_DeltasFromMessageContent(t *testing.T) {
	t.Parallel()

	var body []byte
	srv := ndjsonServer(t, "/api/chat", []string{
		`{"message":{"role":"assistant","content":"2"},"done":false}`,
		`{"message":{"role":"assistant","content":"+"},"done":false}`,
		`{"message":{"role":"assistant","content":"2=4"},"done":false}`,
		`{"message":{"role":"assistant","content":""},"done":true}`,
	}, func(b []byte) { body = b })
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)

	var deltas []string
	full, _, err := client.ChatStream(context.Background(), "llama3-8b",
		[]ChatMessage{{Role: "user", Content: "what is 2+2?"}},
		StreamHandlers{
			OnToken: func(d string) error {
				deltas = append(deltas, d)
				return nil
			},
		})
	require.NoError(t, err)

	assert.Equal(t, []string{"2", "+", "2=4"}, deltas)
	assert.Equal(t, "2+2=4", full)

	var req ChatRequest
	require.NoError(t, json.Unmarshal(body, &req))
	assert.True(t, req.Stream)
	assert.Len(t, req.Messages, 1)
}

// A reasoning model streams its chain of thought in message.thinking while
// message.content stays empty. The worker only ever transmits content, so a
// generation that never leaves the thinking channel has produced no answer
// and must fail rather than settle on chain as zero bytes.
func TestChatStream_ThinkingOnlyIsAnError(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/chat", []string{
		`{"message":{"role":"assistant","content":"","thinking":"Let me work"},"done":false}`,
		`{"message":{"role":"assistant","content":"","thinking":" through this"},"done":false}`,
		`{"message":{"role":"assistant","content":""},"done":true}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)

	_, _, err := client.ChatStream(context.Background(), "qwen3.8-27b",
		[]ChatMessage{{Role: "user", Content: "hi"}}, StreamHandlers{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "thinking")
}

func TestChatStream_ThinkingAlongsideContentSucceeds(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/chat", []string{
		`{"message":{"role":"assistant","content":"","thinking":"pondering"},"done":false}`,
		`{"message":{"role":"assistant","content":"4"},"done":false}`,
		`{"message":{"role":"assistant","content":""},"done":true}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)

	full, _, err := client.ChatStream(context.Background(), "qwen3.8-27b",
		[]ChatMessage{{Role: "user", Content: "2+2?"}}, StreamHandlers{})

	require.NoError(t, err)
	assert.Equal(t, "4", full, "thinking must never be folded into the answer")
}

func TestStream_EmptyGenerationIsAnError(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/generate", []string{
		`{"model":"llama3-8b","response":"","done":true}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)

	_, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty response")
}

// think is omitted entirely when unset, so an operator who has not opted in
// gets byte-identical request bodies to the pre-think client.
func TestThink_OmittedWhenUnset(t *testing.T) {
	t.Parallel()

	var raw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&raw))
		json.NewEncoder(w).Encode(GenerateResponse{Response: "ok", Done: true})
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, err := client.Generate(context.Background(), "llama3-8b", "hi")
	require.NoError(t, err)

	assert.NotContains(t, raw, "think")
}

func TestThink_FalseIsSentExplicitly(t *testing.T) {
	t.Parallel()

	var raw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&raw))
		json.NewEncoder(w).Encode(ChatResponse{
			Message: ChatMessage{Role: "assistant", Content: "ok"},
			Done:    true,
		})
	}))
	defer srv.Close()

	client := NewOllamaClientWithOptions(srv.URL, 5*time.Second, ClientOptions{
		Think: ThinkSetting(false),
	})
	_, err := client.Chat(context.Background(), "qwen3.8-27b",
		[]ChatMessage{{Role: "user", Content: "hi"}})
	require.NoError(t, err)

	require.Contains(t, raw, "think")
	assert.Equal(t, false, raw["think"])
}

func TestGenerateStream_StopsAtDone(t *testing.T) {
	t.Parallel()

	// Anything after the done=true object must be ignored.
	srv := ndjsonServer(t, "/api/generate", []string{
		`{"response":"a","done":false}`,
		`{"response":"b","done":true}`,
		`{"response":"SHOULD-NOT-APPEAR","done":false}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	full, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{})
	require.NoError(t, err)
	assert.Equal(t, "ab", full)
}

func TestGenerateStream_TruncatedStreamIsAnError(t *testing.T) {
	t.Parallel()

	// No done=true object: the generation was cut short. Returning a
	// partial answer would anchor half a response on-chain, so this must
	// fail and let the job retry.
	srv := ndjsonServer(t, "/api/generate", []string{
		`{"response":"partial","done":false}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ended before done")
}

func TestGenerateStream_ServerErrorLine(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/generate", []string{
		`{"error":"model requires more system memory"}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more system memory")
}

func TestGenerateStream_NonOKStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("model not found"))
	}))
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, _, err := client.GenerateStream(context.Background(), "nope", "hi", nil, StreamHandlers{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
	assert.Contains(t, err.Error(), "model not found")
}

func TestGenerateStream_CallbackErrorAborts(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/generate", []string{
		`{"response":"a","done":false}`,
		`{"response":"b","done":false}`,
		`{"response":"","done":true}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	calls := 0
	_, _, err := client.GenerateStream(context.Background(), "llama3-8b", "hi", nil, StreamHandlers{
		OnToken: func(string) error {
			calls++
			return errors.New("consumer went away")
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer went away")
	assert.Equal(t, 1, calls, "the stream must abort on the first callback error")
}

// Reasoning deltas reach OnThinking, and never leak into the returned answer.
// That separation is what keeps the settlement ciphertext stable: the string
// returned here is the one hashed into completeJob.
func TestChatStream_ThinkingReachesItsOwnCallback(t *testing.T) {
	t.Parallel()

	srv := ndjsonServer(t, "/api/chat", []string{
		`{"message":{"role":"assistant","content":"","thinking":"first "},"done":false}`,
		`{"message":{"role":"assistant","content":"","thinking":"second"},"done":false}`,
		`{"message":{"role":"assistant","content":"answer"},"done":false}`,
		`{"message":{"role":"assistant","content":""},"done":true,"eval_count":12,"eval_duration":600000000,"prompt_eval_count":5}`,
	}, nil)
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)

	var thinking, content []string
	full, stats, err := client.ChatStream(context.Background(), "deepseek-r1-32b",
		[]ChatMessage{{Role: "user", Content: "hi"}},
		StreamHandlers{
			OnToken:    func(d string) error { content = append(content, d); return nil },
			OnThinking: func(d string) error { thinking = append(thinking, d); return nil },
		})

	require.NoError(t, err)
	assert.Equal(t, []string{"first ", "second"}, thinking)
	assert.Equal(t, []string{"answer"}, content)
	assert.Equal(t, "answer", full, "reasoning must not enter the settled response")
	assert.Equal(t, 12, stats.EvalTokens)
	assert.Equal(t, 5, stats.PromptTokens)
	assert.InDelta(t, 20.0, stats.TokensPerSecond(), 0.1)
}

// A vision prompt puts base64 image data on the final user turn.
func TestChatStream_ImagesRideOnTheMessage(t *testing.T) {
	t.Parallel()

	var body []byte
	srv := ndjsonServer(t, "/api/chat", []string{
		`{"message":{"role":"assistant","content":"a cat"},"done":true}`,
	}, func(b []byte) { body = b })
	defer srv.Close()

	client := NewOllamaClient(srv.URL, 5*time.Second)
	_, _, err := client.ChatStream(context.Background(), "qwen3-vl-8b",
		[]ChatMessage{{Role: "user", Content: "what is this?", Images: []string{"aGVsbG8="}}},
		StreamHandlers{})
	require.NoError(t, err)

	var req ChatRequest
	require.NoError(t, json.Unmarshal(body, &req))
	require.Len(t, req.Messages, 1)
	assert.Equal(t, []string{"aGVsbG8="}, req.Messages[0].Images)
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

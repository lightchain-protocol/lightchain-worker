package pipeline

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/ollama"
)

// inferenceClientFor is the presence-gated enforcement point for
// MODEL_OPTIONS: a listed model runs on an overridden client, everything
// else gets the shared client unchanged.
func TestInferenceClientFor_AppliesPerModelOverride(t *testing.T) {
	base := ollama.NewOllamaClientWithOptions("http://unused", 5*time.Second, ollama.ClientOptions{
		KeepAlive:  "-1",
		NumPredict: 1024,
	})
	temp := 0.6
	h := &JobHandler{
		ollamaClient: base,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: HandlerConfig{
			ModelOptions: map[string]ollama.ClientOptions{
				"agentworld-35b-max": {NumPredict: 8192, Temperature: &temp},
			},
		},
	}

	client, opts, applied := h.inferenceClientFor(h.logger, "agentworld-35b-max")
	require.True(t, applied)
	assert.Equal(t, 8192, opts.NumPredict)
	assert.Equal(t, "-1", opts.KeepAlive, "unlisted knobs carry over from the base client")
	require.NotNil(t, opts.Temperature)
	assert.InDelta(t, 0.6, *opts.Temperature, 1e-9)

	// The returned client is a copy: the shared base still serves standard
	// jobs at the process default.
	assert.Equal(t, 1024, base.Options().NumPredict,
		"a Max job must not raise the cap for subsequent standard jobs")
	assert.NotSame(t, base, client)
}

func TestInferenceClientFor_NoEntryKeepsSharedClient(t *testing.T) {
	base := ollama.NewOllamaClientWithOptions("http://unused", 5*time.Second, ollama.ClientOptions{
		NumPredict: 1024,
	})
	h := &JobHandler{
		ollamaClient: base,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: HandlerConfig{
			ModelOptions: map[string]ollama.ClientOptions{
				"agentworld-35b-max": {NumPredict: 8192},
			},
		},
	}

	client, opts, applied := h.inferenceClientFor(h.logger, "agentworld-35b")
	assert.False(t, applied)
	assert.Same(t, base, client, "no entry: the shared client is used unchanged")
	assert.Equal(t, 1024, opts.NumPredict,
		"effective options still reported for the stats-frame audit anchor")
}

// A nil map is the "MODEL_OPTIONS unset" case: every model takes the shared
// client and no override bookkeeping happens at all.
func TestInferenceClientFor_NilMapIsByteIdenticalPath(t *testing.T) {
	base := ollama.NewOllamaClientWithOptions("http://unused", 5*time.Second, ollama.ClientOptions{
		NumPredict: 1024,
	})
	h := &JobHandler{
		ollamaClient: base,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	client, opts, applied := h.inferenceClientFor(h.logger, "agentworld-35b-max")
	assert.False(t, applied)
	assert.Same(t, base, client)
	assert.Equal(t, 1024, opts.NumPredict)
}

package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/lightchain/worker/internal/search"
)

func TestBuildSearchAugmentedPrompt_FixedFormat(t *testing.T) {
	sources := []search.Source{
		{Position: 1, Title: "T1", URL: "https://a", Snippet: "s1"},
		{Position: 2, Title: "T2", URL: "https://b", Snippet: "s2"},
	}
	out := buildSearchAugmentedPrompt("original question", sources)
	assert.True(t, strings.Contains(out, "original question"))
	assert.True(t, strings.Contains(out, "https://a"))
	assert.True(t, strings.Contains(out, "[1]"))
	// Deterministic: same inputs → identical output (v2 re-execution depends on this).
	assert.Equal(t, out, buildSearchAugmentedPrompt("original question", sources))
}

func TestSourcesMetadataPayload_ShapeMatchesFrontend(t *testing.T) {
	sources := []search.Source{{Position: 1, Title: "T1", URL: "https://a", Snippet: "s1"}}
	payload := sourcesMetadataJSON(sources)
	var decoded struct {
		Type    string `json:"type"`
		Sources []struct {
			Position int    `json:"position"`
			Title    string `json:"title"`
			URL      string `json:"url"`
			Snippet  string `json:"snippet"`
		} `json:"sources"`
	}
	require.NoError(t, json.Unmarshal(payload, &decoded))
	assert.Equal(t, "webSearchSources", decoded.Type)
	require.Len(t, decoded.Sources, 1)
	assert.Equal(t, "https://a", decoded.Sources[0].URL)
}

// fakeSearcher is a test double for search.Searcher.
type fakeSearcher struct {
	results []search.Source
	err     error
}

func (f *fakeSearcher) Search(_ context.Context, _ string, _ int) ([]search.Source, error) {
	return f.results, f.err
}

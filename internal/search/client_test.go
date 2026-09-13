package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTavilyClient_Search(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/search", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[
			{"title":"T1","url":"https://a.example","content":"snippet one"},
			{"title":"T2","url":"https://b.example","content":"snippet two"}
		]}`))
	}))
	defer srv.Close()

	c := NewTavilyClient(srv.URL, "tvly-test", 5*time.Second)
	got, err := c.Search(context.Background(), "what is lightchain", 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, 1, got[0].Position)
	assert.Equal(t, "T1", got[0].Title)
	assert.Equal(t, "https://a.example", got[0].URL)
	assert.Equal(t, "snippet one", got[0].Snippet)
	assert.Equal(t, 2, got[1].Position)
}

func TestTavilyClient_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := NewTavilyClient(srv.URL, "tvly-test", 5*time.Second)
	_, err := c.Search(context.Background(), "q", 3)
	require.Error(t, err)
}

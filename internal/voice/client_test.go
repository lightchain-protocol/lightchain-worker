package voice

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

func TestTranscribe_Success(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/transcribe", r.URL.Path)
		assert.Equal(t, "audio/wav", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, []byte("RIFF fake wav"), body)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"text":"what is the weather today","language":"en","duration_s":2.1}`))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "", 5*time.Second, 0)
	text, err := c.Transcribe(context.Background(), []byte("RIFF fake wav"), "wav")
	require.NoError(t, err)
	assert.Equal(t, "what is the weather today", text)
}

func TestTranscribe_NonWAVFailsFast(t *testing.T) {
	t.Parallel()

	c := New("http://127.0.0.1:1", "", 5*time.Second, 0) // unroutable; must not be dialed
	_, err := c.Transcribe(context.Background(), []byte("not wav"), "mp3")
	require.ErrorIs(t, err, ErrBadAudio, "non-WAV containers are undecodable per the sidecar contract")
}

func TestTranscribe_EmptyBodyFailsFast(t *testing.T) {
	t.Parallel()

	c := New("http://127.0.0.1:1", "", 5*time.Second, 0)
	_, err := c.Transcribe(context.Background(), nil, "wav")
	require.ErrorIs(t, err, ErrBadAudio)
}

func TestTranscribe_StatusClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status        int
		deterministic bool
	}{
		{http.StatusBadRequest, true},            // bad/empty body
		{http.StatusRequestEntityTooLarge, true}, // over 25 MB
		{http.StatusUnprocessableEntity, true},   // undecodable audio
		{http.StatusInternalServerError, false},  // inference failure
		{http.StatusServiceUnavailable, false},   // model still loading
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, `{"error":"x"}`, tc.status)
		}))

		c := New(srv.URL, "", 5*time.Second, 0)
		_, err := c.Transcribe(context.Background(), []byte("wav"), "wav")
		require.Error(t, err, "status %d", tc.status)
		if tc.deterministic {
			assert.ErrorIs(t, err, ErrBadAudio, "status %d must be deterministic", tc.status)
		} else {
			assert.NotErrorIs(t, err, ErrBadAudio, "status %d must stay retryable", tc.status)
		}
		srv.Close()
	}
}

func TestTranscribe_TransportErrorIsRetryable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	url := srv.URL
	srv.Close()

	c := New(url, "", time.Second, 0)
	_, err := c.Transcribe(context.Background(), []byte("wav"), "wav")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrBadAudio)
}

func TestSynthesize_StreamsPCM(t *testing.T) {
	t.Parallel()

	pcm := []byte("raw-pcm-s16le-chunked-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/tts", r.URL.Path)

		var req ttsRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "hello out there", req.Text)
		assert.Equal(t, "af_heart", req.Voice)
		assert.Equal(t, 1.0, req.Speed)
		assert.True(t, req.Stream, "TTS must always request the streaming PCM contract")

		w.Header().Set("Content-Type", AudioMIME)
		w.Header().Set("X-Format", "pcm-s16le")
		_, _ = w.Write(pcm)
	}))
	t.Cleanup(srv.Close)

	c := New("", srv.URL, 0, 5*time.Second)
	stream, err := c.Synthesize(context.Background(), "hello out there", "af_heart")
	require.NoError(t, err)
	defer stream.Close()

	got, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, pcm, got)
}

func TestSynthesize_StatusClassification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		status        int
		deterministic bool
	}{
		{http.StatusBadRequest, true},            // unknown voice
		{http.StatusRequestEntityTooLarge, true}, // text over 4000 chars
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, `{"error":"x"}`, tc.status)
		}))

		c := New("", srv.URL, 0, 5*time.Second)
		stream, err := c.Synthesize(context.Background(), "hi", "af_heart")
		require.Error(t, err, "status %d", tc.status)
		assert.Nil(t, stream)
		if tc.deterministic {
			assert.ErrorIs(t, err, ErrBadAudio, "status %d must be deterministic", tc.status)
		} else {
			assert.NotErrorIs(t, err, ErrBadAudio, "status %d must stay retryable", tc.status)
		}
		srv.Close()
	}
}

func TestSynthesize_NoURLConfigured(t *testing.T) {
	t.Parallel()

	c := New("http://127.0.0.1:8100", "", 5*time.Second, 5*time.Second)
	_, err := c.Synthesize(context.Background(), "hi", "af_heart")
	require.ErrorIs(t, err, ErrBadAudio)
}

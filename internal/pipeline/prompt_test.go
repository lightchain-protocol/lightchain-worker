package pipeline

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodePrompt_PlainTextPassesThrough(t *testing.T) {
	t.Parallel()

	env, err := decodePrompt([]byte("what is 2+2?"))
	require.NoError(t, err)
	assert.Equal(t, "what is 2+2?", env.Text)
	assert.Empty(t, env.Images)
}

// A prompt that happens to be valid JSON is still a prompt. Reading it as an
// envelope would silently replace the user's question with an empty string.
func TestDecodePrompt_JSONWithoutVersionIsText(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		`{"text":"not an envelope"}`,
		`{"v":0,"text":"version zero is not an envelope"}`,
		`["a","json","array"]`,
		`{"question":"why is my JSON being eaten?"}`,
	} {
		env, err := decodePrompt([]byte(raw))
		require.NoError(t, err)
		assert.Equal(t, raw, env.Text, "raw JSON must survive as text: %s", raw)
		assert.Empty(t, env.Images)
	}
}

func TestDecodePrompt_VersionedEnvelope(t *testing.T) {
	t.Parallel()

	env, err := decodePrompt([]byte(`{"v":1,"text":"what is in this image?","images":["aGVsbG8="]}`))
	require.NoError(t, err)
	assert.Equal(t, 1, env.Version)
	assert.Equal(t, "what is in this image?", env.Text)
	assert.Equal(t, []string{"aGVsbG8="}, env.Images)
}

func TestDecodePrompt_TooManyImagesIsRejected(t *testing.T) {
	t.Parallel()

	imgs := make([]string, 0, maxPromptImages+1)
	for i := 0; i <= maxPromptImages; i++ {
		imgs = append(imgs, `"aGk="`)
	}
	raw := `{"v":1,"text":"x","images":[` + strings.Join(imgs, ",") + `]}`

	_, err := decodePrompt([]byte(raw))
	require.ErrorIs(t, err, errTooManyImages)
}

// bytes counts image payloads, which dominate a multimodal prompt and are the
// reason a vision job can approach the blob size ceiling.
func TestPromptEnvelope_BytesCountsImages(t *testing.T) {
	t.Parallel()

	env := promptEnvelope{Text: "abc", Images: []string{"1234", "56"}}
	assert.Equal(t, 9, env.bytes())
}

// Envelope v2 adds the voice fields. decodePrompt must accept it on any
// version >= 1 so an old worker can still answer a v2 prompt (the voice
// fields silently drop) and a new worker does not reject version bumps.
func TestDecodePrompt_V2VoiceEnvelope(t *testing.T) {
	t.Parallel()

	env, err := decodePrompt([]byte(`{"v":2,"text":"","audio":"UklGRg==","audioFormat":"wav","audioResponse":true,"voice":"af_heart"}`))
	require.NoError(t, err)
	assert.Equal(t, 2, env.Version)
	assert.Equal(t, "UklGRg==", env.Audio)
	assert.Equal(t, "wav", env.AudioFormat)
	assert.True(t, env.AudioResponse)
	assert.Equal(t, "af_heart", env.Voice)
}

// A v1 envelope decodes identically on the v2-aware struct: all voice fields
// stay zero-valued.
func TestDecodePrompt_V1EnvelopeHasNoVoiceFields(t *testing.T) {
	t.Parallel()

	env, err := decodePrompt([]byte(`{"v":1,"text":"hello"}`))
	require.NoError(t, err)
	assert.Empty(t, env.Audio)
	assert.Empty(t, env.AudioFormat)
	assert.False(t, env.AudioResponse)
	assert.Empty(t, env.Voice)
}

// An oversized audio payload is rejected outright rather than truncated -
// a clipped clip would transcribe to words the consumer never said.
func TestDecodePrompt_AudioTooLargeIsRejected(t *testing.T) {
	t.Parallel()

	raw := `{"v":2,"audio":"` + strings.Repeat("QQ==", maxPromptAudioB64/4+1) + `"}`

	_, err := decodePrompt([]byte(raw))
	require.ErrorIs(t, err, errAudioTooLarge)
}

func TestPromptEnvelope_BytesCountsAudio(t *testing.T) {
	t.Parallel()

	env := promptEnvelope{Text: "abc", Audio: "12345678"}
	assert.Equal(t, 11, env.bytes())
}

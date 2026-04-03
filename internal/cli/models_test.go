package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAndDeduplicateModelIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		models    []string
		wantCount int
		wantErr   string
	}{
		{
			name:      "valid names",
			models:    []string{"llama3-8b", "mistral-7b"},
			wantCount: 2,
		},
		{
			name:      "valid hex",
			models:    []string{"0x" + strings.Repeat("aa", 32)},
			wantCount: 1,
		},
		{
			name:      "mixed name and hex",
			models:    []string{"llama3-8b", "0x" + strings.Repeat("bb", 32)},
			wantCount: 2,
		},
		{
			name:    "duplicate names",
			models:  []string{"llama3-8b", "llama3-8b"},
			wantErr: "duplicate",
		},
		{
			name:    "invalid hex — wrong length",
			models:  []string{"0xzz"},
			wantErr: "ParseModelID",
		},
		{
			name:    "invalid hex — bad chars",
			models:  []string{"0x" + strings.Repeat("gg", 32)},
			wantErr: "invalid hex",
		},
		{
			name:      "empty list",
			models:    []string{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ids, err := ParseAndDeduplicateModelIDs(tt.models)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Len(t, ids, tt.wantCount)

			// Verify all IDs are distinct
			seen := make(map[[32]byte]bool)
			for _, id := range ids {
				assert.False(t, seen[id], "duplicate ID in output")
				seen[id] = true
			}
		})
	}
}

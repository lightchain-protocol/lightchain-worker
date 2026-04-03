package cli

import (
	"encoding/hex"
	"fmt"

	"github.com/lightchain/worker/internal/config"
)

// ParseAndDeduplicateModelIDs converts model name/hex strings to bytes32 IDs,
// rejecting duplicates. Returns the IDs in input order.
func ParseAndDeduplicateModelIDs(models []string) ([][32]byte, error) {
	ids := make([][32]byte, 0, len(models))
	seen := make(map[[32]byte]bool, len(models))

	for _, s := range models {
		id, err := config.ParseModelID(s)
		if err != nil {
			return nil, fmt.Errorf("parse model %q: %w", s, err)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate model ID for %q (0x%s)", s, hex.EncodeToString(id[:]))
		}
		seen[id] = true
		ids = append(ids, id)
	}

	return ids, nil
}

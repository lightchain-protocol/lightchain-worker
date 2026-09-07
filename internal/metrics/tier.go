package metrics

import "strings"

// Heat-tier label values for the {tier} metric label. v1 has exactly two
// tiers; the catalog sanity layer (provisioning/tier-catalog.json) validates
// that on-chain modelIds follow the suffix convention this helper encodes.
const (
	TierMax      = "max"
	TierStandard = "standard"
)

// maxTierSuffix marks a model as belonging to the Max tier. Tiers are
// distinct modelIds (owner decision A3); the convention is that Max modelIds
// carry this suffix, e.g. "agentworld-35b-max".
const maxTierSuffix = "-max"

// TierForModel applies the heat-tier extraction rule (DO-2 heat-tier sprint
// §0): a model name ending in "-max" (case-insensitive) is tier "max" and
// its base model is the name with the suffix stripped; every other name is
// tier "standard" with the name itself as base. The dispatcher and watcher
// implement the same rule — do not diverge.
//
// Input is expected to be an already-normalized {model} label value (see
// NormalizeModel); the function lowercases defensively so a raw name passed
// by mistake still classifies correctly. The unknown-model fallback does not
// end in the suffix and therefore labels "standard".
func TierForModel(model string) (tier, base string) {
	m := strings.ToLower(strings.TrimSpace(model))
	if strings.HasSuffix(m, maxTierSuffix) {
		return TierMax, strings.TrimSuffix(m, maxTierSuffix)
	}
	return TierStandard, m
}

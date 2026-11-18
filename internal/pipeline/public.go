package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lightchain/pkg/promptenv"
	"github.com/lightchain/pkg/publicresp"

	"github.com/lightchain/worker/internal/ollama"
)

// ErrPublicOutputDisabled is returned for a public-output job on a worker
// configured with PUBLIC_OUTPUT=false. The job is refused without retry: an
// encrypted answer would never match what the requester can deliver.
var ErrPublicOutputDisabled = errors.New("public output is disabled on this worker")

// publicJob is what stage 5 and stage 6 need for an envelope v4 public-output
// job: the pinned inference client and the hashes that go into R.
type publicJob struct {
	client     *ollama.OllamaClient
	modelName  string
	params     promptenv.Params
	hasFormat  bool
	promptHash string
	formatHash string
}

// preparePublicJob checks a public-output envelope against this worker's
// limits and builds the pinned client the job must run on. plainEnvelope is
// the decrypted prompt exactly as the blob carried it: R.promptHash commits
// to those bytes so the disputer can tell a worker that answered a different
// prompt.
func (h *JobHandler) preparePublicJob(
	env *promptenv.Envelope,
	plainEnvelope []byte,
	searchEnabled bool,
	modelName string,
) (*publicJob, error) {
	switch {
	case !h.cfg.PublicOutput:
		return nil, ErrPublicOutputDisabled
	case searchEnabled:
		return nil, errors.New("public output is not supported for search jobs")
	case env.Params == nil:
		// Decode guarantees this; kept so the method is safe on its own.
		return nil, errors.New("public output needs params")
	case h.cfg.PublicMaxNumCtx > 0 && env.Params.NumCtx > h.cfg.PublicMaxNumCtx:
		return nil, fmt.Errorf("numCtx %d exceeds this worker's limit %d", env.Params.NumCtx, h.cfg.PublicMaxNumCtx)
	case h.cfg.PublicMaxNumPredict > 0 && env.Params.NumPredict > h.cfg.PublicMaxNumPredict:
		return nil, fmt.Errorf("numPredict %d exceeds this worker's limit %d", env.Params.NumPredict, h.cfg.PublicMaxNumPredict)
	}
	oc, ok := h.ollamaClient.(overridingInferenceClient)
	if !ok {
		return nil, errors.New("the inference client cannot pin generation parameters")
	}
	format, err := env.CanonicalFormat()
	if err != nil {
		return nil, fmt.Errorf("format: %w", err)
	}
	formatHash, err := publicresp.FormatHash(format)
	if err != nil {
		return nil, err
	}

	params := *env.Params
	client := oc.WithOverrides(func(o *ollama.ClientOptions) {
		// Exactly the profile R echoes (draft 04 §4.2): the envelope's
		// sizes, greedy decoding, the fixed seed, no thinking. Operator
		// MODEL_OPTIONS do not apply to a public job.
		o.NumCtx = params.NumCtx
		o.NumPredict = params.NumPredict
		greedy := 0.0
		o.Temperature = &greedy
		o.Think = ollama.ThinkSetting(false)
		o.ThinkLevel = ""
		if format != nil {
			o.Format = json.RawMessage(format)
		} else {
			o.Format = nil
		}
	})
	return &publicJob{
		client:     client,
		modelName:  modelName,
		params:     params,
		hasFormat:  format != nil,
		promptHash: publicresp.PromptHash(plainEnvelope),
		formatHash: formatHash,
	}, nil
}

// encode builds R from the engine's text. The bytes returned are what the
// job commits: the terminal frame carries them, the blob anchors them and
// completeJob hashes them.
func (p *publicJob) encode(text string) ([]byte, error) {
	output, answer, valid := publicresp.FromEngineText(text, p.hasFormat)
	return publicresp.Encode(publicresp.Response{
		V:          publicresp.Version,
		PromptHash: p.promptHash,
		FormatHash: p.formatHash,
		Params: publicresp.Params{
			Model:       p.modelName,
			NumCtx:      p.params.NumCtx,
			NumPredict:  p.params.NumPredict,
			Seed:        ollama.DeterministicSeed,
			Temperature: 0,
		},
		Valid:  valid,
		Output: output,
		Answer: answer,
	})
}

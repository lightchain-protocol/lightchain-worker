package pipeline

import "github.com/lightchain/pkg/promptenv"

// promptEnvelope is the decrypted prompt payload. Decoding lives in the
// shared package so the disputer re-runs a job from exactly the input the
// worker served it with.
type promptEnvelope = promptenv.Envelope

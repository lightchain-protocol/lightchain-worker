package blob

import pkgblob "github.com/lightchain/pkg/blob"

// RedisBlobSubmitter is re-exported from pkg/blob for backwards compatibility.
type RedisBlobSubmitter = pkgblob.RedisBlobSubmitter

// NewRedisBlobSubmitter creates a Redis-backed blob submitter for local dev.
var NewRedisBlobSubmitter = pkgblob.NewRedisBlobSubmitter

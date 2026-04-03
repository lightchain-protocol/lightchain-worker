package blob

import pkgblob "github.com/lightchain/pkg/blob"

// RedisBlobFetcher is re-exported from pkg/blob for backwards compatibility.
type RedisBlobFetcher = pkgblob.RedisBlobFetcher

// NewRedisBlobFetcher creates a Redis-backed blob fetcher for local dev.
var NewRedisBlobFetcher = pkgblob.NewRedisBlobFetcher

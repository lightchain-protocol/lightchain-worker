package blob

// Compile-time assertion that BeaconClient satisfies BlobFetcher.
var _ BlobFetcher = (*BeaconClient)(nil)

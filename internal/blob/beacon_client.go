package blob

import (
	pkgblob "github.com/lightchain/pkg/blob"
)

// BlobFetcher is re-exported from pkg/blob.
type BlobFetcher = pkgblob.BlobFetcher

// BeaconClient is re-exported from pkg/blob.
type BeaconClient = pkgblob.BeaconClient

// NewBeaconClient creates a BeaconClient for fetching blob data.
var NewBeaconClient = pkgblob.NewBeaconClient

package blob

import (
	"time"

	pkgblob "github.com/lightchain/pkg/blob"
)

// BlobFetcher is re-exported from pkg/blob.
type BlobFetcher = pkgblob.BlobFetcher

// BeaconClient is re-exported from pkg/blob.
type BeaconClient = pkgblob.BeaconClient

// NewBeaconClient creates a BeaconClient for fetching blob data.
func NewBeaconClient(beaconURL string, elFetcher pkgblob.ELBlockFetcher, timeout time.Duration, maxRetries int) *BeaconClient {
	return pkgblob.NewBeaconClient(beaconURL, elFetcher, timeout, maxRetries)
}

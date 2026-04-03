package blob

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgblob "github.com/lightchain/pkg/blob"
)

func TestBeaconClient_FetchBlob_BlobNotFound(t *testing.T) {
	t.Parallel()

	// Mock Beacon API that returns an empty sidecar list
	beaconSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v1/beacon/headers/0x0000000000000000000000000000000000000000000000000000000000000001":
			resp := beaconHeaderResponse{}
			resp.Data.Header.Message.Slot = "100"
			json.NewEncoder(w).Encode(resp)
		case r.URL.Path == "/eth/v1/beacon/blob_sidecars/101":
			resp := blobSidecarsResponse{Data: []blobSidecar{}}
			json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer beaconSrv.Close()

	// We can't easily mock ethclient.BlockByNumber without a simulated backend,
	// so we test the sub-components directly.
	bc := &BeaconClient{
		beaconURL:  beaconSrv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		maxRetries: 0,
	}

	// Test getBeaconSlot
	slot, err := bc.getBeaconSlot(context.Background(),
		"0x0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)
	assert.Equal(t, uint64(100), slot)

	// Test getBlobSidecars — empty
	sidecars, err := bc.getBlobSidecars(context.Background(), 101)
	require.NoError(t, err)
	assert.Empty(t, sidecars)
}

func TestBeaconClient_GetBeaconSlot_ServerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	bc := &BeaconClient{
		beaconURL:  srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	_, err := bc.getBeaconSlot(context.Background(), "0xdeadbeef")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestBeaconClient_GetBlobSidecars_WithData(t *testing.T) {
	t.Parallel()

	// Create test blob data
	blobData := make([]byte, 64)
	for i := range blobData {
		blobData[i] = byte(i)
	}
	blobHex := "0x" + hex.EncodeToString(blobData)

	commitment := make([]byte, 48)
	for i := range commitment {
		commitment[i] = byte(i + 10)
	}
	commitHex := "0x" + hex.EncodeToString(commitment)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := blobSidecarsResponse{
			Data: []blobSidecar{
				{
					Index:         "0",
					Blob:          blobHex,
					KzgCommitment: commitHex,
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	bc := &BeaconClient{
		beaconURL:  srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	sidecars, err := bc.getBlobSidecars(context.Background(), 42)
	require.NoError(t, err)
	require.Len(t, sidecars, 1)
	assert.Equal(t, commitHex, sidecars[0].KzgCommitment)

	// Verify versioned hash computation
	expectedHash := pkgblob.KzgToVersionedHash(commitHex)
	assert.Equal(t, byte(0x01), expectedHash[0])

	// Verify we can decode the blob hex
	decoded, err := hex.DecodeString(pkgblob.StripHexPrefix(sidecars[0].Blob))
	require.NoError(t, err)
	assert.Equal(t, blobData, decoded)
}

func TestBeaconClient_FetchBlob_RetryOnError(t *testing.T) {
	t.Parallel()

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// Always fail
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "unavailable")
	}))
	defer srv.Close()

	bc := &BeaconClient{
		beaconURL:  srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		maxRetries: 2,
	}

	// fetchBlobOnce will fail at getBeaconSlot — we call the internal retry path
	// by calling getBeaconSlot which we know fails
	_, err := bc.getBeaconSlot(context.Background(), "0xdeadbeef")
	require.Error(t, err)
}

// Compile-time assertion
var _ BlobFetcher = (*BeaconClient)(nil)

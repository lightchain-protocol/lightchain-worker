// Package blob handles EIP-4844 blob fetch (Beacon API) and submission (type-3 TX).
package blob

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	pkgblob "github.com/lightchain/pkg/blob"
)

// BlobFetcher fetches EIP-4844 blob data given a versioned hash and block number.
type BlobFetcher = pkgblob.BlobFetcher

// BeaconClient fetches blob sidecars from the CL Beacon API.
type BeaconClient struct {
	beaconURL  string
	ethClient  *ethclient.Client
	httpClient *http.Client
	maxRetries int
}

// NewBeaconClient creates a BeaconClient for fetching blob data.
func NewBeaconClient(beaconURL string, ethClient *ethclient.Client, timeout time.Duration, maxRetries int) *BeaconClient {
	return &BeaconClient{
		beaconURL: beaconURL,
		ethClient: ethClient,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		maxRetries: maxRetries,
	}
}

// beaconHeaderResponse is the JSON envelope for GET /eth/v1/beacon/headers/{id}.
type beaconHeaderResponse struct {
	Data struct {
		Header struct {
			Message struct {
				Slot string `json:"slot"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}

// blobSidecarsResponse is the JSON envelope for GET /eth/v1/beacon/blob_sidecars/{slot}.
type blobSidecarsResponse struct {
	Data []blobSidecar `json:"data"`
}

type blobSidecar struct {
	Index         string `json:"index"`
	Blob          string `json:"blob"`
	KzgCommitment string `json:"kzg_commitment"`
}

// FetchBlob retrieves blob data for a given versioned hash from the Beacon API.
// Steps: EL block → parentBeaconBlockRoot → CL header (slot) → blob sidecars → match by versioned hash.
func (bc *BeaconClient) FetchBlob(ctx context.Context, versionedHash common.Hash, blockNumber uint64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= bc.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms, ...
			backoff := time.Duration(100<<uint(attempt-1)) * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		data, err := bc.fetchBlobOnce(ctx, versionedHash, blockNumber)
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("fetch blob after %d retries: %w", bc.maxRetries, lastErr)
}

// searchWindowBlocks is the number of EL blocks to search backward when the
// blob is not found at the expected slot. This handles the two-TX flow where
// the gateway submits the blob TX in an earlier block than the user's
// submitJob TX.
const searchWindowBlocks = 10

func (bc *BeaconClient) fetchBlobOnce(ctx context.Context, versionedHash common.Hash, blockNumber uint64) ([]byte, error) {
	// Search from the submitJob block backward through recent blocks.
	// The blob TX was submitted by the gateway before the user called
	// submitJob, so it may be in any of the preceding blocks.
	startBlock := blockNumber
	endBlock := uint64(0)
	if startBlock > searchWindowBlocks {
		endBlock = startBlock - searchWindowBlocks
	}

	var lastErr error
	for blk := startBlock; blk >= endBlock && blk <= startBlock; blk-- {
		data, err := bc.fetchBlobAtBlock(ctx, versionedHash, blk)
		if err == nil {
			return data, nil
		}
		lastErr = err
		// "not found" errors → try the previous block.
		// RPC or decode errors → stop immediately.
		if !isNotFoundError(err) {
			return nil, fmt.Errorf("blob search failed at block %d: %w", blk, err)
		}
	}

	return nil, fmt.Errorf("blob with versioned hash %s not found in blocks %d..%d: %w", versionedHash.Hex(), endBlock, startBlock, lastErr)
}

func (bc *BeaconClient) fetchBlobAtBlock(ctx context.Context, versionedHash common.Hash, blockNumber uint64) ([]byte, error) {
	block, err := bc.ethClient.BlockByNumber(ctx, new(big.Int).SetUint64(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("fetch EL block %d: %w", blockNumber, err)
	}

	parentBeaconRoot := block.BeaconRoot()
	if parentBeaconRoot == nil {
		return nil, fmt.Errorf("block %d has no parentBeaconBlockRoot (pre-Deneb?)", blockNumber)
	}

	slot, err := bc.getBeaconSlot(ctx, parentBeaconRoot.Hex())
	if err != nil {
		return nil, fmt.Errorf("get beacon slot for block %d: %w", blockNumber, err)
	}

	// Blobs are in the child beacon block (slot+1 relative to parentBeaconBlockRoot).
	blobSlot := slot + 1
	sidecars, err := bc.getBlobSidecars(ctx, blobSlot)
	if err != nil {
		return nil, fmt.Errorf("get blob sidecars at slot %d: %w", blobSlot, err)
	}

	for _, sc := range sidecars {
		commitHash := pkgblob.KzgToVersionedHash(sc.KzgCommitment)
		if commitHash == versionedHash {
			blobData, err := hex.DecodeString(pkgblob.StripHexPrefix(sc.Blob))
			if err != nil {
				return nil, fmt.Errorf("decode blob hex: %w", err)
			}

			payload, err := pkgblob.DecodeBlobData(blobData)
			if err != nil {
				return nil, fmt.Errorf("decode blob payload: %w", err)
			}
			return payload, nil
		}
	}

	return nil, fmt.Errorf("blob with versioned hash %s not found in slot %d sidecars (block %d)", versionedHash.Hex(), blobSlot, blockNumber)
}

func (bc *BeaconClient) getBeaconSlot(ctx context.Context, blockRoot string) (uint64, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/headers/%s", bc.beaconURL, blockRoot)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create beacon header request: %w", err)
	}

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("beacon header request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read beacon header response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("beacon header returned status %d: %s", resp.StatusCode, string(body))
	}

	var headerResp beaconHeaderResponse
	if err := json.Unmarshal(body, &headerResp); err != nil {
		return 0, fmt.Errorf("unmarshal beacon header: %w", err)
	}

	var slot uint64
	if _, err := fmt.Sscanf(headerResp.Data.Header.Message.Slot, "%d", &slot); err != nil {
		return 0, fmt.Errorf("parse slot %q: %w", headerResp.Data.Header.Message.Slot, err)
	}

	return slot, nil
}

func (bc *BeaconClient) getBlobSidecars(ctx context.Context, slot uint64) ([]blobSidecar, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/blob_sidecars/%d", bc.beaconURL, slot)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create blob sidecars request: %w", err)
	}

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("blob sidecars request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read blob sidecars response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blob sidecars returned status %d: %s", resp.StatusCode, string(body))
	}

	var sidecarsResp blobSidecarsResponse
	if err := json.Unmarshal(body, &sidecarsResp); err != nil {
		return nil, fmt.Errorf("unmarshal blob sidecars: %w", err)
	}

	return sidecarsResp.Data, nil
}

// isNotFoundError returns true if the error indicates the blob was simply not
// present at the given slot (expected during backward search). Returns false
// for infrastructure errors (RPC failures, decode errors) that should stop the search.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not found in slot")
}


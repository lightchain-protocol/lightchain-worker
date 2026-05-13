package pipeline

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
)

// JobCheckpoint is the per-jobID retry-safety record. It caches the
// deterministic outputs of the pipeline so asynq retries skip
// re-execution of stages 2-6 (in particular the expensive and stochastic
// stage 5 inference).
//
// Consistency guarantee: whoever first persists a ciphertext for a given
// jobID via SetCiphertextIfAbsent is the canonical result. Any concurrent
// goroutine that loses the race refreshes its local ciphertext to the
// canonical one before stages 7-8 — so the on-chain hash committed in
// stage 8b always matches what the user received via stage 7.
type JobCheckpoint struct {
	// Ciphertext is the AES-GCM encryption of the model response under
	// the session key, produced in stage 6. Zero-length means the
	// checkpoint record exists but inference hasn't completed yet (used
	// during SetCiphertextIfAbsent — we write the full record atomically,
	// so the invariant is "either both Ciphertext and Hash are present
	// or neither is").
	Ciphertext []byte
	// VersionedHash is the EIP-4844 blob versioned hash returned by
	// stage 8a's SubmitBlobTx. Zero means stage 8a hasn't run yet.
	VersionedHash common.Hash
	// Delivered is true after stage 7 has published to Redis at least
	// once. Subsequent attempts should skip stage 7 to avoid duplicate
	// relay frames.
	Delivered bool
}

// IsEmpty reports whether a checkpoint value is the zero struct (common
// for a Get that "succeeded but found nothing" — Redis returns nil).
func (c JobCheckpoint) IsEmpty() bool {
	return len(c.Ciphertext) == 0 && c.VersionedHash == (common.Hash{}) && !c.Delivered
}

// HasCiphertext reports whether stages 2-6 completed and wrote a
// canonical ciphertext. Used by the handler to decide if stages 2-6
// can be skipped on a retry.
func (c JobCheckpoint) HasCiphertext() bool {
	return len(c.Ciphertext) > 0
}

// HasVersionedHash reports whether stage 8a submitted a blob tx. Used by
// the handler to decide if stage 8a can be skipped.
func (c JobCheckpoint) HasVersionedHash() bool {
	return c.VersionedHash != (common.Hash{})
}

// CheckpointStore is the Redis-backed persistence for JobCheckpoint.
// Keys are namespaced under "job:{jobID}:ckpt_*" so they coexist with
// the existing "session:{sessionID}:responses" pub/sub channels.
//
// All mutations that must be atomic against concurrent writers (notably
// SetCiphertextIfAbsent) are implemented with Lua scripts to avoid a
// WATCH/MULTI round-trip.
type CheckpointStore struct {
	redis              *redis.Client
	ttl                time.Duration
	maxCiphertextB     int
	tombstoneTTL       time.Duration
	keyPrefix          string
	setCiphertextLR    *redis.Script
	setVersionedHashLR *redis.Script
	markDeliveredLR    *redis.Script
}

// NewCheckpointStore constructs a store. ttl is the normal TTL on fresh
// checkpoint records; tombstoneTTL is the shorter TTL applied after a
// successful on-chain completion. maxCiphertextBytes caps the size of
// the cached ciphertext — oversized responses are refused (the retry
// pays full re-inference, matching pre-checkpoint behavior).
func NewCheckpointStore(client *redis.Client, ttl, tombstoneTTL time.Duration, maxCiphertextBytes int) *CheckpointStore {
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	if tombstoneTTL <= 0 {
		tombstoneTTL = 10 * time.Minute
	}
	if maxCiphertextBytes <= 0 {
		maxCiphertextBytes = 256 * 1024
	}
	return &CheckpointStore{
		redis:          client,
		ttl:            ttl,
		maxCiphertextB: maxCiphertextBytes,
		tombstoneTTL:   tombstoneTTL,
		keyPrefix:      "job",
		setCiphertextLR: redis.NewScript(`
-- KEYS[1] = checkpoint hash key ("job:{jobID}:ckpt")
-- ARGV[1] = proposed ciphertext hex (encoded with hex.EncodeToString upstream)
-- ARGV[2] = TTL seconds
-- Returns table {canonicalCiphertextHex, wasSet} where wasSet is "1" if this
-- call wrote the record, "0" if an earlier writer won.
local existing = redis.call('HGET', KEYS[1], 'ciphertext')
if existing then
  return {existing, '0'}
end
redis.call('HSET', KEYS[1], 'ciphertext', ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return {ARGV[1], '1'}
`),
		// setVersionedHashLR / markDeliveredLR: HSET the new field, but
		// only call EXPIRE when the record is NOT tombstoned. Tombstone()
		// sets the 'tombstoned' marker AND shrinks the TTL after stage 8b
		// completes; without the guard, a slow concurrent retry calling
		// SetVersionedHash or MarkDelivered would extend the key back to
		// the full TTL, defeating the post-completion GC path.
		setVersionedHashLR: redis.NewScript(`
-- KEYS[1] = checkpoint hash key
-- ARGV[1] = versioned-hash hex
-- ARGV[2] = TTL seconds (only applied if not tombstoned)
local tombstoned = redis.call('HGET', KEYS[1], 'tombstoned')
redis.call('HSET', KEYS[1], 'versionedHash', ARGV[1])
if not tombstoned then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return 'OK'
`),
		markDeliveredLR: redis.NewScript(`
-- KEYS[1] = checkpoint hash key
-- ARGV[1] = TTL seconds (only applied if not tombstoned)
local tombstoned = redis.call('HGET', KEYS[1], 'tombstoned')
redis.call('HSET', KEYS[1], 'delivered', '1')
if not tombstoned then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
end
return 'OK'
`),
	}
}

func (s *CheckpointStore) key(jobID uint64) string {
	return fmt.Sprintf("%s:%d:ckpt", s.keyPrefix, jobID)
}

// ErrCiphertextTooLarge is returned by SetCiphertextIfAbsent when the
// proposed ciphertext exceeds the configured cap. Callers should log and
// continue without caching — the retry will re-run stages 2-6.
var ErrCiphertextTooLarge = errors.New("ciphertext exceeds checkpoint cap")

// SetCiphertextIfAbsent stores ciphertext at the checkpoint key if no
// ciphertext is present yet; otherwise returns the existing (canonical)
// ciphertext. Atomic via a Lua script so two concurrent goroutines
// racing on the same jobID converge on exactly one canonical value.
//
// Returns:
//   - canonical: the ciphertext now in Redis (either the proposed one if we
//     won the race, or the earlier writer's value)
//   - wasSet:    true iff this call wrote the record
//   - err:       non-nil on Redis error or oversized proposal
func (s *CheckpointStore) SetCiphertextIfAbsent(
	ctx context.Context,
	jobID uint64,
	proposed []byte,
) (canonical []byte, wasSet bool, err error) {
	if len(proposed) == 0 {
		return nil, false, fmt.Errorf("proposed ciphertext is empty")
	}
	if len(proposed) > s.maxCiphertextB {
		return nil, false, fmt.Errorf("%w: %d > %d bytes", ErrCiphertextTooLarge, len(proposed), s.maxCiphertextB)
	}

	proposedHex := hex.EncodeToString(proposed)
	res, err := s.setCiphertextLR.Run(
		ctx,
		s.redis,
		[]string{s.key(jobID)},
		proposedHex,
		int64(s.ttl.Seconds()),
	).Result()
	if err != nil {
		return nil, false, fmt.Errorf("run setCiphertext Lua: %w", err)
	}

	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return nil, false, fmt.Errorf("unexpected setCiphertext reply: %v", res)
	}

	canonicalHex, ok := arr[0].(string)
	if !ok {
		return nil, false, fmt.Errorf("unexpected setCiphertext reply [0]: %T %v", arr[0], arr[0])
	}
	canonical, err = hex.DecodeString(canonicalHex)
	if err != nil {
		return nil, false, fmt.Errorf("decode canonical ciphertext hex: %w", err)
	}

	wasSetStr, ok := arr[1].(string)
	if !ok {
		return nil, false, fmt.Errorf("unexpected setCiphertext reply [1]: %T %v", arr[1], arr[1])
	}
	return canonical, wasSetStr == "1", nil
}

// SetVersionedHash stores the blob tx versioned hash from stage 8a.
// Non-atomic with SetCiphertextIfAbsent because the invariants differ:
// stage 8a may re-run on a retry even after ciphertext was cached, and
// whichever attempt wins writes its hash. The contract rejects duplicate
// submissions, so on-chain consistency is preserved regardless.
//
// The Lua script atomically checks for the 'tombstoned' marker and skips
// the TTL extension when present — see the script comment for why.
func (s *CheckpointStore) SetVersionedHash(ctx context.Context, jobID uint64, hash common.Hash) error {
	if hash == (common.Hash{}) {
		return fmt.Errorf("versioned hash is zero")
	}
	_, err := s.setVersionedHashLR.Run(
		ctx,
		s.redis,
		[]string{s.key(jobID)},
		hash.Hex(),
		int64(s.ttl.Seconds()),
	).Result()
	if err != nil {
		return fmt.Errorf("set versionedHash: %w", err)
	}
	return nil
}

// MarkDelivered records that stage 7 has published at least once.
// Tombstone-aware via Lua script — see SetVersionedHash for rationale.
func (s *CheckpointStore) MarkDelivered(ctx context.Context, jobID uint64) error {
	_, err := s.markDeliveredLR.Run(
		ctx,
		s.redis,
		[]string{s.key(jobID)},
		int64(s.ttl.Seconds()),
	).Result()
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

// Get returns the checkpoint for jobID, or a zero-value JobCheckpoint
// (via IsEmpty() true) if none exists. A missing record is a normal
// cache miss, not an error.
func (s *CheckpointStore) Get(ctx context.Context, jobID uint64) (JobCheckpoint, error) {
	vals, err := s.redis.HGetAll(ctx, s.key(jobID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return JobCheckpoint{}, fmt.Errorf("get checkpoint: %w", err)
	}
	if len(vals) == 0 {
		return JobCheckpoint{}, nil
	}

	var out JobCheckpoint
	if ctHex, ok := vals["ciphertext"]; ok && ctHex != "" {
		ct, err := hex.DecodeString(ctHex)
		if err != nil {
			return JobCheckpoint{}, fmt.Errorf("decode cached ciphertext: %w", err)
		}
		out.Ciphertext = ct
	}
	if hashHex, ok := vals["versionedHash"]; ok && hashHex != "" {
		// common.HexToHash silently zero-pads/truncates malformed input,
		// so a corrupted Redis value would yield a non-zero garbage hash
		// that HasVersionedHash() reports as present. Stage 8a would skip
		// and stage 8b would proceed with an invalid blob hash. Reject
		// malformed input loudly and fall through to a fresh re-submit.
		if !common.IsHexHash(hashHex) {
			return JobCheckpoint{}, fmt.Errorf("invalid versionedHash format in checkpoint: %q", hashHex)
		}
		out.VersionedHash = common.HexToHash(hashHex)
	}
	if delivered, ok := vals["delivered"]; ok && delivered == "1" {
		out.Delivered = true
	}
	return out, nil
}

// Tombstone shrinks the checkpoint's TTL to tombstoneTTL and sets a
// 'tombstoned' marker. Called after on-chain completion is confirmed so
// an in-flight retry can still observe "already completed" for a short
// window but the record is GC'd soon after. Delete-outright would race a
// retry that's mid-processJob.
//
// The marker is what SetVersionedHash and MarkDelivered's Lua scripts
// check before extending TTL — without it, a slow concurrent retry would
// resurrect the key back to the full retention window and defeat the GC.
func (s *CheckpointStore) Tombstone(ctx context.Context, jobID uint64) error {
	pipe := s.redis.TxPipeline()
	pipe.HSet(ctx, s.key(jobID), "tombstoned", "1")
	pipe.Expire(ctx, s.key(jobID), s.tombstoneTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("tombstone checkpoint: %w", err)
	}
	return nil
}

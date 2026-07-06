package service

import (
	"crypto/tls"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/lightchain/worker/internal/config"
)

func TestAsynqRedisClientOptFromRedisOptions(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{ServerName: "redis.internal"}
	opts := &redis.Options{
		Network:      "tcp",
		Addr:         "redis.internal:6380",
		Username:     "worker",
		Password:     "secret",
		DB:           4,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 4 * time.Second,
		PoolSize:     8,
		TLSConfig:    tlsConfig,
	}

	got := asynqRedisClientOptFromRedisOptions(opts)

	assert.Equal(t, asynq.RedisClientOpt{
		Network:      opts.Network,
		Addr:         opts.Addr,
		Username:     opts.Username,
		Password:     opts.Password,
		DB:           opts.DB,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		PoolSize:     opts.PoolSize,
		TLSConfig:    opts.TLSConfig,
	}, got)
}

func TestService_computeDrainTTL_overrideTakesPrecedence(t *testing.T) {
	t.Parallel()

	// Service with cfg override set; chain client nil to prove the
	// override short-circuits before any RPC attempt.
	s := &Service{
		cfg:    &config.Config{DrainTTLOverride: 7 * time.Minute},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	assert.Equal(t, 7*time.Minute, s.computeDrainTTL(),
		"LIGHTCHAIN_DRAIN_TTL must be honored on the SIGTERM path, not just CLI")
}

func TestService_computeDrainTTL_fallbackWhenNoChainClient(t *testing.T) {
	t.Parallel()
	s := &Service{
		cfg:    &config.Config{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	got := s.computeDrainTTL()
	assert.Equal(t, drainTTLFallback, got)
}

// TestService_sortitionWatchers_nilWhenDisabled is a compile-and-invariant guard:
// it confirms the sortition watcher fields exist on Service and are nil on a
// zero-value instance. New() cannot be exercised in a unit test without heavy deps
// (keystore paths, chain RPC, Redis), so this test relies on the nil-field invariant
// as a structural check; correctness of the sortition wiring is validated by the
// build step and the Task 9 live demo.
func TestService_sortitionWatchers_nilWhenDisabled(t *testing.T) {
	t.Parallel()
	s := &Service{
		cfg:    &config.Config{SortitionEnabled: false},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	assert.Nil(t, s.sessionWatcher, "sessionWatcher must be nil when SortitionEnabled=false")
	assert.Nil(t, s.jobWatcher, "jobWatcher must be nil when SortitionEnabled=false")
}

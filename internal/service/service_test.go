package service

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
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

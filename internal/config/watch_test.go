package config

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clearWatchEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"WATCH_WEBHOOK_URL", "WATCH_INTERVAL", "WATCH_COOLDOWN", "WATCH_NO_CLAIM_AFTER",
		"WORKER_METRICS_ADDR", "HEARTBEAT_INTERVAL", "REDIS_URL", "REDIS_PASSWORD", "SESSION_MANAGER_ADDRESS",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadWatch_DefaultsMatchTheSidecar(t *testing.T) {
	clearWatchEnv(t)
	t.Setenv("WATCH_WEBHOOK_URL", "https://discord.com/api/webhooks/1/token")
	t.Setenv("SESSION_MANAGER_ADDRESS", "0xcccccccccccccccccccccccccccccccccccccccc")

	cfg, err := LoadWatch()

	require.NoError(t, err)
	assert.Equal(t, "https://discord.com/api/webhooks/1/token", cfg.WebhookURL)
	assert.Equal(t, 30*time.Second, cfg.Interval)
	assert.Equal(t, time.Hour, cfg.Cooldown)
	assert.Equal(t, 2*time.Hour, cfg.NoClaimAfter)
	assert.Equal(t, "127.0.0.1:9101", cfg.MetricsAddr)
	assert.Equal(t, 10*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, "redis://localhost:6379", cfg.RedisURL)
	assert.Equal(t, common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc"), cfg.SessionManagerAddress)
}

func TestLoadWatch_Errors(t *testing.T) {
	cases := map[string]struct {
		env  map[string]string
		want string
	}{
		"webhook missing":    {map[string]string{}, "WATCH_WEBHOOK_URL is required"},
		"webhook not a URL":  {map[string]string{"WATCH_WEBHOOK_URL": "discord-secret-token"}, "WATCH_WEBHOOK_URL: must be an http(s) URL"},
		"interval zero":      {map[string]string{"WATCH_INTERVAL": "0s"}, "WATCH_INTERVAL: must be positive"},
		"cooldown garbage":   {map[string]string{"WATCH_COOLDOWN": "soon"}, "WATCH_COOLDOWN"},
		"bad session mgr":    {map[string]string{"SESSION_MANAGER_ADDRESS": "0x12"}, "SESSION_MANAGER_ADDRESS"},
		"no-claim negative":  {map[string]string{"WATCH_NO_CLAIM_AFTER": "-1h"}, "WATCH_NO_CLAIM_AFTER: must be >= 0"},
		"heartbeat negative": {map[string]string{"HEARTBEAT_INTERVAL": "-1s"}, "HEARTBEAT_INTERVAL: must be positive"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			clearWatchEnv(t)
			if name != "webhook missing" {
				t.Setenv("WATCH_WEBHOOK_URL", "https://hooks.example/x")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := LoadWatch()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), "discord-secret-token", "the webhook URL is a secret")
		})
	}
}

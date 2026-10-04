package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// WatchConfig is what the `watch` subcommand reads on top of
// RegistrationConfig: the webhook and its timing, plus the sidecar settings
// that say where the worker's liveness, heartbeat and claims are found.
// Defaults match the sidecar so watch reads the worker's own env file.
type WatchConfig struct {
	WebhookURL   string
	Interval     time.Duration
	Cooldown     time.Duration
	MissedClaims int // missed claims in a row before alerting; 0 disables
	// WorkerAddress is WATCH_WORKER_ADDRESS; zero = read the keystore's address field.
	WorkerAddress common.Address

	MetricsAddr       string
	HeartbeatInterval time.Duration
	// RedisURL defaults like the sidecar's; RegistrationConfig.RedisURL has no
	// default because drain/undrain must be pointed at Redis explicitly.
	RedisURL              string
	RedisPassword         string
	SessionManagerAddress common.Address // zero = no claims check
}

// LoadWatch reads and validates the watch settings. Errors never echo
// WATCH_WEBHOOK_URL: the URL carries the webhook's secret token.
func LoadWatch() (*WatchConfig, error) {
	var errs []string
	cfg := &WatchConfig{
		WebhookURL:        os.Getenv("WATCH_WEBHOOK_URL"),
		Interval:          parseDuration("WATCH_INTERVAL", "30s", &errs),
		Cooldown:          parseDuration("WATCH_COOLDOWN", "1h", &errs),
		MissedClaims:      parseInt("WATCH_MISSED_CLAIMS", 3, &errs),
		MetricsAddr:       envOrDefault("WORKER_METRICS_ADDR", "127.0.0.1:9101"),
		HeartbeatInterval: parseDuration("HEARTBEAT_INTERVAL", "10s", &errs),
		RedisURL:          envOrDefault("REDIS_URL", "redis://localhost:6379"),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
	}

	if cfg.WebhookURL == "" {
		errs = append(errs, "WATCH_WEBHOOK_URL is required")
	} else if u, err := url.Parse(cfg.WebhookURL); err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		errs = append(errs, "WATCH_WEBHOOK_URL: must be an http(s) URL")
	}
	if cfg.Interval <= 0 {
		errs = append(errs, "WATCH_INTERVAL: must be positive")
	}
	if cfg.Cooldown < 0 {
		errs = append(errs, "WATCH_COOLDOWN: must be >= 0")
	}
	if cfg.MissedClaims < 0 {
		errs = append(errs, "WATCH_MISSED_CLAIMS: must be >= 0")
	}
	if cfg.HeartbeatInterval <= 0 {
		errs = append(errs, "HEARTBEAT_INTERVAL: must be positive")
	}
	if sm := os.Getenv("SESSION_MANAGER_ADDRESS"); sm != "" {
		if !common.IsHexAddress(sm) {
			errs = append(errs, fmt.Sprintf("SESSION_MANAGER_ADDRESS: invalid hex address %q", sm))
		} else {
			cfg.SessionManagerAddress = common.HexToAddress(sm)
		}
	}
	if w := os.Getenv("WATCH_WORKER_ADDRESS"); w != "" {
		// The zero address would read as unset and fall back to the keystore.
		if !common.IsHexAddress(w) || common.HexToAddress(w) == (common.Address{}) {
			errs = append(errs, fmt.Sprintf("WATCH_WORKER_ADDRESS: must be a non-zero hex address, got %q", w))
		} else {
			cfg.WorkerAddress = common.HexToAddress(w)
		}
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("watch config errors:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

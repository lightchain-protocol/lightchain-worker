package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/lightchain/worker/internal/config"
	"github.com/lightchain/worker/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	if errs := cfg.Validate(); len(errs) > 0 {
		for _, e := range errs {
			slog.Error("config invalid", "field", e)
		}
		slog.Error("config validation failed", "count", len(errs), "fields", strings.Join(errs, "; "))
		os.Exit(1)
	}

	svc, err := service.New(cfg)
	if err != nil {
		slog.Error("service init failed", "error", err)
		os.Exit(1)
	}

	if err := svc.Run(context.Background()); err != nil {
		slog.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

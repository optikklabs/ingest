package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/optikklabs/ingest/internal/app"
	"github.com/optikklabs/ingest/internal/config"
)

func main() {
	initLogger()

	if err := run(); err != nil {
		slog.Error("ingest exited", slog.Any("error", err))
		os.Exit(1)
	}
}

// run owns every deferred cleanup so main can exit non-zero without skipping
// them (os.Exit does not run deferred calls).
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := app.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

	if err := application.Start(ctx); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}
	return nil
}

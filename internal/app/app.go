package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/oklog/run"

	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/config"
)

type App struct {
	Config  config.Config
	Infra   *Infra
	Modules []registry.Module
}

func New(cfg config.Config) (*App, error) {
	infraDeps, err := newInfra(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize infrastructure: %w", err)
	}

	return &App{
		Config:  cfg,
		Infra:   infraDeps,
		Modules: infraDeps.Ingest,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	var g run.Group
	runAddContextCancelActor(&g, ctx)
	a.addHTTPServerActor(&g)
	if err := a.addGRPCServerActor(&g); err != nil {
		return err
	}
	a.addLagPollerActors(&g, ctx)
	a.addConsumerActors(&g, ctx)

	err := g.Run()
	if closeErr := a.Infra.Close(); closeErr != nil {
		slog.WarnContext(ctx, "error closing infrastructure", slog.Any("error", closeErr))
	}

	return normalizeRunError(err)
}

// runAddContextCancelActor shuts down the run group when ctx is cancelled.
func runAddContextCancelActor(g *run.Group, ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g.Add(func() error { <-ctx.Done(); return ctx.Err() },
		func(error) { cancel() })
}

func normalizeRunError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

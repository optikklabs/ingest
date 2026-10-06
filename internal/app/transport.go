package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // registers the gzip codec so OTLP clients may compress
	"google.golang.org/grpc/keepalive"

	"github.com/optikklabs/ingest/internal/auth"
)

func (a *App) addHTTPServerActor(ctx context.Context, g *run.Group) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", a.health)
	mux.HandleFunc("/health/live", a.health)
	mux.HandleFunc("/health/ready", a.health)

	srv := &http.Server{
		Addr:         ":" + a.Config.Server.Port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	g.Add(func() error {
		return srv.ListenAndServe()
	}, func(error) {
		shutdownServer(ctx, srv, "http")
	})

	if a.Config.OTLP.HTTPPort == "" {
		return
	}
	otlpMux := http.NewServeMux()
	for _, mod := range a.Modules {
		if httpMod, ok := mod.(HTTPModule); ok {
			httpMod.RegisterOTLPHTTP(otlpMux, a.Infra.Authenticator)
		}
	}
	otlpSrv := &http.Server{
		Addr:         ":" + a.Config.OTLP.HTTPPort,
		Handler:      otlpMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	g.Add(func() error {
		return otlpSrv.ListenAndServe()
	}, func(error) {
		shutdownServer(ctx, otlpSrv, "otlp-http")
	})
}

// shutdownServer drains srv within a fixed budget. It runs after ctx is
// cancelled, so it keeps ctx's values but not its cancellation.
func shutdownServer(ctx context.Context, srv *http.Server, name string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.WarnContext(ctx, "server shutdown incomplete", slog.String("server", name), slog.Any("error", err))
	}
}

func (a *App) addGRPCServerActor(ctx context.Context, g *run.Group) error {
	port := a.Config.OTLP.GRPCPort
	if port == "" {
		return errors.New("ingest: gRPC port is not configured (otlp.grpc_port)")
	}

	addr := ":" + port
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("ingest: gRPC listen failed on %s: %w", addr, err)
	}

	slog.InfoContext(ctx, "starting OTLP gRPC server",
		slog.String("addr", addr),
		slog.String("hint", "send gRPC metadata x-api-key (tenant API key); use OTLP gRPC on this port, not HTTP/protobuf"))

	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(a.Config.OTLP.GRPCMaxConcurrentStr),
		grpc.MaxRecvMsgSize(a.Config.OTLP.GRPCMaxRecvMsgSize),
		grpc.ConnectionTimeout(30*time.Second),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),

		grpc.ChainUnaryInterceptor(
			grpcMetricsUnary(),
			auth.UnaryInterceptor(a.Infra.Authenticator),
		),
		grpc.ChainStreamInterceptor(
			grpcMetricsStream(),
			auth.StreamInterceptor(a.Infra.Authenticator), //nolint:contextcheck // runs per stream on the stream's own context
		),
	)
	for _, mod := range a.Modules {
		mod.RegisterGRPC(grpcSrv)
	}
	g.Add(func() error {
		return grpcSrv.Serve(lis)
	}, func(error) {
		done := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			grpcSrv.Stop()
		}
	})
	return nil
}

func (a *App) addLagPollerActors(parentCtx context.Context, g *run.Group) {
	for _, p := range a.Infra.LagPollers {
		pollCtx, cancel := context.WithCancel(parentCtx)
		g.Add(func() error {
			p.Run(pollCtx)
			return nil
		}, func(error) { cancel() })
	}
}

func (a *App) addConsumerActors(parentCtx context.Context, g *run.Group) {
	for _, c := range a.Infra.Consumers {
		runCtx, cancel := context.WithCancel(parentCtx)
		g.Add(func() error {
			c.Run(runCtx)
			return nil
		}, func(error) { cancel() })
	}
}

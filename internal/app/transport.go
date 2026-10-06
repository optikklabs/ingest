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
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	"github.com/optikklabs/ingest/internal/auth"
)

func (a *App) addHTTPServerActor(g *run.Group) {
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
		shutdownServer(srv, "http")
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
		shutdownServer(otlpSrv, "otlp-http")
	})
}

func shutdownServer(srv *http.Server, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("server shutdown incomplete", slog.String("server", name), slog.Any("error", err))
	}
}

func (a *App) addGRPCServerActor(g *run.Group) error {
	port := a.Config.OTLP.GRPCPort
	if port == "" {
		return errors.New("ingest: gRPC port is not configured (otlp.grpc_port)")
	}

	addr := ":" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("ingest: gRPC listen failed on %s: %w", addr, err)
	}

	slog.Info("starting OTLP gRPC server",
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
			auth.StreamInterceptor(a.Infra.Authenticator),
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

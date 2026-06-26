package app

import (
	"context"
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

// addHTTPServerActor serves /metrics and /health probes only — ingest has no
// query API surface.
func (a *App) addHTTPServerActor(g *run.Group) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", a.healthLive)
	mux.HandleFunc("/health/live", a.healthLive)
	mux.HandleFunc("/health/ready", a.healthReady)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", a.Config.Server.Port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	g.Add(func() error {
		return srv.ListenAndServe()
	}, func(error) {
		shutCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		srv.Shutdown(shutCtx)
	})
}

func (a *App) addGRPCServerActor(g *run.Group) error {
	port := a.Config.OTLP.GRPCPort
	if port == "" {
		return fmt.Errorf("ingest: gRPC port is not configured (otlp.grpc_port)")
	}

	addr := fmt.Sprintf(":%s", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("ingest: gRPC listen failed on %s: %w", addr, err)
	}

	slog.Info("starting OTLP gRPC server",
		slog.String("addr", addr),
		slog.String("hint", "send gRPC metadata x-api-key (team API key); use OTLP gRPC on this port, not HTTP/protobuf"))

	maxStreams := a.Config.OTLP.GRPCMaxConcurrentStr
	if maxStreams == 0 {
		maxStreams = 10_000
	}

	maxRecvMsgSize := a.Config.OTLP.GRPCMaxRecvMsgSize
	if maxRecvMsgSize == 0 {
		maxRecvMsgSize = 16 * 1024 * 1024
	}
	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(maxStreams),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
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

func (a *App) addLagPollerActors(g *run.Group, parentCtx context.Context) {
	for _, p := range a.Infra.LagPollers {
		p := p
		pollCtx, cancel := context.WithCancel(parentCtx)
		g.Add(func() error {
			p.Run(pollCtx)
			return nil
		}, func(error) { cancel() })
	}
}

func (a *App) addConsumerActors(g *run.Group, parentCtx context.Context) {
	for _, c := range a.Infra.Consumers {
		c := c
		runCtx, cancel := context.WithCancel(parentCtx)
		g.Add(func() error {
			c.Run(runCtx)
			return nil
		}, func(error) { cancel() })
	}
}

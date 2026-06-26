package app

import (
	"context"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// grpcMetricsUnary records count and duration for unary gRPC calls.
func grpcMetricsUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		metrics.GRPCStarted.WithLabelValues(info.FullMethod).Inc()
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		metrics.GRPCHandled.WithLabelValues(info.FullMethod, code).Inc()
		metrics.GRPCDuration.WithLabelValues(info.FullMethod).
			Observe(time.Since(start).Seconds())
		return resp, err
	}
}

func grpcMetricsStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		metrics.GRPCStarted.WithLabelValues(info.FullMethod).Inc()
		start := time.Now()
		err := handler(srv, ss)
		code := status.Code(err).String()
		metrics.GRPCHandled.WithLabelValues(info.FullMethod, code).Inc()
		metrics.GRPCDuration.WithLabelValues(info.FullMethod).
			Observe(time.Since(start).Seconds())
		return err
	}
}

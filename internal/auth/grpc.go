package auth

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/optikklabs/ingest/internal/infra/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const apiKeyHeader = "x-api-key"

func UnaryInterceptor(resolver TeamResolver, limiter *TenantRateLimiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		tenantID, err := resolveFromContext(ctx, resolver)
		if err != nil {
			return nil, err
		}
		// Rate-limit here so throttled requests skip mapping and publish work.
		if !limiter.Allow(tenantID) {
			metrics.OTLPRateLimitedTotal.WithLabelValues(signalFromMethod(info.FullMethod)).Inc()
			return nil, status.Error(codes.ResourceExhausted, "tenant rate limit exceeded")
		}
		return handler(WithTenantID(ctx, tenantID), req)
	}
}

func signalFromMethod(fullMethod string) string {
	switch {
	case strings.Contains(fullMethod, ".trace."):
		return "spans"
	case strings.Contains(fullMethod, ".logs."):
		return "logs"
	case strings.Contains(fullMethod, ".metrics."):
		return "metrics"
	default:
		return "unknown"
	}
}

func StreamInterceptor(resolver TeamResolver) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		tenantID, err := resolveFromContext(ss.Context(), resolver)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithTenantID(ss.Context(), tenantID)})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func resolveFromContext(ctx context.Context, resolver TeamResolver) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, status.Error(codes.Unauthenticated, "missing metadata")
	}
	keys := md.Get(apiKeyHeader)
	if len(keys) == 0 {
		return 0, status.Error(codes.Unauthenticated, "missing x-api-key metadata header")
	}
	apiKey := keys[0]
	tenantID, err := resolver.ResolveTenantID(ctx, apiKey)
	if err != nil {
		slog.WarnContext(ctx, "ingest auth failed", slog.String("apiKey", maskKey(apiKey)), slog.Any("error", err))
		if errors.Is(err, ErrMissingAPIKey) || errors.Is(err, ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		if errors.Is(err, ErrAuthRateLimited) {
			return 0, status.Error(codes.ResourceExhausted, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}
	return tenantID, nil
}

func maskKey(apiKey string) string {
	if len(apiKey) <= 8 {
		return "***"
	}
	return apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
}

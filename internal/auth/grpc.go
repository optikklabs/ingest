package auth

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const apiKeyHeader = "x-api-key"

// UnaryInterceptor authenticates unary OTLP RPCs and binds team IDs to the
// context, serving as the single team-resolution site for ingest.
func UnaryInterceptor(resolver TeamResolver) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		teamID, err := resolveFromContext(ctx, resolver)
		if err != nil {
			return nil, err
		}
		return handler(WithTeamID(ctx, teamID), req)
	}
}

// StreamInterceptor provides authentication for streaming ingest RPCs.
func StreamInterceptor(resolver TeamResolver) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		teamID, err := resolveFromContext(ss.Context(), resolver)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: WithTeamID(ss.Context(), teamID)})
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
	teamID, err := resolver.ResolveTeamID(ctx, apiKey)
	if err != nil {
		slog.WarnContext(ctx, "ingest auth failed", slog.String("apiKey", maskKey(apiKey)), slog.Any("error", err))
		if errors.Is(err, ErrMissingAPIKey) || errors.Is(err, ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}
	return teamID, nil
}

func maskKey(apiKey string) string {
	if len(apiKey) <= 8 {
		return "***"
	}
	return apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
}

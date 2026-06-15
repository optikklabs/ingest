package auth

import "context"

type teamIDKey struct{}

// WithTeamID returns a derived ctx carrying the resolved ingest team id.
func WithTeamID(ctx context.Context, teamID int64) context.Context {
	return context.WithValue(ctx, teamIDKey{}, teamID)
}

// TeamIDFromContext returns the team id installed by the gRPC auth interceptor.
// The bool is false when ingest code runs outside an authenticated RPC.
func TeamIDFromContext(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(teamIDKey{}).(int64)
	return v, ok
}

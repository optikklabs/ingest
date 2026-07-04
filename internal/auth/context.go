package auth

import "context"

type tenantIDKey struct{}

// WithTenantID returns a derived ctx carrying the resolved ingest team id.
func WithTenantID(ctx context.Context, tenantID int64) context.Context {
	return context.WithValue(ctx, tenantIDKey{}, tenantID)
}

func TenantIDFromContext(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(tenantIDKey{}).(int64)
	return v, ok
}

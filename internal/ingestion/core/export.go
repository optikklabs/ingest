package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ExportOTLP is the shared body of the spans/logs/metrics OTLP handlers:
// resolve the tenant from context, map the request into rows (timed), and
// publish them (timed), converting failures into gRPC status errors.
//
// It returns the mapped rows, the mapper's usage tally and the tenant so
// callers can emit ingestion stats and signal-specific side effects after a
// successful publish. Usage is opaque (U) so core stays independent of the
// ingestionstats package, which itself depends on core.
func ExportOTLP[T Row, U any](
	ctx context.Context,
	signal string,
	mapRows func(tenantID int64) ([]T, U),
	publish func(ctx context.Context, rows []T) error,
) (rows []T, usage U, tenantID int64, err error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, usage, 0, status.Error(codes.Unauthenticated, "team id missing from context")
	}

	mapStart := time.Now()
	rows, usage = mapRows(tenantID)
	metrics.MapperDuration.WithLabelValues(signal).Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues(signal).Observe(float64(len(rows)))
	if len(rows) == 0 {
		return rows, usage, tenantID, nil
	}

	pubStart := time.Now()
	if err := publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues(signal, "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, signal+" handler: publish failed", slog.Any("error", err))
		return nil, usage, tenantID, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues(signal, "ok").Observe(time.Since(pubStart).Seconds())
	return rows, usage, tenantID, nil
}

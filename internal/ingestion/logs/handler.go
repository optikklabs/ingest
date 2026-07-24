package logs

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer *core.Producer[*schema.Row]
	stats    ingestionstats.Recorder
}

func NewHandler(p *core.Producer[*schema.Row], stats ingestionstats.Recorder) *Handler {
	return &Handler{
		producer: p,
		stats:    stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	if !auth.RateLimiter.Allow(tenantID) {
		metrics.OTLPRateLimitedTotal.WithLabelValues("logs").Inc()
		return nil, status.Error(codes.ResourceExhausted, "tenant rate limit exceeded")
	}
	mapStart := time.Now()
	rows := mapRequest(tenantID, req)
	metrics.MapperDuration.WithLabelValues("logs").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("logs").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}

	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("logs", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("logs", "ok").Observe(time.Since(pubStart).Seconds())
	ingestionstats.Emit(h.stats, statRows(uint32(tenantID), req))
	return &logspb.ExportLogsServiceResponse{}, nil
}

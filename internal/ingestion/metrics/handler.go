package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	seriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	metricspb.UnimplementedMetricsServiceServer
	metricsPublisher core.Publisher[*schema.Row]
	seriesPublisher  core.Publisher[*seriesschema.SeriesRow]
	stats            ingestionstats.Recorder
}

func NewHandler(mp core.Publisher[*schema.Row], sp core.Publisher[*seriesschema.SeriesRow], stats ingestionstats.Recorder) *Handler {
	return &Handler{
		metricsPublisher: mp,
		seriesPublisher:  sp,
		stats:            stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	tenantID, ok := auth.TenantIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	if !auth.RateLimiter.Allow(tenantID) {
		obsmetrics.OTLPRateLimitedTotal.WithLabelValues("metrics").Inc()
		return nil, status.Error(codes.ResourceExhausted, "tenant rate limit exceeded")
	}
	mapStart := time.Now()
	rows, seriesRows := mapRequest(tenantID, req)
	obsmetrics.MapperDuration.WithLabelValues("metrics").Observe(time.Since(mapStart).Seconds())
	obsmetrics.MapperRowsPerRequest.WithLabelValues("metrics").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}

	pubStart := time.Now()
	if err := core.PublishMetricPair(ctx, h.seriesPublisher, seriesRows, h.metricsPublisher, rows); err != nil {
		obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "metrics handler: paired publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "ok").Observe(time.Since(pubStart).Seconds())
	ingestionstats.Emit(h.stats, statRows(uint32(tenantID), req))

	return &metricspb.ExportMetricsServiceResponse{}, nil
}

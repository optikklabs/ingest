// Package metrics is the OTLP metrics ingestion path. Same shape as
// ingestion/spans + ingestion/logs.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/auth"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/core"
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
}

func NewHandler(mp core.Publisher[*schema.Row], sp core.Publisher[*seriesschema.SeriesRow]) *Handler {
	return &Handler{
		metricsPublisher: mp,
		seriesPublisher:  sp,
	}
}

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows, seriesRows := mapRequest(teamID, req)
	obsmetrics.MapperDuration.WithLabelValues("metrics").Observe(time.Since(mapStart).Seconds())
	obsmetrics.MapperRowsPerRequest.WithLabelValues("metrics").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}
	pubStart := time.Now()
	if err := h.metricsPublisher.Publish(ctx, rows); err != nil {
		obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "metrics handler: metrics publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "ok").Observe(time.Since(pubStart).Seconds())

	if len(seriesRows) > 0 {
		seriesPubStart := time.Now()
		if err := h.seriesPublisher.Publish(ctx, seriesRows); err != nil {
			obsmetrics.HandlerPublishDuration.WithLabelValues("metric_series", "err").Observe(time.Since(seriesPubStart).Seconds())
			slog.ErrorContext(ctx, "metrics handler: series publish failed", slog.Any("error", err))
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		obsmetrics.HandlerPublishDuration.WithLabelValues("metric_series", "ok").Observe(time.Since(seriesPubStart).Seconds())
	}

	return &metricspb.ExportMetricsServiceResponse{}, nil
}

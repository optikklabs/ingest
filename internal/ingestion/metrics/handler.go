package metrics

import (
	"context"
	"time"

	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	"github.com/optikklabs/ingest/internal/ingestion/metricseries"
	seriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type Handler struct {
	metricspb.UnimplementedMetricsServiceServer
	metricsPublisher core.Publisher[*schema.Row]
	seriesPublisher  core.Publisher[*seriesschema.SeriesRow]
	seriesDedup      *metricseries.Dedup
	stats            ingestionstats.Recorder
}

func NewHandler(mp core.Publisher[*schema.Row], sp core.Publisher[*seriesschema.SeriesRow], dedup *metricseries.Dedup, stats ingestionstats.Recorder) *Handler {
	return &Handler{
		metricsPublisher: mp,
		seriesPublisher:  sp,
		seriesDedup:      dedup,
		stats:            stats,
	}
}

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	var seriesRows []*seriesschema.SeriesRow
	_, usage, tenantID, err := core.ExportOTLP(ctx, "metrics",
		func(tenantID int64) ([]*schema.Row, []ingestionstats.ResourceUsage) {
			rows, series, usage := mapRequest(tenantID, req)
			seriesRows = series
			return rows, usage
		},
		func(ctx context.Context, rows []*schema.Row) error { return h.publishPair(ctx, rows, seriesRows) },
	)
	if err != nil {
		return nil, err
	}
	ingestionstats.EmitUsage(h.stats, tenantID, ingestionstats.SignalMetrics, usage, req)
	return &metricspb.ExportMetricsServiceResponse{}, nil
}

// publishPair publishes metric rows together with their dedup-filtered series
// metadata, marking series only after the synchronous publish succeeded so a
// failed publish keeps them eligible for republish on the next export.
func (h *Handler) publishPair(ctx context.Context, rows []*schema.Row, seriesRows []*seriesschema.SeriesRow) error {
	bucket := h.seriesDedup.Bucket(time.Now())
	unpublished := h.seriesDedup.FilterUnpublished(seriesRows, bucket)
	if err := core.PublishMetricPair(ctx, h.seriesPublisher, unpublished, h.metricsPublisher, rows); err != nil {
		return err
	}
	h.seriesDedup.MarkPublished(unpublished, bucket)
	return nil
}

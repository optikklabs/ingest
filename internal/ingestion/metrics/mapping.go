package metrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
)

var metricsColumns = []string{
	"team_id", "metric_name", "temporality", "fingerprint", "timestamp",
	"value", "hist_sum", "hist_count",
	"hist_buckets", "hist_counts",
}

var seriesColumns = []string{
	"team_id", "timestamp", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint",
	"service", "host", "environment", "k8s_namespace", "pod", "container",
	"attributes",
}

type MetricsWriter struct {
	metricsWriter *core.ClickHouseWriter[*schema.Row]
	seriesWriter  *core.ClickHouseWriter[*schema.Row]
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.Row] {
	return &MetricsWriter{
		metricsWriter: core.NewClickHouseWriter(ch, "optikk.metrics", metricsColumns, metricsRowValues),
		seriesWriter:  core.NewClickHouseWriter(ch, "optikk.metrics_series", seriesColumns, seriesRowValues),
	}
}

func (w *MetricsWriter) Insert(ctx context.Context, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}

	// 1. Write raw samples to metrics table
	if err := w.metricsWriter.Insert(ctx, rows); err != nil {
		return err
	}

	// 2. Write unique series metadata to metrics_series table
	var seriesRows []*schema.Row
	seen := make(map[uint64]struct{}, len(rows))

	for _, r := range rows {
		fp := r.GetFingerprint()
		if fp == 0 {
			continue
		}
		if _, ok := seen[fp]; ok {
			continue
		}
		seen[fp] = struct{}{}

		seriesRows = append(seriesRows, r)
	}

	if len(seriesRows) > 0 {
		if err := w.seriesWriter.Insert(ctx, seriesRows); err != nil {
			return err
		}
	}

	return nil
}

func metricsRowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetMetricName(),
		r.GetTemporality(),
		r.GetFingerprint(),
		time.Unix(0, r.GetTimestampNs()),

		r.GetValue(),
		r.GetHistSum(),
		r.GetHistCount(),
		r.GetHistBuckets(),
		r.GetHistCounts(),
	}
}

func seriesRowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		time.Unix(0, r.GetTimestampNs()).UTC(),
		r.GetMetricName(),
		r.GetMetricType(),
		r.GetTemporality(),
		r.GetIsMonotonic(),
		r.GetUnit(),
		r.GetDescription(),
		r.GetFingerprint(),
		r.GetService(),
		r.GetHost(),
		r.GetEnvironment(),
		r.GetK8SNamespace(),
		r.GetPod(),
		r.GetContainer(),
		r.GetAttributes(),
	}
}

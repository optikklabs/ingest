package metrics

import (
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

// NewMetricsClickHouseWriter returns a ClickHouse writer specifically for the raw metrics samples.
func NewMetricsClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.Row] {
	return core.NewClickHouseWriter(ch, "optikk.metrics", metricsColumns, metricsRowValues)
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

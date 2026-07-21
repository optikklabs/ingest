package metrics

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
)

var metricsColumns = []string{
	"tenant_id", "metric_name", "temporality", "fingerprint", "timestamp",
	"value", "hist_sum", "hist_count",
	"hist_buckets", "hist_counts",
	"summary_quantiles", "summary_values",
}

// NewMetricsClickHouseWriter returns a ClickHouse writer specifically for the raw metrics samples.
func NewMetricsClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.Row] {
	return core.NewClickHouseWriter(ch, "optikk.metrics", metricsColumns, metricsRowValues)
}

func metricsRowValues(r *schema.Row, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = r.GetMetricName()
	dst[2] = r.GetTemporality()
	dst[3] = r.GetFingerprint()
	dst[4] = time.Unix(0, r.GetTimestampNs())
	dst[5] = r.GetValue()
	dst[6] = r.GetHistSum()
	dst[7] = r.GetHistCount()
	dst[8] = r.GetHistBuckets()
	dst[9] = r.GetHistCounts()
	dst[10] = r.GetSummaryQuantiles()
	dst[11] = r.GetSummaryValues()
}

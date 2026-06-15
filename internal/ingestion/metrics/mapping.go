package metrics

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
)

const chTable = "observability.metrics"

var chColumns = []string{
	"team_id", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint", "timestamp",
	"ts_bucket",
	"value", "hist_sum", "hist_count",
	"hist_buckets", "hist_counts",
	"service", "host", "environment", "k8s_namespace", "pod", "container",
	"resource", "attributes",
}

func NewClickHouseWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTable, chColumns, rowValues)
}

func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetMetricName(),
		r.GetMetricType(),
		r.GetTemporality(),
		r.GetIsMonotonic(),
		r.GetUnit(),
		r.GetDescription(),
		r.GetFingerprint(),
		time.Unix(0, r.GetTimestampNs()),
		// ts_bucket is 5-min-aligned UInt32; field name kept for proto compat.
		uint32(r.GetTsBucketHourSeconds()),

		r.GetValue(),
		r.GetHistSum(),
		r.GetHistCount(),
		r.GetHistBuckets(),
		r.GetHistCounts(),
		r.GetService(),
		r.GetHost(),
		r.GetEnvironment(),
		r.GetK8SNamespace(),
		r.GetPod(),
		r.GetContainer(),
		r.GetResource(),
		r.GetAttributes(),
	}
}

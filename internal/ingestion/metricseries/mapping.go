// Package metricseries owns the decoupled metric series ingestion path: its own
// wire schema and the ClickHouse sink. Series rows are built by the metrics
// mapper and published to the metric_series topic.
package metricseries

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
)

var seriesColumns = []string{
	"team_id", "timestamp", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint",
	"service", "host", "environment", "k8s_namespace", "pod", "container",
	"attributes",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.SeriesRow] {
	return core.NewClickHouseWriter(ch, "optikk.metrics_series", seriesColumns, seriesRowValues)
}

func seriesRowValues(r *schema.SeriesRow) []any {
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

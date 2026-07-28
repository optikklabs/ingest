package metricseries

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
)

var seriesColumns = []string{
	"tenant_id", "timestamp", "metric_name", "metric_type", "temporality", "is_monotonic",
	"unit", "description", "fingerprint",
	"service", "host", "environment", "k8s_namespace", "pod", "container",
	"attributes", "resource_attributes",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.SeriesRow] {
	return core.NewClickHouseWriter(ch, "optikk.metrics_series", seriesColumns, seriesRowValues)
}

func seriesRowValues(r *schema.SeriesRow, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = time.Unix(0, r.GetTimestampNs()).UTC()
	dst[2] = r.GetMetricName()
	dst[3] = r.GetMetricType()
	dst[4] = r.GetTemporality()
	dst[5] = r.GetIsMonotonic()
	dst[6] = r.GetUnit()
	dst[7] = r.GetDescription()
	dst[8] = r.GetFingerprint()
	dst[9] = r.GetService()
	dst[10] = r.GetHost()
	dst[11] = r.GetEnvironment()
	dst[12] = r.GetK8SNamespace()
	dst[13] = r.GetPod()
	dst[14] = r.GetContainer()
	dst[15] = r.GetAttributes()
	dst[16] = r.GetResourceAttributes()
}

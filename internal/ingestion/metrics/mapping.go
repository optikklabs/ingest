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
	"service", "host", "pod", "container", "k8s_namespace", "environment",
	"attributes",
	"cloud_provider", "cloud_account", "cloud_region", "cloud_platform", "k8s_node",
}

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
	dst[12] = r.GetService()
	dst[13] = r.GetHost()
	dst[14] = r.GetPod()
	dst[15] = r.GetContainer()
	dst[16] = r.GetK8SNamespace()
	dst[17] = r.GetEnvironment()
	dst[18] = r.GetAttributes()
	dst[19] = r.GetCloudProvider()
	dst[20] = r.GetCloudAccount()
	dst[21] = r.GetCloudRegion()
	dst[22] = r.GetCloudPlatform()
	dst[23] = r.GetK8SNode()
}

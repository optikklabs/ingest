package logs

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
)

const chTableData = "optikk.logs"

var chColumnsData = []string{
	"tenant_id", "ts_bucket", "trace_id", "log_id", "timestamp",
	"service", "environment", "host", "pod", "container",
	"severity_text", "severity_number", "severity_bucket",
	"attributes_string", "attributes_number", "attributes_bool",
	"observed_timestamp", "span_id", "trace_flags",
	"body", "resource", "scope_name", "scope_version",
}

func NewDataWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTableData, chColumnsData, rowDataValues)
}

func rowDataValues(r *schema.Row, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = r.GetTsBucket()
	dst[2] = r.GetTraceId()
	dst[3] = r.GetLogId()
	dst[4] = time.Unix(0, r.GetTimestampNs())
	dst[5] = r.GetService()
	dst[6] = r.GetEnvironment()
	dst[7] = r.GetHost()
	dst[8] = r.GetPod()
	dst[9] = r.GetContainer()
	dst[10] = r.GetSeverityText()
	dst[11] = uint8(r.GetSeverityNumber())
	dst[12] = severityBucketFor(r.GetSeverityNumber())
	dst[13] = r.GetAttributesString()
	dst[14] = r.GetAttributesNumber()
	dst[15] = r.GetAttributesBool()
	dst[16] = r.GetObservedTimestampNs()
	dst[17] = r.GetSpanId()
	dst[18] = r.GetTraceFlags()
	dst[19] = r.GetBody()
	dst[20] = r.GetResource()
	dst[21] = r.GetScopeName()
	dst[22] = r.GetScopeVersion()
}

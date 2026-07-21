package logs

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
)

const chTableData = "optikk.logs"

var chColumnsData = []string{
	"tenant_id", "fingerprint", "ts_bucket", "trace_id", "log_id", "timestamp",
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
	dst[1] = r.GetFingerprint()
	dst[2] = r.GetTsBucket()
	dst[3] = r.GetTraceId()
	dst[4] = r.GetLogId()
	dst[5] = time.Unix(0, r.GetTimestampNs())
	dst[6] = r.GetService()
	dst[7] = r.GetEnvironment()
	dst[8] = r.GetHost()
	dst[9] = r.GetPod()
	dst[10] = r.GetContainer()
	dst[11] = r.GetSeverityText()
	dst[12] = uint8(r.GetSeverityNumber())
	dst[13] = severityBucketFor(r.GetSeverityNumber())
	dst[14] = r.GetAttributesString()
	dst[15] = r.GetAttributesNumber()
	dst[16] = r.GetAttributesBool()
	dst[17] = r.GetObservedTimestampNs()
	dst[18] = r.GetSpanId()
	dst[19] = r.GetTraceFlags()
	dst[20] = r.GetBody()
	dst[21] = r.GetResource()
	dst[22] = r.GetScopeName()
	dst[23] = r.GetScopeVersion()
}

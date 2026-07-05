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
	"body", "body_search", "resource", "scope_name", "scope_version",
}

func NewDataWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTableData, chColumnsData, rowDataValues)
}

func rowDataValues(r *schema.Row) []any {
	return []any{
		r.GetTenantId(),
		r.GetFingerprint(),
		r.GetTsBucket(),
		r.GetTraceId(),
		r.GetLogId(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetService(),
		r.GetEnvironment(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetSeverityText(),
		uint8(r.GetSeverityNumber()),
		severityBucketFor(r.GetSeverityNumber()),
		r.GetAttributesString(),
		r.GetAttributesNumber(),
		r.GetAttributesBool(),
		r.GetObservedTimestampNs(),
		r.GetSpanId(),
		r.GetTraceFlags(),
		r.GetBody(),
		r.GetBody(),
		r.GetResource(),
		r.GetScopeName(),
		r.GetScopeVersion(),
	}
}

package logs

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
)

const chTable = "optikk.logs"

var chColumns = []string{
	"team_id", "ts_bucket", "timestamp", "observed_timestamp",
	"trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "severity_bucket", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "fingerprint", "log_id",
	"scope_name", "scope_version",
	"service", "host", "pod", "container", "environment",
}

func NewClickHouseWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTable, chColumns, rowValues)
}

func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetTsBucket(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetObservedTimestampNs(),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetTraceFlags(),
		r.GetSeverityText(),
		uint8(r.GetSeverityNumber()),
		severityBucketFor(r.GetSeverityNumber()),
		r.GetBody(),
		r.GetAttributesString(),
		r.GetAttributesNumber(),
		r.GetAttributesBool(),
		r.GetResource(),
		r.GetFingerprint(),
		r.GetLogId(),
		r.GetScopeName(),
		r.GetScopeVersion(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetEnvironment(),
	}
}

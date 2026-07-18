package spans

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

const chTable = "optikk.spans"

// chColumns mirrors the column order in db/clickhouse/01_spans.sql.
var chColumns = []string{
	"tenant_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host",
	"response_status_code",
	"service", "host", "pod", "service_version", "environment",
	"peer_service", "db_system", "db_name", "db_statement", "http_route",
	"http_status_bucket",
	"attributes",
	"fingerprint",
	"events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
	"gen_ai_system", "gen_ai_operation", "gen_ai_request_model", "gen_ai_response_model",
	"gen_ai_input_tokens", "gen_ai_output_tokens", "is_gen_ai",
	"gen_ai_prompt", "gen_ai_completion",
}

func NewClickHouseWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTable, chColumns, rowValues)
}

func rowValues(r *schema.Row) []any {
	return []any{
		r.GetTenantId(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetParentSpanId(),
		r.GetTraceState(),
		r.GetFlags(),
		r.GetName(),
		int8(r.GetKind()),
		r.GetKindString(),
		r.GetDurationNano(),
		r.GetHasError(),
		int16(r.GetStatusCode()),
		r.GetStatusCodeString(),
		r.GetStatusMessage(),
		r.GetHttpUrl(),
		r.GetHttpMethod(),
		r.GetHttpHost(),
		r.GetResponseStatusCode(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetServiceVersion(),
		r.GetEnvironment(),
		r.GetPeerService(),
		r.GetDbSystem(),
		r.GetDbName(),
		r.GetDbStatement(),
		r.GetHttpRoute(),
		r.GetHttpStatusBucket(),
		r.GetAttributes(),
		r.GetFingerprint(),
		r.GetEvents(),
		r.GetLinks(),
		r.GetExceptionType(),
		r.GetExceptionMessage(),
		r.GetExceptionStacktrace(),
		r.GetExceptionEscaped(),
		r.GetGenAiSystem(),
		r.GetGenAiOperation(),
		r.GetGenAiRequestModel(),
		r.GetGenAiResponseModel(),
		r.GetGenAiInputTokens(),
		r.GetGenAiOutputTokens(),
		r.GetIsGenAi(),
		r.GetGenAiPrompt(),
		r.GetGenAiCompletion(),
	}
}

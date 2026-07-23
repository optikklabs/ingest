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
	"events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
	"gen_ai_system", "gen_ai_operation", "gen_ai_request_model", "gen_ai_response_model",
	"gen_ai_input_tokens", "gen_ai_output_tokens", "is_gen_ai",
	"gen_ai_prompt", "gen_ai_completion",
	"llm_user_id", "llm_session_id", "llm_tags", "llm_release",
	"llm_prompt_name", "llm_prompt_version", "gen_ai_span_kind",
}

func NewClickHouseWriter(ch clickhouse.Conn) *core.ClickHouseWriter[*schema.Row] {
	return core.NewClickHouseWriter(ch, chTable, chColumns, rowValues)
}

func rowValues(r *schema.Row, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = time.Unix(0, r.GetTimestampNs())
	dst[2] = r.GetTraceId()
	dst[3] = r.GetSpanId()
	dst[4] = r.GetParentSpanId()
	dst[5] = r.GetTraceState()
	dst[6] = r.GetFlags()
	dst[7] = r.GetName()
	dst[8] = int8(r.GetKind())
	dst[9] = r.GetKindString()
	dst[10] = r.GetDurationNano()
	dst[11] = r.GetHasError()
	dst[12] = int16(r.GetStatusCode())
	dst[13] = r.GetStatusCodeString()
	dst[14] = r.GetStatusMessage()
	dst[15] = r.GetHttpUrl()
	dst[16] = r.GetHttpMethod()
	dst[17] = r.GetHttpHost()
	dst[18] = r.GetResponseStatusCode()
	dst[19] = r.GetService()
	dst[20] = r.GetHost()
	dst[21] = r.GetPod()
	dst[22] = r.GetServiceVersion()
	dst[23] = r.GetEnvironment()
	dst[24] = r.GetPeerService()
	dst[25] = r.GetDbSystem()
	dst[26] = r.GetDbName()
	dst[27] = r.GetDbStatement()
	dst[28] = r.GetHttpRoute()
	dst[29] = r.GetHttpStatusBucket()
	dst[30] = r.GetAttributes()
	dst[31] = r.GetEvents()
	dst[32] = r.GetLinks()
	dst[33] = r.GetExceptionType()
	dst[34] = r.GetExceptionMessage()
	dst[35] = r.GetExceptionStacktrace()
	dst[36] = r.GetExceptionEscaped()
	dst[37] = r.GetGenAiSystem()
	dst[38] = r.GetGenAiOperation()
	dst[39] = r.GetGenAiRequestModel()
	dst[40] = r.GetGenAiResponseModel()
	dst[41] = r.GetGenAiInputTokens()
	dst[42] = r.GetGenAiOutputTokens()
	dst[43] = r.GetIsGenAi()
	dst[44] = r.GetGenAiPrompt()
	dst[45] = r.GetGenAiCompletion()
	dst[46] = r.GetLlmUserId()
	dst[47] = r.GetLlmSessionId()
	dst[48] = r.GetLlmTags()
	dst[49] = r.GetLlmRelease()
	dst[50] = r.GetLlmPromptName()
	dst[51] = r.GetLlmPromptVersion()
	dst[52] = r.GetGenAiSpanKind()
}

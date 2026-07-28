package spans

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

const chTable = "optikk.spans"

var chColumns = []string{
	"tenant_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "duration_nano", "has_error",
	"status_code", "status_message",
	"http_url", "http_method", "http_host",
	"response_status_code",
	"service", "host", "pod", "service_version", "environment",
	"peer_service", "db_system", "db_name", "db_statement", "http_route",
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
	dst[9] = r.GetDurationNano()
	dst[10] = r.GetHasError()
	dst[11] = int16(r.GetStatusCode())
	dst[12] = r.GetStatusMessage()
	dst[13] = r.GetHttpUrl()
	dst[14] = r.GetHttpMethod()
	dst[15] = r.GetHttpHost()
	dst[16] = r.GetResponseStatusCode()
	dst[17] = r.GetService()
	dst[18] = r.GetHost()
	dst[19] = r.GetPod()
	dst[20] = r.GetServiceVersion()
	dst[21] = r.GetEnvironment()
	dst[22] = r.GetPeerService()
	dst[23] = r.GetDbSystem()
	dst[24] = r.GetDbName()
	dst[25] = r.GetDbStatement()
	dst[26] = r.GetHttpRoute()
	dst[27] = r.GetAttributes()
	dst[28] = r.GetEvents()
	dst[29] = r.GetLinks()
	dst[30] = r.GetExceptionType()
	dst[31] = r.GetExceptionMessage()
	dst[32] = r.GetExceptionStacktrace()
	dst[33] = r.GetExceptionEscaped()
	dst[34] = r.GetGenAiSystem()
	dst[35] = r.GetGenAiOperation()
	dst[36] = r.GetGenAiRequestModel()
	dst[37] = r.GetGenAiResponseModel()
	dst[38] = r.GetGenAiInputTokens()
	dst[39] = r.GetGenAiOutputTokens()
	dst[40] = r.GetIsGenAi()
	dst[41] = r.GetGenAiPrompt()
	dst[42] = r.GetGenAiCompletion()
	dst[43] = r.GetLlmUserId()
	dst[44] = r.GetLlmSessionId()
	dst[45] = r.GetLlmTags()
	dst[46] = r.GetLlmRelease()
	dst[47] = r.GetLlmPromptName()
	dst[48] = r.GetLlmPromptVersion()
	dst[49] = r.GetGenAiSpanKind()
}

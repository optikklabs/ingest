package llmscores

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
)

const chTable = "optikk.llm_scores"

// chColumns mirrors the column order in db/12_llm_scores.sql.
var chColumns = []string{
	"tenant_id", "timestamp", "trace_id", "span_id", "session_id", "user_id",
	"service", "environment", "name", "source", "data_type",
	"value", "string_value", "comment", "evaluator_id",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.ScoreRow] {
	return core.NewClickHouseWriter(ch, chTable, chColumns, rowValues)
}

func rowValues(r *schema.ScoreRow, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = time.Unix(0, r.GetTimestampNs())
	dst[2] = r.GetTraceId()
	dst[3] = r.GetSpanId()
	dst[4] = r.GetSessionId()
	dst[5] = r.GetUserId()
	dst[6] = r.GetService()
	dst[7] = r.GetEnvironment()
	dst[8] = r.GetName()
	dst[9] = r.GetSource()
	dst[10] = r.GetDataType()
	dst[11] = r.GetValue()
	dst[12] = r.GetStringValue()
	dst[13] = r.GetComment()
	dst[14] = r.GetEvaluatorId()
}

package logsresource

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
)

const chResourceTable = "optikk.logs_resource"

var chResourceColumns = []string{
	"team_id", "fingerprint", "service", "host", "pod", "container", "environment",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.Row] {
	return core.NewClickHouseWriter(ch, chResourceTable, chResourceColumns, resourceRowValues)
}

func resourceRowValues(r *schema.Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetFingerprint(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetEnvironment(),
	}
}

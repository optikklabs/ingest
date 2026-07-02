package spansresource

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
)

const chResourceTable = "optikk.spans_resource"

var chResourceColumns = []string{
	"team_id", "fingerprint", "service", "host", "pod", "environment",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.ResourceRow] {
	return core.NewClickHouseWriter(ch, chResourceTable, chResourceColumns, resourceRowValues)
}

func resourceRowValues(r *schema.ResourceRow) []any {
	return []any{
		r.GetTeamId(),
		r.GetFingerprint(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetEnvironment(),
	}
}


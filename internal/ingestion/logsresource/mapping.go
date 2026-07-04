package logsresource

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/logsresource/schema"
)

const chResourceTable = "optikk.logs_resource"

var chResourceColumns = []string{
	"tenant_id", "fingerprint", "ts_bucket", "service", "host", "pod", "container", "environment",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.ResourceRow] {
	return core.NewClickHouseWriter(ch, chResourceTable, chResourceColumns, resourceRowValues)
}

func resourceRowValues(r *schema.ResourceRow) []any {
	return []any{
		r.GetTenantId(),
		r.GetFingerprint(),
		r.GetTsBucket(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetEnvironment(),
	}
}

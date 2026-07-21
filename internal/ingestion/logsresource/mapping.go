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

func resourceRowValues(r *schema.ResourceRow, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = r.GetFingerprint()
	dst[2] = r.GetTsBucket()
	dst[3] = r.GetService()
	dst[4] = r.GetHost()
	dst[5] = r.GetPod()
	dst[6] = r.GetContainer()
	dst[7] = r.GetEnvironment()
}

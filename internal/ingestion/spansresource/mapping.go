package spansresource

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
)

const chResourceTable = "optikk.spans_resource"

var chResourceColumns = []string{
	"tenant_id", "fingerprint", "service", "host", "pod", "environment", "last_seen",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.ResourceRow] {
	return core.NewClickHouseWriter(ch, chResourceTable, chResourceColumns, resourceRowValues)
}

func resourceRowValues(r *schema.ResourceRow, dst []any) {
	dst[0] = r.GetTenantId()
	dst[1] = r.GetFingerprint()
	dst[2] = r.GetService()
	dst[3] = r.GetHost()
	dst[4] = r.GetPod()
	dst[5] = r.GetEnvironment()
	dst[6] = time.Now()
}

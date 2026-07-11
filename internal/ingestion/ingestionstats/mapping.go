package ingestionstats

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

const chStatsTable = "optikk.ingestion_stats"

var chStatsColumns = []string{
	"tenant_id", "bucket_hour", "signal", "service", "environment", "record_count", "byte_count",
}

func NewClickHouseWriter(ch clickhouse.Conn) core.Writer[*schema.StatRow] {
	return core.NewClickHouseWriter(ch, chStatsTable, chStatsColumns, statRowValues)
}

func statRowValues(r *schema.StatRow) []any {
	return []any{
		r.GetTenantId(),
		time.Unix(r.GetBucketUnix(), 0).UTC(),
		r.GetSignal(),
		r.GetService(),
		r.GetEnvironment(),
		r.GetRecordCount(),
		r.GetByteCount(),
	}
}

package logs

import (
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	statsschema "github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// statRows meters one usage row per resource block: record_count is the log
// records in the block, byte_count its exact OTLP wire size.
func statRows(tenantID uint32, req *logspb.ExportLogsServiceRequest) []*statsschema.StatRow {
	rls := req.GetResourceLogs()
	rows := make([]*statsschema.StatRow, 0, len(rls))
	for _, rl := range rls {
		var records uint64
		for _, sl := range rl.GetScopeLogs() {
			records += uint64(len(sl.GetLogRecords()))
		}
		if records == 0 {
			continue
		}
		var resAttrs = rl.GetResource().GetAttributes()
		res := otlp.AttrsToMap(resAttrs)
		rows = append(rows, ingestionstats.NewRow(
			tenantID, ingestionstats.SignalLogs,
			res["service.name"], res["deployment.environment"],
			records, uint64(proto.Size(rl)),
		))
	}
	return rows
}

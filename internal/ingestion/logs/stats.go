package logs

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	statsschema "github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

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
		dims := fingerprint.ResolveResource(res)
		rows = append(rows, ingestionstats.NewRow(
			tenantID, ingestionstats.SignalLogs,
			dims.Service, dims.Environment,
			records, uint64(proto.Size(rl)),
		))
	}
	return rows
}

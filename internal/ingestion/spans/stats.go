package spans

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	statsschema "github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// statRows meters one usage row per resource block: record_count is the spans
// in the block, byte_count its exact OTLP wire size.
func statRows(tenantID uint32, req *tracepb.ExportTraceServiceRequest) []*statsschema.StatRow {
	rss := req.GetResourceSpans()
	rows := make([]*statsschema.StatRow, 0, len(rss))
	for _, rs := range rss {
		var records uint64
		for _, ss := range rs.GetScopeSpans() {
			records += uint64(len(ss.GetSpans()))
		}
		if records == 0 {
			continue
		}
		res := otlp.AttrsToMap(rs.GetResource().GetAttributes())
		dims := fingerprint.ResolveResource(res)
		rows = append(rows, ingestionstats.NewRow(
			tenantID, ingestionstats.SignalSpans,
			dims.Service, dims.Environment,
			records, uint64(proto.Size(rs)),
		))
	}
	return rows
}

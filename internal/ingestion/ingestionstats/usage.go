package ingestionstats

import (
	"google.golang.org/protobuf/proto"

	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

// ResourceUsage is the per-resource record tally each signal's mapRequest
// already computes, so stat rows reuse it instead of re-resolving resources.
type ResourceUsage struct {
	Service     string
	Environment string
	Records     uint64
}

// EmitUsage records a request's per-resource usage against the recorder,
// sizing the whole OTLP request once for byte attribution.
func EmitUsage(rec Recorder, tenantID int64, signal Signal, usage []ResourceUsage, req proto.Message) {
	if rec == nil {
		return
	}
	if rows := usageRows(uint32(tenantID), signal, usage, uint64(proto.Size(req))); len(rows) > 0 {
		rec.Record(rows)
	}
}

// usageRows builds stat rows from mapper usage. The request byte size is
// measured once and apportioned across resources by record share; the last
// row takes the remainder so totals stay exact.
func usageRows(tenantID uint32, signal Signal, usage []ResourceUsage, totalBytes uint64) []*schema.StatRow {
	var totalRecords uint64
	for _, u := range usage {
		totalRecords += u.Records
	}
	if totalRecords == 0 {
		return nil
	}
	rows := make([]*schema.StatRow, 0, len(usage))
	remaining := totalBytes
	for i, u := range usage {
		bytes := totalBytes * u.Records / totalRecords
		if i == len(usage)-1 {
			bytes = remaining
		}
		remaining -= bytes
		rows = append(rows, newRow(tenantID, signal, u.Service, u.Environment, u.Records, bytes))
	}
	return rows
}

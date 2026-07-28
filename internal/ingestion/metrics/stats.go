package metrics

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	statsschema "github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func statRows(tenantID uint32, req *metricspb.ExportMetricsServiceRequest) []*statsschema.StatRow {
	rms := req.GetResourceMetrics()
	rows := make([]*statsschema.StatRow, 0, len(rms))
	for _, rm := range rms {
		var records uint64
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				records += dataPointCount(m)
			}
		}
		if records == 0 {
			continue
		}
		res := otlp.AttrsToMap(rm.GetResource().GetAttributes())
		dims := fingerprint.ResolveResource(res)
		rows = append(rows, ingestionstats.NewRow(
			tenantID, ingestionstats.SignalMetrics,
			dims.Service, dims.Environment,
			records, uint64(proto.Size(rm)),
		))
	}
	return rows
}

func dataPointCount(m *metricsdatapb.Metric) uint64 {
	switch d := m.GetData().(type) {
	case *metricsdatapb.Metric_Gauge:
		return uint64(len(d.Gauge.GetDataPoints()))
	case *metricsdatapb.Metric_Sum:
		return uint64(len(d.Sum.GetDataPoints()))
	case *metricsdatapb.Metric_Histogram:
		return uint64(len(d.Histogram.GetDataPoints()))
	case *metricsdatapb.Metric_ExponentialHistogram:
		return uint64(len(d.ExponentialHistogram.GetDataPoints()))
	case *metricsdatapb.Metric_Summary:
		return uint64(len(d.Summary.GetDataPoints()))
	}
	return 0
}

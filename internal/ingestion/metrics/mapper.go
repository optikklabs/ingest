package metrics

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	seriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	teamID uint32
	resMap map[string]string
}

func mapRequest(teamID int64, req *metricspb.ExportMetricsServiceRequest) ([]*schema.Row, []*seriesschema.SeriesRow) {
	acc := &rowAccumulator{seen: make(map[uint64]struct{})}
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := rowHeader{
			teamID: uint32(teamID),
			resMap: resMap,
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				appendMetric(acc, hdr, m)
			}
		}
	}
	return acc.rows, acc.series
}

// rowAccumulator collects metric rows and the unique series derived from them,
// deduplicating series by fingerprint as rows are added.
type rowAccumulator struct {
	rows   []*schema.Row
	series []*seriesschema.SeriesRow
	seen   map[uint64]struct{}
}

func (a *rowAccumulator) add(row *schema.Row, series *seriesschema.SeriesRow) {
	a.rows = append(a.rows, row)
	fp := series.GetFingerprint()
	if fp == 0 {
		return
	}
	if _, ok := a.seen[fp]; ok {
		return
	}
	a.seen[fp] = struct{}{}
	a.series = append(a.series, series)
}

func appendMetric(acc *rowAccumulator, hdr rowHeader, m *metricsdatapb.Metric) {
	switch data := m.Data.(type) {
	case *metricsdatapb.Metric_Gauge:
		for _, dp := range data.Gauge.GetDataPoints() {
			acc.add(gaugeRow(hdr, m, dp))
		}
	case *metricsdatapb.Metric_Sum:
		temp := temporalityString(data.Sum.GetAggregationTemporality())
		for _, dp := range data.Sum.GetDataPoints() {
			acc.add(sumRow(hdr, m, temp, data.Sum.GetIsMonotonic(), dp))
		}
	case *metricsdatapb.Metric_Histogram:
		temp := temporalityString(data.Histogram.GetAggregationTemporality())
		for _, dp := range data.Histogram.GetDataPoints() {
			acc.add(histogramRow(hdr, m, temp, dp))
		}
	}
}

func gaugeRow(hdr rowHeader, m *metricsdatapb.Metric, dp *metricsdatapb.NumberDataPoint) (*schema.Row, *seriesschema.SeriesRow) {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, m.GetName(), "Gauge", "Unspecified", false, tsNs, attrs, numberValue(dp))
}

func sumRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, isMono bool, dp *metricsdatapb.NumberDataPoint) (*schema.Row, *seriesschema.SeriesRow) {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, m.GetName(), "Sum", temporality, isMono, tsNs, attrs, numberValue(dp))
}

func histogramRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, dp *metricsdatapb.HistogramDataPoint) (*schema.Row, *seriesschema.SeriesRow) {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	row, series := scalarRow(hdr, m, m.GetName(), "Histogram", temporality, false, tsNs, attrs, 0)
	if dp.Sum != nil {
		row.HistSum = *dp.Sum
	}
	row.HistCount = dp.GetCount()
	row.HistBuckets = dp.GetExplicitBounds()
	row.HistCounts = dp.GetBucketCounts()
	return row, series
}

func scalarRow(hdr rowHeader, m *metricsdatapb.Metric, name, metricType, temporality string, isMonotonic bool, tsNs int64, attrs map[string]string, value float64) (*schema.Row, *seriesschema.SeriesRow) {
	return baseRow(hdr, m, name, metricType, temporality, isMonotonic, tsNs, attrs, value)
}

func baseRow(
	hdr rowHeader, m *metricsdatapb.Metric, name string,
	metricType, temporality string, isMonotonic bool,
	tsNs int64, attrs map[string]string,
	value float64,
) (*schema.Row, *seriesschema.SeriesRow) {
	normalizeAttrs(name, attrs)
	fp := fingerprint.SeriesHash(name, temporality, hdr.resMap, attrs)
	row := &schema.Row{
		TeamId:      hdr.teamID,
		MetricName:  name,
		Temporality: temporality,
		Fingerprint: fp,
		TimestampNs: tsNs,
		Value:       value,
	}
	series := &seriesschema.SeriesRow{
		TeamId:       hdr.teamID,
		Fingerprint:  fp,
		TimestampNs:  tsNs,
		MetricName:   name,
		MetricType:   metricType,
		Temporality:  temporality,
		IsMonotonic:  isMonotonic,
		Unit:         m.GetUnit(),
		Description:  m.GetDescription(),
		Service:      hdr.resMap["service.name"],
		Host:         hdr.resMap["host.name"],
		Environment:  hdr.resMap["deployment.environment"],
		K8SNamespace: hdr.resMap["k8s.namespace.name"],
		Pod:          hdr.resMap["k8s.pod.name"],
		Container:    hdr.resMap["k8s.container.name"],
		Attributes:   attrs,
	}
	return row, series
}

func temporalityString(t metricsdatapb.AggregationTemporality) string {
	switch t {
	case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return "Delta"
	case metricsdatapb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return "Cumulative"
	default:
		return "Unspecified"
	}
}

func numberValue(dp *metricsdatapb.NumberDataPoint) float64 {
	switch v := dp.Value.(type) {
	case *metricsdatapb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricsdatapb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	}
	return 0
}

package metrics

import (
	"math"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	seriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	tenantID uint32
	resMap map[string]string
}

func mapRequest(tenantID int64, req *metricspb.ExportMetricsServiceRequest) ([]*schema.Row, []*seriesschema.SeriesRow) {
	acc := &rowAccumulator{seen: make(map[uint64]struct{})}
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := rowHeader{
			tenantID: uint32(tenantID),
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
	case *metricsdatapb.Metric_ExponentialHistogram:
		temp := temporalityString(data.ExponentialHistogram.GetAggregationTemporality())
		for _, dp := range data.ExponentialHistogram.GetDataPoints() {
			acc.add(expHistogramRow(hdr, m, temp, dp))
		}
	case *metricsdatapb.Metric_Summary:
		for _, dp := range data.Summary.GetDataPoints() {
			acc.add(summaryRow(hdr, m, dp))
		}
	default:
		obsmetrics.MapperUnsupportedType.WithLabelValues("metrics").Inc()
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

func expHistogramRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, dp *metricsdatapb.ExponentialHistogramDataPoint) (*schema.Row, *seriesschema.SeriesRow) {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	row, series := scalarRow(hdr, m, m.GetName(), "ExponentialHistogram", temporality, false, tsNs, attrs, 0)
	if dp.Sum != nil {
		row.HistSum = *dp.Sum
	}
	row.HistCount = dp.GetCount()
	row.HistBuckets, row.HistCounts = expBucketsToExplicit(dp)
	return row, series
}

// expBucketsToExplicit converts OTLP exponential positive buckets to the
// explicit-bounds form (upper bounds + per-bucket counts, counts one longer
// than bounds) that the metrics rollup MV expects. Negative buckets are
// dropped; latency metrics are non-negative.
func expBucketsToExplicit(dp *metricsdatapb.ExponentialHistogramDataPoint) ([]float64, []uint64) {
	counts := dp.GetPositive().GetBucketCounts()
	if len(counts) == 0 {
		return nil, nil
	}
	base := math.Pow(2, math.Pow(2, float64(-dp.GetScale())))
	offset := int(dp.GetPositive().GetOffset())
	bounds := make([]float64, 0, len(counts)+1)
	out := make([]uint64, 0, len(counts)+2)
	// First bound/count cover the zero region below the lowest positive bucket.
	bounds = append(bounds, math.Pow(base, float64(offset)))
	out = append(out, dp.GetZeroCount())
	for i, c := range counts {
		bounds = append(bounds, math.Pow(base, float64(offset+i+1)))
		out = append(out, c)
	}
	out = append(out, 0) // no overflow above the top exponential bucket
	return bounds, out
}

func summaryRow(hdr rowHeader, m *metricsdatapb.Metric, dp *metricsdatapb.SummaryDataPoint) (*schema.Row, *seriesschema.SeriesRow) {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	row, series := scalarRow(hdr, m, m.GetName(), "Summary", "Cumulative", false, tsNs, attrs, 0)
	row.HistSum = dp.GetSum()
	row.HistCount = dp.GetCount()
	qs := dp.GetQuantileValues()
	row.SummaryQuantiles = make([]float64, 0, len(qs))
	row.SummaryValues = make([]float64, 0, len(qs))
	for _, q := range qs {
		row.SummaryQuantiles = append(row.SummaryQuantiles, q.GetQuantile())
		row.SummaryValues = append(row.SummaryValues, q.GetValue())
	}
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
		TenantId:      hdr.tenantID,
		MetricName:  name,
		Temporality: temporality,
		Fingerprint: fp,
		TimestampNs: tsNs,
		Value:       value,
	}
	series := &seriesschema.SeriesRow{
		TenantId:       hdr.tenantID,
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

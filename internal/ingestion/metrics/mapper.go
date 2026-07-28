package metrics

import (
	"math"
	"strings"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	seriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	tenantID uint32
	resMap   map[string]string
	resource fingerprint.ResourceDimensions

	hostResAttrs map[string]string
}

var hostResourceAttrKeys = map[string]struct{}{
	"k8s.node.name":    {},
	"k8s.cluster.name": {},
}

func hostResourceAttrs(resMap map[string]string) map[string]string {
	var out map[string]string
	for k, v := range resMap {
		_, keep := hostResourceAttrKeys[k]
		if !keep {
			keep = strings.HasPrefix(k, "os.") ||
				strings.HasPrefix(k, "host.") ||
				strings.HasPrefix(k, "cloud.")
		}
		if !keep {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[k] = v
	}
	return out
}

func mapRequest(tenantID int64, req *metricspb.ExportMetricsServiceRequest) ([]*schema.Row, []*seriesschema.SeriesRow, []ingestionstats.ResourceUsage) {
	acc := &rowAccumulator{seen: make(map[uint64]struct{})}
	usage := make([]ingestionstats.ResourceUsage, 0, len(req.GetResourceMetrics()))
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := rowHeader{
			tenantID:     uint32(tenantID),
			resMap:       resMap,
			resource:     fingerprint.ResolveResource(resMap),
			hostResAttrs: hostResourceAttrs(resMap),
		}
		before := len(acc.rows)
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				appendMetric(acc, hdr, m)
			}
		}
		if n := len(acc.rows) - before; n > 0 {
			usage = append(usage, ingestionstats.ResourceUsage{Service: hdr.resource.Service, Environment: hdr.resource.Environment, Records: uint64(n)})
		}
	}
	return acc.rows, acc.series, usage
}

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

func expBucketsToExplicit(dp *metricsdatapb.ExponentialHistogramDataPoint) ([]float64, []uint64) {
	counts := dp.GetPositive().GetBucketCounts()
	if len(counts) == 0 {
		return nil, nil
	}
	base := math.Pow(2, math.Pow(2, float64(-dp.GetScale())))
	offset := int(dp.GetPositive().GetOffset())
	bounds := make([]float64, 0, len(counts)+1)
	out := make([]uint64, 0, len(counts)+2)

	bounds = append(bounds, math.Pow(base, float64(offset)))
	out = append(out, dp.GetZeroCount())
	for i, c := range counts {
		bounds = append(bounds, math.Pow(base, float64(offset+i+1)))
		out = append(out, c)
	}
	out = append(out, 0)
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

func scalarRow(
	hdr rowHeader, m *metricsdatapb.Metric, name string,
	metricType, temporality string, isMonotonic bool,
	tsNs int64, attrs map[string]string,
	value float64,
) (*schema.Row, *seriesschema.SeriesRow) {
	normalizeAttrs(name, attrs)
	fp := fingerprint.SeriesHash(name, temporality, hdr.resMap, attrs)
	row := &schema.Row{
		TenantId:    hdr.tenantID,
		MetricName:  name,
		Temporality: temporality,
		Fingerprint: fp,
		TimestampNs: tsNs,
		Value:       value,
	}
	series := &seriesschema.SeriesRow{
		TenantId:     hdr.tenantID,
		Fingerprint:  fp,
		TimestampNs:  tsNs,
		MetricName:   name,
		MetricType:   metricType,
		Temporality:  temporality,
		IsMonotonic:  isMonotonic,
		Unit:         m.GetUnit(),
		Description:  m.GetDescription(),
		Service:      hdr.resource.Service,
		Host:         hdr.resource.Host,
		Environment:  hdr.resource.Environment,
		K8SNamespace: hdr.resource.Namespace,
		Pod:          hdr.resource.Pod,
		Container:    hdr.resource.Container,
		Attributes:   attrs,

		ResourceAttributes: hdr.hostResAttrs,
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

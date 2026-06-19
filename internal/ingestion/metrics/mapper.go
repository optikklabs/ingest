package metrics

import (
	"math"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	teamID      uint32
	fingerprint uint64
	resMap      map[string]string
}

func mapRequest(teamID int64, req *metricspb.ExportMetricsServiceRequest) []*schema.Row {
	var rows []*schema.Row
	for _, rm := range req.GetResourceMetrics() {
		var resAttrs []*commonpb.KeyValue
		if rm.Resource != nil {
			resAttrs = rm.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		hdr := rowHeader{
			teamID:      uint32(teamID),
			fingerprint: fingerprint.CalculateHash(resMap),
			resMap:      resMap,
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				rows = appendMetric(rows, hdr, m)
			}
		}
	}
	return rows
}

func appendMetric(rows []*schema.Row, hdr rowHeader, m *metricsdatapb.Metric) []*schema.Row {
	switch data := m.Data.(type) {
	case *metricsdatapb.Metric_Gauge:
		for _, dp := range data.Gauge.GetDataPoints() {
			rows = append(rows, gaugeRow(hdr, m, dp))
		}
	case *metricsdatapb.Metric_Sum:
		temp := temporalityString(data.Sum.GetAggregationTemporality())
		for _, dp := range data.Sum.GetDataPoints() {
			rows = append(rows, sumRow(hdr, m, temp, data.Sum.GetIsMonotonic(), dp))
		}
	case *metricsdatapb.Metric_Histogram:
		temp := temporalityString(data.Histogram.GetAggregationTemporality())
		for _, dp := range data.Histogram.GetDataPoints() {
			rows = append(rows, histogramRow(hdr, m, temp, dp))
		}
	}
	return rows
}

func gaugeRow(hdr rowHeader, m *metricsdatapb.Metric, dp *metricsdatapb.NumberDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, "Gauge", "Unspecified", false, tsNs, attrs, numberValue(dp))
}

func sumRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, isMono bool, dp *metricsdatapb.NumberDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, "Sum", temporality, isMono, tsNs, attrs, numberValue(dp))
}

// histogramRow maps an OTel histogram data point to a Schema Row,
// converting bounds to hist_buckets and keeping counts in hist_counts.
func histogramRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, dp *metricsdatapb.HistogramDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	sum := 0.0
	if dp.Sum != nil {
		sum = *dp.Sum
	}
	bounds := dp.GetExplicitBounds()
	counts := dp.GetBucketCounts()
	histBuckets := make([]float64, len(counts))
	for i := range counts {
		if i < len(bounds) {
			histBuckets[i] = bounds[i]
		} else {
			histBuckets[i] = math.Inf(1)
		}
	}
	row := baseRow(hdr, m, "Histogram", temporality, false, tsNs, attrs, 0)
	row.HistSum = sum
	row.HistCount = dp.GetCount()
	row.HistBuckets = histBuckets
	row.HistCounts = append([]uint64(nil), counts...)
	return row
}

func scalarRow(hdr rowHeader, m *metricsdatapb.Metric, metricType, temporality string, isMonotonic bool, tsNs int64, attrs map[string]string, value float64) *schema.Row {
	return baseRow(hdr, m, metricType, temporality, isMonotonic, tsNs, attrs, value)
}

func baseRow(
	hdr rowHeader, m *metricsdatapb.Metric,
	metricType, temporality string, isMonotonic bool,
	tsNs int64, attrs map[string]string,
	value float64,
) *schema.Row {
	normalizeAttrs(m.GetName(), attrs)
	bucket := timebucket.BucketStart(tsNs / 1_000_000_000)
	return &schema.Row{
		TeamId:              hdr.teamID,
		MetricName:          m.GetName(),
		MetricType:          metricType,
		Temporality:         temporality,
		IsMonotonic:         isMonotonic,
		Unit:                m.GetUnit(),
		Description:         m.GetDescription(),
		Fingerprint:         hdr.fingerprint,
		TimestampNs:         tsNs,
		TsBucketHourSeconds: int64(bucket),
		Value:               value,
		Resource:            hdr.resMap,
		Attributes:          attrs,
		Service:             hdr.resMap["service.name"],
		Host:                hdr.resMap["host.name"],
		Environment:         hdr.resMap["deployment.environment"],
		K8SNamespace:        hdr.resMap["k8s.namespace.name"],
		Pod:                 hdr.resMap["k8s.pod.name"],
		Container:           hdr.resMap["k8s.container.name"],
	}
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

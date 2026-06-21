package metrics

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type rowHeader struct {
	teamID uint32
	resMap map[string]string
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
			teamID: uint32(teamID),
			resMap: resMap,
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
	return scalarRow(hdr, m, m.GetName(), "Gauge", "Unspecified", false, tsNs, attrs, numberValue(dp))
}

func sumRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, isMono bool, dp *metricsdatapb.NumberDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	return scalarRow(hdr, m, m.GetName(), "Sum", temporality, isMono, tsNs, attrs, numberValue(dp))
}

// histogramRow maps an OTel histogram data point to a single row carrying the
// raw bounds/counts arrays plus sum/count. Percentiles are computed at read time
// (or pre-aggregated into the metrics_hist_* rollup's latency_state).
func histogramRow(hdr rowHeader, m *metricsdatapb.Metric, temporality string, dp *metricsdatapb.HistogramDataPoint) *schema.Row {
	tsNs := int64(dp.GetTimeUnixNano())
	attrs := otlp.AttrsToMap(dp.GetAttributes())
	row := scalarRow(hdr, m, m.GetName(), "Histogram", temporality, false, tsNs, attrs, 0)
	if dp.Sum != nil {
		row.HistSum = *dp.Sum
	}
	row.HistCount = dp.GetCount()
	row.HistBuckets = dp.GetExplicitBounds()
	row.HistCounts = dp.GetBucketCounts()
	return row
}

func scalarRow(hdr rowHeader, m *metricsdatapb.Metric, name, metricType, temporality string, isMonotonic bool, tsNs int64, attrs map[string]string, value float64) *schema.Row {
	return baseRow(hdr, m, name, metricType, temporality, isMonotonic, tsNs, attrs, value)
}

func baseRow(
	hdr rowHeader, m *metricsdatapb.Metric, name string,
	metricType, temporality string, isMonotonic bool,
	tsNs int64, attrs map[string]string,
	value float64,
) *schema.Row {
	normalizeAttrs(name, attrs)
	bucket := timebucket.BucketStart(tsNs / 1_000_000_000)
	return &schema.Row{
		TeamId:              hdr.teamID,
		MetricName:          name,
		MetricType:          metricType,
		Temporality:         temporality,
		IsMonotonic:         isMonotonic,
		Unit:                m.GetUnit(),
		Description:         m.GetDescription(),
		Fingerprint:         fingerprint.SeriesHash(name, temporality, hdr.resMap, attrs),
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

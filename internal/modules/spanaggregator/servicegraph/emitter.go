package servicegraph

import (
	"context"
	"fmt"
	"time"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
)

const (
	serverMetricName = "traces_service_graph_request_server"
	clientMetricName = "traces_service_graph_request_client"
	totalMetricName  = "traces_service_graph_request_total"
	failedMetricName = "traces_service_graph_request_failed_total"
)

// flush converts aggregated edge state into metric_series and metrics rows and
// publishes them. Series identity and payload share one fingerprint. Each edge
// emits a server histogram and, when observed, a client (caller-side) histogram.
func (c *Consumer) flush(ctx context.Context, state map[EdgeKey]*EdgeAgg) error {
	metricsRows := make([]*metricsschema.Row, 0, len(state)*2)
	seriesRows := make([]*metricseriesschema.SeriesRow, 0, len(state)*2)
	nowNs := time.Now().UnixNano()

	for k, e := range state {
		attrs := edgeAttrs(k)

		if e.Server.Count > 0 {
			appendHistogram(&seriesRows, &metricsRows, k.TenantId, serverMetricName, attrs, e.Server, nowNs)
		}
		if e.Client.Count > 0 {
			appendHistogram(&seriesRows, &metricsRows, k.TenantId, clientMetricName, attrs, e.Client, nowNs)
		}
		if e.Total > 0 {
			appendCounter(&seriesRows, &metricsRows, k.TenantId, totalMetricName, attrs, e.Total, nowNs)
		}
		if e.Failed > 0 {
			appendCounter(&seriesRows, &metricsRows, k.TenantId, failedMetricName, attrs, e.Failed, nowNs)
		}
	}

	if err := core.PublishMetricPair(ctx, c.seriesPub, seriesRows, c.metricsPub, metricsRows); err != nil {
		return fmt.Errorf("servicegraph paired publish: %w", err)
	}
	return nil
}

// edgeAttrs builds the label set shared by an edge's metrics. virtual_node and
// connection.type appear only on virtual edges, so ordinary-edge series
// identity (and fingerprints) stay unchanged from before this feature.
func edgeAttrs(k EdgeKey) map[string]string {
	attrs := map[string]string{
		"client": k.Client,
		"server": k.Server,
	}
	if k.VirtualNode != "" {
		attrs["virtual_node"] = k.VirtualNode
	}
	if k.ConnectionType != "" {
		attrs["connection.type"] = k.ConnectionType
	}
	return attrs
}

// appendHistogram emits one Delta histogram series+payload for a metric name.
func appendHistogram(
	series *[]*metricseriesschema.SeriesRow,
	metrics *[]*metricsschema.Row,
	tenantId uint32,
	metricName string,
	attrs map[string]string,
	agg *common.AggState,
	nowNs int64,
) {
	fp := fingerprint.SeriesHash(metricName, "Delta", attrs, nil)

	*series = append(*series, &metricseriesschema.SeriesRow{
		TenantId:    tenantId,
		Fingerprint: fp,
		MetricName:  metricName,
		TimestampNs: nowNs,
		MetricType:  "Histogram",
		Description: "Generated from spans",
		Unit:        "ms",
		Attributes:  attrs,
	})

	*metrics = append(*metrics, &metricsschema.Row{
		TenantId:    tenantId,
		MetricName:  metricName,
		Temporality: "Delta",
		Fingerprint: fp,
		TimestampNs: nowNs,
		HistSum:     agg.Sum,
		HistCount:   agg.Count,
		HistBuckets: common.HistogramBuckets,
		HistCounts:  agg.BucketCount,
	})
}

// appendCounter emits one monotonic Delta Sum series+payload for a counter.
func appendCounter(
	series *[]*metricseriesschema.SeriesRow,
	metrics *[]*metricsschema.Row,
	tenantId uint32,
	metricName string,
	attrs map[string]string,
	count uint64,
	nowNs int64,
) {
	fp := fingerprint.SeriesHash(metricName, "Delta", attrs, nil)

	*series = append(*series, &metricseriesschema.SeriesRow{
		TenantId:    tenantId,
		Fingerprint: fp,
		MetricName:  metricName,
		TimestampNs: nowNs,
		MetricType:  "Sum",
		IsMonotonic: true,
		Description: "Generated from spans",
		Attributes:  attrs,
	})

	*metrics = append(*metrics, &metricsschema.Row{
		TenantId:    tenantId,
		MetricName:  metricName,
		Temporality: "Delta",
		Fingerprint: fp,
		TimestampNs: nowNs,
		Value:       float64(count),
	})
}

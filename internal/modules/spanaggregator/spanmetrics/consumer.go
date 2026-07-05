package spanmetrics

import (
	"context"
	"fmt"
	"time"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Consumer reads span rows from Kafka, aggregates them into RED metrics,
// and publishes to the metrics and metric_series topics every 10s.
type Consumer struct {
	client     *kafkainfra.Consumer
	metricsPub *core.Producer[*metricsschema.Row]
	seriesPub  *core.Producer[*metricseriesschema.SeriesRow]
	aggregator *Aggregator
}

func NewConsumer(
	client *kafkainfra.Consumer, 
	metricsPub *core.Producer[*metricsschema.Row], 
	seriesPub *core.Producer[*metricseriesschema.SeriesRow],
) *Consumer {
	return &Consumer{
		client:     client,
		metricsPub: metricsPub,
		seriesPub:  seriesPub,
		aggregator: NewAggregator(),
	}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) error {
	for _, r := range recs {
		row := &spansschema.Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			continue
		}
		c.aggregator.Add(row)
	}
	
	stateToFlush := c.aggregator.FlushIfReady(10 * time.Second)
	if len(stateToFlush) > 0 {
		return c.flush(ctx, stateToFlush)
	}
	
	return nil
}

func (c *Consumer) flush(ctx context.Context, state map[AggKey]*common.AggState) error {
	var metricsRows []*metricsschema.Row
	var seriesRows []*metricseriesschema.SeriesRow
	
	nowNs := time.Now().UnixNano()
	
	for k, s := range state {
		dpAttrs := map[string]string{
			"span.name":   k.SpanName,
			"span.kind":   k.SpanKind,
			"status.code": k.StatusCode,
		}
		if k.HttpRoute != "" {
			dpAttrs["http.route"] = k.HttpRoute
		}
		if k.HttpStatusCode != "" {
			dpAttrs["http.status_code"] = k.HttpStatusCode
		}
		if k.DbSystem != "" {
			dpAttrs["db.system"] = k.DbSystem
		}
		
		fp := fingerprint.SeriesHash("traces.span.metrics.duration", "Delta", map[string]string{"service.name": k.Service}, dpAttrs)
		
		seriesRows = append(seriesRows, &metricseriesschema.SeriesRow{
			TenantId:    k.TenantId,
			Fingerprint: fp,
			MetricName:  "traces.span.metrics.duration",
			MetricType:  "Histogram",
			Description: "Generated from spans",
			Unit:        "ms",
			Service:     k.Service,
			Attributes:  dpAttrs,
		})
		
		metricsRows = append(metricsRows, &metricsschema.Row{
			TenantId:    k.TenantId,
			MetricName:  "traces.span.metrics.duration",
			Temporality: "Delta",
			Fingerprint: fp,
			TimestampNs: nowNs,
			HistSum:     s.Sum,
			HistCount:   s.Count,
			HistBuckets: common.HistogramBuckets,
			HistCounts:  s.BucketCount,
		})
	}
	
	// Publish series first
	if err := c.seriesPub.Publish(ctx, seriesRows); err != nil {
		return fmt.Errorf("spanmetrics series publish: %w", err)
	}
	
	// Publish metrics next
	if err := c.metricsPub.Publish(ctx, metricsRows); err != nil {
		return fmt.Errorf("spanmetrics metrics publish: %w", err)
	}
	
	return nil
}

package spanmetrics

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Consumer reads span rows from Kafka, aggregates them into RED metrics,
// and publishes to the metrics and metric_series topics every 10s.
//
// Accepted loss: aggregates buffered since the last ticker flush (<= flush
// interval) are lost on restart or rebalance. Source spans are unaffected;
// this matches the OTel spanmetrics connector trade-off. Do not add a
// shutdown flush — the loss window is an accepted design decision.
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
	go common.RunPeriodic(ctx, 10*time.Second, c.flushCurrent)
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

	return nil
}

func (c *Consumer) flushCurrent(ctx context.Context) {
	state := c.aggregator.Drain()
	if len(state) == 0 {
		return
	}
	if err := c.flush(ctx, state); err != nil {
		// The source spans remain durable in Kafka; a later interval proceeds
		// rather than blocking aggregation indefinitely.
		slog.ErrorContext(ctx, "spanmetrics flush failed", slog.Any("error", err))
	}
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
			TimestampNs: nowNs,
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

	if err := core.PublishMetricPair(ctx, c.seriesPub, seriesRows, c.metricsPub, metricsRows); err != nil {
		return fmt.Errorf("spanmetrics paired publish: %w", err)
	}

	return nil
}

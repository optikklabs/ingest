package servicegraph

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

// Consumer reads span rows from spans_tracegraph, pairs CLIENT and SERVER spans,
// and publishes edge metrics to metrics and metric_series topics.
type Consumer struct {
	client     *kafkainfra.Consumer
	metricsPub *core.Producer[*metricsschema.Row]
	seriesPub  *core.Producer[*metricseriesschema.SeriesRow]
	
	store      *Store
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
		store:      NewStore(),
		aggregator: NewAggregator(),
	}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) error {
	now := time.Now()
	
	for _, r := range recs {
		row := &spansschema.Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			continue
		}
		
		isClient := row.GetKindString() == "SPAN_KIND_CLIENT" || row.GetKindString() == "SPAN_KIND_PRODUCER"
		isServer := row.GetKindString() == "SPAN_KIND_SERVER" || row.GetKindString() == "SPAN_KIND_CONSUMER"
		
		if !isClient && !isServer {
			continue
		}
		
		var matchKey string
		if isClient {
			matchKey = fmt.Sprintf("%s:%s", row.GetTraceId(), row.GetSpanId())
		} else {
			matchKey = fmt.Sprintf("%s:%s", row.GetTraceId(), row.GetParentSpanId())
		}
		
		durMs := float64(row.GetDurationNano()) / 1000000.0
		
		if paired, exists := c.store.GetAndRemove(matchKey); exists {
			var clientSvc, serverSvc, statusCode string
			var edgeDur float64
			
			if isClient && paired.IsServer {
				clientSvc = row.GetService()
				serverSvc = paired.Service
				edgeDur = paired.Duration 
				statusCode = paired.StatusCode
			} else if isServer && paired.IsClient {
				clientSvc = paired.Service
				serverSvc = row.GetService()
				edgeDur = durMs
				statusCode = row.GetStatusCodeString()
			} else {
				continue
			}
			
			eKey := EdgeKey{
				TenantId:   row.GetTenantId(),
				Client:     clientSvc,
				Server:     serverSvc,
				StatusCode: statusCode,
			}
			
			c.aggregator.Add(eKey, edgeDur)
		} else {
			c.store.Add(matchKey, CachedSpan{
				TenantId:   row.GetTenantId(),
				Service:    row.GetService(),
				IsClient:   isClient,
				IsServer:   isServer,
				StatusCode: row.GetStatusCodeString(),
				Duration:   durMs,
				ExpiresAt:  now.Add(2 * time.Second),
			})
		}
	}
	
	stateToFlush := c.aggregator.FlushIfReady(10 * time.Second)
	if len(stateToFlush) > 0 {
		c.store.EvictExpired(time.Now())
		return c.flush(ctx, stateToFlush)
	}
	
	return nil
}

func (c *Consumer) flush(ctx context.Context, state map[EdgeKey]*common.AggState) error {
	var metricsRows []*metricsschema.Row
	var seriesRows []*metricseriesschema.SeriesRow
	nowNs := time.Now().UnixNano()
	
	for k, s := range state {
		resAttrs := map[string]string{
			"client": k.Client,
			"server": k.Server,
		}
		dpAttrs := map[string]string{
			"status.code": k.StatusCode,
		}
		
		fp := fingerprint.SeriesHash("traces_service_graph_request_server", "Delta", resAttrs, dpAttrs)
		
		mergedAttrs := map[string]string{
			"client": k.Client,
			"server": k.Server,
			"status.code": k.StatusCode,
		}

		seriesRows = append(seriesRows, &metricseriesschema.SeriesRow{
			TenantId:           k.TenantId,
			Fingerprint:        fp,
			MetricName:         "traces_service_graph_request_server",
			MetricType:         "Histogram",
			Description:        "Generated from spans",
			Unit:               "ms",
			Attributes:         mergedAttrs,
		})
		
		metricsRows = append(metricsRows, &metricsschema.Row{
			TenantId:    k.TenantId,
			MetricName:  "traces_service_graph_request_server",
			Temporality: "Delta",
			Fingerprint: fp,
			TimestampNs: nowNs,
			HistSum:     s.Sum,
			HistCount:   s.Count,
			HistBuckets: common.HistogramBuckets,
			HistCounts:  s.BucketCount,
		})
	}
	
	if err := c.seriesPub.Publish(ctx, seriesRows); err != nil {
		return fmt.Errorf("servicegraph series publish: %w", err)
	}
	if err := c.metricsPub.Publish(ctx, metricsRows); err != nil {
		return fmt.Errorf("servicegraph metrics publish: %w", err)
	}
	return nil
}

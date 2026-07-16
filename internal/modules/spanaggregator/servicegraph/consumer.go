package servicegraph

import (
	"context"
	"log/slog"
	"time"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Consumer reads span rows from spans_tracegraph, pairs CLIENT and SERVER spans,
// and publishes edge metrics to metrics and metric_series topics.
//
// Accepted loss: edges buffered since the last ticker flush (<= flush
// interval) are lost on restart or rebalance. Source spans are unaffected;
// this matches the OTel servicegraph connector trade-off. Do not add a
// shutdown flush — the loss window is an accepted design decision.
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
	go c.flushLoop(ctx)
	c.client.Run(ctx, c.handle)
}

// flushLoop publishes aggregated edges and expires unpaired spans on a fixed
// cadence, decoupling freshness from Kafka batch arrival.
func (c *Consumer) flushLoop(ctx context.Context) {
	common.RunPeriodic(ctx, flushInterval, func(ctx context.Context) {
		c.store.EvictExpired(time.Now(), c.onExpire)
		state := c.aggregator.Drain()
		if len(state) == 0 {
			return
		}
		if err := c.flush(ctx, state); err != nil {
			slog.ErrorContext(ctx, "servicegraph flush failed", slog.Any("error", err))
		}
	})
}

// onExpire synthesizes a virtual edge to an uninstrumented peer when a client
// span expires without ever pairing with a server span. Latency is the client
// span's own duration, since no server span exists.
func (c *Consumer) onExpire(span CachedSpan) {
	if !span.IsClient || span.PeerName == "" {
		return
	}
	// No server span exists, so the client's own duration is the only latency
	// signal; record it on the server histogram to keep the edge measurable.
	key := EdgeKey{
		TenantId:       span.TenantId,
		Client:         span.Service,
		Server:         span.PeerName,
		VirtualNode:    virtualNodeServer,
		ConnectionType: span.ConnectionType,
	}
	c.aggregator.AddServer(key, span.Duration)
	c.aggregator.Record(key, isErrorStatus(span.StatusCode))
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) error {
	now := time.Now()

	for _, r := range recs {
		row := &spansschema.Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			continue
		}

		isClient := row.GetKindString() == "CLIENT" || row.GetKindString() == "PRODUCER"
		isServer := row.GetKindString() == "SERVER" || row.GetKindString() == "CONSUMER"

		if !isClient && !isServer {
			continue
		}

		var matchKey spanKey
		if isClient {
			matchKey = spanKey{TraceID: row.GetTraceId(), SpanID: row.GetSpanId()}
		} else {
			matchKey = spanKey{TraceID: row.GetTraceId(), SpanID: row.GetParentSpanId()}
		}

		durMs := float64(row.GetDurationNano()) / 1000000.0

		if paired, exists := c.store.GetAndRemove(matchKey); exists {
			var clientSvc, serverSvc string
			var clientDur, serverDur float64

			if isClient && paired.IsServer {
				clientSvc = row.GetService()
				serverSvc = paired.Service
				clientDur = durMs
				serverDur = paired.Duration
			} else if isServer && paired.IsClient {
				clientSvc = paired.Service
				serverSvc = row.GetService()
				clientDur = paired.Duration
				serverDur = durMs
			} else {
				continue
			}

			eKey := EdgeKey{
				TenantId: row.GetTenantId(),
				Client:   clientSvc,
				Server:   serverSvc,
			}

			// A request is failed if either paired span reports an error.
			failed := isErrorStatus(row.GetStatusCodeString()) || isErrorStatus(paired.StatusCode)

			c.aggregator.AddServer(eKey, serverDur)
			c.aggregator.AddClient(eKey, clientDur)
			c.aggregator.Record(eKey, failed)
		} else {
			cached := CachedSpan{
				TenantId:   row.GetTenantId(),
				Service:    row.GetService(),
				IsClient:   isClient,
				IsServer:   isServer,
				StatusCode: row.GetStatusCodeString(),
				Duration:   durMs,
				ExpiresAt:  now.Add(pairingTTL),
			}
			if isClient {
				cached.PeerName, cached.ConnectionType = resolvePeer(row)
			}
			c.store.Add(matchKey, cached)
		}
	}

	return nil
}

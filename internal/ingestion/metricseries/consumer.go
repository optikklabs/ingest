package metricseries

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
)

// Consumer reads metric series rows from Kafka and writes them to ClickHouse.
type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

func NewConsumer(client *kafkainfra.Consumer, w core.Writer[*schema.SeriesRow], dlq *core.DLQ) *Consumer {
	return &Consumer{client: client, handle: core.NewInsertHandler("metric_series", w, dlq, func() *schema.SeriesRow { return &schema.SeriesRow{} })}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

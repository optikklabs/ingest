package ingestionstats

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

// Consumer reads usage StatRows from Kafka and writes them to ClickHouse.
type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

func NewConsumer(client *kafkainfra.Consumer, w core.Writer[*schema.StatRow], dlq *core.DLQ) *Consumer {
	return &Consumer{client: client, handle: core.NewInsertHandler("ingestion_stats", w, dlq, func() *schema.StatRow { return &schema.StatRow{} })}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

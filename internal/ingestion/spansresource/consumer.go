package spansresource

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
)

// Consumer reads spans resource rows from Kafka and writes them to ClickHouse.
type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

func NewConsumer(client *kafkainfra.Consumer, w core.Writer[*schema.ResourceRow], dlq *core.DLQ) *Consumer {
	return &Consumer{client: client, handle: core.NewInsertHandler("spans_resource", w, dlq, func() *schema.ResourceRow { return &schema.ResourceRow{} })}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

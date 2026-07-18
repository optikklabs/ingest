package spans

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

// Consumer reads span rows from Kafka and writes them to ClickHouse.
type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

func NewConsumer(client *kafkainfra.Consumer, w core.Writer[*schema.Row], dlq *core.DLQ) *Consumer {
	return &Consumer{client: client, handle: core.NewInsertHandler("spans", w, dlq, func() *schema.Row { return &schema.Row{} })}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

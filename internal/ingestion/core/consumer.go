package core

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
)

type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

func NewInsertConsumer[T Row](client *kafkainfra.Consumer, signal string, writer Writer[T], dlq *DLQ, newRow func() T) *Consumer {
	return &Consumer{
		client: client,
		handle: NewInsertHandler(signal, writer, dlq, newRow),
	}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

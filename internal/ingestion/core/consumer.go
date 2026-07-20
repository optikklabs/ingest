package core

import (
	"context"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
)

// Consumer is a standard Kafka record consumer that delegates execution to a RecordHandler.
type Consumer struct {
	client *kafkainfra.Consumer
	handle kafkainfra.RecordHandler
}

// NewConsumer creates a new Consumer wrapping a Kafka consumer client and handler.
func NewConsumer(client *kafkainfra.Consumer, handle kafkainfra.RecordHandler) *Consumer {
	return &Consumer{client: client, handle: handle}
}

// NewInsertConsumer creates a Consumer wired with the standard durable ClickHouse InsertHandler.
func NewInsertConsumer[T Row](client *kafkainfra.Consumer, signal string, writer Writer[T], dlq *DLQ, newRow func() T) *Consumer {
	return &Consumer{
		client: client,
		handle: NewInsertHandler(signal, writer, dlq, newRow),
	}
}

// Run starts consuming records from Kafka until context cancellation.
func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

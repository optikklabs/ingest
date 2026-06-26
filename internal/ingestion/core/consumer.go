package core

import (
	"context"
	"log/slog"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Consumer processes Kafka records, writes them, and routes failures to DLQ.
type Consumer[T Row] struct {
	client *kafkainfra.Consumer
	writer Writer[T]
	dlq    *DLQ
	newRow func() T
	signal string
}

func NewConsumer[T Row](client *kafkainfra.Consumer, w Writer[T], dlq *DLQ, signal string, newRow func() T) *Consumer[T] {
	return &Consumer[T]{client: client, writer: w, dlq: dlq, signal: signal, newRow: newRow}
}

func (c *Consumer[T]) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

func (c *Consumer[T]) handle(ctx context.Context, recs []*kgo.Record) error {
	rows := make([]T, 0, len(recs))
	for _, r := range recs {
		row := c.newRow()
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.WarnContext(ctx, c.signal+" consumer: dropped malformed record",
				slog.Int("partition", int(r.Partition)),
				slog.Int64("offset", r.Offset),
				slog.Any("error", err),
			)
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	if err := c.writer.Insert(ctx, rows); err != nil {
		slog.ErrorContext(ctx, c.signal+" consumer: CH insert failed → DLQ",
			slog.Int("rows", len(rows)),
			slog.Any("error", err),
		)
		c.dlq.PublishAll(ctx, recs, err)

		return nil
	}
	return nil
}

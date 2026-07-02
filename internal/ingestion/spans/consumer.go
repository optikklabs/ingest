package spans

import (
	"context"
	"log/slog"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Consumer reads span rows from Kafka and writes them to ClickHouse.
type Consumer struct {
	client *kafkainfra.Consumer
	writer core.Writer[*schema.Row]
	dlq    *core.DLQ
}

func NewConsumer(client *kafkainfra.Consumer, w core.Writer[*schema.Row], dlq *core.DLQ) *Consumer {
	return &Consumer{client: client, writer: w, dlq: dlq}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) error {
	rows := make([]*schema.Row, 0, len(recs))
	for _, r := range recs {
		row := &schema.Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.WarnContext(ctx, "spans consumer: dropped malformed record",
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
		slog.ErrorContext(ctx, "spans consumer: CH insert failed → DLQ",
			slog.Int("rows", len(rows)),
			slog.Any("error", err),
		)
		c.dlq.PublishAll(ctx, recs, err)
		return nil
	}
	return nil
}

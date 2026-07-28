package core

import (
	"context"
	"log/slog"
	"sync"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func NewInsertHandler[T Row](signal string, writer Writer[T], dlq *DLQ, newRow func() T) kafkainfra.RecordHandler {
	rowPool := sync.Pool{
		New: func() any { return newRow() },
	}

	return func(ctx context.Context, recs []*kgo.Record) error {
		rows := make([]T, 0, len(recs))

		for _, rec := range recs {
			row := rowPool.Get().(T)
			if msg, ok := any(row).(proto.Message); ok {
				proto.Reset(msg)
			}
			if err := proto.Unmarshal(rec.Value, row); err != nil {
				slog.WarnContext(ctx, "ingest consumer: dropped malformed record",
					slog.String("signal", signal),
					slog.Int("partition", int(rec.Partition)),
					slog.Int64("offset", rec.Offset),
					slog.Any("error", err),
				)
				rowPool.Put(row)
				continue
			}
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			return nil
		}
		if err := writer.Insert(ctx, rows); err != nil {
			slog.ErrorContext(ctx, "ingest consumer: ClickHouse insert failed → DLQ",
				slog.String("signal", signal),
				slog.Int("rows", len(rows)),
				slog.Any("error", err),
			)
			dlq.PublishAll(ctx, recs, err)
		}
		for _, r := range rows {
			rowPool.Put(r)
		}
		return nil
	}
}

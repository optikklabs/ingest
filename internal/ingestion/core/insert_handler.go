package core

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
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
		// A panic must not escape to the consumer: it would halt commits and
		// kill every signal consumer in the pod. DLQ the batch like an insert
		// failure so offsets never advance past an un-inserted, un-DLQ'd batch.
		defer func() {
			if p := recover(); p != nil {
				slog.ErrorContext(ctx, "ingest consumer: handler panic → DLQ",
					slog.String("signal", signal),
					slog.Int("records", len(recs)),
					slog.Any("panic", p),
					slog.String("stack", string(debug.Stack())),
				)
				dlq.PublishAll(ctx, recs, DLQReasonPanic, fmt.Errorf("panic: %v", p))
			}
		}()
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
			dlq.PublishAll(ctx, recs, DLQReasonInsertFailure, err)
		}
		for _, r := range rows {
			rowPool.Put(r)
		}
		return nil
	}
}

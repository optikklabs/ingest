package core

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"runtime/debug"
	"slices"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const insertTimeout = 30 * time.Second

// dedupToken derives a deterministic insert_deduplication_token from the
// batch's sorted (partition, offset-range) spans, so ClickHouse drops the
// block when Kafka redelivers the same record set after a crash before
// commit. Redelivery with different batch boundaries still duplicates —
// accepted, matching the platform's accepted-loss semantics.
func dedupToken(signal string, recs []*kgo.Record) string {
	type offsetSpan struct{ min, max int64 }
	byPartition := make(map[int32]*offsetSpan)
	for _, rec := range recs {
		s, ok := byPartition[rec.Partition]
		if !ok {
			byPartition[rec.Partition] = &offsetSpan{min: rec.Offset, max: rec.Offset}
			continue
		}
		s.min = min(s.min, rec.Offset)
		s.max = max(s.max, rec.Offset)
	}
	partitions := make([]int32, 0, len(byPartition))
	for p := range byPartition {
		partitions = append(partitions, p)
	}
	slices.Sort(partitions)

	h := fnv.New64a()
	_, _ = io.WriteString(h, signal)
	for _, p := range partitions {
		s := byPartition[p]
		_, _ = fmt.Fprintf(h, "|%d:%d-%d", p, s.min, s.max)
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

func NewInsertHandler[T Row](signal string, writer Writer[T], dlq *DLQ, newRow func() T) kafkainfra.RecordHandler {
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
			row := newRow()
			if err := proto.Unmarshal(rec.Value, row); err != nil {
				metrics.MalformedRecords.WithLabelValues(signal).Inc()
				slog.WarnContext(ctx, "ingest consumer: dropped malformed record",
					slog.String("signal", signal),
					slog.Int("partition", int(rec.Partition)),
					slog.Int64("offset", rec.Offset),
					slog.Any("error", err),
				)
				continue
			}
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			return nil
		}
		// A hung ClickHouse connection must not wedge the worker (and, via
		// in-order commits, every batch behind it). On expiry the batch is
		// DLQ'd like any other insert failure — a deadline, not a retry.
		insertCtx, cancel := context.WithTimeout(ctx, insertTimeout)
		defer cancel()
		insertCtx = clickhouse.Context(insertCtx, clickhouse.WithSettings(clickhouse.Settings{
			"insert_deduplication_token": dedupToken(signal, recs),
		}))
		if err := writer.Insert(insertCtx, rows); err != nil {
			slog.ErrorContext(ctx, "ingest consumer: ClickHouse insert failed → DLQ",
				slog.String("signal", signal),
				slog.Int("rows", len(rows)),
				slog.Any("error", err),
			)
			dlq.PublishAll(ctx, recs, DLQReasonInsertFailure, err)
		}
		return nil
	}
}

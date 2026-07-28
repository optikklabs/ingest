package core

import (
	"context"
	"log/slog"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type DLQ struct {
	base   *kafkainfra.Producer
	topic  string
	signal string
}

func NewDLQ(base *kafkainfra.Producer, topic, signal string) *DLQ {
	return &DLQ{base: base, topic: topic, signal: signal}
}

// Low-cardinality reason classes for the dlq_records_total metric; the full
// error text travels in the x-dlq-reason record header instead.
const (
	DLQReasonInsertFailure = "insert_failure"
	DLQReasonPanic         = "panic"
)

func (d *DLQ) PublishAll(ctx context.Context, recs []*kgo.Record, reasonClass string, reason error) {
	if d == nil || len(recs) == 0 {
		return
	}
	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}
	out := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		out = append(out, &kgo.Record{
			Topic: d.topic,
			Key:   r.Key,
			Value: r.Value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(d.signal)},
			},
		})
	}
	if err := d.base.PublishBatch(ctx, out); err == nil {
		metrics.DLQRecordsTotal.WithLabelValues(d.signal, reasonClass).Add(float64(len(recs)))
	} else {
		metrics.RecordsLost.WithLabelValues(d.signal).Add(float64(len(recs)))
		first, last := recs[0], recs[len(recs)-1]
		slog.ErrorContext(ctx, "core dlq: publish failed, records lost",
			slog.String("topic", d.topic),
			slog.String("signal", d.signal),
			slog.Int("records", len(out)),
			slog.Int("first_partition", int(first.Partition)),
			slog.Int64("first_offset", first.Offset),
			slog.Int("last_partition", int(last.Partition)),
			slog.Int64("last_offset", last.Offset),
			slog.Any("error", err),
		)
	}
}

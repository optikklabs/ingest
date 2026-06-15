package core

import (
	"context"
	"log/slog"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DLQ republishes original record bytes to the DLQ topic on writer failure.
type DLQ struct {
	base   *kafkainfra.Producer
	topic  string
	signal string
}

func NewDLQ(base *kafkainfra.Producer, topic, signal string) *DLQ {
	return &DLQ{base: base, topic: topic, signal: signal}
}

// PublishAll forwards all records to the DLQ topic. Errors are logged but
// do not block the consumer.
func (d *DLQ) PublishAll(ctx context.Context, recs []*kgo.Record, reason error) {
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
	if err := d.base.PublishBatch(ctx, out); err != nil {
		slog.WarnContext(ctx, "core dlq: publish failed",
			slog.String("topic", d.topic),
			slog.Int("records", len(out)),
			slog.Any("error", err),
		)
	}
}

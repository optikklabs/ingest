package core

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer publishes mapped Rows to Kafka using the shared base producer.
// The key is fingerprint for balanced per-series partitioning.
type Producer[T Row] struct {
	topic string
	base  *kafkainfra.Producer
}

func NewProducer[T Row](topic string, base *kafkainfra.Producer) *Producer[T] {
	return &Producer[T]{topic: topic, base: base}
}

// Publish marshals each row into a kgo.Record and produces them in a batch.
// The call blocks until the broker has acknowledged all records.
func (p *Producer[T]) Publish(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now()
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("core producer: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic:     p.topic,
			Key:       []byte(strconv.FormatUint(r.GetFingerprint(), 10)),
			Value:     value,
			Timestamp: now,
		})
	}
	if err := p.base.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("core producer: publish batch: %w", err)
	}
	return nil
}

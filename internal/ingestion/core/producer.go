package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type Producer[T Row] struct {
	topic   string
	base    *kafkainfra.Producer
	keyFunc func(T) []byte
}

func NewProducer[T Row](topic string, base *kafkainfra.Producer) *Producer[T] {
	return &Producer[T]{
		topic: topic,
		base:  base,
		keyFunc: func(r T) []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, r.GetTenantId())
			return b
		},
	}
}

func (p *Producer[T]) WithKeyFunc(f func(T) []byte) *Producer[T] {
	p.keyFunc = f
	return p
}

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
			Key:       p.keyFunc(r),
			Value:     value,
			Timestamp: now,
		})
	}

	if err := p.base.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("core producer: publish batch: %w", err)
	}
	return nil
}

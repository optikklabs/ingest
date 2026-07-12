package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// marshalBufPool reuses one backing buffer per Publish call. PublishBatch
// blocks until every record is acked, so the buffer is free to reclaim once it
// returns.
var marshalBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 4096); return &b },
}

// Producer publishes mapped Rows to Kafka using the shared base producer.
// The key is fingerprint for balanced per-series partitioning.
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
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, r.GetFingerprint())
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
	bufp := marshalBufPool.Get().(*[]byte)
	records, buf, err := p.marshalBatch(rows, time.Now(), (*bufp)[:0])
	if err != nil {
		*bufp = buf
		marshalBufPool.Put(bufp)
		return fmt.Errorf("core producer: marshal: %w", err)
	}

	pubErr := p.base.PublishBatch(ctx, records)
	*bufp = buf
	marshalBufPool.Put(bufp)
	if pubErr != nil {
		return fmt.Errorf("core producer: publish batch: %w", pubErr)
	}
	return nil
}

// marshalBatch packs every row into one pre-sized buffer and returns records
// whose Values are subslices of it. Pre-sizing to the exact wire size prevents
// a mid-loop realloc from invalidating earlier subslices. Returns the grown
// buffer so the caller can return it to the pool.
func (p *Producer[T]) marshalBatch(rows []T, now time.Time, buf []byte) ([]*kgo.Record, []byte, error) {
	total := 0
	for _, r := range rows {
		total += proto.Size(r)
	}
	if cap(buf) < total {
		buf = make([]byte, 0, total)
	}

	var opts proto.MarshalOptions
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		start := len(buf)
		var err error
		buf, err = opts.MarshalAppend(buf, r)
		if err != nil {
			return nil, buf, err
		}
		// Full three-index slice so franz-go can never append past this row.
		records = append(records, &kgo.Record{
			Topic:     p.topic,
			Key:       p.keyFunc(r),
			Value:     buf[start:len(buf):len(buf)],
			Timestamp: now,
		})
	}
	return records, buf, nil
}

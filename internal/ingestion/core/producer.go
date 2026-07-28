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

var marshalBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 4096); return &b },
}

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

const maxPooledBufCap = 512 * 1024

func (p *Producer[T]) Publish(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	bufp := marshalBufPool.Get().(*[]byte)
	records, buf, err := p.marshalBatch(rows, time.Now(), (*bufp)[:0])
	if err != nil {
		p.recycleBuf(bufp, buf)
		return fmt.Errorf("core producer: marshal: %w", err)
	}

	pubErr := p.base.PublishBatch(ctx, records)
	p.recycleBuf(bufp, buf)
	if pubErr != nil {
		return fmt.Errorf("core producer: publish batch: %w", pubErr)
	}
	return nil
}

func (p *Producer[T]) recycleBuf(bufp *[]byte, buf []byte) {
	if cap(buf) > maxPooledBufCap {

		return
	}
	*bufp = buf
	marshalBufPool.Put(bufp)
}

func (p *Producer[T]) marshalBatch(rows []T, now time.Time, buf []byte) ([]*kgo.Record, []byte, error) {
	total := 0
	for _, r := range rows {
		total += proto.Size(r)
	}
	if cap(buf) < total {
		buf = make([]byte, 0, total)
	}

	// proto.Size above cached each row's size; reuse it during marshal.
	opts := proto.MarshalOptions{UseCachedSize: true}
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		start := len(buf)
		var err error
		buf, err = opts.MarshalAppend(buf, r)
		if err != nil {
			return nil, buf, err
		}

		records = append(records, &kgo.Record{
			Topic:     p.topic,
			Key:       p.keyFunc(r),
			Value:     buf[start:len(buf):len(buf)],
			Timestamp: now,
		})
	}
	return records, buf, nil
}

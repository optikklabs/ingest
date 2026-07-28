package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(client *kgo.Client) *Producer { return &Producer{client: client} }

func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

func (p *Producer) Close() { p.client.Close() }

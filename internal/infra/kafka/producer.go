package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a thin wrapper around *kgo.Client shared across all signal
// producers, batching concurrent Produce calls.
type Producer struct {
	client *kgo.Client
}

func NewProducer(client *kgo.Client) *Producer { return &Producer{client: client} }

func (p *Producer) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	wg.Add(len(records))
	for _, r := range records {
		p.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return firstErr
}

func (p *Producer) PublishSync(ctx context.Context, rec *kgo.Record) error {
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *Producer) Flush(ctx context.Context) error { return p.client.Flush(ctx) }
func (p *Producer) Close()                          { p.client.Close() }
func (p *Producer) Client() *kgo.Client             { return p.client }

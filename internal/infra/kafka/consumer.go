package kafka

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer wraps a kgo.Client to poll and commit records in a loop.
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(client *kgo.Client) *Consumer { return &Consumer{client: client} }

func (c *Consumer) Client() *kgo.Client { return c.client }
func (c *Consumer) Close()              { c.client.Close() }

type RecordHandler func(ctx context.Context, recs []*kgo.Record) error

func (c *Consumer) Run(ctx context.Context, handle RecordHandler) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := c.client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				return
			}
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.WarnContext(ctx, "kafka fetch error",
				slog.String("topic", t),
				slog.Int("partition", int(p)),
				slog.Any("error", err),
			)
		})
		recs := make([]*kgo.Record, 0, fetches.NumRecords())
		iter := fetches.RecordIter()
		for !iter.Done() {
			recs = append(recs, iter.Next())
		}
		if len(recs) == 0 {
			continue
		}
		if err := handle(ctx, recs); err != nil {
			slog.ErrorContext(ctx, "kafka handler error",
				slog.Any("error", err),
				slog.Int("records", len(recs)),
			)
			continue
		}
		if err := c.client.CommitRecords(ctx, recs...); err != nil {
			slog.ErrorContext(ctx, "kafka commit error",
				slog.Any("error", err),
				slog.Int("records", len(recs)),
			)
		}
	}
}

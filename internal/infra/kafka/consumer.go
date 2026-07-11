package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	fetchBackoffMin = 100 * time.Millisecond
	fetchBackoffMax = 2 * time.Second
)

// Consumer wraps a kgo.Client to poll and commit records in a loop.
type Consumer struct {
	client         *kgo.Client
	maxPollRecords int
}

func NewConsumer(client *kgo.Client, maxPollRecords int) *Consumer {
	if maxPollRecords <= 0 {
		maxPollRecords = 5_000
	}
	return &Consumer{client: client, maxPollRecords: maxPollRecords}
}

func (c *Consumer) Client() *kgo.Client { return c.client }
func (c *Consumer) Close()              { c.client.Close() }

// RecordHandler processes a polled batch. Handlers own the durability
// policy: they must return nil once records are written, DLQ'd, or
// intentionally dropped (retry/DLQ live in ingestion/core, not here).
type RecordHandler func(ctx context.Context, recs []*kgo.Record) error

// Run continuously polls Kafka and delegates to the handler.
func (c *Consumer) Run(ctx context.Context, handle RecordHandler) {
	backoff := fetchBackoffMin
	for {
		fetches := c.client.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		// Franz-go reconnects internally, but avoid a tight retry loop while a
		// broker or partition is returning only errors.
		hadFetchErr := false
		fetches.EachError(func(t string, p int32, err error) {
			hadFetchErr = true
			slog.WarnContext(ctx, "kafka fetch error", slog.String("topic", t), slog.Int("partition", int(p)), slog.Any("error", err))
		})

		recs := fetches.Records()
		if len(recs) == 0 {
			if hadFetchErr {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > fetchBackoffMax {
					backoff = fetchBackoffMax
				}
			} else {
				backoff = fetchBackoffMin
			}
			continue
		}
		backoff = fetchBackoffMin

		// A handler error is a bug (handlers return nil after retry/DLQ):
		// skip the commit so the batch is redelivered after a rebalance
		// or restart rather than silently dropped.
		if err := handle(ctx, recs); err != nil {
			slog.ErrorContext(ctx, "kafka handler error", slog.Any("error", err), slog.Int("records", len(recs)))
			continue
		}

		// Commit offsets
		if err := c.client.CommitRecords(ctx, recs...); err != nil {
			slog.ErrorContext(ctx, "kafka commit error", slog.Any("error", err))
		}
	}
}

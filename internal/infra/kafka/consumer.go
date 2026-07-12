package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

const (
	fetchBackoffMin = 100 * time.Millisecond
	fetchBackoffMax = 2 * time.Second
)

// Consumer wraps a kgo.Client to poll and commit records in a loop.
type Consumer struct {
	client         *kgo.Client
	maxPollRecords int
	signal         string
}

func NewConsumer(client *kgo.Client, maxPollRecords int, signal string) *Consumer {
	if maxPollRecords <= 0 {
		maxPollRecords = 5_000
	}
	return &Consumer{client: client, maxPollRecords: maxPollRecords, signal: signal}
}

func (c *Consumer) Client() *kgo.Client { return c.client }
func (c *Consumer) Close()              { c.client.Close() }

// RecordHandler processes a polled batch. Handlers own the durability
// policy: they must return nil once records are written, DLQ'd, or
// intentionally dropped (retry/DLQ live in ingestion/core, not here).
type RecordHandler func(ctx context.Context, recs []*kgo.Record) error

// commitFunc commits a batch's offsets; a seam for testing the worker
// without a live broker.
type commitFunc func(ctx context.Context, recs []*kgo.Record) error

// Run double-buffers the consume loop: a fetcher polls the next batch while a
// single worker inserts + commits the current one, connected by a depth-1
// channel. Depth-1 bounds in-flight work to two batches (one fetching, one
// inserting) and gives natural backpressure — the fetcher blocks on a full
// channel instead of buffering unbounded.
func (c *Consumer) Run(ctx context.Context, handle RecordHandler) {
	batches := make(chan []*kgo.Record, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processBatches(ctx, batches, handle, c.commit, c.signal)
	}()

	// Fetch on the calling goroutine; on exit, close the channel so the
	// worker drains the last buffered batch and returns, then join.
	c.fetchLoop(ctx, batches)
	close(batches)
	wg.Wait()
}

// fetchLoop polls Kafka and hands each non-empty batch to the worker. It owns
// fetch-error backoff and blocks on a full channel (backpressure).
func (c *Consumer) fetchLoop(ctx context.Context, out chan<- []*kgo.Record) {
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

		select {
		case out <- recs:
			metrics.ConsumerInflightBatches.WithLabelValues(c.signal).Inc()
		case <-ctx.Done():
			return
		}
	}
}

// commit wraps CommitRecords so the worker can be tested against a fake.
func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) error {
	return c.client.CommitRecords(ctx, recs...)
}

// processBatches drains the fetcher's channel FIFO, keeping commits strictly
// ordered under a single worker. It returns once the channel is closed and
// drained (clean shutdown).
func processBatches(ctx context.Context, in <-chan []*kgo.Record, handle RecordHandler, commit commitFunc, signal string) {
	for recs := range in {
		processBatch(ctx, recs, handle, commit, signal)
		metrics.ConsumerInflightBatches.WithLabelValues(signal).Dec()
	}
}

// processBatch handles one batch then commits only on success. A handler error
// skips the commit so the batch is redelivered rather than silently dropped.
func processBatch(ctx context.Context, recs []*kgo.Record, handle RecordHandler, commit commitFunc, signal string) {
	start := time.Now()
	if err := handle(ctx, recs); err != nil {
		slog.ErrorContext(ctx, "kafka handler error", slog.Any("error", err), slog.Int("records", len(recs)))
		return
	}
	metrics.ConsumerBatchInsertDuration.WithLabelValues(signal).Observe(time.Since(start).Seconds())

	if err := commit(ctx, recs); err != nil {
		slog.ErrorContext(ctx, "kafka commit error", slog.Any("error", err))
	}
}

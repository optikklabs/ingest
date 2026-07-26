package kafka

import (
	"context"
	"fmt"
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
	workers        int
	signal         string
}

func NewConsumer(client *kgo.Client, maxPollRecords, workers int, signal string) *Consumer {
	if maxPollRecords <= 0 {
		maxPollRecords = 5_000
	}
	if workers <= 0 {
		workers = 1
	}
	return &Consumer{client: client, maxPollRecords: maxPollRecords, workers: workers, signal: signal}
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

type batchJob struct {
	recs []*kgo.Record
	done chan error
}

// Run double-buffers the consume loop with a configurable number of parallel
// worker goroutines. The fetcher polls batches and routes them to available
// workers. A single committer goroutine awaits each batch's completion in strict
// poll order to guarantee offset progression correctness without data loss.
func (c *Consumer) Run(ctx context.Context, handle RecordHandler) {
	// Cancelled by the committer when a handler fails, so the fetch loop stops
	// pulling records that can no longer be committed.
	ctx, halt := context.WithCancel(ctx)
	defer halt()

	// Worker channel depth limits in-flight batches when all workers are busy.
	workerChan := make(chan batchJob, c.workers*2)
	// Committer channel must be unbounded enough to not block the fetcher,
	// or similarly bounded since the fetcher blocks on workerChan anyway.
	committerChan := make(chan batchJob, c.workers*2)

	var wg sync.WaitGroup

	// Start parallel workers for inserting
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.workerLoop(ctx, workerChan, handle)
		}()
	}

	// Start single ordered committer
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.committerLoop(ctx, committerChan, c.commit, halt)
	}()

	// Fetch on the calling goroutine.
	c.fetchLoop(ctx, workerChan, committerChan)

	close(workerChan)
	close(committerChan)
	wg.Wait()
}

// fetchLoop polls Kafka and hands each non-empty batch to both the workers
// and the committer. It blocks (backpressure) if workerChan is full.
func (c *Consumer) fetchLoop(ctx context.Context, workerChan chan<- batchJob, committerChan chan<- batchJob) {
	backoff := fetchBackoffMin
	for {
		fetches := c.client.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

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

		job := batchJob{
			recs: recs,
			done: make(chan error, 1),
		}

		metrics.ConsumerInflightBatches.WithLabelValues(c.signal).Inc()

		// Route to both worker and committer
		select {
		case committerChan <- job:
		case <-ctx.Done():
			return
		}

		select {
		case workerChan <- job:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) workerLoop(ctx context.Context, in <-chan batchJob, handle RecordHandler) {
	for job := range in {
		(func(j batchJob) {
			var err error
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("worker panic: %v", r)
					slog.ErrorContext(ctx, "kafka worker panic recovered", slog.Any("panic", r))
				}
				j.done <- err
			}()
			start := time.Now()
			err = handle(ctx, j.recs)
			if err != nil {
				slog.ErrorContext(ctx, "kafka handler error", slog.Any("error", err), slog.Int("records", len(j.recs)))
			} else {
				metrics.ConsumerBatchInsertDuration.WithLabelValues(c.signal).Observe(time.Since(start).Seconds())
			}
		})(job)
	}
}

// committerLoop commits each batch's offsets in strict poll order, and stops
// committing for good once any batch's handler has failed.
//
// A Kafka offset commit is a high-water mark: committing batch N+1 also
// commits N. So with parallel workers, committing a later success after an
// earlier failure would silently discard the failed batch's records. Halting
// instead leaves the offset where it is; the process exits, and Kafka
// redelivers from the last committed point.
//
// It keeps draining after halting so in-flight workers never block on send.
// halt unwinds the fetch loop and may be nil in tests.
func (c *Consumer) committerLoop(ctx context.Context, in <-chan batchJob, commit commitFunc, halt context.CancelFunc) {
	halted := false
	for job := range in {
		err := <-job.done
		switch {
		case halted:
			// Draining only. Committing now would advance past the failure.
		case err != nil:
			halted = true
			metrics.ConsumerHalts.WithLabelValues(c.signal).Inc()
			slog.ErrorContext(ctx, "kafka: handler failed, halting commits to avoid losing the batch",
				slog.String("signal", c.signal),
				slog.Int("records", len(job.recs)),
				slog.Any("error", err),
			)
			if halt != nil {
				halt()
			}
		default:
			if cerr := commit(ctx, job.recs); cerr != nil {
				slog.ErrorContext(ctx, "kafka commit error", slog.Any("error", cerr))
			}
		}
		metrics.ConsumerInflightBatches.WithLabelValues(c.signal).Dec()
	}
}

// commit wraps CommitRecords so the worker can be tested against a fake.
func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) error {
	return c.client.CommitRecords(ctx, recs...)
}

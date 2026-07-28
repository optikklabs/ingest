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

type Consumer struct {
	client         *kgo.Client
	maxPollRecords int
	workers        int
	signal         string
}

func NewConsumer(client *kgo.Client, maxPollRecords, workers int, signal string) *Consumer {
	return &Consumer{client: client, maxPollRecords: maxPollRecords, workers: workers, signal: signal}
}

func (c *Consumer) Client() *kgo.Client { return c.client }
func (c *Consumer) Close()              { c.client.Close() }

type RecordHandler func(ctx context.Context, recs []*kgo.Record) error

type commitFunc func(ctx context.Context, recs []*kgo.Record) error

type batchJob struct {
	recs []*kgo.Record
	done chan error
}

func (c *Consumer) Run(ctx context.Context, handle RecordHandler) {

	ctx, halt := context.WithCancel(ctx)
	defer halt()

	workerChan := make(chan batchJob, c.workers*2)

	committerChan := make(chan batchJob, c.workers*2)

	var wg sync.WaitGroup

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.workerLoop(ctx, workerChan, handle)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.committerLoop(ctx, committerChan, c.commit, halt)
	}()

	c.fetchLoop(ctx, workerChan, committerChan)

	close(workerChan)
	close(committerChan)
	wg.Wait()
}

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

func (c *Consumer) committerLoop(ctx context.Context, in <-chan batchJob, commit commitFunc, halt context.CancelFunc) {
	halted := false
	for job := range in {
		err := <-job.done
		switch {
		case halted:

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

func (c *Consumer) commit(ctx context.Context, recs []*kgo.Record) error {
	return c.client.CommitRecords(ctx, recs...)
}

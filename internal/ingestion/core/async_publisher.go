package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

// asyncPublishTimeout bounds one async side-topic publish attempt.
const asyncPublishTimeout = 5 * time.Second

// resourcePublisher is the subset of Producer the async publisher drives.
type resourcePublisher[T Row] interface {
	Publish(ctx context.Context, rows []T) error
}

// asyncJob is one enqueued publish plus its rollback hook.
type asyncJob[T Row] struct {
	rows   []T
	onFail func()
}

// AsyncPublisher publishes best-effort rows off the request path. A bounded
// queue drops on overflow so Export never blocks; each job's onFail hook rolls
// back caller state (e.g. a resource-cache key) on drop or publish failure.
type AsyncPublisher[T Row] struct {
	pub    resourcePublisher[T]
	signal string
	topic  string
	queue  chan asyncJob[T]
	wg     sync.WaitGroup
}

// NewAsyncPublisher starts workers draining a queue of queueSize into pub.
func NewAsyncPublisher[T Row](pub resourcePublisher[T], signal, topic string, queueSize, workers int) *AsyncPublisher[T] {
	if queueSize <= 0 {
		queueSize = 4096
	}
	if workers <= 0 {
		workers = 2
	}
	a := &AsyncPublisher[T]{
		pub:    pub,
		signal: signal,
		topic:  topic,
		queue:  make(chan asyncJob[T], queueSize),
	}
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker()
	}
	return a
}

// Enqueue submits rows for async publish. It never blocks: a full queue drops
// the batch, runs onFail, and returns false so the caller can roll back inline.
func (a *AsyncPublisher[T]) Enqueue(rows []T, onFail func()) bool {
	if len(rows) == 0 {
		return true
	}
	select {
	case a.queue <- asyncJob[T]{rows: rows, onFail: onFail}:
		return true
	default:
		a.drop(len(rows), onFail)
		return false
	}
}

func (a *AsyncPublisher[T]) worker() {
	defer a.wg.Done()
	for job := range a.queue {
		ctx, cancel := context.WithTimeout(context.Background(), asyncPublishTimeout)
		if err := a.pub.Publish(ctx, job.rows); err != nil {
			slog.WarnContext(ctx, "core: async side-publish failed, rolling back",
				slog.String("signal", a.signal),
				slog.String("topic", a.topic),
				slog.Int("rows", len(job.rows)),
				slog.Any("error", err),
			)
			a.drop(len(job.rows), job.onFail)
		}
		cancel()
	}
}

// drop records the loss and runs the rollback hook, if any.
func (a *AsyncPublisher[T]) drop(rows int, onFail func()) {
	metrics.SidePublishDropped.WithLabelValues(a.signal, a.topic).Add(float64(rows))
	if onFail != nil {
		onFail()
	}
}

// Close stops accepting jobs, drains the queue, and waits for workers.
func (a *AsyncPublisher[T]) Close() {
	close(a.queue)
	a.wg.Wait()
}

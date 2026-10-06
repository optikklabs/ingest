package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

const asyncPublishTimeout = 5 * time.Second

// AsyncPublisher publishes side-effect rows off the request path. It is
// best-effort: rows are dropped (and counted) when the queue is full, the
// publisher is closed, or the publish fails.
type AsyncPublisher[T Row] struct {
	pub    Publisher[T]
	signal string
	topic  string
	queue  chan []T
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

func NewAsyncPublisher[T Row](pub Publisher[T], signal, topic string, queueSize, workers int) *AsyncPublisher[T] {
	a := &AsyncPublisher[T]{
		pub:    pub,
		signal: signal,
		topic:  topic,
		queue:  make(chan []T, queueSize),
	}
	for range workers {
		a.wg.Go(a.worker)
	}
	return a
}

func (a *AsyncPublisher[T]) Enqueue(rows []T) {
	if len(rows) == 0 {
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		a.drop(len(rows))
		return
	}
	select {
	case a.queue <- rows:
	default:
		a.drop(len(rows))
	}
}

func (a *AsyncPublisher[T]) worker() {
	for rows := range a.queue {
		ctx, cancel := context.WithTimeout(context.Background(), asyncPublishTimeout)
		if err := a.pub.Publish(ctx, rows); err != nil {
			slog.WarnContext(ctx, "core: async side-publish failed, dropping rows",
				slog.String("signal", a.signal),
				slog.String("topic", a.topic),
				slog.Int("rows", len(rows)),
				slog.Any("error", err),
			)
			a.drop(len(rows))
		}
		cancel()
	}
}

func (a *AsyncPublisher[T]) drop(rows int) {
	metrics.SidePublishDropped.WithLabelValues(a.signal, a.topic).Add(float64(rows))
}

func (a *AsyncPublisher[T]) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	close(a.queue)
	a.mu.Unlock()
	a.wg.Wait()
}

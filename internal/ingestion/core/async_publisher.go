package core

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

const asyncPublishTimeout = 5 * time.Second

type resourcePublisher[T Row] interface {
	Publish(ctx context.Context, rows []T) error
}

type asyncJob[T Row] struct {
	rows   []T
	onFail func()
}

type AsyncPublisher[T Row] struct {
	pub    resourcePublisher[T]
	signal string
	topic  string
	queue  chan asyncJob[T]
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

func NewAsyncPublisher[T Row](pub resourcePublisher[T], signal, topic string, queueSize, workers int) *AsyncPublisher[T] {
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

func (a *AsyncPublisher[T]) Enqueue(rows []T, onFail func()) bool {
	if len(rows) == 0 {
		return true
	}
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		a.drop(len(rows), onFail)
		return false
	}
	select {
	case a.queue <- asyncJob[T]{rows: rows, onFail: onFail}:
		a.mu.RUnlock()
		return true
	default:
		a.mu.RUnlock()
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

func (a *AsyncPublisher[T]) drop(rows int, onFail func()) {
	metrics.SidePublishDropped.WithLabelValues(a.signal, a.topic).Add(float64(rows))
	if onFail != nil {
		onFail()
	}
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

package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

// insertBackoff is the per-attempt wait before an insert retry; the last
// entry repeats when max_retries exceeds its length.
var insertBackoff = []time.Duration{250 * time.Millisecond, time.Second}

// RetryWriter retries failed inserts before the caller falls back to the
// DLQ. Only the insert is retried: DLQ publishing stays single-attempt by
// design (accepted-loss policy). Configured via kafka.consumer_max_retries.
type RetryWriter[T Row] struct {
	next       Writer[T]
	signal     string
	maxRetries int
	sleep      func(ctx context.Context, d time.Duration) error
}

func NewRetryWriter[T Row](next Writer[T], signal string, maxRetries int) *RetryWriter[T] {
	return &RetryWriter[T]{
		next:       next,
		signal:     signal,
		maxRetries: maxRetries,
		sleep:      sleepCtx,
	}
}

// Insert attempts the underlying insert up to 1+maxRetries times, backing
// off between attempts. It returns the last error once retries are spent
// or the context is canceled mid-backoff.
func (w *RetryWriter[T]) Insert(ctx context.Context, rows []T) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = w.next.Insert(ctx, rows)
		if err == nil || attempt >= w.maxRetries {
			return err
		}
		metrics.InsertRetries.WithLabelValues(w.signal).Inc()
		slog.WarnContext(ctx, "core retry writer: insert failed, retrying",
			slog.String("signal", w.signal),
			slog.Int("attempt", attempt+1),
			slog.Int("max_retries", w.maxRetries),
			slog.Int("rows", len(rows)),
			slog.Any("error", err),
		)
		if sleepErr := w.sleep(ctx, backoffFor(attempt)); sleepErr != nil {
			return err
		}
	}
}

func backoffFor(attempt int) time.Duration {
	if attempt >= len(insertBackoff) {
		return insertBackoff[len(insertBackoff)-1]
	}
	return insertBackoff[attempt]
}

// sleepCtx waits for d or until ctx is canceled, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

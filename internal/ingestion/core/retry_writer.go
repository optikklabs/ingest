package core

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/optikklabs/ingest/internal/infra/metrics"
)

// RetryWriter retries failed inserts before the caller falls back to the
// DLQ. Only the insert is retried: DLQ publishing stays single-attempt by
// design (accepted-loss policy). Configured via kafka.consumer_max_retries.
type RetryWriter[T Row] struct {
	next       Writer[T]
	signal     string
	maxRetries int
	newBackOff func() backoff.BackOff
}

func NewRetryWriter[T Row](next Writer[T], signal string, maxRetries int) *RetryWriter[T] {
	return &RetryWriter[T]{
		next:       next,
		signal:     signal,
		maxRetries: maxRetries,
		newBackOff: newInsertBackOff,
	}
}

// Insert attempts the insert up to 1+maxRetries times. Its error retains both
// the last insert failure and the reason retrying stopped.
func (w *RetryWriter[T]) Insert(ctx context.Context, rows []T) error {
	attempts := 0
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		attempts++
		err := w.next.Insert(ctx, rows)
		if isPermanentInsertError(err) {
			return struct{}{}, backoff.Permanent(err)
		}
		return struct{}{}, err
	},
		backoff.WithBackOff(w.newBackOff()),
		backoff.WithMaxTries(totalAttempts(w.maxRetries)),
		backoff.WithMaxElapsedTime(0),
		backoff.WithNotify(func(err error, next time.Duration) {
			metrics.InsertRetries.WithLabelValues(w.signal).Inc()
			slog.WarnContext(ctx, "core retry writer: insert failed, retrying",
				slog.String("signal", w.signal),
				slog.Int("attempt", attempts),
				slog.Int("max_retries", w.maxRetries),
				slog.Int("rows", len(rows)),
				slog.Duration("backoff", next),
				slog.Any("error", err),
			)
		}))
	return err
}

func newInsertBackOff() backoff.BackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     250 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          4,
		MaxInterval:         time.Second,
	}
}

func totalAttempts(maxRetries int) uint {
	if maxRetries <= 0 {
		return 1
	}
	return uint(maxRetries) + 1
}

func isPermanentInsertError(err error) bool {
	var permanent *permanentInsertError
	return errors.As(err, &permanent)
}

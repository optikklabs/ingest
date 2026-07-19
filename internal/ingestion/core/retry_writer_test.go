package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v7"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type fakeWriter struct {
	calls    int
	failures int // fail the first N calls
	err      error
}

func (f *fakeWriter) Insert(ctx context.Context, rows []*spansschema.Row) error {
	f.calls++
	if f.calls <= f.failures {
		if f.err != nil {
			return f.err
		}
		return errors.New("insert failed")
	}
	return nil
}

func newTestRetryWriter(next Writer[*spansschema.Row], maxRetries int) *RetryWriter[*spansschema.Row] {
	w := NewRetryWriter(next, "spans", maxRetries)
	w.newBackOff = func() backoff.BackOff { return &backoff.ZeroBackOff{} }
	return w
}

func TestRetryWriterInsert(t *testing.T) {
	rows := []*spansschema.Row{{}}
	tests := []struct {
		name       string
		failures   int
		maxRetries int
		wantErr    bool
		wantCalls  int
	}{
		{"succeeds first try", 0, 2, false, 1},
		{"succeeds after one retry", 1, 2, false, 2},
		{"succeeds on last retry", 2, 2, false, 3},
		{"fails after retries spent", 3, 2, true, 3},
		{"no retries configured", 1, 0, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeWriter{failures: tt.failures}
			w := newTestRetryWriter(fake, tt.maxRetries)

			err := w.Insert(context.Background(), rows)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if fake.calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", fake.calls, tt.wantCalls)
			}
		})
	}
}

func TestRetryWriterBackoffPolicy(t *testing.T) {
	policy, ok := newInsertBackOff().(*backoff.ExponentialBackOff)
	if !ok {
		t.Fatalf("newInsertBackOff() = %T, want exponential", policy)
	}
	if policy.InitialInterval != 250*time.Millisecond || policy.MaxInterval != time.Second {
		t.Fatalf("backoff intervals = %v..%v", policy.InitialInterval, policy.MaxInterval)
	}
	if policy.RandomizationFactor == 0 {
		t.Fatal("backoff must include jitter")
	}
}

func TestRetryWriterCanceledContextStopsRetries(t *testing.T) {
	fake := &fakeWriter{failures: 10}
	w := newTestRetryWriter(fake, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.Insert(ctx, []*spansschema.Row{{}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Insert() error = %v, want context cancellation", err)
	}
	if fake.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry after canceled backoff)", fake.calls)
	}
}

func TestRetryWriterDoesNotRetryPermanentInsertError(t *testing.T) {
	want := errors.New("invalid row")
	fake := &fakeWriter{failures: 10, err: &permanentInsertError{err: want}}
	w := newTestRetryWriter(fake, 2)

	err := w.Insert(context.Background(), []*spansschema.Row{{}})
	if !errors.Is(err, want) {
		t.Fatalf("Insert() error = %v, want %v", err, want)
	}
	if fake.calls != 1 {
		t.Fatalf("calls = %d, want 1", fake.calls)
	}
}

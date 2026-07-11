package core

import (
	"context"
	"errors"
	"testing"
	"time"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type fakeWriter struct {
	calls    int
	failures int // fail the first N calls
}

func (f *fakeWriter) Insert(ctx context.Context, rows []*spansschema.Row) error {
	f.calls++
	if f.calls <= f.failures {
		return errors.New("insert failed")
	}
	return nil
}

func newTestRetryWriter(next Writer[*spansschema.Row], maxRetries int, slept *[]time.Duration) *RetryWriter[*spansschema.Row] {
	w := NewRetryWriter(next, "spans", maxRetries)
	w.sleep = func(_ context.Context, d time.Duration) error {
		*slept = append(*slept, d)
		return nil
	}
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
			var slept []time.Duration
			w := newTestRetryWriter(fake, tt.maxRetries, &slept)

			err := w.Insert(context.Background(), rows)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if fake.calls != tt.wantCalls {
				t.Errorf("calls = %d, want %d", fake.calls, tt.wantCalls)
			}
			if len(slept) != tt.wantCalls-1 {
				t.Errorf("backoffs = %d, want %d", len(slept), tt.wantCalls-1)
			}
		})
	}
}

func TestRetryWriterBackoffSchedule(t *testing.T) {
	fake := &fakeWriter{failures: 3}
	var slept []time.Duration
	w := newTestRetryWriter(fake, 3, &slept)

	if err := w.Insert(context.Background(), []*spansschema.Row{{}}); err != nil {
		t.Fatalf("Insert() = %v, want nil", err)
	}
	want := []time.Duration{250 * time.Millisecond, time.Second, time.Second}
	if len(slept) != len(want) {
		t.Fatalf("backoffs = %v, want %v", slept, want)
	}
	for i := range want {
		if slept[i] != want[i] {
			t.Errorf("backoff[%d] = %v, want %v", i, slept[i], want[i])
		}
	}
}

func TestRetryWriterCanceledContextStopsRetries(t *testing.T) {
	fake := &fakeWriter{failures: 10}
	w := NewRetryWriter[*spansschema.Row](fake, "spans", 2)
	w.sleep = func(ctx context.Context, _ time.Duration) error {
		return context.Canceled
	}

	err := w.Insert(context.Background(), []*spansschema.Row{{}})
	if err == nil {
		t.Fatal("Insert() = nil, want error")
	}
	if fake.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry after canceled backoff)", fake.calls)
	}
}

package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func batch(n int) []*kgo.Record {
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Offset: int64(i)}
	}
	return recs
}

// TestProcessBatchCommitsOnlyOnSuccess asserts the commit fires exactly once
// after a nil handler, and never when the handler errors.
func TestProcessBatchCommitsOnlyOnSuccess(t *testing.T) {
	tests := []struct {
		name        string
		handleErr   error
		wantCommits int
	}{
		{"success commits", nil, 1},
		{"handler error skips commit", errors.New("insert failed"), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var commits int
			handle := func(context.Context, []*kgo.Record) error { return tt.handleErr }
			commit := func(context.Context, []*kgo.Record) error { commits++; return nil }

			processBatch(context.Background(), batch(3), handle, commit, "test")

			if commits != tt.wantCommits {
				t.Errorf("commits = %d, want %d", commits, tt.wantCommits)
			}
		})
	}
}

// TestProcessBatchRedeliversOnError proves an errored batch is not committed,
// so the same offsets are re-handed on redelivery and eventually commit.
func TestProcessBatchRedeliversOnError(t *testing.T) {
	recs := batch(2)
	var committed [][]*kgo.Record
	commit := func(_ context.Context, r []*kgo.Record) error {
		committed = append(committed, r)
		return nil
	}

	// First delivery fails → no commit.
	processBatch(context.Background(), recs, func(context.Context, []*kgo.Record) error {
		return errors.New("down")
	}, commit, "test")
	// Redelivery succeeds → commit.
	processBatch(context.Background(), recs, func(context.Context, []*kgo.Record) error {
		return nil
	}, commit, "test")

	if len(committed) != 1 {
		t.Fatalf("commit count = %d, want 1 (only the redelivery)", len(committed))
	}
	if len(committed[0]) != len(recs) {
		t.Errorf("committed %d records, want %d", len(committed[0]), len(recs))
	}
}

// TestProcessBatchesDrainsAndReturns feeds batches through the worker channel
// and asserts a clean shutdown: closing the channel drains the buffer and the
// worker returns. Run with -race to exercise the goroutine handoff.
func TestProcessBatchesDrainsAndReturns(t *testing.T) {
	in := make(chan []*kgo.Record, 1)
	var mu sync.Mutex
	var handled, committed int
	handle := func(context.Context, []*kgo.Record) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	}
	commit := func(context.Context, []*kgo.Record) error {
		mu.Lock()
		committed++
		mu.Unlock()
		return nil
	}

	done := make(chan struct{})
	go func() {
		processBatches(context.Background(), in, handle, commit, "test")
		close(done)
	}()

	const n = 5
	for i := 0; i < n; i++ {
		in <- batch(1)
	}
	close(in)
	<-done

	if handled != n || committed != n {
		t.Errorf("handled=%d committed=%d, want %d each", handled, committed, n)
	}
}

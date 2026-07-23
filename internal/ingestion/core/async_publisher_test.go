package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type fakeAsyncPublisher struct {
	mu        sync.Mutex
	published int
	err       error
	block     chan struct{} // if non-nil, Publish blocks until closed
}

func (f *fakeAsyncPublisher) Publish(_ context.Context, rows []*spansschema.Row) error {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published += len(rows)
	return nil
}

func resourceRows(n int) []*spansschema.Row {
	rows := make([]*spansschema.Row, n)
	for i := range rows {
		rows[i] = &spansschema.Row{TenantId: uint32(i + 1)}
	}
	return rows
}

// TestAsyncPublisherSuccessKeepsState: a successful publish never runs onFail.
func TestAsyncPublisherSuccessKeepsState(t *testing.T) {
	pub := &fakeAsyncPublisher{}
	ap := NewAsyncPublisher[*spansschema.Row](pub, "spans", "side_topic", 16, 1)

	var rolledBack atomic.Bool
	ap.Enqueue(resourceRows(3), func() { rolledBack.Store(true) })
	ap.Close() // drains + waits for workers

	if pub.published != 3 {
		t.Errorf("published = %d, want 3", pub.published)
	}
	if rolledBack.Load() {
		t.Error("onFail ran after a successful publish, want no rollback")
	}
}

// TestAsyncPublisherFailureRollsBack: a failed publish runs onFail so the
// caller can re-emit next window.
func TestAsyncPublisherFailureRollsBack(t *testing.T) {
	pub := &fakeAsyncPublisher{err: errors.New("kafka down")}
	ap := NewAsyncPublisher[*spansschema.Row](pub, "spans", "side_topic", 16, 1)

	var rolledBack atomic.Bool
	ap.Enqueue(resourceRows(2), func() { rolledBack.Store(true) })
	ap.Close()

	if !rolledBack.Load() {
		t.Error("onFail did not run after a failed publish, want rollback")
	}
}

// TestAsyncPublisherQueueFullDrops: a saturated queue drops without blocking
// and runs the caller's rollback inline.
func TestAsyncPublisherQueueFullDrops(t *testing.T) {
	pub := &fakeAsyncPublisher{block: make(chan struct{})}
	ap := NewAsyncPublisher[*spansschema.Row](pub, "spans", "side_topic", 1, 1)

	var drops int
	var accepted int
	for i := 0; i < 20; i++ {
		if ap.Enqueue(resourceRows(1), func() { drops++ }) {
			accepted++
		}
	}
	close(pub.block) // let the worker drain
	ap.Close()

	if drops == 0 {
		t.Error("no drops with a saturated queue, want > 0")
	}
	if accepted+drops != 20 {
		t.Errorf("accepted(%d)+drops(%d) = %d, want 20", accepted, drops, accepted+drops)
	}
}

// TestAsyncPublisherEnqueueEmptyNoop: an empty batch is a no-op success.
func TestAsyncPublisherEnqueueEmptyNoop(t *testing.T) {
	pub := &fakeAsyncPublisher{}
	ap := NewAsyncPublisher[*spansschema.Row](pub, "spans", "side_topic", 16, 1)
	defer ap.Close()

	if !ap.Enqueue(nil, func() { t.Error("onFail ran for empty batch") }) {
		t.Error("Enqueue(nil) = false, want true (no-op)")
	}
}

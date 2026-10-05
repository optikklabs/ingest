package core

import (
	"context"
	"sync"
	"testing"

	"github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
)

type recordingPublisher struct {
	mu      sync.Mutex
	rows    int
	release chan struct{}
}

func (p *recordingPublisher) Publish(_ context.Context, rows []*schema.ScoreRow) error {
	if p.release != nil {
		<-p.release
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows += len(rows)
	return nil
}

func (p *recordingPublisher) published() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rows
}

func scoreRows(n int) []*schema.ScoreRow {
	rows := make([]*schema.ScoreRow, n)
	for i := range rows {
		rows[i] = &schema.ScoreRow{TenantId: 1}
	}
	return rows
}

func TestAsyncPublisherPublishesQueuedRows(t *testing.T) {
	pub := &recordingPublisher{}
	a := NewAsyncPublisher[*schema.ScoreRow](pub, "spans", "llm_scores", 8, 2)
	a.Enqueue(scoreRows(3))
	a.Enqueue(scoreRows(2))
	a.Close()

	if got := pub.published(); got != 5 {
		t.Fatalf("published %d rows, want 5", got)
	}
}

func TestAsyncPublisherDropsWhenFullAndAfterClose(t *testing.T) {
	pub := &recordingPublisher{release: make(chan struct{})}
	a := NewAsyncPublisher[*schema.ScoreRow](pub, "spans", "llm_scores", 1, 1)

	// The worker blocks on the first batch, the second fills the queue, and
	// the third must be dropped rather than block the request path.
	a.Enqueue(scoreRows(1))
	a.Enqueue(scoreRows(1))
	a.Enqueue(scoreRows(1))
	close(pub.release)
	a.Close()

	a.Enqueue(scoreRows(1)) // must not panic on the closed queue

	if got := pub.published(); got > 2 {
		t.Fatalf("published %d rows, want at most 2 (queue size 1 + in-flight)", got)
	}
}

package ingestionstats

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

type fakePublisher struct {
	mu       sync.Mutex
	failures int
	batches  [][]*schema.StatRow
}

func (p *fakePublisher) Publish(_ context.Context, rows []*schema.StatRow) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("broker unavailable")
	}
	p.batches = append(p.batches, rows)
	return nil
}

func TestFlushRestoresFailedSnapshotAndMergesNewRows(t *testing.T) {
	pub := &fakePublisher{failures: 1}
	r := &HourlyRecorder{pub: pub, rows: make(map[statKey]*schema.StatRow)}
	r.Record([]*schema.StatRow{NewRow(1, SignalSpans, "api", "prod", 2, 20)})

	if r.flush() {
		t.Fatal("first flush unexpectedly succeeded")
	}
	r.Record([]*schema.StatRow{NewRow(1, SignalSpans, "api", "prod", 3, 30)})
	if !r.flush() {
		t.Fatal("second flush failed")
	}

	if len(pub.batches) != 1 || len(pub.batches[0]) != 1 {
		t.Fatalf("published batches = %#v, want one row", pub.batches)
	}
	got := pub.batches[0][0]
	if got.GetRecordCount() != 5 || got.GetByteCount() != 50 {
		t.Errorf("published row = records=%d bytes=%d, want 5/50", got.GetRecordCount(), got.GetByteCount())
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	r := NewHourlyRecorder(&fakePublisher{})
	r.Close()
	r.Close()
}

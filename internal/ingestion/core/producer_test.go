package core

import (
	"bytes"
	"testing"
	"time"

	spansresourceschema "github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// TestMarshalBatchByteIdentical proves the pooled/pre-sized packing produces
// exactly the same bytes as a fresh proto.Marshal per row (guards the subslice
// + three-index logic against contamination).
func TestMarshalBatchByteIdentical(t *testing.T) {
	p := NewProducer[*spansresourceschema.ResourceRow]("topic", nil)

	rows := []*spansresourceschema.ResourceRow{
		{TenantId: 1, Fingerprint: 100, Service: "checkout"},
		{TenantId: 2, Fingerprint: 200, Service: "orders", Host: "h1"},
		{TenantId: 3, Fingerprint: 300, Service: "payments", Pod: "p9", Environment: "prod"},
	}

	records, _, err := p.marshalBatch(rows, time.Unix(0, 0), nil)
	if err != nil {
		t.Fatalf("marshalBatch: %v", err)
	}
	if len(records) != len(rows) {
		t.Fatalf("records = %d, want %d", len(records), len(rows))
	}
	for i, r := range rows {
		want, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(records[i].Value, want) {
			t.Errorf("row %d bytes mismatch:\n got %x\nwant %x", i, records[i].Value, want)
		}
		if records[i].Topic != "topic" {
			t.Errorf("row %d topic = %q, want topic", i, records[i].Topic)
		}
	}
}

func BenchmarkMarshalBatch(b *testing.B) {
	p := NewProducer[*spansresourceschema.ResourceRow]("topic", nil)
	rows := make([]*spansresourceschema.ResourceRow, 256)
	for i := range rows {
		rows[i] = &spansresourceschema.ResourceRow{TenantId: 1, Fingerprint: uint64(i), Service: "svc"}
	}
	now := time.Unix(0, 0)
	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var recs []*kgo.Record
		recs, buf, _ = p.marshalBatch(rows, now, buf[:0])
		_ = recs
	}
}

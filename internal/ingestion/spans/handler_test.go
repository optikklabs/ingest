package spans

import (
	"testing"

	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

func TestTracegraphRowsKeepsOnlyPairableKinds(t *testing.T) {
	rows := []*schema.Row{
		{SpanId: "a", KindString: "CLIENT"},
		{SpanId: "b", KindString: "SERVER"},
		{SpanId: "c", KindString: "PRODUCER"},
		{SpanId: "d", KindString: "CONSUMER"},
		{SpanId: "e", KindString: "INTERNAL"},
		{SpanId: "f", KindString: "UNSPECIFIED"},
		{SpanId: "g", KindString: ""},
	}

	got := tracegraphRows(rows)

	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("kept %d rows, want %d", len(got), len(want))
	}
	for i, id := range want {
		if got[i].GetSpanId() != id {
			t.Errorf("row[%d] = %q, want %q", i, got[i].GetSpanId(), id)
		}
	}
}

func TestTracegraphRowsEmptyInput(t *testing.T) {
	if got := tracegraphRows(nil); len(got) != 0 {
		t.Errorf("tracegraphRows(nil) = %d rows, want 0", len(got))
	}
}

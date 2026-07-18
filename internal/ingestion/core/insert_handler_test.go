package core

import (
	"context"
	"testing"

	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type recordingWriter struct {
	rows []*metricsschema.Row
}

func (w *recordingWriter) Insert(_ context.Context, rows []*metricsschema.Row) error {
	w.rows = append(w.rows, rows...)
	return nil
}

func TestInsertHandlerDecodesAndWritesRows(t *testing.T) {
	value, err := proto.Marshal(&metricsschema.Row{TenantId: 7})
	if err != nil {
		t.Fatal(err)
	}
	w := &recordingWriter{}
	h := NewInsertHandler("metrics", w, nil, func() *metricsschema.Row {
		return &metricsschema.Row{}
	})
	if err := h(context.Background(), []*kgo.Record{{Value: value}}); err != nil {
		t.Fatal(err)
	}
	if len(w.rows) != 1 || w.rows[0].GetTenantId() != 7 {
		t.Fatalf("rows = %#v, want one decoded row", w.rows)
	}
}

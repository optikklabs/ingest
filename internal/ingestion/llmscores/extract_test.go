package llmscores

import (
	"testing"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

func spanWithEvent(name string, attrs map[string]string) *spansschema.Row {
	return &spansschema.Row{
		TenantId:     1,
		TraceId:      "t1",
		SpanId:       "s1",
		LlmSessionId: "sess1",
		LlmUserId:    "alex@acme.co",
		Service:      "support-agent",
		Events: []*spansschema.Row_SpanEvent{
			{Name: name, Attributes: attrs},
		},
	}
}

func TestExtractOTelNumericScore(t *testing.T) {
	rows := []*spansschema.Row{spanWithEvent("gen_ai.evaluation.result", map[string]string{
		"gen_ai.evaluation.name":        "helpfulness",
		"gen_ai.evaluation.score.value": "0.92",
		"gen_ai.evaluation.explanation": "resolved with a concrete action",
	})}
	got := ExtractFromSpans(rows)
	if len(got) != 1 {
		t.Fatalf("score count = %d, want 1", len(got))
	}
	s := got[0]
	if s.Name != "helpfulness" || s.DataType != typeNumeric || s.Value != 0.92 {
		t.Errorf("score = %+v", s)
	}
	if s.SessionId != "sess1" || s.UserId != "alex@acme.co" || s.Source != sourceOTel {
		t.Errorf("context not carried: %+v", s)
	}
}

func TestExtractLangfuseBooleanScore(t *testing.T) {
	rows := []*spansschema.Row{spanWithEvent("langfuse.score", map[string]string{
		"langfuse.score.name":  "hallucination",
		"langfuse.score.value": "false",
	})}
	got := ExtractFromSpans(rows)
	if len(got) != 1 {
		t.Fatalf("score count = %d, want 1", len(got))
	}
	if got[0].DataType != typeBoolean || got[0].Value != 0 {
		t.Errorf("bool score = %+v", got[0])
	}
}

func TestExtractIgnoresUnrelatedEvents(t *testing.T) {
	rows := []*spansschema.Row{spanWithEvent("exception", map[string]string{"exception.type": "x"})}
	if got := ExtractFromSpans(rows); len(got) != 0 {
		t.Fatalf("score count = %d, want 0", len(got))
	}
}

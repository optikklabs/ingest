package llmscores

import (
	"strings"
	"testing"
	"unicode/utf8"

	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	"google.golang.org/protobuf/proto"
)

// A comment cut mid-rune makes proto.Marshal fail, which would drop the whole
// scores batch on publish.
func TestExtractFromSpansCapsCommentOnRuneBoundary(t *testing.T) {
	comment := strings.Repeat("a", maxCommentBytes-1) + "é" // 2-byte rune straddles the cap
	rows := []*spansschema.Row{{
		TenantId: 1,
		TraceId:  "trace",
		Events: []*spansschema.Row_SpanEvent{{
			Name: "gen_ai.evaluation.result",
			Attributes: map[string]string{
				"gen_ai.evaluation.name":        "relevance",
				"gen_ai.evaluation.score.value": "0.9",
				"gen_ai.evaluation.explanation": comment,
			},
		}},
	}}

	scores := ExtractFromSpans(rows)
	if len(scores) != 1 {
		t.Fatalf("got %d scores, want 1", len(scores))
	}
	got := scores[0].GetComment()
	if len(got) > maxCommentBytes || !utf8.ValidString(got) {
		t.Fatalf("comment not capped on a rune boundary: len=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if _, err := proto.Marshal(scores[0]); err != nil {
		t.Fatalf("score row does not marshal: %v", err)
	}
}

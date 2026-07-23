package llmscores

import (
	"strconv"
	"strings"

	"github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

// Source and data-type tags persisted alongside each score.
const (
	sourceOTel = "otel"

	typeNumeric     = "numeric"
	typeBoolean     = "boolean"
	typeCategorical = "categorical"

	maxCommentBytes = 2048
)

// ExtractFromSpans walks span events for evaluation scores and returns one
// ScoreRow per score. Two conventions are recognised:
//   - OTel GenAI: event "gen_ai.evaluation.result" with gen_ai.evaluation.*
//   - Langfuse:   event "langfuse.score" with langfuse.score.*
func ExtractFromSpans(rows []*spansschema.Row) []*schema.ScoreRow {
	var out []*schema.ScoreRow
	for _, row := range rows {
		for _, ev := range row.GetEvents() {
			switch ev.GetName() {
			case "gen_ai.evaluation.result":
				if s := otelScore(row, ev.GetAttributes()); s != nil {
					out = append(out, s)
				}
			case "langfuse.score":
				if s := langfuseScore(row, ev.GetAttributes()); s != nil {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func otelScore(row *spansschema.Row, attrs map[string]string) *schema.ScoreRow {
	name := attrs["gen_ai.evaluation.name"]
	if name == "" {
		return nil
	}
	s := baseScore(row, name)
	s.Comment = capBytes(attrs["gen_ai.evaluation.explanation"], maxCommentBytes)
	assignValue(s, attrs["gen_ai.evaluation.score.value"], attrs["gen_ai.evaluation.score.label"])
	return s
}

func langfuseScore(row *spansschema.Row, attrs map[string]string) *schema.ScoreRow {
	name := attrs["langfuse.score.name"]
	if name == "" {
		return nil
	}
	s := baseScore(row, name)
	s.Comment = capBytes(attrs["langfuse.score.comment"], maxCommentBytes)
	if dt := attrs["langfuse.score.dataType"]; dt != "" {
		s.DataType = normalizeType(dt)
	}
	assignValue(s, attrs["langfuse.score.value"], attrs["langfuse.score.stringValue"])
	return s
}

func baseScore(row *spansschema.Row, name string) *schema.ScoreRow {
	return &schema.ScoreRow{
		TenantId:    row.GetTenantId(),
		TimestampNs: row.GetTimestampNs(),
		TraceId:     row.GetTraceId(),
		SpanId:      row.GetSpanId(),
		SessionId:   row.GetLlmSessionId(),
		UserId:      row.GetLlmUserId(),
		Service:     row.GetService(),
		Environment: row.GetEnvironment(),
		Name:        name,
		Source:      sourceOTel,
	}
}

// assignValue fills value/string_value/data_type from a raw numeric string and
// an optional label. A parseable number wins as numeric; otherwise the label
// is classified as boolean (true/false/pass/fail) or categorical.
func assignValue(s *schema.ScoreRow, raw, label string) {
	if raw != "" {
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			if s.DataType == "" {
				s.DataType = typeNumeric
			}
			s.Value = f
			return
		}
	}
	if label == "" {
		label = raw
	}
	s.StringValue = label
	switch strings.ToLower(label) {
	case "true", "pass", "yes":
		s.Value = 1
		if s.DataType == "" {
			s.DataType = typeBoolean
		}
	case "false", "fail", "no":
		s.Value = 0
		if s.DataType == "" {
			s.DataType = typeBoolean
		}
	default:
		if s.DataType == "" {
			s.DataType = typeCategorical
		}
	}
}

func normalizeType(dt string) string {
	switch strings.ToLower(dt) {
	case "numeric", "number":
		return typeNumeric
	case "boolean", "bool":
		return typeBoolean
	default:
		return typeCategorical
	}
}

func capBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

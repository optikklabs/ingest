package spans

import (
	"cmp"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/optikklabs/ingest/internal/infra/otlp"
)

const maxGenAIContentBytes = 16 * 1024

type genAI struct {
	System        string
	Operation     string
	RequestModel  string
	ResponseModel string
	Prompt        string
	Completion    string
	InputTokens   uint64
	OutputTokens  uint64
	Present       bool

	UserID        string
	SessionID     string
	Tags          []string
	Release       string
	PromptName    string
	PromptVersion uint32

	SpanKind string
}

// genAIMarkerKeys are GenAI-specific keys that mark a span as GenAI even
// without a system, operation or model. The generic genAIIdentityKeys are
// deliberately absent: ordinary HTTP spans carry them too.
var genAIMarkerKeys = []string{
	"gen_ai.conversation.id", "gen_ai.request.user",
	"langfuse.session.id", "langfuse.user.id",
	"langfuse.observation.type", "gen_ai.observation.type",
}

// extractGenAI reads the GenAI fields of a span. resourceVersion is the
// resource's service.version, the release fallback when langfuse.release is
// unset.
func extractGenAI(spanMap map[string]string, durationNano uint64, resourceVersion string) genAI {
	g := genAI{
		// gen_ai.provider.name replaced gen_ai.system in GenAI semconv 1.37.
		System:        otlp.FirstNonEmpty(spanMap, "gen_ai.provider.name", "gen_ai.system"),
		RequestModel:  spanMap["gen_ai.request.model"],
		ResponseModel: spanMap["gen_ai.response.model"],
		Prompt:        otlp.TruncateUTF8(otlp.FirstNonEmpty(spanMap, "gen_ai.prompt", "gen_ai.input.messages"), maxGenAIContentBytes),
		Completion:    otlp.TruncateUTF8(otlp.FirstNonEmpty(spanMap, "gen_ai.completion", "gen_ai.output.messages"), maxGenAIContentBytes),
		InputTokens:   parseTokenCount(otlp.FirstNonEmpty(spanMap, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens")),
		OutputTokens:  parseTokenCount(otlp.FirstNonEmpty(spanMap, "gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens")),
		PromptName:    otlp.FirstNonEmpty(spanMap, "langfuse.prompt.name", "optikk.prompt.name"),
	}
	op := spanMap["gen_ai.operation.name"]
	g.Operation = normalizeGenAIOperation(op)
	g.Tags = parseTags(otlp.FirstNonEmpty(spanMap, "langfuse.trace.tags", "optikk.llm.tags"))
	g.PromptVersion = uint32(parseTokenCount(otlp.FirstNonEmpty(spanMap, "langfuse.prompt.version", "optikk.prompt.version")))
	g.Present = g.System != "" || op != "" || g.RequestModel != "" || g.ResponseModel != "" ||
		otlp.FirstNonEmpty(spanMap, genAIMarkerKeys...) != ""
	if g.Present {
		g.UserID = otlp.FirstNonEmpty(spanMap, "gen_ai.request.user", "user.id", "enduser.id", "langfuse.user.id")
		g.SessionID = otlp.FirstNonEmpty(spanMap, "gen_ai.conversation.id", "session.id", "langfuse.session.id")
		g.Release = cmp.Or(spanMap["langfuse.release"], resourceVersion)
	}
	g.SpanKind = genAISpanKind(spanMap, g, durationNano)
	return g
}

func genAISpanKind(spanMap map[string]string, g genAI, durationNano uint64) string {
	if !g.Present {
		return ""
	}
	if _, ok := spanMap["optikk.eval"]; ok {
		return "eval"
	}
	if t := otlp.FirstNonEmpty(spanMap, "langfuse.observation.type", "gen_ai.observation.type"); t != "" {
		switch t {
		case "generation", "event", "span", "eval":
			return t
		}
	}
	switch {
	case g.RequestModel != "" || g.ResponseModel != "":
		return "generation"
	case durationNano == 0:
		return "event"
	default:
		return "span"
	}
}

func parseTags(v string) []string {
	if v == "" {
		return nil
	}
	if strings.HasPrefix(strings.TrimSpace(v), "[") {
		var tags []string
		if err := json.Unmarshal([]byte(v), &tags); err == nil {
			return nonEmptyTrimmed(tags)
		}
	}
	return nonEmptyTrimmed(strings.Split(v, ","))
}

func nonEmptyTrimmed(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeGenAIOperation(op string) string {
	switch op {
	case "":
		return ""
	case "chat", "text_completion", "generate_content":
		return "chat"
	case "execute_tool":
		return "tool"
	case "embeddings":
		return "embedding"
	case "retrieval", "retrieve":
		return "retrieval"
	case "invoke_agent", "create_agent", "agent":
		return "agent"
	default:
		return "other"
	}
}

func parseTokenCount(v string) uint64 {
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

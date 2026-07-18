package spans

import (
	"strconv"
	"unicode/utf8"
)

// maxGenAIContentBytes caps promoted prompt/completion content per span.
const maxGenAIContentBytes = 16 * 1024

// genAI holds span fields promoted from OTel gen_ai.* semconv attributes.
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
}

// extractGenAI promotes gen_ai.* attrs; legacy prompt/completion token keys
// are accepted as fallbacks for older instrumentations.
func extractGenAI(spanMap map[string]string) genAI {
	g := genAI{
		System:        spanMap["gen_ai.system"],
		RequestModel:  spanMap["gen_ai.request.model"],
		ResponseModel: spanMap["gen_ai.response.model"],
		Prompt:        capUTF8(firstNonEmpty(spanMap, "gen_ai.prompt", "gen_ai.input.messages"), maxGenAIContentBytes),
		Completion:    capUTF8(firstNonEmpty(spanMap, "gen_ai.completion", "gen_ai.output.messages"), maxGenAIContentBytes),
		InputTokens:   parseTokenCount(firstNonEmpty(spanMap, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens")),
		OutputTokens:  parseTokenCount(firstNonEmpty(spanMap, "gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens")),
	}
	op := spanMap["gen_ai.operation.name"]
	g.Operation = normalizeGenAIOperation(op)
	g.Present = g.System != "" || op != ""
	return g
}

// normalizeGenAIOperation buckets semconv operation names into the kinds the
// LLM Observability UI groups by: chat, tool, embedding, retrieval, agent.
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

// capUTF8 truncates s to at most max bytes without splitting a rune.
func capUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
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

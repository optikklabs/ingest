package spans

import "strconv"

// genAI holds span fields promoted from OTel gen_ai.* semconv attributes.
type genAI struct {
	System        string
	Operation     string
	RequestModel  string
	ResponseModel string
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

package spans

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"
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

func extractGenAI(spanMap map[string]string, durationNano uint64) genAI {
	g := genAI{
		System:        spanMap["gen_ai.system"],
		RequestModel:  spanMap["gen_ai.request.model"],
		ResponseModel: spanMap["gen_ai.response.model"],
		Prompt:        capUTF8(firstNonEmpty(spanMap, "gen_ai.prompt", "gen_ai.input.messages"), maxGenAIContentBytes),
		Completion:    capUTF8(firstNonEmpty(spanMap, "gen_ai.completion", "gen_ai.output.messages"), maxGenAIContentBytes),
		InputTokens:   parseTokenCount(firstNonEmpty(spanMap, "gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens")),
		OutputTokens:  parseTokenCount(firstNonEmpty(spanMap, "gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens")),
		UserID:        firstNonEmpty(spanMap, "gen_ai.request.user", "user.id", "enduser.id", "langfuse.user.id"),
		SessionID:     firstNonEmpty(spanMap, "gen_ai.conversation.id", "session.id", "langfuse.session.id"),
		Release:       firstNonEmpty(spanMap, "langfuse.release", "service.version"),
		PromptName:    firstNonEmpty(spanMap, "langfuse.prompt.name", "optikk.prompt.name"),
	}
	op := spanMap["gen_ai.operation.name"]
	g.Operation = normalizeGenAIOperation(op)
	g.Tags = parseTags(firstNonEmpty(spanMap, "langfuse.trace.tags", "optikk.llm.tags"))
	g.PromptVersion = uint32(parseTokenCount(firstNonEmpty(spanMap, "langfuse.prompt.version", "optikk.prompt.version")))
	g.Present = g.System != "" || op != "" || g.SessionID != "" || g.UserID != ""
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
	if t := firstNonEmpty(spanMap, "langfuse.observation.type", "gen_ai.observation.type"); t != "" {
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

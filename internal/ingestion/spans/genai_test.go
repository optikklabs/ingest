package spans

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestMapRequestPromotesGenAIContent(t *testing.T) {
	rows := mapRequest(1, req("bot",
		strKV("gen_ai.system", "openai"),
		strKV("gen_ai.prompt", "refund my duplicate charge"),
		strKV("gen_ai.completion", "refund issued"),
	))
	if len(rows) != 1 {
		t.Fatalf("row count = %d, want 1", len(rows))
	}
	r := rows[0]
	if !r.IsGenAi {
		t.Fatal("IsGenAi = false, want true")
	}
	if r.GenAiPrompt != "refund my duplicate charge" || r.GenAiCompletion != "refund issued" {
		t.Errorf("prompt/completion = %q, %q", r.GenAiPrompt, r.GenAiCompletion)
	}
	for _, k := range []string{"gen_ai.prompt", "gen_ai.completion"} {
		if _, ok := r.Attributes[k]; ok {
			t.Errorf("%s not stripped from attributes", k)
		}
	}
}

func TestExtractGenAIMessagesFallback(t *testing.T) {
	g := extractGenAI(map[string]string{
		"gen_ai.system":          "anthropic",
		"gen_ai.input.messages":  `[{"role":"user","content":"hi"}]`,
		"gen_ai.output.messages": `[{"role":"assistant","content":"hello"}]`,
	})
	if g.Prompt != `[{"role":"user","content":"hi"}]` {
		t.Errorf("prompt fallback = %q", g.Prompt)
	}
	if g.Completion != `[{"role":"assistant","content":"hello"}]` {
		t.Errorf("completion fallback = %q", g.Completion)
	}
}

func TestExtractGenAICapsContent(t *testing.T) {
	long := strings.Repeat("é", maxGenAIContentBytes) // 2 bytes per rune
	g := extractGenAI(map[string]string{"gen_ai.prompt": long})
	if len(g.Prompt) > maxGenAIContentBytes {
		t.Errorf("prompt length = %d, want <= %d", len(g.Prompt), maxGenAIContentBytes)
	}
	if !utf8.ValidString(g.Prompt) {
		t.Error("capped prompt is not valid UTF-8")
	}
}

func TestCapUTF8RuneBoundary(t *testing.T) {
	// "aé" is 3 bytes; capping at 2 must not split the 2-byte rune.
	if got := capUTF8("aé", 2); got != "a" {
		t.Errorf(`capUTF8("aé", 2) = %q, want "a"`, got)
	}
	if got := capUTF8("abc", 5); got != "abc" {
		t.Errorf(`capUTF8("abc", 5) = %q, want "abc"`, got)
	}
}

package otlp

import (
	"maps"
	"slices"
	"testing"
)

func TestCapStringMapKeepsSmallestKeys(t *testing.T) {
	m := map[string]string{"d": "4", "a": "1", "c": "3", "b": "2"}
	if dropped := CapStringMap(m, 2); dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if got := slices.Sorted(maps.Keys(m)); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("kept %v, want [a b]", got)
	}
	if CapStringMap(m, 0) != 0 || CapStringMap(m, 5) != 0 || len(m) != 2 {
		t.Fatal("no-op limits changed the map")
	}
}

func TestTruncateUTF8KeepsRunesWhole(t *testing.T) {
	for _, tc := range []struct {
		s    string
		max  int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello", 3, "hel"},
		{"héllo", 2, "h"},
		{"héllo", 3, "hé"},
	} {
		if got := TruncateUTF8(tc.s, tc.max); got != tc.want {
			t.Errorf("TruncateUTF8(%q, %d) = %q, want %q", tc.s, tc.max, got, tc.want)
		}
	}
}

func TestFirstNonEmpty(t *testing.T) {
	attrs := map[string]string{"old": "", "new": "v"}
	if got := FirstNonEmpty(attrs, "old", "new"); got != "v" {
		t.Fatalf("FirstNonEmpty = %q, want v", got)
	}
	if got := FirstNonEmpty(attrs, "missing"); got != "" {
		t.Fatalf("FirstNonEmpty = %q, want empty", got)
	}
}

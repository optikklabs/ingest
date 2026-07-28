package logs

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

// referenceComputeLogID is the pre-optimization implementation, kept verbatim
// as the equivalence oracle: log IDs are stored, output must never change.
func referenceComputeLogID(traceID string, tsNs uint64, body string) string {
	const (
		offset64      uint64 = 14695981039346656037
		prime64       uint64 = 1099511628211
		separatorByte byte   = 255
	)
	addStr := func(h uint64, s string) uint64 {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= prime64
		}
		return h
	}
	addByte := func(h uint64, b byte) uint64 {
		h ^= uint64(b)
		h *= prime64
		return h
	}
	h := offset64
	h = addStr(h, traceID)
	h = addByte(h, separatorByte)
	h = addStr(h, strconv.FormatUint(tsNs, 10))
	h = addByte(h, separatorByte)
	h = addStr(h, body)
	return fmt.Sprintf("%016x", h)
}

func TestComputeLogIDMatchesReference(t *testing.T) {
	cases := []struct {
		traceID string
		tsNs    uint64
		body    string
	}{
		{"", 0, ""},
		{"4bf92f3577b34da6a3ce929d0e0e4736", 1721900000123456789, "GET /api/v1/cart 200"},
		{"", 1, "short"},
		{"abc", 18446744073709551615, "max uint64 timestamp"},
		{"4bf92f3577b34da6a3ce929d0e0e4736", 999999999999999999, ""},
	}
	for _, tc := range cases {
		want := referenceComputeLogID(tc.traceID, tc.tsNs, tc.body)
		got := computeLogID(tc.traceID, tc.tsNs, tc.body)
		if got != want {
			t.Fatalf("computeLogID(%q, %d, %q) = %q, want %q", tc.traceID, tc.tsNs, tc.body, got, want)
		}
	}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 1000; i++ {
		ts := rng.Uint64()
		body := fmt.Sprintf("body-%d", rng.Int63())
		want := referenceComputeLogID("4bf92f3577b34da6a3ce929d0e0e4736", ts, body)
		got := computeLogID("4bf92f3577b34da6a3ce929d0e0e4736", ts, body)
		if got != want {
			t.Fatalf("iteration %d: got %q want %q", i, got, want)
		}
	}
}

func BenchmarkComputeLogIDReference(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = referenceComputeLogID("4bf92f3577b34da6a3ce929d0e0e4736", 1721900000123456789, "GET /api/v1/cart 200")
	}
}

func BenchmarkComputeLogID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = computeLogID("4bf92f3577b34da6a3ce929d0e0e4736", 1721900000123456789, "GET /api/v1/cart 200")
	}
}

package spans

import (
	"fmt"
	"testing"

	"github.com/optikklabs/ingest/internal/infra/otlp"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

// referenceMergeAndCapAttrs is the pre-optimization per-span merge, kept as
// the baseline the precomputed-resource-base version is benchmarked against.
func referenceMergeAndCapAttrs(resMap, spanMap map[string]string) map[string]string {
	merged := make(map[string]string, len(resMap)+len(spanMap))
	for k, v := range resMap {
		if !isPromotedKey(k) {
			merged[k] = v
		}
	}
	for k, v := range spanMap {
		if !isPromotedKey(k) {
			merged[k] = v
		}
	}
	_ = otlp.CapStringMap(merged, maxSpanAttributes)
	return merged
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

// benchRequest mimics a typical batch: one resource shared by many spans,
// most spans carrying a few own attrs and some carrying none.
func benchRequest(spans int) *tracepb.ExportTraceServiceRequest {
	resAttrs := make([]*commonpb.KeyValue, 0, 12)
	for i := 0; i < 10; i++ {
		resAttrs = append(resAttrs, strAttr(fmt.Sprintf("resource.attr.%d", i), "value"))
	}
	resAttrs = append(resAttrs, strAttr("service.name", "checkout"), strAttr("deployment.environment", "prod"))

	ss := &trace.ScopeSpans{Spans: make([]*trace.Span, 0, spans)}
	for i := 0; i < spans; i++ {
		s := &trace.Span{
			TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, byte(i), byte(i >> 8)},
			SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, byte(i)},
			Name:              "GET /api/v1/cart",
			Kind:              trace.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: 1721900000000000000 + uint64(i),
			EndTimeUnixNano:   1721900000100000000 + uint64(i),
		}
		// Every 4th span has no own attributes (aliases the resource base).
		if i%4 != 0 {
			s.Attributes = []*commonpb.KeyValue{
				strAttr("http.method", "GET"),
				strAttr("http.route", "/api/v1/cart"),
				strAttr("app.cart.items", "3"),
			}
		}
		ss.Spans = append(ss.Spans, s)
	}
	return &tracepb.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource:   &resourcepb.Resource{Attributes: resAttrs},
			ScopeSpans: []*trace.ScopeSpans{ss},
		}},
	}
}

func TestMapRequestSharesResourceBase(t *testing.T) {
	req := benchRequest(8)
	rows, usage := mapRequest(1, req)
	if len(rows) != 8 {
		t.Fatalf("rows = %d, want 8", len(rows))
	}
	if len(usage) != 1 || usage[0].Records != 8 || usage[0].Service != "checkout" {
		t.Fatalf("usage = %+v, want one entry with 8 records for checkout", usage)
	}
	// Attr merge must match the per-span reference implementation.
	resMap := otlp.AttrsToMap(req.ResourceSpans[0].Resource.Attributes)
	for i, row := range rows {
		spanMap := otlp.AttrsToMap(req.ResourceSpans[0].ScopeSpans[0].Spans[i].Attributes)
		want := referenceMergeAndCapAttrs(resMap, spanMap)
		if len(row.Attributes) != len(want) {
			t.Fatalf("span %d: attrs len %d, want %d", i, len(row.Attributes), len(want))
		}
		for k, v := range want {
			if row.Attributes[k] != v {
				t.Fatalf("span %d: attr %q = %q, want %q", i, k, row.Attributes[k], v)
			}
		}
	}
	// Attr-less spans must share one map instance across the resource.
	if len(rows[0].Attributes) == 0 {
		t.Fatal("expected non-empty base attrs")
	}
	rows[0].Attributes["sharing.test"] = "shared"
	if rows[4].Attributes["sharing.test"] != "shared" {
		t.Fatal("attr-less spans do not share the resource attribute map")
	}
}

func BenchmarkMapRequest(b *testing.B) {
	req := benchRequest(200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := mapRequest(1, req)
		if len(rows) != 200 {
			b.Fatal("unexpected row count")
		}
	}
}

// BenchmarkMergeAttrsReference measures the old per-span full re-copy of
// resource attrs, for comparison with the precomputed base in MapRequest.
func BenchmarkMergeAttrsReference(b *testing.B) {
	req := benchRequest(200)
	resMap := otlp.AttrsToMap(req.ResourceSpans[0].Resource.Attributes)
	spans := req.ResourceSpans[0].ScopeSpans[0].Spans
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range spans {
			spanMap := otlp.AttrsToMap(s.Attributes)
			_ = referenceMergeAndCapAttrs(resMap, spanMap)
		}
	}
}

// BenchmarkMergeAttrs measures the new path over the same spans.
func BenchmarkMergeAttrs(b *testing.B) {
	req := benchRequest(200)
	resMap := otlp.AttrsToMap(req.ResourceSpans[0].Resource.Attributes)
	spans := req.ResourceSpans[0].ScopeSpans[0].Spans
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := resourceBaseAttrs(resMap)
		for _, s := range spans {
			spanMap := otlp.AttrsToMap(s.Attributes)
			_ = mergeAndCapAttrs(base, spanMap)
		}
	}
}

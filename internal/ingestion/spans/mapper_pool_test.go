package spans

import (
	"testing"

	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func req(service string, spanAttrs ...*commonpb.KeyValue) *tracepb.ExportTraceServiceRequest {
	return &tracepb.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", service)}},
			ScopeSpans: []*trace.ScopeSpans{{
				Spans: []*trace.Span{{Name: "op", Attributes: spanAttrs}},
			}},
		}},
	}
}

// TestMapperPoolNoContaminationAcrossRequests maps two distinct requests in
// sequence so the pooled resMap/spanMap are reused, then asserts neither the
// resource nor the span attributes of the first request leak into the second.
func TestMapperPoolNoContaminationAcrossRequests(t *testing.T) {
	first := mapRequest(1, req("alpha", strKV("db.system", "postgres"), strKV("custom.a", "1")))
	second := mapRequest(2, req("beta", strKV("custom.b", "2")))

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("row counts = %d, %d, want 1, 1", len(first), len(second))
	}
	if first[0].Service != "alpha" || second[0].Service != "beta" {
		t.Fatalf("services = %q, %q; want alpha, beta", first[0].Service, second[0].Service)
	}
	// The second span carried no attributes: nothing from the first must remain.
	if second[0].DbSystem != "" {
		t.Errorf("db.system leaked into second request: %q", second[0].DbSystem)
	}
	if _, ok := second[0].Attributes["custom.a"]; ok {
		t.Errorf("custom.a leaked into second request attributes: %v", second[0].Attributes)
	}
	if second[0].Attributes["custom.b"] != "2" {
		t.Errorf("second merged custom.b = %q, want 2", second[0].Attributes["custom.b"])
	}
	// The first row's retained merged map must be intact after reuse.
	if first[0].DbSystem != "postgres" {
		t.Errorf("first db.system = %q, want postgres", first[0].DbSystem)
	}
	if first[0].Attributes["custom.a"] != "1" {
		t.Errorf("first retained custom.a = %q, want 1", first[0].Attributes["custom.a"])
	}
}

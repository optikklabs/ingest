package logs

import (
	"testing"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

// Record-level fallbacks must not leak across records sharing a ResourceLogs.
func TestResourceFallbackDoesNotLeakAcrossRecords(t *testing.T) {
	req := &logspb.ExportLogsServiceRequest{
		ResourceLogs: []*logv1.ResourceLogs{{
			// No resource-level service.name → fallback to record attrs.
			ScopeLogs: []*logv1.ScopeLogs{{
				LogRecords: []*logv1.LogRecord{
					{Attributes: []*commonpb.KeyValue{strAttr("service.name", "foo")}},
					{}, // no service.name anywhere
				},
			}},
		}},
	}

	rows := mapRequest(7, req)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Service != "foo" {
		t.Fatalf("record A service = %q, want foo", rows[0].Service)
	}
	if rows[1].Service != "" {
		t.Fatalf("record B leaked service = %q, want empty", rows[1].Service)
	}
}

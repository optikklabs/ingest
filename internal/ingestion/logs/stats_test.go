package logs

import (
	"testing"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func resourceLogs(service, env string, records int) *logv1.ResourceLogs {
	logRecords := make([]*logv1.LogRecord, records)
	for i := range logRecords {
		logRecords[i] = &logv1.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello world"}}}
	}
	return &logv1.ResourceLogs{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			strAttr("service.name", service),
			strAttr("deployment.environment", env),
		}},
		ScopeLogs: []*logv1.ScopeLogs{{LogRecords: logRecords}},
	}
}

func TestStatRows(t *testing.T) {
	req := &logspb.ExportLogsServiceRequest{ResourceLogs: []*logv1.ResourceLogs{
		resourceLogs("checkout", "prod", 3),
		resourceLogs("payments", "staging", 2),
		resourceLogs("empty", "prod", 0), // no records → skipped
	}}

	rows := statRows(7, req)
	if len(rows) != 2 {
		t.Fatalf("want 2 stat rows (empty resource skipped), got %d", len(rows))
	}

	byService := map[string]uint64{}
	for _, r := range rows {
		if r.GetTenantId() != 7 {
			t.Errorf("tenant: want 7, got %d", r.GetTenantId())
		}
		if r.GetSignal() != "logs" {
			t.Errorf("signal: want logs, got %q", r.GetSignal())
		}
		if r.GetByteCount() == 0 {
			t.Errorf("service %q: byte_count should be > 0", r.GetService())
		}
		byService[r.GetService()] = r.GetRecordCount()
	}
	if byService["checkout"] != 3 {
		t.Errorf("checkout records: want 3, got %d", byService["checkout"])
	}
	if byService["payments"] != 2 {
		t.Errorf("payments records: want 2, got %d", byService["payments"])
	}
}

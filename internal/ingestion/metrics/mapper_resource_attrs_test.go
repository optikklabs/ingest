package metrics

import (
	"testing"

	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestHostResourceAttrsAllowlist(t *testing.T) {
	got := hostResourceAttrs(map[string]string{
		"host.name":              "web-1",
		"host.arch":              "arm64",
		"os.type":                "linux",
		"cloud.provider":         "gcp",
		"cloud.region":           "asia-south1",
		"k8s.node.name":          "gke-node-3",
		"k8s.cluster.name":       "optikk-prod",
		"service.name":           "api",
		"service.instance.id":    "f00d",
		"telemetry.sdk.name":     "opentelemetry",
		"process.runtime.name":   "OpenJDK",
		"deployment.environment": "production",
		"k8s.pod.name":           "api-6d8f9c7b4-x2x",
	})
	want := map[string]string{
		"host.name":        "web-1",
		"host.arch":        "arm64",
		"os.type":          "linux",
		"cloud.provider":   "gcp",
		"cloud.region":     "asia-south1",
		"k8s.node.name":    "gke-node-3",
		"k8s.cluster.name": "optikk-prod",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d attrs %v, want %d", len(got), got, len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attr %q = %q, want %q", k, got[k], v)
		}
	}
}

func TestHostResourceAttrsEmptyStaysNil(t *testing.T) {
	if got := hostResourceAttrs(map[string]string{"service.name": "api"}); got != nil {
		t.Fatalf("expected nil map for non-host resource, got %v", got)
	}
}

func TestSeriesRowCarriesHostResourceAttrs(t *testing.T) {
	resMap := map[string]string{"service.name": "api", "host.name": "web-1", "os.type": "linux"}
	hdr := rowHeader{tenantID: 1, resMap: resMap, hostResAttrs: hostResourceAttrs(resMap)}
	m := &metricsdatapb.Metric{Name: "system.cpu.utilization"}
	_, series := gaugeRow(hdr, m, gaugeWith())
	ra := series.GetResourceAttributes()
	if ra["host.name"] != "web-1" || ra["os.type"] != "linux" {
		t.Fatalf("resource_attributes missing host keys: %v", ra)
	}
	if _, ok := ra["service.name"]; ok {
		t.Fatalf("service.name must not be persisted in resource_attributes: %v", ra)
	}
}

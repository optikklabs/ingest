package metrics

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// strKV builds a string-valued OTLP attribute.
func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

func gaugeWith(kvs ...*commonpb.KeyValue) *metricsdatapb.NumberDataPoint {
	return &metricsdatapb.NumberDataPoint{
		TimeUnixNano: 1_700_000_000_000_000_000,
		Attributes:   kvs,
		Value:        &metricsdatapb.NumberDataPoint_AsDouble{AsDouble: 1},
	}
}

func TestRowFingerprintOrderIndependent(t *testing.T) {
	hdr := rowHeader{teamID: 1, resMap: map[string]string{"service.name": "api"}}
	m := &metricsdatapb.Metric{Name: "kafka.consumer.commit_rate"}

	a, _ := gaugeRow(hdr, m, gaugeWith(
		strKV("topic", "orders"), strKV("client-id", "consumer-checkout-1")))
	b, _ := gaugeRow(hdr, m, gaugeWith(
		strKV("client-id", "consumer-checkout-1"), strKV("topic", "orders")))

	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("fingerprint order-dependent: %d != %d", a.Fingerprint, b.Fingerprint)
	}
}

func TestRowFingerprintDistinctOnValue(t *testing.T) {
	hdr := rowHeader{teamID: 1, resMap: map[string]string{"service.name": "api"}}
	m := &metricsdatapb.Metric{Name: "kafka.consumer_group.lag"}

	a, _ := gaugeRow(hdr, m, gaugeWith(strKV("topic", "orders")))
	b, _ := gaugeRow(hdr, m, gaugeWith(strKV("topic", "payments")))

	if a.Fingerprint == b.Fingerprint {
		t.Fatal("distinct attribute value collided into one fingerprint")
	}
}

func TestRowFingerprintDropsHighCardinality(t *testing.T) {
	hdr := rowHeader{teamID: 1, resMap: map[string]string{"service.name": "api"}}
	m := &metricsdatapb.Metric{Name: "http.server.duration"}

	a, _ := gaugeRow(hdr, m, gaugeWith(strKV("route", "/x"), strKV("k8s.pod.uid", "pod-aaa")))
	b, _ := gaugeRow(hdr, m, gaugeWith(strKV("route", "/x"), strKV("k8s.pod.uid", "pod-bbb")))

	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("high-cardinality key leaked into identity: %d != %d", a.Fingerprint, b.Fingerprint)
	}
}

func TestRowFingerprintResourceDataPointNoCollision(t *testing.T) {
	m := &metricsdatapb.Metric{Name: "app.requests"}

	a, _ := gaugeRow(rowHeader{teamID: 1, resMap: map[string]string{"k8s.namespace.name": "prod"}},
		m, gaugeWith(strKV("k8s.namespace.name", "team-a")))
	b, _ := gaugeRow(rowHeader{teamID: 1, resMap: map[string]string{"k8s.namespace.name": "team-a"}},
		m, gaugeWith(strKV("k8s.namespace.name", "prod")))

	if a.Fingerprint == b.Fingerprint {
		t.Fatal("resource/data-point key collision merged distinct series")
	}
}

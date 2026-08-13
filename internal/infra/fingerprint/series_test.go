package fingerprint

import "testing"

// SeriesHash must be order-independent across both maps.
func TestSeriesHashOrderIndependent(t *testing.T) {
	resA := map[string]string{"service.name": "api", "host.name": "h1"}
	resB := map[string]string{"host.name": "h1", "service.name": "api"}
	dpA := map[string]string{"db.system": "postgres", "topic": "orders"}
	dpB := map[string]string{"topic": "orders", "db.system": "postgres"}
	if SeriesHash("m", "Unspecified", resA, dpA) != SeriesHash("m", "Unspecified", resB, dpB) {
		t.Fatal("map insertion order changed the fingerprint")
	}
}

func TestSeriesHashDistinctOnLabel(t *testing.T) {
	res := map[string]string{"service.name": "api"}
	a := SeriesHash("m", "Unspecified", res, map[string]string{"foo": "a"})
	b := SeriesHash("m", "Unspecified", res, map[string]string{"foo": "b"})
	if a == b {
		t.Fatal("distinct labels collided into one fingerprint")
	}
}

func TestSeriesHashDistinctOnResource(t *testing.T) {
	dp := map[string]string{"foo": "a"}
	a := SeriesHash("m", "Unspecified", map[string]string{"host.name": "h1"}, dp)
	b := SeriesHash("m", "Unspecified", map[string]string{"host.name": "h2"}, dp)
	if a == b {
		t.Fatal("distinct resources collided into one fingerprint")
	}
}

// High-cardinality keys must not contribute to series identity.
func TestSeriesHashDropsHighCardinalityKeys(t *testing.T) {
	res := map[string]string{"service.name": "api", "k8s.pod.uid": "u1", "container.id": "c1"}
	dp := map[string]string{"db.system": "postgres", "process.pid": "123"}

	want := SeriesHash("m", "Cumulative", map[string]string{"service.name": "api"}, map[string]string{"db.system": "postgres"})
	got := SeriesHash("m", "Cumulative", res, dp)
	if got != want {
		t.Fatalf("high-cardinality keys changed the fingerprint: got %d want %d", got, want)
	}
}

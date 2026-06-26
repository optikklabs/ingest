package fingerprint

import "testing"

// SeriesHash must be order-independent across both maps.
func TestSeriesHashOrderIndependent(t *testing.T) {
	res := map[string]string{"service.name": "api", "host.name": "h1"}
	dp := map[string]string{"db.system": "postgres", "topic": "orders"}
	if SeriesHash("m", "Unspecified", res, dp) != SeriesHash("m", "Unspecified", res, dp) {
		t.Fatal("hash not stable for identical input")
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

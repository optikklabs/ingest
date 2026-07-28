package fingerprint

import (
	"fmt"
	"math/rand"
	"testing"
)

// referenceSeriesHash is the original implementation, kept verbatim as the
// equivalence oracle: fingerprints persist in metrics_series, so SeriesHash
// must produce byte-identical hashes forever. It expects resource attributes
// already stripped of high-cardinality keys.
func referenceSeriesHash(metricName, temporality string, filteredResAttrs, dpAttrs map[string]string) uint64 {
	merged := make(map[string]string, len(filteredResAttrs)+len(dpAttrs)+2)
	for k, v := range filteredResAttrs {
		merged[k] = v
	}
	for k, v := range dpAttrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			merged[k] = v
		}
	}
	merged["__temporality__"] = temporality
	merged["__name__"] = metricName

	return FingerprintHash(merged)
}

func dropHighCardinality(attrs map[string]string) map[string]string {
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			out[k] = v
		}
	}
	return out
}

func TestSeriesHashMatchesReference(t *testing.T) {
	cases := []struct {
		name     string
		res, dp  map[string]string
		metric   string
		temporal string
	}{
		{name: "empty maps", metric: "up", temporal: "Unspecified"},
		{
			name:     "resource only",
			res:      map[string]string{"service.name": "api", "host.name": "n1"},
			metric:   "http_requests_total",
			temporal: "Cumulative",
		},
		{
			name:     "dp only",
			dp:       map[string]string{"method": "GET", "code": "200"},
			metric:   "http_requests_total",
			temporal: "Delta",
		},
		{
			name:     "overlapping key dp wins",
			res:      map[string]string{"service.name": "api", "region": "res-value"},
			dp:       map[string]string{"region": "dp-value", "code": "500"},
			metric:   "errors",
			temporal: "Cumulative",
		},
		{
			name:     "high-cardinality keys dropped from both maps",
			res:      map[string]string{"container.id": "res-cid", "service.name": "api"},
			dp:       map[string]string{"container.id": "dp-cid", "k8s.pod.uid": "u1"},
			metric:   "cpu",
			temporal: "Cumulative",
		},
		{
			name:     "attr named like sentinel loses to sentinel",
			res:      map[string]string{"__name__": "spoof-res"},
			dp:       map[string]string{"__temporality__": "spoof-dp"},
			metric:   "real_name",
			temporal: "Delta",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := referenceSeriesHash(tc.metric, tc.temporal, dropHighCardinality(tc.res), tc.dp)
			got := SeriesHash(tc.metric, tc.temporal, tc.res, tc.dp)
			if got != want {
				t.Fatalf("hash mismatch: got %d want %d", got, want)
			}
		})
	}
}

func TestSeriesHashMatchesReferenceRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	keyPool := []string{
		"service.name", "host.name", "region", "method", "code", "zone",
		"container.id", "k8s.pod.uid", "process.pid", "__name__", "a", "b",
	}
	randAttrs := func(n int) map[string]string {
		m := make(map[string]string, n)
		for i := 0; i < n; i++ {
			m[keyPool[rng.Intn(len(keyPool))]] = fmt.Sprintf("v%d", rng.Intn(5))
		}
		return m
	}
	for i := 0; i < 1000; i++ {
		res := randAttrs(rng.Intn(8))
		dp := randAttrs(rng.Intn(6))
		want := referenceSeriesHash("m", "Cumulative", dropHighCardinality(res), dp)
		got := SeriesHash("m", "Cumulative", res, dp)
		if got != want {
			t.Fatalf("iteration %d mismatch (res=%v dp=%v): got %d want %d", i, res, dp, got, want)
		}
	}
}

package metrics

import (
	"testing"

	metricsdatapb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// Summary and ExponentialHistogram must not be silently dropped.
func TestExponentialHistogramMapped(t *testing.T) {
	hdr := rowHeader{teamID: 1, resMap: map[string]string{"service.name": "api"}}
	m := &metricsdatapb.Metric{
		Name: "http.server.duration",
		Data: &metricsdatapb.Metric_ExponentialHistogram{
			ExponentialHistogram: &metricsdatapb.ExponentialHistogram{
				DataPoints: []*metricsdatapb.ExponentialHistogramDataPoint{{
					TimeUnixNano: 1_700_000_000_000_000_000,
					Scale:        0, // base = 2
					ZeroCount:    3,
					Count:        9,
					Positive: &metricsdatapb.ExponentialHistogramDataPoint_Buckets{
						Offset:       0,
						BucketCounts: []uint64{2, 4}, // (1,2] and (2,4]
					},
				}},
			},
		},
	}

	acc := &rowAccumulator{seen: make(map[uint64]struct{})}
	appendMetric(acc, hdr, m)
	if len(acc.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(acc.rows))
	}
	row := acc.rows[0]
	// counts must be exactly one longer than bounds (explicit-histogram contract).
	if len(row.HistCounts) != len(row.HistBuckets)+1 {
		t.Fatalf("counts/bounds contract violated: %d counts, %d bounds",
			len(row.HistCounts), len(row.HistBuckets))
	}
	// First count is zero_count; positive bucket counts follow.
	wantCounts := []uint64{3, 2, 4, 0}
	if len(row.HistCounts) != len(wantCounts) {
		t.Fatalf("counts = %v, want %v", row.HistCounts, wantCounts)
	}
	for i, c := range wantCounts {
		if row.HistCounts[i] != c {
			t.Fatalf("counts = %v, want %v", row.HistCounts, wantCounts)
		}
	}
	// base=2, offset=0: bounds are 2^0,2^1,2^2 = 1,2,4.
	wantBounds := []float64{1, 2, 4}
	for i, b := range wantBounds {
		if row.HistBuckets[i] != b {
			t.Fatalf("bounds = %v, want %v", row.HistBuckets, wantBounds)
		}
	}
}

func TestSummaryMapped(t *testing.T) {
	hdr := rowHeader{teamID: 1, resMap: map[string]string{"service.name": "api"}}
	m := &metricsdatapb.Metric{
		Name: "rpc.duration",
		Data: &metricsdatapb.Metric_Summary{
			Summary: &metricsdatapb.Summary{
				DataPoints: []*metricsdatapb.SummaryDataPoint{{
					TimeUnixNano: 1_700_000_000_000_000_000,
					Count:        10,
					Sum:          55,
					QuantileValues: []*metricsdatapb.SummaryDataPoint_ValueAtQuantile{
						{Quantile: 0.5, Value: 4},
						{Quantile: 0.99, Value: 9},
					},
				}},
			},
		},
	}

	acc := &rowAccumulator{seen: make(map[uint64]struct{})}
	appendMetric(acc, hdr, m)
	if len(acc.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(acc.rows))
	}
	row := acc.rows[0]
	if row.HistCount != 10 || row.HistSum != 55 {
		t.Fatalf("summary count/sum = %d/%v, want 10/55", row.HistCount, row.HistSum)
	}
	if len(row.SummaryQuantiles) != 2 || row.SummaryQuantiles[1] != 0.99 || row.SummaryValues[1] != 9 {
		t.Fatalf("quantiles = %v values = %v", row.SummaryQuantiles, row.SummaryValues)
	}
}

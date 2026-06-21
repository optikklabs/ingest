package timebucket

import "testing"

// BucketSeconds is baked into the spans/logs/metrics PKs and both rollup MVs.
// Changing it is a breaking schema change requiring a table rebuild.
func TestBucketSecondsInvariant(t *testing.T) {
	if BucketSeconds != 300 {
		t.Fatalf("BucketSeconds = %d; changing it requires a CH table rebuild", BucketSeconds)
	}
}

// BucketStart must match the MV-side derivation:
// intDiv(toUnixTimestamp(timestamp), 300) * 300.
func TestBucketStartMatchesMVDerivation(t *testing.T) {
	cases := []int64{
		0, 1, 299, 300, 301, 599, 600,
		1735689600,     // 2025-01-01 00:00:00 (bucket-aligned)
		1735689600 + 7, // mid-bucket
		1735689600 + 299,
	}
	for _, s := range cases {
		want := uint32((s / 300) * 300)
		if got := BucketStart(s); got != want {
			t.Errorf("BucketStart(%d) = %d, want %d", s, got, want)
		}
	}
}

package timebucket

import (
	"testing"
	"time"
)

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

// Display grain windows: <=3h: 1m, <=24h: 5m, <=7d: 1h, else 1d.
func TestDisplayGrainWindows(t *testing.T) {
	cases := []struct {
		windowMs int64
		want     time.Duration
	}{
		{int64(time.Hour / time.Millisecond), time.Minute},
		{3 * int64(time.Hour/time.Millisecond), time.Minute},
		{3*int64(time.Hour/time.Millisecond) + 1, 5 * time.Minute},
		{24 * int64(time.Hour/time.Millisecond), 5 * time.Minute},
		{25 * int64(time.Hour/time.Millisecond), time.Hour},
		{7 * 24 * int64(time.Hour/time.Millisecond), time.Hour},
		{8 * 24 * int64(time.Hour/time.Millisecond), 24 * time.Hour},
	}
	for _, c := range cases {
		if got := DisplayGrain(c.windowMs); got != c.want {
			t.Errorf("DisplayGrain(%dms) = %v, want %v", c.windowMs, got, c.want)
		}
	}
}

// DisplayGrainSQL must dispatch to the matching toStartOfX function.
func TestDisplayGrainSQLDispatch(t *testing.T) {
	cases := []struct {
		windowMs int64
		want     string
	}{
		{int64(time.Hour / time.Millisecond), "toStartOfMinute(timestamp)"},
		{12 * int64(time.Hour/time.Millisecond), "toStartOfFiveMinutes(timestamp)"},
		{3 * 24 * int64(time.Hour/time.Millisecond), "toStartOfHour(timestamp)"},
		{30 * 24 * int64(time.Hour/time.Millisecond), "toStartOfDay(timestamp)"},
	}
	for _, c := range cases {
		if got := DisplayGrainSQL(c.windowMs); got != c.want {
			t.Errorf("DisplayGrainSQL(%dms) = %q, want %q", c.windowMs, got, c.want)
		}
	}
}

// DisplayBucket must agree with DisplayGrainSQL's truncation semantics.
func TestDisplayBucketTruncation(t *testing.T) {
	rowSec := int64(1735693271) // 2025-01-01 01:01:11 UTC
	cases := []struct {
		windowMs int64
		want     string
	}{
		{int64(time.Hour / time.Millisecond), "2025-01-01 01:01:00"},
		{12 * int64(time.Hour/time.Millisecond), "2025-01-01 01:00:00"},
		{3 * 24 * int64(time.Hour/time.Millisecond), "2025-01-01 01:00:00"},
		{30 * 24 * int64(time.Hour/time.Millisecond), "2025-01-01 00:00:00"},
	}
	for _, c := range cases {
		if got := FormatDisplayBucket(DisplayBucket(rowSec, c.windowMs)); got != c.want {
			t.Errorf("DisplayBucket(%d, %dms) = %q, want %q", rowSec, c.windowMs, got, c.want)
		}
	}
}

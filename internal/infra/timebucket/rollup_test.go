package timebucket

import (
	"testing"
	"time"
)

func TestUseHourRollupBoundary(t *testing.T) {
	day := (24 * time.Hour).Milliseconds()
	if UseHourRollup(day) {
		t.Fatal("exactly 24h must stay on the 1m tier (display grain 5m)")
	}
	if !UseHourRollup(day + 1) {
		t.Fatal("just over 24h must route to the 1h tier (display grain 1h)")
	}
}

func TestRollupTableSelection(t *testing.T) {
	short := (6 * time.Hour).Milliseconds()
	long := (7 * 24 * time.Hour).Milliseconds()
	if got := SpansRollup(short); got != "observability.spans_1m" {
		t.Fatalf("SpansRollup(short) = %s", got)
	}
	if got := SpansRollup(long); got != "observability.spans_1h" {
		t.Fatalf("SpansRollup(long) = %s", got)
	}
	if got := MetricsRollup(long); got != "observability.metrics_1h" {
		t.Fatalf("MetricsRollup(long) = %s", got)
	}
	if got := MetricsHistRollup(long); got != "observability.metrics_hist_1h" {
		t.Fatalf("MetricsHistRollup(long) = %s", got)
	}
}

func TestSnapRangeForRollup(t *testing.T) {
	// 2026-06-10 10:35:00 UTC
	start := time.Date(2026, 6, 10, 10, 35, 0, 0, time.UTC).UnixMilli()
	endShort := start + (6 * time.Hour).Milliseconds()
	if s, e := SnapRangeForRollup(start, endShort); s != start || e != endShort {
		t.Fatal("short windows must pass through unsnapped")
	}
	endLong := start + (3 * 24 * time.Hour).Milliseconds()
	s, e := SnapRangeForRollup(start, endLong)
	if e != endLong {
		t.Fatal("end must stay as-is (in-progress hour contributes partial data)")
	}
	wantStart := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC).UnixMilli()
	if s != wantStart {
		t.Fatalf("start = %d, want hour-floored %d", s, wantStart)
	}
	// Hour-floored starts are valid 5-minute bucket boundaries.
	if BucketStart(s/1000) != uint32(s/1000) {
		t.Fatal("snapped start must be a valid ts_bucket boundary")
	}
}

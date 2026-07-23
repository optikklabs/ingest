package kafka

import "testing"

func TestSignalFromTopic(t *testing.T) {
	cases := map[string]string{
		"optikk.ingest.spans":            SignalSpans,
		"optikk.ingest.spans_tracegraph": SignalSpansTracegraph,
		"optikk.ingest.logs":             SignalLogs,
		"optikk.ingest.metrics":          SignalMetrics,
		"optikk.ingest.metric_series":    SignalMetricSeries,
		"optikk.ingest.ingestion_stats":  SignalIngestionStats,
		"optikk.dlq.metrics":             SignalMetrics,
		"optikk.ingest.somethingelse":    "unknown",
		"nodots":                         "unknown",
	}
	for topic, want := range cases {
		if got := signalFromTopic(topic); got != want {
			t.Errorf("signalFromTopic(%q) = %q, want %q", topic, got, want)
		}
	}
}

package app

import (
	"testing"

	"github.com/optikklabs/ingest/internal/config"
)

func TestIngestTopicSpecsDeduplicatesSharedSpanTopic(t *testing.T) {
	wirings := []signalWiring{
		{signal: "spans", cfg: config.SignalConfig{Partitions: 8, Replicas: 1, RetentionHours: 24}},
		{signal: "spans", cfg: config.SignalConfig{Partitions: 8, Replicas: 1, RetentionHours: 24}},
	}
	specs := ingestTopicSpecs(wirings, "optikk.ingest", "optikk.dlq")
	if len(specs) != 2 {
		t.Fatalf("topic specs = %d, want one ingest topic and one DLQ topic", len(specs))
	}
}

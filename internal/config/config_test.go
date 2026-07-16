package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSpanmetricsConsumerGroupEnvironmentOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("mysql:\n  password: mysql\nclickhouse:\n  password: clickhouse\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPTIKK_INGESTION_SPANMETRICS_CONSUMER_GROUP", "spanmetrics-workers")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.SpanmetricsConsumerGroup(); got != "spanmetrics-workers" {
		t.Errorf("spanmetrics group = %q, want environment override", got)
	}
}

package config

import "fmt"

// IngestionConfig owns per-signal Kafka topology (topic partitions, replicas,
// retention) and the consumer-group identity.
type IngestionConfig struct {
	Spans        SignalConfig `yaml:"spans"`
	Logs         SignalConfig `yaml:"logs"`
	Metrics      SignalConfig `yaml:"metrics"`
	MetricSeries SignalConfig `yaml:"metric_series"`
}

type SignalConfig struct {
	Partitions     int    `yaml:"partitions"`
	Replicas       int    `yaml:"replicas"`
	RetentionHours int    `yaml:"retention_hours"`
	ConsumerGroup  string `yaml:"consumer_group"`
}

func SignalDefaults(signal string) SignalConfig {
	return SignalConfig{
		Partitions:     8,
		Replicas:       1,
		RetentionHours: 24,
		ConsumerGroup:  fmt.Sprintf("optikk-ingest.%s.consumer", signal),
	}
}

func (c Config) IngestSignal(signal string) SignalConfig {
	var raw SignalConfig
	switch signal {
	case "spans":
		raw = c.Ingestion.Spans
	case "logs":
		raw = c.Ingestion.Logs
	case "metrics":
		raw = c.Ingestion.Metrics
	case "metric_series":
		raw = c.Ingestion.MetricSeries
	}
	def := SignalDefaults(signal)
	if raw.Partitions <= 0 {
		raw.Partitions = def.Partitions
	}
	if raw.Replicas <= 0 {
		raw.Replicas = def.Replicas
	}
	if raw.RetentionHours <= 0 {
		raw.RetentionHours = def.RetentionHours
	}
	if raw.ConsumerGroup == "" {
		raw.ConsumerGroup = def.ConsumerGroup
	}
	return raw
}

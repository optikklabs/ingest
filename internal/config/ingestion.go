package config

import (
	"fmt"
	"time"
)

// IngestionConfig owns per-signal Kafka topology (topic partitions, replicas,
// retention) and the consumer-group identity.
type IngestionConfig struct {
	Spans          SignalConfig `yaml:"spans"`
	Logs           SignalConfig `yaml:"logs"`
	Metrics        SignalConfig `yaml:"metrics"`
	MetricSeries   SignalConfig `yaml:"metric_series"`
	IngestionStats SignalConfig `yaml:"ingestion_stats"`

	SidePublish           SidePublishConfig `yaml:"side_publish"`
	ResourceCacheSize     int               `yaml:"resource_cache_size"`
	APIKeyCacheTTLSeconds int               `yaml:"api_key_cache_ttl_seconds"`
	APIKeyCacheSize       int               `yaml:"api_key_cache_size"`
}

// SidePublishConfig tunes the async best-effort publisher used for the
// resource and llm-scores side-topics (off the OTLP request path).
type SidePublishConfig struct {
	QueueSize int `yaml:"queue_size"`
	Workers   int `yaml:"workers"`
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
	case "ingestion_stats":
		raw = c.Ingestion.IngestionStats
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

// SidePublishQueueSize bounds the async side-topic queue. Values <= 0 default
// to 4096; a full queue drops best-effort rows rather than blocking Export.
func (c Config) SidePublishQueueSize() int {
	if n := c.Ingestion.SidePublish.QueueSize; n > 0 {
		return n
	}
	return 4096
}

// SidePublishWorkers is the number of goroutines draining the side-topic
// queue. Values <= 0 default to 2.
func (c Config) SidePublishWorkers() int {
	if n := c.Ingestion.SidePublish.Workers; n > 0 {
		return n
	}
	return 2
}

// ResourceCacheSize is the total per-signal resource-cache capacity (split
// across shards). Sized to active-series cardinality, not tenant count.
// Values <= 0 default to 500,000.
func (c Config) ResourceCacheSize() int {
	if n := c.Ingestion.ResourceCacheSize; n > 0 {
		return n
	}
	return 500_000
}

// APIKeyCacheTTL limits how long a rotated or revoked API key can remain valid
// in an ingest process. Values <= 0 default to 30 seconds.
func (c Config) APIKeyCacheTTL() time.Duration {
	if seconds := c.Ingestion.APIKeyCacheTTLSeconds; seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return 30 * time.Second
}

// APIKeyCacheSize bounds valid and invalid API-key entries so random keys
// cannot grow ingest memory without limit.
func (c Config) APIKeyCacheSize() int {
	if size := c.Ingestion.APIKeyCacheSize; size > 0 {
		return size
	}
	return 50_000
}

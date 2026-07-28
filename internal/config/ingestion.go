package config

import (
	"fmt"
	"time"
)

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

func (c Config) SidePublishQueueSize() int {
	if n := c.Ingestion.SidePublish.QueueSize; n > 0 {
		return n
	}
	return 4096
}

func (c Config) SidePublishWorkers() int {
	if n := c.Ingestion.SidePublish.Workers; n > 0 {
		return n
	}
	return 2
}

func (c Config) ResourceCacheSize() int {
	if n := c.Ingestion.ResourceCacheSize; n > 0 {
		return n
	}
	return 500_000
}

func (c Config) APIKeyCacheTTL() time.Duration {
	if seconds := c.Ingestion.APIKeyCacheTTLSeconds; seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return 30 * time.Second
}

func (c Config) APIKeyCacheSize() int {
	if size := c.Ingestion.APIKeyCacheSize; size > 0 {
		return size
	}
	return 50_000
}

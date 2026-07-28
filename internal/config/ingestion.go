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
	LLMScores      SignalConfig `yaml:"llm_scores"`

	SidePublish                    SidePublishConfig `yaml:"side_publish"`
	ResourceCacheSize              int               `yaml:"resource_cache_size"`
	SeriesRepublishIntervalSeconds int               `yaml:"series_republish_interval_seconds"`
	APIKeyCacheTTLSeconds          int               `yaml:"api_key_cache_ttl_seconds"`
	APIKeyCacheSize                int               `yaml:"api_key_cache_size"`
	StatsFlushIntervalSeconds      int               `yaml:"stats_flush_interval_seconds"`
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

// SignalDefaults feeds viper's setDefaults, the single source of defaults.
func SignalDefaults(signal string) SignalConfig {
	return SignalConfig{
		Partitions:     8,
		Replicas:       1,
		RetentionHours: 24,
		ConsumerGroup:  fmt.Sprintf("optikk-ingest.%s.consumer", signal),
	}
}

func (c Config) IngestSignal(signal string) SignalConfig {
	switch signal {
	case "spans":
		return c.Ingestion.Spans
	case "logs":
		return c.Ingestion.Logs
	case "metrics":
		return c.Ingestion.Metrics
	case "metric_series":
		return c.Ingestion.MetricSeries
	case "ingestion_stats":
		return c.Ingestion.IngestionStats
	case "llm_scores":
		return c.Ingestion.LLMScores
	}
	return SignalConfig{}
}

func (c Config) SidePublishQueueSize() int { return c.Ingestion.SidePublish.QueueSize }

func (c Config) SidePublishWorkers() int { return c.Ingestion.SidePublish.Workers }

// Capacity (entries) of the cross-request series-dedup cache.
func (c Config) ResourceCacheSize() int { return c.Ingestion.ResourceCacheSize }

// How often an active series' metadata row is republished despite dedup.
func (c Config) SeriesRepublishInterval() time.Duration {
	return time.Duration(c.Ingestion.SeriesRepublishIntervalSeconds) * time.Second
}

func (c Config) APIKeyCacheTTL() time.Duration {
	return time.Duration(c.Ingestion.APIKeyCacheTTLSeconds) * time.Second
}

func (c Config) APIKeyCacheSize() int { return c.Ingestion.APIKeyCacheSize }

func (c Config) StatsFlushInterval() time.Duration {
	return time.Duration(c.Ingestion.StatsFlushIntervalSeconds) * time.Second
}

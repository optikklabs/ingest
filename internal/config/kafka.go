package config

import (
	"strings"
)

// KafkaConfig configures the Kafka-backed OTLP ingest queue.
// Signal topologies live in IngestionConfig; this holds connectivity & tuning.
type KafkaConfig struct {
	BrokerList string   `yaml:"broker_list"`
	Brokers    []string `yaml:"brokers"`

	TopicPrefix string `yaml:"topic_prefix"`

	DLQPrefix string `yaml:"dlq_prefix"`

	Compression string `yaml:"compression"`

	LingerMs int `yaml:"linger_ms"`

	BatchMaxBytes int `yaml:"batch_max_bytes"`
}

func (c Config) KafkaBrokers() []string {
	if c.Kafka.BrokerList != "" {
		return strings.Split(c.Kafka.BrokerList, ",")
	}
	return c.Kafka.Brokers
}

func (c Config) KafkaTopicPrefix() string {
	if c.Kafka.TopicPrefix != "" {
		return c.Kafka.TopicPrefix
	}
	return "optikk.ingest"
}

func (c Config) KafkaDLQPrefix() string {
	if c.Kafka.DLQPrefix != "" {
		return c.Kafka.DLQPrefix
	}
	return "optikk.dlq"
}

func (c Config) KafkaCompression() string {
	if s := strings.ToLower(c.Kafka.Compression); s != "" {
		return s
	}
	return "zstd"
}

func (c Config) KafkaLingerMs() int {
	if n := c.Kafka.LingerMs; n > 0 {
		return n
	}
	return 20
}

func (c Config) KafkaBatchMaxBytes() int {
	if n := c.Kafka.BatchMaxBytes; n > 0 {
		return n
	}
	return 1 << 20
}

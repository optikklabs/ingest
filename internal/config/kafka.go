package config

import (
	"fmt"
	"strings"
)

// KafkaConfig configures the Kafka-backed OTLP ingest queue.
// Signal topologies live in IngestionConfig; this holds connectivity & tuning.
type KafkaConfig struct {
	// BrokerList is a list of host:port brokers (takes precedence).
	// Env equivalent: OPTIKK_KAFKA_BROKER_LIST.
	BrokerList string   `yaml:"broker_list"`
	Brokers    []string `yaml:"brokers"`

	// TopicPrefix is the ingest-topic prefix (default "optikk.ingest").
	TopicPrefix string `yaml:"topic_prefix"`
	// DLQPrefix is the DLQ-topic prefix (default "optikk.dlq").
	DLQPrefix string `yaml:"dlq_prefix"`

	// Producer-side batching knobs (kgo).
	// Compression is the codec to use, default "zstd".
	Compression string `yaml:"compression"`
	// LingerMs is the producer linger duration in ms, default 20.
	LingerMs int `yaml:"linger_ms"`
	// BatchMaxBytes is the maximum batch size, default 1 MiB.
	BatchMaxBytes int `yaml:"batch_max_bytes"`
}

func (c Config) validateKafkaIngestion() error {
	if len(c.KafkaBrokers()) == 0 {
		return fmt.Errorf("kafka: at least one broker required for OTLP ingest (set kafka.broker_list or kafka.brokers, or OPTIKK_KAFKA_BROKER_LIST)")
	}
	return nil
}

// KafkaBrokers returns broker addresses from BrokerList when set,
// otherwise from Brokers.
func (c Config) KafkaBrokers() []string {
	if s := strings.TrimSpace(c.Kafka.BrokerList); s != "" {
		var out []string
		for _, p := range strings.Split(s, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return c.Kafka.Brokers
}

// KafkaTopicPrefix returns the ingest topic prefix (default "optikk.ingest").
func (c Config) KafkaTopicPrefix() string {
	if s := strings.TrimSpace(c.Kafka.TopicPrefix); s != "" {
		return s
	}
	return "optikk.ingest"
}

// KafkaDLQPrefix returns the DLQ topic prefix (default "optikk.dlq").
func (c Config) KafkaDLQPrefix() string {
	if s := strings.TrimSpace(c.Kafka.DLQPrefix); s != "" {
		return s
	}
	return "optikk.dlq"
}

// KafkaCompression returns the batch compression codec (default zstd).
func (c Config) KafkaCompression() string {
	if s := strings.TrimSpace(strings.ToLower(c.Kafka.Compression)); s != "" {
		return s
	}
	return "zstd"
}

// KafkaLingerMs returns the producer linger in milliseconds (default 20).
func (c Config) KafkaLingerMs() int {
	if n := c.Kafka.LingerMs; n > 0 {
		return n
	}
	return 20
}

// KafkaBatchMaxBytes returns the producer batch max bytes (default 1 MiB).
func (c Config) KafkaBatchMaxBytes() int {
	if n := c.Kafka.BatchMaxBytes; n > 0 {
		return n
	}
	return 1 << 20
}

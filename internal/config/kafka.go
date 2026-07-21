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

	FetchMaxBytes int `yaml:"fetch_max_bytes"`

	FetchMaxPartitionBytes int `yaml:"fetch_max_partition_bytes"`

	ConsumerMaxPollRecords int `yaml:"consumer_max_poll_records"`

	ConsumerMaxRetries int `yaml:"consumer_max_retries"`

	ConsumerInsertWorkers int `yaml:"consumer_insert_workers"`
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

func (c Config) KafkaFetchMaxBytes() int {
	if n := c.Kafka.FetchMaxBytes; n > 0 {
		return n
	}
	return 8 << 20
}

func (c Config) KafkaFetchMaxPartitionBytes() int {
	if n := c.Kafka.FetchMaxPartitionBytes; n > 0 {
		return n
	}
	return 1 << 20
}

// KafkaConsumerMaxPollRecords bounds one Kafka poll and therefore one
// ClickHouse insert batch. Values <= 0 use the production default of 5,000.
func (c Config) KafkaConsumerMaxPollRecords() int {
	if n := c.Kafka.ConsumerMaxPollRecords; n > 0 {
		return n
	}
	return 5_000
}

// KafkaConsumerMaxRetries is how many times a failed ClickHouse insert is
// retried before the batch falls back to the DLQ. Values <= 0 use the
// default of 2.
func (c Config) KafkaConsumerMaxRetries() int {
	if n := c.Kafka.ConsumerMaxRetries; n > 0 {
		return n
	}
	return 2
}

// KafkaConsumerInsertWorkers controls how many parallel goroutines insert
// fetched batches into ClickHouse per signal. Values <= 0 default to 1.
func (c Config) KafkaConsumerInsertWorkers() int {
	if n := c.Kafka.ConsumerInsertWorkers; n > 0 {
		return n
	}
	return 1
}

package config

import (
	"strings"
)

type KafkaConfig struct {
	BrokerList string   `yaml:"broker_list"`
	Brokers    []string `yaml:"brokers"`

	TopicPrefix string `yaml:"topic_prefix"`

	DLQPrefix string `yaml:"dlq_prefix"`

	// DLQ topics outlive ingest topics so operators can inspect and
	// replay failed records before they age out.
	DLQRetentionHours int `yaml:"dlq_retention_hours"`

	Compression string `yaml:"compression"`

	LingerMs int `yaml:"linger_ms"`

	BatchMaxBytes int `yaml:"batch_max_bytes"`

	FetchMaxBytes int `yaml:"fetch_max_bytes"`

	FetchMaxPartitionBytes int `yaml:"fetch_max_partition_bytes"`

	ConsumerMaxPollRecords int `yaml:"consumer_max_poll_records"`

	ConsumerInsertWorkers int `yaml:"consumer_insert_workers"`
}

func (c Config) KafkaBrokers() []string {
	if c.Kafka.BrokerList != "" {
		return strings.Split(c.Kafka.BrokerList, ",")
	}
	return c.Kafka.Brokers
}

func (c Config) KafkaTopicPrefix() string { return c.Kafka.TopicPrefix }

func (c Config) KafkaDLQPrefix() string { return c.Kafka.DLQPrefix }

func (c Config) KafkaDLQRetentionHours() int { return c.Kafka.DLQRetentionHours }

func (c Config) KafkaCompression() string { return strings.ToLower(c.Kafka.Compression) }

func (c Config) KafkaLingerMs() int { return c.Kafka.LingerMs }

func (c Config) KafkaBatchMaxBytes() int { return c.Kafka.BatchMaxBytes }

func (c Config) KafkaFetchMaxBytes() int { return c.Kafka.FetchMaxBytes }

func (c Config) KafkaFetchMaxPartitionBytes() int { return c.Kafka.FetchMaxPartitionBytes }

func (c Config) KafkaConsumerMaxPollRecords() int { return c.Kafka.ConsumerMaxPollRecords }

func (c Config) KafkaConsumerInsertWorkers() int { return c.Kafka.ConsumerInsertWorkers }

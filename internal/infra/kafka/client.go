// Package kafka provides shared Kafka client helpers for producers and
// consumers. Per-signal configs are declared next to their handlers.
package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers []string

	LingerMs int

	BatchMaxBytes int

	Compression string
}

func NewProducerClient(cfg Config) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(linger(cfg)),
		kgo.ProducerBatchMaxBytes(batchMax(cfg)),
		kgo.MaxBufferedRecords(1<<18),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compression(cfg.Compression)),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		WithHooks(),
	)
}

func NewConsumerClient(cfg Config, groupID, topic string) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers required")
	}
	if groupID == "" || topic == "" {
		return nil, fmt.Errorf("kafka: group and topic required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(2*time.Second),
		WithHooks(),
	)
}

func linger(cfg Config) time.Duration {
	if cfg.LingerMs > 0 {
		return time.Duration(cfg.LingerMs) * time.Millisecond
	}
	return 20 * time.Millisecond
}

func batchMax(cfg Config) int32 {
	if cfg.BatchMaxBytes > 0 {
		return int32(cfg.BatchMaxBytes)
	}
	return 1 << 20
}

func compression(name string) kgo.CompressionCodec {
	switch name {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	default:
		return kgo.ZstdCompression()
	}
}

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Signal name constants used by topic naming and the observability hooks.
const (
	SignalSpans           = "spans"
	SignalSpansTracegraph = "spans_tracegraph"
	SignalSpansResource   = "spans_resource"
	SignalLogs            = "logs"
	SignalLogsResource    = "logs_resource"
	SignalMetrics         = "metrics"
	SignalMetricSeries    = "metric_series"
	SignalIngestionStats  = "ingestion_stats"
)

type TopicSpec struct {
	Name           string
	Partitions     int32
	Replicas       int16
	RetentionHours int
}

func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	cli, err := NewProducerClient(Config{Brokers: brokers})
	if err != nil {
		return fmt.Errorf("kafka ensure topics: client: %w", err)
	}
	defer cli.Close()
	adm := kadm.NewClient(cli)
	for _, s := range specs {
		if s.Name == "" || s.Partitions <= 0 || s.Replicas <= 0 {
			return fmt.Errorf("kafka ensure topics: invalid spec %+v", s)
		}
		cfg := map[string]*string{}
		if s.RetentionHours > 0 {
			ms := strconv.FormatInt(int64(s.RetentionHours)*3600*1000, 10)
			cfg["retention.ms"] = &ms
		}
		resp, err := adm.CreateTopics(ctx, s.Partitions, s.Replicas, cfg, s.Name)
		if err != nil && !isTopicExists(err) {
			return fmt.Errorf("kafka ensure topics: create %q: %w", s.Name, err)
		}
		for _, r := range resp {
			if r.Err != nil && !isTopicExists(r.Err) {
				return fmt.Errorf("kafka ensure topics: create %q: %w", r.Topic, r.Err)
			}
		}
		// CreateTopics no-ops on an existing topic, so grow partitions
		// separately to keep the desired count declarative and idempotent.
		if err := EnsureTopicPartitions(ctx, adm, s.Name, s.Partitions); err != nil {
			return fmt.Errorf("kafka ensure topics: partitions %q: %w", s.Name, err)
		}
		slog.Info("kafka topic ready",
			slog.String("topic", s.Name),
			slog.Int("partitions", int(s.Partitions)),
			slog.Int("replicas", int(s.Replicas)),
			slog.Int("retention_hours", s.RetentionHours),
		)
	}
	return nil
}

// partitionAction is how a topic's live partition count reconciles to target.
type partitionAction int

const (
	partitionNoop       partitionAction = iota // already at target
	partitionGrow                              // below target, safe to grow
	partitionShrinkSkip                        // above target, Kafka forbids shrink
)

// decidePartitionAction reconciles current→target. Kafka never shrinks
// partitions, so a higher current count is skipped with a warning upstream.
func decidePartitionAction(current, target int32) partitionAction {
	switch {
	case current < target:
		return partitionGrow
	case current > target:
		return partitionShrinkSkip
	default:
		return partitionNoop
	}
}

// EnsureTopicPartitions grows an existing topic to target partitions. It never
// shrinks (Kafka forbids it) and no-ops when already at or above target.
func EnsureTopicPartitions(ctx context.Context, adm *kadm.Client, topic string, target int32) error {
	td, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("list topic %q: %w", topic, err)
	}
	detail, ok := td[topic]
	if !ok || detail.Err != nil {
		return fmt.Errorf("topic %q metadata unavailable: %w", topic, detail.Err)
	}
	current := int32(len(detail.Partitions))

	switch decidePartitionAction(current, target) {
	case partitionNoop:
		return nil
	case partitionShrinkSkip:
		slog.Warn("kafka topic has more partitions than desired, skipping shrink",
			slog.String("topic", topic),
			slog.Int("current", int(current)),
			slog.Int("desired", int(target)),
		)
		return nil
	case partitionGrow:
		resp, err := adm.UpdatePartitions(ctx, int(target), topic)
		if err != nil {
			return fmt.Errorf("update partitions %q: %w", topic, err)
		}
		if err := resp.Error(); err != nil {
			return fmt.Errorf("update partitions %q: %w", topic, err)
		}
		slog.Info("kafka topic partitions grown",
			slog.String("topic", topic),
			slog.Int("old", int(current)),
			slog.Int("new", int(target)),
		)
		return nil
	}
	return nil
}

func isTopicExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return true
	}

	return strings.Contains(strings.ToLower(err.Error()), "topic already exists")
}

func IngestTopic(prefix, signal string) string { return prefix + "." + signal }

func DLQTopic(dlqPrefix, signal string) string { return dlqPrefix + "." + signal }

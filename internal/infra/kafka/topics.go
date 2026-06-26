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
	SignalSpans        = "spans"
	SignalLogs         = "logs"
	SignalMetrics      = "metrics"
	SignalMetricSeries = "metric_series"
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
		slog.Info("kafka topic ready",
			slog.String("topic", s.Name),
			slog.Int("partitions", int(s.Partitions)),
			slog.Int("replicas", int(s.Replicas)),
			slog.Int("retention_hours", s.RetentionHours),
		)
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

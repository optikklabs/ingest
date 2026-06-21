package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hooks implements the kgo.Hook* interfaces we instrument: produce/fetch
// counters, broker connect counter, group rebalance error counter.
type hooks struct{}

var (
	_ kgo.HookProduceRecordUnbuffered = (*hooks)(nil)
	_ kgo.HookFetchRecordUnbuffered   = (*hooks)(nil)
	_ kgo.HookBrokerConnect           = (*hooks)(nil)
	_ kgo.HookGroupManageError        = (*hooks)(nil)
)

// WithHooks returns the kgo option that installs the package-level hooks.
func WithHooks() kgo.Opt { return kgo.WithHooks(hooks{}) }

func (hooks) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	signal := signalFromTopic(r.Topic)
	result := "ok"
	if err != nil {
		result = "err"
		slog.Debug("kafka produce failed",
			slog.String("topic", r.Topic),
			slog.Any("error", err),
		)
	}
	metrics.IngestRecordsTotal.WithLabelValues(signal, result).Inc()
	metrics.IngestRecordBytes.WithLabelValues(signal).Add(float64(len(r.Value)))
	if !r.Timestamp.IsZero() {
		metrics.KafkaProduceDuration.WithLabelValues(r.Topic).Observe(time.Since(r.Timestamp).Seconds())
	}
}

func (hooks) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	signal := signalFromTopic(r.Topic)
	metrics.IngestRecordsTotal.WithLabelValues(signal, "ok").Inc()
}

func (hooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	host := net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port)))
	result := "ok"
	if err != nil {
		result = "err"
		slog.Warn("kafka broker connect failed",
			slog.String("host", host),
			slog.Any("error", err),
		)
	}
	metrics.KafkaBrokerConnects.WithLabelValues(result).Inc()
}

func (hooks) OnGroupManageError(err error) {
	if err == nil {
		return
	}
	metrics.KafkaRebalances.WithLabelValues("manage_error").Inc()
	slog.Warn("kafka group manage error", slog.Any("error", err))
}

func signalFromTopic(topic string) string {
	switch {
	case strings.HasSuffix(topic, "."+SignalSpans):
		return SignalSpans
	case strings.HasSuffix(topic, "."+SignalLogs):
		return SignalLogs
	case strings.HasSuffix(topic, "."+SignalMetrics):
		return SignalMetrics
	case strings.HasSuffix(topic, "."+SignalMetricSeries):
		return SignalMetricSeries
	default:
		return "unknown"
	}
}

// LagPoller emits optikk_kafka_consumer_lag_records every 15s. One per
// (consumer client, group, topic) pair, started by app.Start.
type LagPoller struct {
	client   *kgo.Client
	groupID  string
	topic    string
	interval time.Duration
}

func NewLagPoller(client *kgo.Client, groupID, topic string) *LagPoller {
	return &LagPoller{client: client, groupID: groupID, topic: topic, interval: 15 * time.Second}
}

func (p *LagPoller) Run(ctx context.Context) {
	tick := time.NewTicker(p.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.sample(ctx)
		}
	}
}

func (p *LagPoller) sample(ctx context.Context) {
	lag, err := fetchGroupLag(ctx, p.client, p.groupID, p.topic)
	if err != nil {
		slog.DebugContext(ctx, "kafka lag poll failed",
			slog.String("group", p.groupID),
			slog.String("topic", p.topic),
			slog.Any("error", err),
		)
		return
	}
	for k, delta := range lag {
		metrics.KafkaConsumerLag.
			WithLabelValues(k.topic, strconv.Itoa(int(k.partition)), p.groupID).
			Set(float64(delta))
	}
}

type partKey struct {
	topic     string
	partition int32
}

func fetchGroupLag(ctx context.Context, client *kgo.Client, groupID, topic string) (map[partKey]int64, error) {
	partitions, err := fetchPartitions(ctx, client, topic)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	if len(partitions) == 0 {
		return nil, nil
	}
	committed, err := fetchCommittedOffsets(ctx, client, groupID, topic, partitions)
	if err != nil {
		return nil, fmt.Errorf("offset-fetch: %w", err)
	}
	end, err := fetchEndOffsets(ctx, client, topic, partitions)
	if err != nil {
		return nil, fmt.Errorf("list-offsets: %w", err)
	}
	return diffLag(topic, partitions, committed, end), nil
}

func fetchPartitions(ctx context.Context, client *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	t := kmsg.NewMetadataRequestTopic()
	t.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	for _, tr := range resp.Topics {
		if tr.Topic == nil || *tr.Topic != topic {
			continue
		}
		out := make([]int32, 0, len(tr.Partitions))
		for _, pr := range tr.Partitions {
			out = append(out, pr.Partition)
		}
		return out, nil
	}
	return nil, nil
}

func fetchCommittedOffsets(ctx context.Context, client *kgo.Client, groupID, topic string, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = groupID
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = append(rt.Partitions, partitions...)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(partitions))
	for _, tr := range resp.Topics {
		if tr.Topic != topic {
			continue
		}
		for _, pr := range tr.Partitions {
			out[pr.Partition] = pr.Offset
		}
	}
	return out, nil
}

func fetchEndOffsets(ctx context.Context, client *kgo.Client, topic string, partitions []int32) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, pid := range partitions {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = pid
		rp.Timestamp = -1
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(partitions))
	for _, tr := range resp.Topics {
		if tr.Topic != topic {
			continue
		}
		for _, pr := range tr.Partitions {
			out[pr.Partition] = pr.Offset
		}
	}
	return out, nil
}

func diffLag(topic string, partitions []int32, committed, end map[int32]int64) map[partKey]int64 {
	out := make(map[partKey]int64, len(partitions))
	for _, p := range partitions {
		endOff, ok := end[p]
		if !ok {
			continue
		}
		commOff := committed[p]
		lag := endOff - commOff
		if commOff < 0 {
			lag = endOff
		}
		if lag < 0 {
			lag = 0
		}
		out[partKey{topic: topic, partition: p}] = lag
	}
	return out
}

package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/config"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	ingestionstats "github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	statsschema "github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
	llmscores "github.com/optikklabs/ingest/internal/ingestion/llmscores"
	llmscoresschema "github.com/optikklabs/ingest/internal/ingestion/llmscores/schema"
	logsignal "github.com/optikklabs/ingest/internal/ingestion/logs"
	logsschema "github.com/optikklabs/ingest/internal/ingestion/logs/schema"

	metricsignal "github.com/optikklabs/ingest/internal/ingestion/metrics"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseries "github.com/optikklabs/ingest/internal/ingestion/metricseries"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	spansignal "github.com/optikklabs/ingest/internal/ingestion/spans"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type ingestBundle struct {
	modules         []registry.Module
	producerClient  *kgo.Client
	consumerClients []*kgo.Client
	lagPollers      []*kafkainfra.LagPoller
	consumers       []ConsumerRunner
	closers         []func()
}

type signalWiring struct {
	signal string
	cfg    config.SignalConfig
	wire   func(signalWireInput) (registry.Module, ConsumerRunner)
}

type signalWireInput struct {
	signal                string
	topicPrefix           string
	ingestTopic, dlqTopic string
	producerBase          *kafkainfra.Producer
	consumer              *kafkainfra.Consumer
	ch                    clickhouse.Conn
	insertMaxRetries      int
	sidePublishQueueSize  int
	sidePublishWorkers    int
	stats                 ingestionstats.Recorder
	registerCloser        func(func())
}

func newStatsProducer(in signalWireInput) *core.Producer[*statsschema.StatRow] {
	topic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalIngestionStats)
	return core.NewProducer[*statsschema.StatRow](topic, in.producerBase).
		WithKeyFunc(func(r *statsschema.StatRow) []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, r.GetTenantId())
			return b
		})
}

func wireSpans(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*spansschema.Row](in.ingestTopic, in.producerBase)

	scoresTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalLLMScores)
	scoresProducer := core.NewProducer[*llmscoresschema.ScoreRow](scoresTopic, in.producerBase).WithKeyFunc(func(r *llmscoresschema.ScoreRow) []byte {
		return []byte(r.GetTraceId())
	})

	scoresPublisher := core.NewAsyncPublisher[*llmscoresschema.ScoreRow](scoresProducer, kafkainfra.SignalSpans, kafkainfra.SignalLLMScores, in.sidePublishQueueSize, in.sidePublishWorkers)
	in.registerCloser(scoresPublisher.Close)

	writer := core.NewRetryWriter(spansignal.NewClickHouseWriter(in.ch), kafkainfra.SignalSpans, in.insertMaxRetries)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalSpans)
	consumer := core.NewInsertConsumer(in.consumer, kafkainfra.SignalSpans, writer, dlq, func() *spansschema.Row { return &spansschema.Row{} })

	handler := spansignal.NewHandler(producer, scoresPublisher, in.stats)
	mod := spansignal.NewModule(spansignal.Deps{Handler: handler})
	return mod, consumer
}

func wireLogs(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*logsschema.Row](in.ingestTopic, in.producerBase)

	dataWriter := core.NewRetryWriter(logsignal.NewDataWriter(in.ch), kafkainfra.SignalLogs, in.insertMaxRetries)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalLogs)
	consumer := core.NewInsertConsumer(in.consumer, kafkainfra.SignalLogs, dataWriter, dlq, func() *logsschema.Row { return &logsschema.Row{} })

	handler := logsignal.NewHandler(producer, in.stats)
	mod := logsignal.NewModule(logsignal.Deps{Handler: handler})
	return mod, consumer
}

func wireMetrics(in signalWireInput) (registry.Module, ConsumerRunner) {
	metricsProducer := core.NewProducer[*metricsschema.Row](in.ingestTopic, in.producerBase)
	seriesTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetricSeries)
	seriesProducer := core.NewProducer[*metricseriesschema.SeriesRow](seriesTopic, in.producerBase)

	writer := core.NewRetryWriter(metricsignal.NewMetricsClickHouseWriter(in.ch), kafkainfra.SignalMetrics, in.insertMaxRetries)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalMetrics)
	consumer := core.NewInsertConsumer(in.consumer, kafkainfra.SignalMetrics, writer, dlq, func() *metricsschema.Row { return &metricsschema.Row{} })
	mod := metricsignal.NewModule(metricsignal.Deps{
		Handler: metricsignal.NewHandler(metricsProducer, seriesProducer, in.stats),
	})
	return mod, consumer
}

func insertOnly[T core.Row](
	newWriter func(clickhouse.Conn) core.Writer[T],
	newRow func() T,
) func(signalWireInput) (registry.Module, ConsumerRunner) {
	return func(in signalWireInput) (registry.Module, ConsumerRunner) {
		writer := core.NewRetryWriter(newWriter(in.ch), in.signal, in.insertMaxRetries)
		dlq := core.NewDLQ(in.producerBase, in.dlqTopic, in.signal)
		return nil, core.NewInsertConsumer(in.consumer, in.signal, writer, dlq, newRow)
	}
}

func newRow[T any]() *T { return new(T) }

func ingestTopicSpecs(wirings []signalWiring, topicPrefix, dlqPrefix string) []kafkainfra.TopicSpec {
	specs := make([]kafkainfra.TopicSpec, 0, len(wirings)*2)
	seen := make(map[string]struct{}, len(wirings)*2)
	for _, w := range wirings {
		for _, name := range []string{kafkainfra.IngestTopic(topicPrefix, w.signal), kafkainfra.DLQTopic(dlqPrefix, w.signal)} {
			if _, exists := seen[name]; exists {
				continue
			}
			seen[name] = struct{}{}
			specs = append(specs, kafkainfra.TopicSpec{Name: name, Partitions: int32(w.cfg.Partitions), Replicas: int16(w.cfg.Replicas), RetentionHours: w.cfg.RetentionHours})
		}
	}
	return specs
}

func buildIngest(cfg config.Config, ch clickhouse.Conn) (ingestBundle, error) {
	brokers := cfg.KafkaBrokers()
	topicPrefix := cfg.KafkaTopicPrefix()
	dlqPrefix := cfg.KafkaDLQPrefix()

	wirings := []signalWiring{
		{signal: kafkainfra.SignalSpans, cfg: cfg.IngestSignal("spans"), wire: wireSpans},
		{signal: kafkainfra.SignalLogs, cfg: cfg.IngestSignal("logs"), wire: wireLogs},

		{signal: kafkainfra.SignalMetrics, cfg: cfg.IngestSignal("metrics"), wire: wireMetrics},
		{signal: kafkainfra.SignalMetricSeries, cfg: cfg.IngestSignal("metric_series"), wire: insertOnly(metricseries.NewClickHouseWriter, newRow[metricseriesschema.SeriesRow])},
		{signal: kafkainfra.SignalIngestionStats, cfg: cfg.IngestSignal("ingestion_stats"), wire: insertOnly(ingestionstats.NewClickHouseWriter, newRow[statsschema.StatRow])},
		{signal: kafkainfra.SignalLLMScores, cfg: cfg.IngestSignal("llm_scores"), wire: insertOnly(llmscores.NewClickHouseWriter, newRow[llmscoresschema.ScoreRow])},
	}

	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kafkainfra.EnsureTopics(ensureCtx, brokers, ingestTopicSpecs(wirings, topicPrefix, dlqPrefix)); err != nil {
		return ingestBundle{}, err
	}

	kcfg := kafkainfra.Config{
		Brokers:                brokers,
		LingerMs:               cfg.KafkaLingerMs(),
		BatchMaxBytes:          cfg.KafkaBatchMaxBytes(),
		FetchMaxBytes:          cfg.KafkaFetchMaxBytes(),
		FetchMaxPartitionBytes: cfg.KafkaFetchMaxPartitionBytes(),
		Compression:            cfg.KafkaCompression(),
	}

	producerClient, err := kafkainfra.NewProducerClient(kcfg)
	if err != nil {
		return ingestBundle{}, fmt.Errorf("kafka producer client: %w", err)
	}
	slog.Info("kafka producer client connected", slog.Any("brokers", brokers))
	producerBase := kafkainfra.NewProducer(producerClient)
	stats := ingestionstats.NewHourlyRecorder(newStatsProducer(signalWireInput{topicPrefix: topicPrefix, producerBase: producerBase}))

	b := ingestBundle{
		producerClient:  producerClient,
		modules:         make([]registry.Module, 0, len(wirings)),
		consumerClients: make([]*kgo.Client, 0, len(wirings)),
		lagPollers:      make([]*kafkainfra.LagPoller, 0, len(wirings)),
		consumers:       make([]ConsumerRunner, 0, len(wirings)),
	}
	b.closers = append(b.closers, stats.Close)
	closeOnErr := func() {
		for _, c := range b.consumerClients {
			c.Close()
		}
		stats.Close()
		producerClient.Close()
	}

	for _, w := range wirings {
		ingestTopic := kafkainfra.IngestTopic(topicPrefix, w.signal)
		client, err := kafkainfra.NewConsumerClient(kcfg, w.cfg.ConsumerGroup, ingestTopic)
		if err != nil {
			closeOnErr()
			return ingestBundle{}, fmt.Errorf("kafka %s consumer: %w", w.signal, err)
		}
		b.consumerClients = append(b.consumerClients, client)
		b.lagPollers = append(b.lagPollers, kafkainfra.NewLagPoller(client, w.cfg.ConsumerGroup, ingestTopic))

		mod, consumer := w.wire(signalWireInput{
			signal:               w.signal,
			topicPrefix:          topicPrefix,
			ingestTopic:          ingestTopic,
			dlqTopic:             kafkainfra.DLQTopic(dlqPrefix, w.signal),
			producerBase:         producerBase,
			consumer:             kafkainfra.NewConsumer(client, cfg.KafkaConsumerMaxPollRecords(), cfg.KafkaConsumerInsertWorkers(), w.signal),
			ch:                   ch,
			insertMaxRetries:     cfg.KafkaConsumerMaxRetries(),
			sidePublishQueueSize: cfg.SidePublishQueueSize(),
			sidePublishWorkers:   cfg.SidePublishWorkers(),
			registerCloser:       func(f func()) { b.closers = append(b.closers, f) },
			stats:                stats,
		})
		if mod != nil {
			b.modules = append(b.modules, mod)
		}
		b.consumers = append(b.consumers, consumer)
	}

	return b, nil
}

package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/config"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
	"github.com/optikklabs/ingest/internal/ingestion/core"
	logsignal "github.com/optikklabs/ingest/internal/ingestion/logs"
	logsschema "github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logsresource "github.com/optikklabs/ingest/internal/ingestion/logsresource"
	logsresourceschema "github.com/optikklabs/ingest/internal/ingestion/logsresource/schema"

	metricsignal "github.com/optikklabs/ingest/internal/ingestion/metrics"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	metricseries "github.com/optikklabs/ingest/internal/ingestion/metricseries"
	metricseriesschema "github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
	spansignal "github.com/optikklabs/ingest/internal/ingestion/spans"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	spansresource "github.com/optikklabs/ingest/internal/ingestion/spansresource"
	spansresourceschema "github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/servicegraph"
	"github.com/optikklabs/ingest/internal/modules/spanaggregator/spanmetrics"
)

// ingestBundle is everything buildIngest produces for the Infra.
type ingestBundle struct {
	modules         []registry.Module
	producerClient  *kgo.Client
	consumerClients []*kgo.Client
	lagPollers      []*kafkainfra.LagPoller
	consumers       []ConsumerRunner
}

type signalWiring struct {
	signal string
	cfg    config.SignalConfig
	wire   func(signalWireInput) (registry.Module, ConsumerRunner)
}

type signalWireInput struct {
	topicPrefix                  string
	ingestTopic, dlqTopic, group string
	sc                           config.SignalConfig
	producerBase                 *kafkainfra.Producer
	consumer                     *kafkainfra.Consumer
	ch                           clickhouse.Conn
}

var (
	spansResourceCache = core.NewResourceCache(100000)
	logsResourceCache  = core.NewResourceCache(100000)
)

func wireSpans(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*spansschema.Row](in.ingestTopic, in.producerBase)
	resourceTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalSpansResource)
	resourceProducer := core.NewProducer[*spansresourceschema.ResourceRow](resourceTopic, in.producerBase)

	tracegraphTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalSpansTracegraph)
	tracegraphProducer := core.NewProducer[*spansschema.Row](tracegraphTopic, in.producerBase).WithKeyFunc(func(r *spansschema.Row) []byte {
		return []byte(r.GetTraceId())
	})

	writer := spansignal.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalSpans)
	consumer := spansignal.NewConsumer(in.consumer, writer, dlq)
	
	handler := spansignal.NewHandler(producer, tracegraphProducer, resourceProducer, spansResourceCache)
	mod := spansignal.NewModule(spansignal.Deps{Handler: handler})
	return mod, consumer
}

func wireSpansResource(in signalWireInput) (registry.Module, ConsumerRunner) {
	writer := spansresource.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalSpansResource)
	consumer := spansresource.NewConsumer(in.consumer, writer, dlq)
	return nil, consumer
}

func wireLogs(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*logsschema.Row](in.ingestTopic, in.producerBase)
	resourceTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalLogsResource)
	resourceProducer := core.NewProducer[*logsresourceschema.ResourceRow](resourceTopic, in.producerBase)

	dataWriter := logsignal.NewDataWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalLogs)
	consumer := logsignal.NewConsumer(in.consumer, dataWriter, dlq)

	handler := logsignal.NewHandler(producer, resourceProducer, logsResourceCache)
	mod := logsignal.NewModule(logsignal.Deps{Handler: handler})
	return mod, consumer
}

func wireLogsResource(in signalWireInput) (registry.Module, ConsumerRunner) {
	writer := logsresource.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalLogsResource)
	consumer := logsresource.NewConsumer(in.consumer, writer, dlq)
	return nil, consumer
}

func wireMetrics(in signalWireInput) (registry.Module, ConsumerRunner) {
	metricsProducer := core.NewProducer[*metricsschema.Row](in.ingestTopic, in.producerBase)
	seriesTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetricSeries)
	seriesProducer := core.NewProducer[*metricseriesschema.SeriesRow](seriesTopic, in.producerBase)

	writer := metricsignal.NewMetricsClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalMetrics)
	consumer := metricsignal.NewConsumer(in.consumer, writer, dlq)
	mod := metricsignal.NewModule(metricsignal.Deps{
		Handler: metricsignal.NewHandler(metricsProducer, seriesProducer),
	})
	return mod, consumer
}

func wireMetricSeries(in signalWireInput) (registry.Module, ConsumerRunner) {
	writer := metricseries.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalMetricSeries)
	consumer := metricseries.NewConsumer(in.consumer, writer, dlq)
	return nil, consumer
}

func wireSpanmetrics(in signalWireInput) (registry.Module, ConsumerRunner) {
	metricsTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetrics)
	seriesTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetricSeries)
	metricsPub := core.NewProducer[*metricsschema.Row](metricsTopic, in.producerBase)
	seriesPub := core.NewProducer[*metricseriesschema.SeriesRow](seriesTopic, in.producerBase)

	consumer := spanmetrics.NewConsumer(in.consumer, metricsPub, seriesPub)
	return nil, consumer
}

func wireServicegraph(in signalWireInput) (registry.Module, ConsumerRunner) {
	metricsTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetrics)
	seriesTopic := kafkainfra.IngestTopic(in.topicPrefix, kafkainfra.SignalMetricSeries)
	metricsPub := core.NewProducer[*metricsschema.Row](metricsTopic, in.producerBase)
	seriesPub := core.NewProducer[*metricseriesschema.SeriesRow](seriesTopic, in.producerBase)

	consumer := servicegraph.NewConsumer(in.consumer, metricsPub, seriesPub)
	return nil, consumer
}

func ingestTopicSpecs(wirings []signalWiring, topicPrefix, dlqPrefix string) []kafkainfra.TopicSpec {
	specs := make([]kafkainfra.TopicSpec, 0, len(wirings)*2)
	for _, w := range wirings {
		specs = append(specs,
			kafkainfra.TopicSpec{Name: kafkainfra.IngestTopic(topicPrefix, w.signal), Partitions: int32(w.cfg.Partitions), Replicas: int16(w.cfg.Replicas), RetentionHours: w.cfg.RetentionHours},
			kafkainfra.TopicSpec{Name: kafkainfra.DLQTopic(dlqPrefix, w.signal), Partitions: int32(w.cfg.Partitions), Replicas: int16(w.cfg.Replicas), RetentionHours: w.cfg.RetentionHours},
		)
	}
	return specs
}

func buildIngest(cfg config.Config, ch clickhouse.Conn) (ingestBundle, error) {
	brokers := cfg.KafkaBrokers()
	topicPrefix := cfg.KafkaTopicPrefix()
	dlqPrefix := cfg.KafkaDLQPrefix()

	wirings := []signalWiring{
		{signal: kafkainfra.SignalSpans, cfg: cfg.IngestSignal("spans"), wire: wireSpans},
		{signal: kafkainfra.SignalSpansResource, cfg: cfg.IngestSignal("spans_resource"), wire: wireSpansResource},
		{signal: kafkainfra.SignalLogs, cfg: cfg.IngestSignal("logs"), wire: wireLogs},
		{signal: kafkainfra.SignalLogsResource, cfg: cfg.IngestSignal("logs_resource"), wire: wireLogsResource},

		{signal: kafkainfra.SignalMetrics, cfg: cfg.IngestSignal("metrics"), wire: wireMetrics},
		{signal: kafkainfra.SignalMetricSeries, cfg: cfg.IngestSignal("metric_series"), wire: wireMetricSeries},
		
		{signal: kafkainfra.SignalSpans, cfg: config.SignalConfig{
			Partitions:     cfg.IngestSignal("spans").Partitions,
			Replicas:       cfg.IngestSignal("spans").Replicas,
			RetentionHours: cfg.IngestSignal("spans").RetentionHours,
			ConsumerGroup:  "optikk-ingest.spanaggregator.spanmetrics.consumer",
		}, wire: wireSpanmetrics},
		
		{signal: kafkainfra.SignalSpansTracegraph, cfg: cfg.IngestSignal("spans_tracegraph"), wire: wireServicegraph},
	}

	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kafkainfra.EnsureTopics(ensureCtx, brokers, ingestTopicSpecs(wirings, topicPrefix, dlqPrefix)); err != nil {
		return ingestBundle{}, err
	}

	kcfg := kafkainfra.Config{
		Brokers:       brokers,
		LingerMs:      cfg.KafkaLingerMs(),
		BatchMaxBytes: cfg.KafkaBatchMaxBytes(),
		Compression:   cfg.KafkaCompression(),
	}

	producerClient, err := kafkainfra.NewProducerClient(kcfg)
	if err != nil {
		return ingestBundle{}, fmt.Errorf("kafka producer client: %w", err)
	}
	slog.Info("kafka producer client connected", slog.Any("brokers", brokers))
	producerBase := kafkainfra.NewProducer(producerClient)

	b := ingestBundle{
		producerClient:  producerClient,
		modules:         make([]registry.Module, 0, len(wirings)),
		consumerClients: make([]*kgo.Client, 0, len(wirings)),
		lagPollers:      make([]*kafkainfra.LagPoller, 0, len(wirings)),
		consumers:       make([]ConsumerRunner, 0, len(wirings)),
	}
	closeOnErr := func() {
		for _, c := range b.consumerClients {
			c.Close()
		}
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
			topicPrefix:  topicPrefix,
			ingestTopic:  ingestTopic,
			dlqTopic:     kafkainfra.DLQTopic(dlqPrefix, w.signal),
			group:        w.cfg.ConsumerGroup,
			sc:           w.cfg,
			producerBase: producerBase,
			consumer:     kafkainfra.NewConsumer(client),
			ch:           ch,
		})
		if mod != nil {
			b.modules = append(b.modules, mod)
		}
		b.consumers = append(b.consumers, consumer)
	}

	return b, nil
}

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
	metricsignal "github.com/optikklabs/ingest/internal/ingestion/metrics"
	metricsschema "github.com/optikklabs/ingest/internal/ingestion/metrics/schema"
	spansignal "github.com/optikklabs/ingest/internal/ingestion/spans"
	spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

// ingestBundle is everything buildIngest produces for the Infra.
type ingestBundle struct {
	modules         []registry.Module
	producerClient  *kgo.Client
	consumerClients []*kgo.Client
	lagPollers      []*kafkainfra.LagPoller
	consumers       []ConsumerRunner
}

// signalWiring binds a signal's identity and config to its package wiring.
type signalWiring struct {
	signal string
	cfg    config.SignalConfig
	wire   func(signalWireInput) (registry.Module, ConsumerRunner)
}

// signalWireInput carries the shared dependencies each wire func needs.
type signalWireInput struct {
	ingestTopic, dlqTopic, group string
	sc                           config.SignalConfig
	producerBase                 *kafkainfra.Producer
	consumer                     *kafkainfra.Consumer
	ch                           clickhouse.Conn
}

// wireSpans builds the spans module and consumer from shared deps.
func wireSpans(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*spansschema.Row](in.ingestTopic, in.producerBase)
	writer := spansignal.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalSpans)
	consumer := core.NewConsumer[*spansschema.Row](in.consumer, writer, dlq, kafkainfra.SignalSpans, func() *spansschema.Row { return &spansschema.Row{} })
	mod := spansignal.NewModule(spansignal.Deps{Handler: spansignal.NewHandler(producer)})
	return mod, consumer
}

// wireLogs builds the logs module and consumer from shared deps.
func wireLogs(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*logsschema.Row](in.ingestTopic, in.producerBase)
	writer := logsignal.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalLogs)
	consumer := core.NewConsumer[*logsschema.Row](in.consumer, writer, dlq, kafkainfra.SignalLogs, func() *logsschema.Row { return &logsschema.Row{} })
	mod := logsignal.NewModule(logsignal.Deps{Handler: logsignal.NewHandler(producer)})
	return mod, consumer
}

// wireMetrics builds the metrics module and consumer from shared deps.
func wireMetrics(in signalWireInput) (registry.Module, ConsumerRunner) {
	producer := core.NewProducer[*metricsschema.Row](in.ingestTopic, in.producerBase)
	writer := metricsignal.NewClickHouseWriter(in.ch)
	dlq := core.NewDLQ(in.producerBase, in.dlqTopic, kafkainfra.SignalMetrics)
	consumer := core.NewConsumer[*metricsschema.Row](in.consumer, writer, dlq, kafkainfra.SignalMetrics, func() *metricsschema.Row { return &metricsschema.Row{} })
	mod := metricsignal.NewModule(metricsignal.Deps{Handler: metricsignal.NewHandler(producer)})
	return mod, consumer
}

// ingestTopicSpecs derives the ingest + DLQ topic spec for each signal.
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

// buildIngest sets up Kafka topics, clients, lag pollers, and signal modules.
func buildIngest(cfg config.Config, ch clickhouse.Conn) (ingestBundle, error) {
	brokers := cfg.KafkaBrokers()
	topicPrefix := cfg.KafkaTopicPrefix()
	dlqPrefix := cfg.KafkaDLQPrefix()

	wirings := []signalWiring{
		{signal: kafkainfra.SignalSpans, cfg: cfg.IngestSignal("spans"), wire: wireSpans},
		{signal: kafkainfra.SignalLogs, cfg: cfg.IngestSignal("logs"), wire: wireLogs},
		{signal: kafkainfra.SignalMetrics, cfg: cfg.IngestSignal("metrics"), wire: wireMetrics},
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
			ingestTopic:  ingestTopic,
			dlqTopic:     kafkainfra.DLQTopic(dlqPrefix, w.signal),
			group:        w.cfg.ConsumerGroup,
			sc:           w.cfg,
			producerBase: producerBase,
			consumer:     kafkainfra.NewConsumer(client),
			ch:           ch,
		})
		b.modules = append(b.modules, mod)
		b.consumers = append(b.consumers, consumer)
	}

	return b, nil
}

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ingest counters measure records/sec received on OTLP endpoints.
// Signal label splits throughput by pipeline.
var (
	IngestRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "records_total",
		Help:      "Records published to Kafka by the ingest pipeline, by signal and result (ok/err).",
	}, []string{"signal", "result"})

	// ConsumedRecordsTotal counts fetch-side records separately from
	// records_total: publish success must never mask a stalled consumer.
	ConsumedRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumed_records_total",
		Help:      "Records fetched from Kafka by ingest consumers, by signal.",
	}, []string{"signal"})

	IngestRecordBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "record_bytes_total",
		Help:      "OTLP record payload bytes ingested, by signal.",
	}, []string{"signal"})

	HandlerPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "handler_publish_duration_seconds",
		Help:      "OTLP handler → Kafka PublishBatch latency, labeled signal + result (ok/err).",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"signal", "result"})

	MapperDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_duration_seconds",
		Help:      "MapRequest OTLP → internal Row wall-clock latency, by signal.",
		Buckets: []float64{
			0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
		},
	}, []string{"signal"})

	MapperRowsPerRequest = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_rows_per_request",
		Help:      "Rows produced by MapRequest per OTLP RPC, by signal.",
		Buckets:   []float64{1, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000},
	}, []string{"signal"})

	MapperAttrsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_attrs_dropped_total",
		Help:      "Attribute-map entries dropped by the per-record cap, by signal.",
	}, []string{"signal"})

	MapperUnsupportedType = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "mapper_unsupported_type_total",
		Help:      "Metric data points dropped because their OTLP type is unsupported, by signal.",
	}, []string{"signal"})

	InsertRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "insert_retries_total",
		Help:      "ClickHouse insert retry attempts after a failed write, by signal.",
	}, []string{"signal"})

	// CHInsertDuration isolates the ClickHouse batch.Send() latency
	// from the overall consumer batch duration, by signal and result.
	CHInsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "ch_insert_duration_seconds",
		Help:      "ClickHouse batch.Send() round-trip latency, by table and result (ok/err).",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"table", "result"})

	RecordsLost = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "records_lost_total",
		Help:      "Records dropped after both CH insert retries and the DLQ publish failed, by signal.",
	}, []string{"signal"})

	TracegraphRowsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "tracegraph_rows_published_total",
		Help:      "Span rows published to the tracegraph topic (client/server/producer/consumer kinds).",
	})

	TracegraphRowsFiltered = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "tracegraph_rows_filtered_total",
		Help:      "Span rows excluded from the tracegraph topic because their kind cannot form a service-graph edge.",
	})

	// ResourceCacheHits counts CheckAndUpdateBucket calls served without a
	// resource re-publish, by signal.
	ResourceCacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "resource_cache_hits_total",
		Help:      "Resource-cache lookups that matched an existing key+bucket, by signal.",
	}, []string{"signal"})

	// ResourceCacheMisses counts lookups that triggered a resource re-publish.
	ResourceCacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "resource_cache_misses_total",
		Help:      "Resource-cache lookups that were new or changed bucket, by signal.",
	}, []string{"signal"})

	// ResourceCacheEvictions counts LRU evictions, by signal.
	ResourceCacheEvictions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "resource_cache_evictions_total",
		Help:      "Resource-cache keys evicted by LRU capacity pressure, by signal.",
	}, []string{"signal"})

	// AggregatorKeys tracks live key cardinality in in-memory aggregators.
	AggregatorKeys = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "aggregator_keys",
		Help:      "Live key count held by an in-memory aggregator, by signal.",
	}, []string{"signal"})

	// AggregatorKeysDropped counts aggregator keys shed by a cardinality cap.
	AggregatorKeysDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "aggregator_keys_dropped_total",
		Help:      "Aggregator keys dropped after hitting a cardinality cap, by signal.",
	}, []string{"signal"})

	// ConsumerBatchInsertDuration measures per-batch handle latency (unmarshal
	// + ClickHouse insert + commit), by signal.
	ConsumerBatchInsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_batch_insert_duration_seconds",
		Help:      "Consumer batch handle latency (unmarshal + CH insert + commit), by signal.",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"signal"})

	// ConsumerInflightBatches tracks batches held between fetch and commit.
	ConsumerInflightBatches = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_inflight_batches",
		Help:      "Batches in flight between fetch and commit, by signal.",
	}, []string{"signal"})

	// SidePublishDropped counts best-effort side-topic rows dropped because the
	// async queue was full or the async publish failed, by signal and topic.
	SidePublishDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "sidepublish_dropped_total",
		Help:      "Best-effort side-topic publishes dropped (queue full or publish error), by signal and topic.",
	}, []string{"signal", "topic"})

	IngestionStatsPublishRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "ingestion_stats_publish_retries_total",
		Help:      "Hourly ingestion-stat snapshot publish failures retained for retry.",
	})

	IngestionStatsPublishDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "ingestion_stats_publish_dropped_total",
		Help:      "Hourly ingestion-stat rows discarded only after bounded shutdown retries.",
	})

	ServicegraphPendingSpans = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "servicegraph_pending_spans",
		Help:      "Unique unpaired spans currently retained for service-graph matching.",
	})

	ServicegraphPendingSpansDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "servicegraph_pending_spans_dropped_total",
		Help:      "Unique unpaired spans rejected because the service-graph pairing store is full.",
	})
)

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	IngestRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "records_total",
		Help:      "Records published to Kafka by the ingest pipeline, by signal and result (ok/err).",
	}, []string{"signal", "result"})

	OTLPRateLimitedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "otlp_rate_limited_total",
		Help:      "Total OTLP requests rate limited, labeled by signal.",
	}, []string{"signal"})

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

	CHInsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "ch_insert_duration_seconds",
		Help:      "ClickHouse batch.Send() round-trip latency, by table and result (ok/err).",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"table", "result"})

	DLQRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "dlq_records_total",
		Help:      "Records published to a DLQ topic, by signal and coarse reason class.",
	}, []string{"signal", "reason"})

	RecordsLost = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "records_lost_total",
		Help:      "Records dropped after both the CH insert and the DLQ publish failed, by signal.",
	}, []string{"signal"})

	MalformedRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "malformed_records_total",
		Help:      "Kafka records dropped because their protobuf failed to unmarshal, by signal.",
	}, []string{"signal"})

	ConsumerFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_fetch_errors_total",
		Help:      "Kafka fetch errors surfaced by the consume loop, by signal.",
	}, []string{"signal"})

	ConsumerBatchesDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_batches_dropped_total",
		Help:      "Batches whose handler failed; offsets commit anyway (accepted loss), by signal.",
	}, []string{"signal"})

	SeriesDedupHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "series_dedup_hits_total",
		Help:      "Series-dedup lookups that matched an existing key+bucket, by signal.",
	}, []string{"signal"})

	SeriesDedupMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "series_dedup_misses_total",
		Help:      "Series-dedup lookups that were new or changed bucket, by signal.",
	}, []string{"signal"})

	SeriesDedupEvictions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "series_dedup_evictions_total",
		Help:      "Series-dedup keys evicted by LRU capacity pressure, by signal.",
	}, []string{"signal"})

	ConsumerBatchInsertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_batch_insert_duration_seconds",
		Help:      "Consumer batch handle latency (unmarshal + CH insert + commit), by signal.",
		Buckets: []float64{
			0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}, []string{"signal"})

	ConsumerInflightBatches = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "consumer_inflight_batches",
		Help:      "Batches in flight between fetch and commit, by signal.",
	}, []string{"signal"})

	SidePublishDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "sidepublish_dropped_total",
		Help:      "Best-effort side-topic publishes dropped (queue full or publish error), by signal and topic.",
	}, []string{"signal", "topic"})

	IngestionStatsPublishDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "ingest",
		Name:      "ingestion_stats_publish_dropped_total",
		Help:      "Hourly ingestion-stat rows dropped after a failed publish.",
	})
)

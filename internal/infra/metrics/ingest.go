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
		Help:      "OTLP records accepted into the ingest pipeline, by signal (traces/logs/metrics) and result (ok/err).",
	}, []string{"signal", "result"})

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
)

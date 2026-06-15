package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// grpcBuckets are optimized for fast OTLP writes (sub-10ms) ranging from
// 0.5ms to 1s.
var grpcBuckets = []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1}

var (
	GRPCStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "started_total",
		Help:      "Total gRPC calls started, by full-method path.",
	}, []string{"method"})

	GRPCHandled = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "handled_total",
		Help:      "Total gRPC calls completed, by full-method and response code.",
	}, []string{"method", "code"})

	GRPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "grpc",
		Name:      "handling_duration_seconds",
		Help:      "gRPC call handling duration in seconds, by full-method.",
		Buckets:   grpcBuckets,
	}, []string{"method"})
)

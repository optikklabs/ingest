package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Kafka produce buckets: tighter low end than HTTP.
// Consumer lag is a gauge; no histogram needed.
var kafkaProduceBuckets = []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5}

var (
	KafkaProduceDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "kafka",
		Name:      "produce_duration_seconds",
		Help:      "Kafka produce-record latency in seconds, by topic.",
		Buckets:   kafkaProduceBuckets,
	}, []string{"topic"})

	KafkaConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "optikk",
		Subsystem: "kafka",
		Name:      "consumer_lag_records",
		Help:      "Consumer-group lag in records per (topic, partition, group). Updated by a background poller.",
	}, []string{"topic", "partition", "group"})

	KafkaRebalances = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "kafka",
		Name:      "rebalances_total",
		Help:      "Consumer-group rebalance events, by reason.",
	}, []string{"reason"})

	KafkaBrokerConnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "kafka",
		Name:      "broker_connects_total",
		Help:      "Kafka broker connection events, by outcome (ok/err).",
	}, []string{"result"})
)

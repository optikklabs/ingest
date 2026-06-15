package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// dbBuckets provides histogram buckets from 1ms to 1s for database queries.
var dbBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1}

var (
	DBQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "db",
		Name:      "query_duration_seconds",
		Help:      "DB query duration in seconds, by engine (clickhouse/mysql) and operation label.",
		Buckets:   dbBuckets,
	}, []string{"system", "op"})

	DBQueriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "db",
		Name:      "queries_total",
		Help:      "Total DB queries executed, by engine, operation, and result (ok/err).",
	}, []string{"system", "op", "result"})
)

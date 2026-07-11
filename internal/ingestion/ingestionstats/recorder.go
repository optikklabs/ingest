// Package ingestionstats owns the ingestion usage meter: the per-hour record
// and byte counts the OTLP handlers emit, published to Kafka and summed into
// optikk.ingestion_stats. It rides the same producer/consumer/writer path as
// every other signal so a slow ClickHouse never back-pressures ingestion.
package ingestionstats

import (
	"context"
	"log/slog"
	"time"

	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

// Signal names the telemetry type a StatRow counts. Defined once here so the
// column value is never stringly-typed across the three handlers.
type Signal string

const (
	SignalLogs    Signal = "logs"
	SignalSpans   Signal = "spans"
	SignalMetrics Signal = "metrics"
)

// Recorder publishes usage StatRows. Handlers depend on this abstraction, not
// the concrete Kafka producer, so the path is testable and can be disabled.
type Recorder interface {
	Publish(ctx context.Context, rows []*schema.StatRow) error
}


// Emit publishes usage rows best-effort in the background; a slow or failed
// meter never blocks or fails ingestion.
func Emit(rec Recorder, rows []*schema.StatRow) {
	if rec == nil || len(rows) == 0 {
		return
	}
	go func(r Recorder, rs []*schema.StatRow) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Publish(ctx, rs); err != nil {
			slog.WarnContext(ctx, "ingestion_stats: publish failed", slog.Any("error", err))
		}
	}(rec, rows)
}

// NewRow builds a StatRow stamped with the current ingest-time hour. Fingerprint
// stays zero; the producer keys stat rows by tenant, not fingerprint.
func NewRow(tenantID uint32, signal Signal, service, env string, records, bytes uint64) *schema.StatRow {
	return &schema.StatRow{
		TenantId:    tenantID,
		BucketUnix:  time.Now().Truncate(time.Hour).Unix(),
		Signal:      string(signal),
		Service:     service,
		Environment: env,
		RecordCount: records,
		ByteCount:   bytes,
	}
}

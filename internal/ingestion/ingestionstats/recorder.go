package ingestionstats

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats/schema"
)

type Signal string

const (
	SignalLogs    Signal = "logs"
	SignalSpans   Signal = "spans"
	SignalMetrics Signal = "metrics"
)

type Recorder interface{ Record([]*schema.StatRow) }

type publisher interface {
	Publish(context.Context, []*schema.StatRow) error
}
type statKey struct {
	tenant                       uint32
	hour                         int64
	signal, service, environment string
}

type HourlyRecorder struct {
	pub           publisher
	flushInterval time.Duration
	mu            sync.Mutex
	rows          map[statKey]*schema.StatRow
	closing       bool
	done          chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
}

const (
	publishTimeout = 5 * time.Second
	retryDelay     = 30 * time.Second
	closeRetries   = 3
)

func NewHourlyRecorder(pub publisher, flushInterval time.Duration) *HourlyRecorder {
	r := &HourlyRecorder{pub: pub, flushInterval: flushInterval, rows: make(map[statKey]*schema.StatRow), done: make(chan struct{})}
	r.wg.Add(1)
	go r.run()
	return r
}

func (r *HourlyRecorder) Record(rows []*schema.StatRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return
	}
	for _, row := range rows {
		if row == nil {
			continue
		}
		k := statKey{row.GetTenantId(), row.GetBucketUnix(), row.GetSignal(), row.GetService(), row.GetEnvironment()}
		if cur := r.rows[k]; cur != nil {
			cur.RecordCount += row.GetRecordCount()
			cur.ByteCount += row.GetByteCount()
			continue
		}
		r.rows[k] = cloneRow(row)
	}
}

func cloneRow(row *schema.StatRow) *schema.StatRow {
	return &schema.StatRow{
		TenantId:    row.GetTenantId(),
		Fingerprint: row.GetFingerprint(),
		BucketUnix:  row.GetBucketUnix(),
		Signal:      row.GetSignal(),
		Service:     row.GetService(),
		Environment: row.GetEnvironment(),
		RecordCount: row.GetRecordCount(),
		ByteCount:   row.GetByteCount(),
	}
}

func (r *HourlyRecorder) run() {
	defer r.wg.Done()
	// Jittered start so replicas never flush in lockstep: aligned flushes
	// produced identical insert blocks that ClickHouse's
	// replicated_deduplication_window silently dropped (prod incident).
	// Rows keep their hourly bucket; partial increments sum in the table.
	delay := time.Duration(rand.Int64N(int64(r.flushInterval)))
	for {
		timer := time.NewTimer(delay)
		select {
		case <-r.done:
			timer.Stop()
			r.flushOnClose()
			return
		case <-timer.C:
			if r.flush() {
				delay = r.flushInterval
			} else {
				delay = retryDelay
			}
		}
	}
}

func (r *HourlyRecorder) flush() bool {
	rows := r.snapshot()
	if len(rows) == 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	err := r.pub.Publish(ctx, rows)
	cancel()
	if err == nil {
		return true
	}
	r.restore(rows)
	metrics.IngestionStatsPublishRetries.Inc()
	slog.Warn("ingestion_stats: publish failed; retry scheduled", slog.Any("error", err), slog.Int("rows", len(rows)))
	return false
}

func (r *HourlyRecorder) flushOnClose() {
	for attempt := 0; attempt < closeRetries; attempt++ {
		if r.flush() {
			return
		}
		if attempt+1 < closeRetries {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}
	r.mu.Lock()
	dropped := len(r.rows)
	r.rows = make(map[statKey]*schema.StatRow)
	r.mu.Unlock()
	if dropped > 0 {
		metrics.IngestionStatsPublishDropped.Add(float64(dropped))
		slog.Error("ingestion_stats: dropping rows after shutdown retries", slog.Int("rows", dropped))
	}
}

func (r *HourlyRecorder) snapshot() []*schema.StatRow {
	r.mu.Lock()
	rows := make([]*schema.StatRow, 0, len(r.rows))
	for _, row := range r.rows {
		rows = append(rows, row)
	}
	r.rows = make(map[statKey]*schema.StatRow)
	r.mu.Unlock()
	return rows
}

func (r *HourlyRecorder) restore(rows []*schema.StatRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, row := range rows {
		k := statKey{row.GetTenantId(), row.GetBucketUnix(), row.GetSignal(), row.GetService(), row.GetEnvironment()}
		if current := r.rows[k]; current != nil {
			current.RecordCount += row.GetRecordCount()
			current.ByteCount += row.GetByteCount()
			continue
		}
		r.rows[k] = row
	}
}

func (r *HourlyRecorder) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closing = true
		close(r.done)
		r.mu.Unlock()
		r.wg.Wait()
	})
}

func Emit(rec Recorder, rows []*schema.StatRow) {
	if rec != nil && len(rows) > 0 {
		rec.Record(rows)
	}
}

func NewRow(tenantID uint32, signal Signal, service, env string, records, bytes uint64) *schema.StatRow {
	return &schema.StatRow{TenantId: tenantID, BucketUnix: time.Now().UTC().Truncate(time.Hour).Unix(), Signal: string(signal), Service: service, Environment: env, RecordCount: records, ByteCount: bytes}
}

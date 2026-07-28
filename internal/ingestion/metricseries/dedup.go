package metricseries

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/ingestion/metricseries/schema"
)

const signalLabel = "metric_series"

type dedupKey struct {
	tenant      uint32
	fingerprint uint64
}

// Dedup suppresses cross-request series metadata republish: a series row is
// published at most once per republish bucket instead of on every scrape.
// Rows are marked only after a successful publish, so a failed publish is
// retried on the client's next export. Concurrent duplicate publishes are
// harmless: metrics_series is a ReplacingMergeTree.
type Dedup struct {
	bucketSecs int64
	cache      *lru.Cache[dedupKey, int64]
}

func NewDedup(capacity int, republishInterval time.Duration) *Dedup {
	if capacity <= 0 || republishInterval <= 0 {
		return nil
	}
	cache, _ := lru.NewWithEvict(capacity, func(dedupKey, int64) {
		obsmetrics.ResourceCacheEvictions.WithLabelValues(signalLabel).Inc()
	})
	return &Dedup{bucketSecs: int64(republishInterval / time.Second), cache: cache}
}

func (d *Dedup) Bucket(now time.Time) int64 {
	if d == nil {
		return 0
	}
	return now.Unix() / d.bucketSecs
}

// FilterUnpublished drops rows already published in this bucket. A nil *Dedup
// disables filtering.
func (d *Dedup) FilterUnpublished(rows []*schema.SeriesRow, bucket int64) []*schema.SeriesRow {
	if d == nil {
		return rows
	}
	out := make([]*schema.SeriesRow, 0, len(rows))
	for _, r := range rows {
		k := dedupKey{tenant: r.GetTenantId(), fingerprint: r.GetFingerprint()}
		if last, ok := d.cache.Get(k); ok && last >= bucket {
			continue
		}
		out = append(out, r)
	}
	if hits := len(rows) - len(out); hits > 0 {
		obsmetrics.ResourceCacheHits.WithLabelValues(signalLabel).Add(float64(hits))
	}
	if len(out) > 0 {
		obsmetrics.ResourceCacheMisses.WithLabelValues(signalLabel).Add(float64(len(out)))
	}
	return out
}

// MarkPublished records rows as published for bucket; call after a
// successful publish only.
func (d *Dedup) MarkPublished(rows []*schema.SeriesRow, bucket int64) {
	if d == nil {
		return
	}
	for _, r := range rows {
		k := dedupKey{tenant: r.GetTenantId(), fingerprint: r.GetFingerprint()}
		d.cache.Add(k, bucket)
	}
}

-- metrics_1m_mv guarded hist_buckets alone, but the aggregate requires both
-- array arguments to be the same length. A histogram with no explicit bounds
-- (single +Inf bucket) has hist_buckets=[] and hist_counts=[n], so the guard
-- kept buckets empty against a length-1 counts array and the view threw
-- SIZES_OF_ARRAYS_DONT_MATCH -- failing the whole INSERT INTO optikk.metrics
-- batch, including healthy rows riding alongside it.
--
-- Guard on the bounds/counts length contract instead (counts = bounds + 1), so
-- single-bucket histograms aggregate correctly and inconsistent pairs degrade
-- to an empty state rather than dropping the batch.
--
-- 08_metrics_1m.sql is left as-is: it is already applied everywhere and the
-- runner tracks by filename, so the fix has to land as its own migration.
-- Recreating the view does not touch metrics_1m; rows written between the DROP
-- and CREATE below are not rolled up.

DROP VIEW IF EXISTS optikk.metrics_1m_mv;

CREATE MATERIALIZED VIEW optikk.metrics_1m_mv
TO optikk.metrics_1m AS
SELECT
    tenant_id,
    metric_name,
    fingerprint,
    toStartOfMinute(timestamp) AS timestamp,
    argMaxState(value, timestamp) AS val_last,
    min(value)     AS val_min,
    max(value)     AS val_max,
    sum(value)     AS val_sum,
    count()        AS val_count,
    sum(hist_sum)   AS hist_sum,
    sum(hist_count) AS hist_count,
    quantilesPrometheusHistogramArrayState(0.5, 0.95, 0.99)(
        if(length(hist_counts) = length(hist_buckets) + 1, arrayPushBack(hist_buckets, inf), emptyArrayFloat64()),
        if(length(hist_counts) = length(hist_buckets) + 1, arrayCumSum(hist_counts), emptyArrayUInt64())
    ) AS latency_state
FROM optikk.metrics
GROUP BY tenant_id, metric_name, fingerprint, timestamp;

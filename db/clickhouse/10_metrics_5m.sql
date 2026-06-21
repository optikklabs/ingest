-- 5-minute scalar rollup. Fills the 5h-25h window range where the display ladder
-- already buckets at 5m but reads scanned metrics_1m (~5x too many rows).
-- Cascaded from metrics_1m (lossless SimpleAggregateFunction re-apply, like 1h).
-- Same columns/key as metrics_1m; 5m boundaries are valid 1m boundaries.

CREATE TABLE IF NOT EXISTS optikk.metrics_5m (
    team_id     UInt32 CODEC(T64, ZSTD(1)),
    metric_name LowCardinality(String),
    fingerprint UInt64 CODEC(ZSTD(1)),
    timestamp   DateTime CODEC(DoubleDelta, LZ4),
    val_last    SimpleAggregateFunction(anyLast, Float64) CODEC(Gorilla, ZSTD(1)),
    val_min     SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max     SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    val_sum     SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count   SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    hist_sum    SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count  SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_5m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, fingerprint, timestamp)
TTL timestamp + INTERVAL 14 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_5m_mv
TO optikk.metrics_5m AS
SELECT
    team_id,
    metric_name,
    fingerprint,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    anyLast(val_last) AS val_last,
    min(val_min)      AS val_min,
    max(val_max)      AS val_max,
    sum(val_sum)      AS val_sum,
    sum(val_count)    AS val_count,
    sum(hist_sum)     AS hist_sum,
    sum(hist_count)   AS hist_count,
    quantilesPrometheusHistogramMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.metrics_1m
GROUP BY team_id, metric_name, fingerprint, timestamp;

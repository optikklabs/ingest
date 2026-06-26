-- 1-minute scalar rollup. Rollup structure (last/min/max/sum/
-- count SimpleAggregateFunction) with Optikk val_* names. Labels stay off the hot
-- rows: resolve any dim via metrics_series on fingerprint. timestamp is the
-- 1m-aligned bucket, derived server-side in the MV to match the display ladder.

CREATE TABLE IF NOT EXISTS optikk.metrics_1m (
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
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, fingerprint, timestamp)
TTL timestamp + INTERVAL 7 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_1m_mv
TO optikk.metrics_1m AS
SELECT
    team_id,
    metric_name,
    fingerprint,
    toStartOfMinute(timestamp) AS timestamp,
    anyLast(value) AS val_last,
    min(value)     AS val_min,
    max(value)     AS val_max,
    sum(value)     AS val_sum,
    count()        AS val_count,
    sum(hist_sum)   AS hist_sum,
    sum(hist_count) AS hist_count,
    
    
    quantilesPrometheusHistogramArrayState(0.5, 0.95, 0.99)(if(empty(hist_buckets), hist_buckets, arrayPushBack(hist_buckets, toFloat64(inf))), arrayCumSum(hist_counts)) AS latency_state
FROM optikk.metrics
GROUP BY team_id, metric_name, fingerprint, timestamp;

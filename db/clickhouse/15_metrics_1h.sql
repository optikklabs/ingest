-- 1-hour scalar (Gauge/Sum) rollup, cascaded from metrics_1m.
-- Query-acceleration tier: readers route here when the query window exceeds
-- 24h (timebucket.UseHourRollup). Column set and ORDER BY mirror metrics_1m
-- so reader SQL differs only by table name and grain.
-- ts_bucket is hour-aligned here; hour boundaries are also valid 5-min
-- boundaries, so Go-side BETWEEN bucket filters keep working unchanged.

CREATE TABLE IF NOT EXISTS optikk.metrics_1h (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    metric_name          LowCardinality(String),
    fingerprint          UInt64 CODEC(ZSTD(1)),

    -- Fixed columns replace attr_hash
    db_system                     LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    db_connection_state           LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    messaging_destination         LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    messaging_consumer_group      LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    messaging_system              LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),

    -- Scalar (Gauge / Sum) — four aggregations per row.
    val_min              SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max              SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    val_sum              SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count            SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_1h', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, ts_bucket, fingerprint, db_system, db_connection_state, messaging_destination, messaging_consumer_group, messaging_system, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_1h_mv
TO optikk.metrics_1h AS
SELECT
    team_id,
    toUInt32(toUnixTimestamp(toStartOfHour(timestamp))) AS ts_bucket,
    toStartOfHour(timestamp)                            AS timestamp,
    metric_name,
    fingerprint,

    db_system,
    db_connection_state,
    messaging_destination,
    messaging_consumer_group,
    messaging_system,

    min(val_min)   AS val_min,
    max(val_max)   AS val_max,
    sum(val_sum)   AS val_sum,
    sum(val_count) AS val_count
FROM optikk.metrics_1m
GROUP BY
    team_id,
    ts_bucket,
    timestamp,
    metric_name,
    fingerprint,
    db_system,
    db_connection_state,
    messaging_destination,
    messaging_consumer_group,
    messaging_system;

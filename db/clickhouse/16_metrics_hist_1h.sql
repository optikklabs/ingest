-- 1-hour histogram rollup, cascaded from metrics_hist_1m. The
-- quantilesPrometheusHistogram state merges losslessly across tiers via
-- -MergeState. Query-acceleration tier: readers route here when the query
-- window exceeds 24h (timebucket.UseHourRollup). Column set and ORDER BY
-- mirror metrics_hist_1m so reader SQL differs only by table name and grain.
-- ts_bucket is hour-aligned here; hour boundaries are also valid 5-min
-- boundaries, so Go-side BETWEEN bucket filters keep working unchanged.

CREATE TABLE IF NOT EXISTS optikk.metrics_hist_1h (
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

    hist_sum             SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count           SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state        AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_hist_1h', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, ts_bucket, fingerprint, db_system, db_connection_state, messaging_destination, messaging_consumer_group, messaging_system, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_hist_1h_mv
TO optikk.metrics_hist_1h AS
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

    sum(hist_sum)   AS hist_sum,
    sum(hist_count) AS hist_count,
    quantilesPrometheusHistogramMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.metrics_hist_1m
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

-- 1-minute histogram rollup from observability.metrics via metrics_hist_1m_mv.
-- Carries series identity + histogram aggregate state + fixed attributes.

CREATE TABLE IF NOT EXISTS observability.metrics_hist_1m (
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
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/observability/metrics_hist_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, ts_bucket, fingerprint, db_system, db_connection_state, messaging_destination, messaging_consumer_group, messaging_system, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hist_1m_mv
TO observability.metrics_hist_1m AS
SELECT
    team_id,
    toUInt32(intDiv(toUnixTimestamp(timestamp), 300) * 300) AS ts_bucket,
    toStartOfMinute(timestamp)                              AS timestamp,
    metric_name,
    fingerprint,

    -- Extract fixed columns
    attributes.'db.system'::String                     AS db_system,
    attributes.'db.client.connection.state'::String    AS db_connection_state,
    attributes.'messaging.destination.name'::String    AS messaging_destination,
    attributes.'messaging.consumer.group.name'::String AS messaging_consumer_group,
    attributes.'messaging.system'::String              AS messaging_system,

    sum(hist_sum)   AS hist_sum,
    sum(hist_count) AS hist_count,
    quantilesPrometheusHistogramArrayState(0.5, 0.95, 0.99)(
        hist_buckets,
        arrayCumSum(hist_counts)
    ) AS latency_state
FROM observability.metrics
WHERE metric_type = 'Histogram'
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

-- Spans resource helper — narrow MV-fed dictionary used to resolve fingerprint
-- before the raw-table scan. ts_bucket passes through from the upstream raw table.

CREATE TABLE IF NOT EXISTS optikk.spans_resource (
    team_id     UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket   UInt32 CODEC(DoubleDelta, LZ4),
    fingerprint UInt64 CODEC(ZSTD(1)),
    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),
    pod         LowCardinality(String) CODEC(ZSTD(1)),
    environment LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/optikk/spans_resource', '{replica}')
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket))
ORDER BY (team_id, ts_bucket, service, host, pod, fingerprint)
TTL toDateTime(ts_bucket) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_spans_resource
TO optikk.spans_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket,
    fingerprint,
    service,
    host,
    pod,
    environment
FROM optikk.spans
WHERE fingerprint != 0;

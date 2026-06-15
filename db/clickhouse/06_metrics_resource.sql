-- Metrics resource helper — narrow MV-fed dictionary used to resolve fingerprint
-- before the raw-table scan. ts_bucket passes through from the upstream raw table.

CREATE TABLE IF NOT EXISTS observability.metrics_resource (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket        UInt32 CODEC(DoubleDelta, LZ4),
    fingerprint      UInt64 CODEC(ZSTD(1)),
    service          LowCardinality(String) CODEC(ZSTD(1)),
    host             LowCardinality(String) CODEC(ZSTD(1)),
    environment      LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace    LowCardinality(String) CODEC(ZSTD(1)),
    pod              LowCardinality(String) CODEC(ZSTD(1)),
    container        LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/observability/metrics_resource', '{replica}')
-- Weekly partitions: ORDER BY carries no ts_bucket, so ReplacingMergeTree only
-- dedups within a partition — daily partitions would duplicate each fingerprint
-- up to ~30x over the TTL window; weekly caps it at ~5x with <=7d drop lag.
PARTITION BY toMonday(toDateTime(ts_bucket))
ORDER BY (team_id, fingerprint)
TTL toDateTime(ts_bucket) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_metrics_resource
TO observability.metrics_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket,
    fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    pod,
    container
FROM observability.metrics
WHERE fingerprint != 0;

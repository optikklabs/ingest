-- Logs resource helper — narrow metadata table used to resolve fingerprint
-- before the logs table scan. Written directly from Go with LRU cache.

CREATE TABLE IF NOT EXISTS optikk.logs_resource (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    fingerprint UInt64 CODEC(ZSTD(1)),
    ts_bucket   UInt32 CODEC(DoubleDelta, LZ4),
    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),
    pod         LowCardinality(String) CODEC(ZSTD(1)),
    container   LowCardinality(String) CODEC(ZSTD(1)),
    environment LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/optikk/logs_resource', '{replica}')
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket))
ORDER BY (tenant_id, service, ts_bucket, fingerprint)
TTL
    toDateTime(ts_bucket) + INTERVAL 3 DAY TO VOLUME 'main',
    toDateTime(ts_bucket) + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192;

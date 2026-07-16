-- Spans resource helper — narrow metadata table used to resolve fingerprint
-- before the raw-table scan. Written directly from Go with LRU cache.

CREATE TABLE IF NOT EXISTS optikk.spans_resource (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    fingerprint UInt64 CODEC(ZSTD(1)),
    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),
    pod         LowCardinality(String) CODEC(ZSTD(1)),
    environment LowCardinality(String) CODEC(ZSTD(1)),
    last_seen   DateTime DEFAULT now() CODEC(DoubleDelta, LZ4)
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/optikk/spans_resource', '{replica}')
PARTITION BY toYYYYMMDD(last_seen)
ORDER BY (tenant_id, service, host, pod, environment, fingerprint)
TTL
    last_seen + INTERVAL 3 DAY TO VOLUME 'main',
    last_seen + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192;

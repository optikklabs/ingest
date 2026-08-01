-- Exact-count log rollup for overview, trend, and facet queries over promoted
-- dimensions. Queries that need body, trace/span IDs, or arbitrary attributes
-- continue to read raw logs; compatible aggregate queries read this table.

CREATE TABLE IF NOT EXISTS optikk.logs_stats_1m (
    tenant_id       UInt32                 CODEC(T64, ZSTD(1)),
    timestamp       DateTime               CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String) CODEC(ZSTD(1)),
    host            LowCardinality(String) CODEC(ZSTD(1)),
    pod             LowCardinality(String) CODEC(ZSTD(1)),
    container       LowCardinality(String) CODEC(ZSTD(1)),
    environment     LowCardinality(String) CODEC(ZSTD(1)),
    severity_text   LowCardinality(String) CODEC(ZSTD(1)),
    severity_bucket UInt8                  CODEC(T64, ZSTD(1)),
    log_count       UInt64                 CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/optikk/logs_stats_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, service, host, pod, container, environment, severity_text, severity_bucket)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.logs_to_stats_1m
TO optikk.logs_stats_1m AS
SELECT
    tenant_id,
    toStartOfMinute(timestamp) AS timestamp,
    service,
    host,
    pod,
    container,
    environment,
    severity_text,
    severity_bucket,
    count() AS log_count
FROM optikk.logs
GROUP BY tenant_id, timestamp, service, host, pod, container, environment, severity_text, severity_bucket;

CREATE TABLE IF NOT EXISTS optikk.metrics_series (
    tenant_id          UInt32 CODEC(T64, ZSTD(1)),
    timestamp        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    metric_type      LowCardinality(String) CODEC(ZSTD(1)),
    temporality      LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic     Bool CODEC(T64, ZSTD(1)),
    unit             LowCardinality(String) DEFAULT '',
    description      LowCardinality(String) DEFAULT '',
    fingerprint      UInt64 CODEC(ZSTD(1)),
    service          LowCardinality(String) CODEC(ZSTD(1)),
    host             LowCardinality(String) CODEC(ZSTD(1)),
    environment      LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace    LowCardinality(String) CODEC(ZSTD(1)),
    pod              LowCardinality(String) CODEC(ZSTD(1)),
    container        LowCardinality(String) CODEC(ZSTD(1)),
    attributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    -- Host-scoped resource attributes (os.*, host.*, cloud.*, k8s node/cluster)
    -- retained per series for host metadata panels. Allowlisted at ingest.
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_series_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, metric_name, toStartOfInterval(timestamp, INTERVAL 6 HOUR), service, fingerprint)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

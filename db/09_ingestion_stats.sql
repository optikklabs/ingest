CREATE TABLE IF NOT EXISTS optikk.ingestion_stats (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    bucket_hour   DateTime CODEC(DoubleDelta, LZ4),
    signal        LowCardinality(String),
    service       LowCardinality(String),
    environment   LowCardinality(String),
    record_count  UInt64 CODEC(T64, ZSTD(1)),
    byte_count    UInt64 CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/optikk/ingestion_stats', '{replica}')
PARTITION BY toYYYYMM(bucket_hour)
ORDER BY (tenant_id, bucket_hour, signal, service, environment)
TTL
    bucket_hour + INTERVAL 30 DAY TO VOLUME 'main',
    bucket_hour + INTERVAL 400 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
                                                                            
                                          
    replicated_deduplication_window = 1000,
    replicated_deduplication_window_seconds = 86400;

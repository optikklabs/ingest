CREATE TABLE IF NOT EXISTS optikk.logs_index (
    tenant_id              UInt32 CODEC(T64, ZSTD(1)),
    timestamp            DateTime64(9) CODEC(DoubleDelta, LZ4),
    log_id               String CODEC(ZSTD(1)),
    trace_id             String CODEC(ZSTD(1)),
    
    service              LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    container            LowCardinality(String) CODEC(ZSTD(1)),
    
    severity_text        LowCardinality(String) CODEC(ZSTD(1)),
    severity_number      UInt8 DEFAULT 0,
    severity_bucket      UInt8 CODEC(T64, ZSTD(1)),
    attributes_string    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number    Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool      Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    
    body_search          String CODEC(ZSTD(2)),
    
    INDEX idx_log_id       log_id       TYPE bloom_filter(0.01)        GRANULARITY 4,
    INDEX idx_body_text    body_search  TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lowerUTF8(body_search)) GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/logs_index', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, service, timestamp)
TTL timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;
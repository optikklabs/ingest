CREATE TABLE IF NOT EXISTS optikk.logs (
    tenant_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    trace_id             String CODEC(ZSTD(1)),
    log_id               String CODEC(ZSTD(1)),
    timestamp            DateTime64(9) CODEC(DoubleDelta, LZ4),

    service              LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    container            LowCardinality(String) CODEC(ZSTD(1)),

    severity_text        LowCardinality(String) CODEC(ZSTD(1)),
    severity_number      UInt8 DEFAULT 0,
    -- OTel severity ladder: 0 unset, then one bucket per 4 numbers up to FATAL.
    severity_bucket      UInt8 MATERIALIZED if(severity_number = 0, 0, least(intDiv(severity_number - 1, 4), 5)) CODEC(T64, ZSTD(1)),
    attributes_string    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number    Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool      Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),

    observed_timestamp   UInt64 CODEC(DoubleDelta, LZ4),
    span_id              String CODEC(ZSTD(1)),
    trace_flags          UInt32 DEFAULT 0,
    body                 String CODEC(ZSTD(2)),
    resource             Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    scope_name           LowCardinality(String) CODEC(ZSTD(1)),
    scope_version        LowCardinality(String) CODEC(ZSTD(1)),

    INDEX idx_trace_id     trace_id     TYPE bloom_filter(0.01)        GRANULARITY 1,
    INDEX idx_log_id       log_id       TYPE bloom_filter(0.01)        GRANULARITY 4,
    INDEX idx_body_text    body TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lowerUTF8(body)) GRANULARITY 1,
    -- Prunes the explorer's case-insensitive substring search
    -- (lowerUTF8(body) LIKE '%term%'); position* functions cannot use it.
    INDEX idx_body_ngram   lowerUTF8(body) TYPE ngrambf_v1(4, 1024, 3, 7) GRANULARITY 4
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/logs_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, ts_bucket, timestamp)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    min_bytes_for_wide_part = 10485760,
    -- Dedup window matched by the consumer's insert_deduplication_token
    -- (hash of the batch's partition/offset spans). Catches same-boundary
    -- Kafka redelivery only; different boundaries duplicate (accepted).
    replicated_deduplication_window = 10000,
    replicated_deduplication_window_seconds = 3600,
    ttl_only_drop_parts = 1;

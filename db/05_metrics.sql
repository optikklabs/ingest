CREATE TABLE IF NOT EXISTS optikk.metrics (
    tenant_id              UInt32 CODEC(T64, ZSTD(1)),
    metric_name          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    fingerprint          UInt64 CODEC(ZSTD(1)),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64)  CODEC(T64, ZSTD(1)),
    -- OTLP Summary client-computed quantiles: non-aggregatable, stored raw.
    summary_quantiles    Array(Float64) CODEC(ZSTD(1)),
    summary_values       Array(Float64) CODEC(Gorilla, ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/metrics', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, metric_name, fingerprint, timestamp)
TTL timestamp + INTERVAL 2 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    min_bytes_for_wide_part = 10485760,
    enable_mixed_granularity_parts = 1,
    -- Dedup window matched by the consumer's insert_deduplication_token
    -- (hash of the batch's partition/offset spans). Catches same-boundary
    -- Kafka redelivery only; different boundaries duplicate (accepted).
    replicated_deduplication_window = 10000,
    replicated_deduplication_window_seconds = 3600,
    ttl_only_drop_parts = 1;

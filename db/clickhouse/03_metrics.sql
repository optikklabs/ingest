CREATE TABLE IF NOT EXISTS observability.metrics (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    metric_name          LowCardinality(String),
    metric_type          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic         Bool CODEC(T64, ZSTD(1)),
    unit                 LowCardinality(String) DEFAULT '',
    description          LowCardinality(String) DEFAULT '',
    fingerprint          UInt64 CODEC(ZSTD(1)),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64)  CODEC(T64, ZSTD(1)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace        LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    container            LowCardinality(String) CODEC(ZSTD(1)),
    resource             JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    attributes           JSON(max_dynamic_paths=100) CODEC(ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/observability/metrics', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, fingerprint, metric_name, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;

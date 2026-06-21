CREATE TABLE IF NOT EXISTS optikk.metrics (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    metric_name          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    fingerprint          UInt64 CODEC(ZSTD(1)),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64)  CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/metrics', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, fingerprint, timestamp)
TTL timestamp + INTERVAL 7 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;


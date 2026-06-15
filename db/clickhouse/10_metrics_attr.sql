-- Metrics attribute (label) dictionary. Resolves the per-data-point attributes
-- JSON by attr_hash before the rollup scan — the data-point analog of
-- metrics_resource (which resolves resource dims by fingerprint).
--
-- The same attribute set repeats on every rollup row of a series for every
-- minute it is active (~187x in practice). Storing it once per (metric_name,
-- attr_hash, bucket) here keeps the JSON — 34% of metrics_1m's scanned bytes —
-- off the hot value/percentile path. Readers join on attr_hash when a query
-- groups or filters by a non-resource label (e.g. db.name), and faceting reads
-- this table directly. metric_name leads attr_hash so faceting and the
-- attr-resolution CTE stay scoped to a single metric on a contiguous range.
-- ts_bucket carries the 5-min Go-side value through from the raw table for
-- time pruning.

CREATE TABLE IF NOT EXISTS observability.metrics_attr (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket        UInt32 CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    attr_hash        UInt64 CODEC(ZSTD(1)),
    attributes       JSON(max_dynamic_paths=100) CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/observability/metrics_attr', '{replica}')
-- Weekly partitions: ORDER BY carries no ts_bucket, so ReplacingMergeTree only
-- dedups within a partition — daily partitions would duplicate each label set
-- up to ~30x over the TTL window; weekly caps it at ~5x with <=7d drop lag.
PARTITION BY toMonday(toDateTime(ts_bucket))
ORDER BY (team_id, metric_name, attr_hash)
TTL toDateTime(ts_bucket) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_metrics_attr
TO observability.metrics_attr AS
SELECT DISTINCT
    team_id,
    ts_bucket,
    metric_name,
    cityHash64(toJSONString(attributes)) AS attr_hash,
    attributes
FROM observability.metrics;

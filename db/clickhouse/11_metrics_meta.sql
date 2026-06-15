-- Metric metadata dictionary. Resolves metric_type / unit / description by
-- metric_name — one row per metric, not one per rollup row. These
-- LowCardinality strings are functionally determined by metric_name, so the
-- catalog/list view reads them here instead of carrying them on every
-- metrics_1m row. No partition or TTL: the table is tiny (cardinality = number
-- of distinct metrics) and the MV refreshes it continuously.

CREATE TABLE IF NOT EXISTS observability.metrics_meta (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    metric_name      LowCardinality(String),
    metric_type      LowCardinality(String) CODEC(ZSTD(1)),
    unit             LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    description      LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/observability/metrics_meta', '{replica}')
ORDER BY (team_id, metric_name)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_metrics_meta
TO observability.metrics_meta AS
SELECT DISTINCT
    team_id,
    metric_name,
    metric_type,
    unit,
    description
FROM observability.metrics;

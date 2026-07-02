-- Reverse-key lookup table. Used by query service to map trace_id -> root timestamp.
-- Only indexes root spans to minimize write amplification and storage footprint.

CREATE TABLE IF NOT EXISTS optikk.trace_index (
    trace_id    String         CODEC(ZSTD(1)),
    team_id     UInt32         CODEC(T64, ZSTD(1)),
    timestamp   DateTime64(9)  CODEC(DoubleDelta, LZ4),
    span_id     String         CODEC(ZSTD(1)),
    fingerprint UInt64         CODEC(ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/trace_index', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, trace_id, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_trace_index
TO optikk.trace_index AS
SELECT
    trace_id,
    team_id,
    timestamp,
    span_id,
    fingerprint
FROM optikk.spans
WHERE trace_id != ''
  AND (parent_span_id = '' OR parent_span_id = '0000000000000000');

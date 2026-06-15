-- INVARIANT: bucket values are computed Go-side (internal/infra/timebucket); no CH bucket functions in this file.
-- Reverse-key projection of observability.spans. Used by:
--   - traces/explorer.GetByID to resolve (team_id, trace_id) → (ts_bucket bounds,
--     fingerprint set, root span_id) in O(one granule) before scanning raw spans.
--   - logs/trace_logs to resolve (team_id, trace_id) → (ts_bucket bounds,
--     fingerprint set) before scanning observability.logs. log_id is fetched
--     from logs directly (it stays in the row payload there).
-- Leading PK = trace_id so trace-id-keyed lookups land on a single granule.
--
-- Spans-fed (not logs-fed): a trace's services emit both spans and logs from
-- the same resource fingerprint, so the spans-side fingerprint set covers the
-- logs side too. Edge case: services emitting logs without spans for a given
-- trace are invisible to trace_logs after this change (acceptable — a trace
-- without span emission isn't really part of the trace from a tracing model).

CREATE TABLE IF NOT EXISTS observability.trace_index (
    trace_id    String         CODEC(ZSTD(1)),
    team_id     UInt32         CODEC(T64, ZSTD(1)),
    ts_bucket   UInt32         CODEC(DoubleDelta, LZ4),
    fingerprint UInt64         CODEC(ZSTD(1)),
    timestamp   DateTime64(9)  CODEC(DoubleDelta, LZ4),
    span_id     String         CODEC(ZSTD(1)),
    is_root     UInt8          CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/observability/trace_index', '{replica}')
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket))
ORDER BY (trace_id, team_id, ts_bucket, fingerprint, timestamp, span_id)
TTL toDateTime(ts_bucket) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_trace_index
TO observability.trace_index AS
SELECT
    trace_id,
    team_id,
    ts_bucket,
    fingerprint,
    timestamp,
    span_id,
    if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0) AS is_root
FROM observability.spans
WHERE trace_id != '';

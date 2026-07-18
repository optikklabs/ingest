-- Rebuild optikk.trace_index to cover every span, not just root spans.
--
-- The root-only index (07_trace_index.sql) made a trace unreachable whenever its
-- root span was missing: the query service located spans via the index timestamp,
-- and a missing row collapsed the scan window to the DateTime64 default (1970),
-- silently returning zero spans.
--
-- The replacement aggregates min(start)/max(end) over all spans of a trace, so the
-- scan window is derived from the data instead of the root span plus a guessed
-- -5m/+24h pad. Engine and ORDER BY cannot be altered in place, so the table is
-- built beside the old one and renamed into position.
--
-- The runner has no transaction and records a migration only on full success, so a
-- mid-file failure re-runs this file from the top. The leading DROP makes that
-- replay clean.

DROP TABLE IF EXISTS optikk.trace_index_v2;

CREATE TABLE IF NOT EXISTS optikk.trace_index_v2 (
    tenant_id UInt32                                          CODEC(T64, ZSTD(1)),
    trace_id  String                                          CODEC(ZSTD(1)),
    start_ts  SimpleAggregateFunction(min, DateTime64(9))      CODEC(DoubleDelta, LZ4),
    end_ts    SimpleAggregateFunction(max, DateTime64(9))      CODEC(DoubleDelta, LZ4)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/trace_index_v2', '{replica}')
PARTITION BY toYYYYMMDD(start_ts)
ORDER BY (tenant_id, trace_id)
TTL
    start_ts + INTERVAL 3 DAY TO VOLUME 'main',
    start_ts + INTERVAL 30 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    replicated_deduplication_window = 0,
    ttl_only_drop_parts = 1;

-- Carry the existing index forward. Traces already in retention have only a root
-- row, so their window is seeded with the -5m/+24h pad the query layer used to
-- apply. This keeps pre-cutover traces working exactly as before; the pad now
-- lives as data on legacy rows only and ages out with the TTL. Spans ingested
-- after the cutover get exact bounds from the materialized view below.
INSERT INTO optikk.trace_index_v2
SELECT
    tenant_id,
    trace_id,
    timestamp - INTERVAL 5 MINUTE  AS start_ts,
    timestamp + INTERVAL 24 HOUR   AS end_ts
FROM optikk.trace_index;

-- Stop writes to the old table before swapping it out.
DROP VIEW IF EXISTS optikk.spans_to_trace_index;

RENAME TABLE
    optikk.trace_index    TO optikk.trace_index_old,
    optikk.trace_index_v2 TO optikk.trace_index;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_trace_index
TO optikk.trace_index AS
SELECT
    tenant_id,
    trace_id,
    min(timestamp)                                       AS start_ts,
    max(timestamp + toIntervalNanosecond(duration_nano)) AS end_ts
FROM optikk.spans
WHERE trace_id != ''
GROUP BY tenant_id, trace_id;

DROP TABLE IF EXISTS optikk.trace_index_old;

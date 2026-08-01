-- Root-span index: a narrow copy of every root span, powering the traces
-- explorer list / facets / trend. Those queries page and aggregate one row per
-- trace, so scanning the full spans table (root + child) decoded ~N× the rows
-- it needed. Reading root-only spans here removes that fan-out.
--
-- Carries only the columns the explorer scan reads; resource dims and any-span
-- predicates resolve against spans. is_error mirrors the
-- spans ALIAS so trend error counts stay identical.

CREATE TABLE IF NOT EXISTS optikk.spans_root (
    tenant_id            UInt32                 CODEC(T64, ZSTD(1)),
    timestamp            DateTime64(9)          CODEC(DoubleDelta, LZ4),
    trace_id             String                 CODEC(ZSTD(1)),
    span_id              String                 CODEC(ZSTD(1)),
    duration_nano        UInt64                 CODEC(T64, ZSTD(1)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    name                 LowCardinality(String) CODEC(ZSTD(1)),
    kind_string          LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string   LowCardinality(String) CODEC(ZSTD(1)),
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code LowCardinality(String) CODEC(ZSTD(1)),
    http_route           LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    has_error            Bool                   CODEC(T64, ZSTD(1)),

    is_error UInt8 ALIAS if(has_error OR (kind_string = 'CLIENT' AND toUInt16OrZero(response_status_code) >= 400) OR toUInt16OrZero(response_status_code) >= 500, 1, 0)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/spans_root_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, trace_id, span_id)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_root
TO optikk.spans_root AS
SELECT
    tenant_id,
    timestamp,
    trace_id,
    span_id,
    duration_nano,
    service,
    name,
    kind_string,
    status_code_string,
    http_method,
    response_status_code,
    http_route,
    environment,
    has_error
FROM optikk.spans
WHERE is_root = 1;

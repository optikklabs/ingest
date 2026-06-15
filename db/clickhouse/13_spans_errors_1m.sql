-- 1-minute error-tracking rollup, split out of spans_1m so unbounded error
-- grouping never multiplies the RED rollup's cardinality. MV gates on the
-- error condition (matches the spans.is_error ALIAS), so the table only holds
-- error rows — a tiny fraction of span volume.
-- Carries the error group identity (error_group_id = service|name|
-- exception_type|http_status_bucket, see 01_spans.sql) plus the facet dims
-- exposed by services/errors (service_version, environment, pod, http_route).
-- ts_bucket invariant preserved (5-min Go-side semantics; the MV derives it
-- server-side at MV evaluation only).

CREATE TABLE IF NOT EXISTS optikk.spans_errors_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    fingerprint          UInt64 CODEC(ZSTD(1)),
    error_group_id       String CODEC(ZSTD(1)),

    service              LowCardinality(String) CODEC(ZSTD(1)),
    name                 LowCardinality(String) CODEC(ZSTD(1)),
    exception_type       LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket   LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code LowCardinality(String) CODEC(ZSTD(1)),

    -- Facet dims (services/errors facetColumns).
    service_version      LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    http_route           LowCardinality(String) CODEC(ZSTD(1)),

    error_count          SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/spans_errors_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, service, name, error_group_id, fingerprint, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_errors_1m_mv
TO optikk.spans_errors_1m AS
SELECT
    team_id,
    toUInt32(intDiv(toUnixTimestamp(timestamp), 300) * 300) AS ts_bucket,
    toStartOfMinute(timestamp)                              AS timestamp,
    fingerprint,
    lower(hex(halfMD5(concat(service, '|', name, '|', exception_type, '|', http_status_bucket)))) AS error_group_id,

    service,
    name,
    exception_type,
    http_status_bucket,
    response_status_code,

    service_version,
    environment,
    pod,
    http_route,

    count() AS error_count
FROM optikk.spans
WHERE has_error OR toUInt16OrZero(response_status_code) >= 400
GROUP BY
    team_id, ts_bucket, timestamp, fingerprint, error_group_id,
    service, name, exception_type, http_status_bucket, response_status_code,
    service_version, environment, pod, http_route;

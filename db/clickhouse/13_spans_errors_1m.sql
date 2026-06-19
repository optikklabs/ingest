CREATE TABLE IF NOT EXISTS optikk.spans_errors_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
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
ORDER BY (team_id, ts_bucket, service, name, error_group_id, timestamp)
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
    team_id, ts_bucket, timestamp, error_group_id,
    service, name, exception_type, http_status_bucket, response_status_code,
    service_version, environment, pod, http_route;

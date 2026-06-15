-- 1-hour spans RED rollup, cascaded from spans_1m (not from raw spans — the
-- aggregate states merge losslessly, so re-reading raw rows would only add
-- MV cost on the hot insert path). Query-acceleration tier: readers route
-- here when the query window exceeds 24h (timebucket.UseHourRollup), scanning
-- ~60x fewer rows. Column set and ORDER BY mirror spans_1m so reader SQL
-- differs only by table name and grain.
-- ts_bucket is hour-aligned here; hour boundaries are also valid 5-min
-- boundaries, so Go-side BETWEEN bucket filters keep working unchanged.

CREATE TABLE IF NOT EXISTS optikk.spans_1h (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    fingerprint          UInt64 CODEC(ZSTD(1)),

    service              LowCardinality(String) CODEC(ZSTD(1)),
    name                 LowCardinality(String) CODEC(ZSTD(1)),
    kind_string          LowCardinality(String) CODEC(ZSTD(1)),
    peer_service         LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    is_root              UInt8 CODEC(T64, ZSTD(1)),

    -- HTTP semantics
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    http_route           LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket   LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string   LowCardinality(String) CODEC(ZSTD(1)),

    -- Service identity
    service_version      LowCardinality(String) CODEC(ZSTD(1)),

    -- DB semantics
    db_system            LowCardinality(String) CODEC(ZSTD(1)),
    db_operation_name    LowCardinality(String) CODEC(ZSTD(1)),
    db_collection_name   LowCardinality(String) CODEC(ZSTD(1)),
    db_namespace         LowCardinality(String) CODEC(ZSTD(1)),
    db_response_status   LowCardinality(String) CODEC(ZSTD(1)),
    server_address       LowCardinality(String) CODEC(ZSTD(1)),

    -- Bounded error dim for DB-saturation group-bys (OTel error.type).
    error_type           LowCardinality(String) CODEC(ZSTD(1)),

    -- Latency state — quantileTimingMerge(q)(latency_state) at read time.
    latency_state        AggregateFunction(quantileTiming, Float64) CODEC(ZSTD(1)),

    -- Counts + sums.
    request_count        SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    error_count          SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    duration_ms_sum      SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/spans_1h', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, fingerprint, service, name, kind_string, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_1h_mv
TO optikk.spans_1h AS
SELECT
    team_id,
    toUInt32(toUnixTimestamp(toStartOfHour(timestamp))) AS ts_bucket,
    toStartOfHour(timestamp)                            AS timestamp,
    fingerprint,

    service,
    name,
    kind_string,
    peer_service,
    host,
    pod,
    environment,
    is_root,

    http_method,
    http_route,
    http_status_bucket,
    response_status_code,
    status_code_string,

    service_version,

    db_system,
    db_operation_name,
    db_collection_name,
    db_namespace,
    db_response_status,
    server_address,

    error_type,

    quantileTimingMergeState(latency_state) AS latency_state,
    sum(request_count)                      AS request_count,
    sum(error_count)                        AS error_count,
    sum(duration_ms_sum)                    AS duration_ms_sum
FROM optikk.spans_1m
GROUP BY
    team_id, ts_bucket, timestamp, fingerprint,
    service, name, kind_string, peer_service, host, pod, environment, is_root,
    http_method, http_route, http_status_bucket, response_status_code, status_code_string,
    service_version,
    db_system, db_operation_name, db_collection_name, db_namespace, db_response_status, server_address,
    error_type;

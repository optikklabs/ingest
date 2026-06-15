-- 1-minute spans RED rollup. Mirrors the metrics_1m pattern: AggregatingMergeTree
-- fed by an MV from the raw table. quantileTimingState replaces SQL-side
-- quantileTiming() in percentile call sites; SimpleAggregateFunction columns
-- replace count()/sum() over raw spans.
-- Error-tracking dimensions (error_group_id, exception_type) live in
-- spans_errors_1m (13_spans_errors_1m.sql) — keeping them here multiplied
-- rollup cardinality by the number of distinct error groups. error_type stays:
-- it is a bounded OTel dim used by saturation/database group-bys.
-- ts_bucket invariant preserved (5-min Go-side semantics; the MV derives it
-- server-side at MV evaluation only).

CREATE TABLE IF NOT EXISTS observability.spans_1m (
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
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/observability/spans_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, fingerprint, service, name, kind_string, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_1m_mv
TO observability.spans_1m AS
SELECT
    team_id,
    toUInt32(intDiv(toUnixTimestamp(timestamp), 300) * 300) AS ts_bucket,
    toStartOfMinute(timestamp)                              AS timestamp,
    fingerprint,

    service,
    name,
    kind_string,
    peer_service,
    host,
    pod,
    environment,
    if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0) AS is_root,

    http_method,
    http_route,
    http_status_bucket,
    response_status_code,
    status_code_string,

    service_version,

    db_system,
    attributes.'db.operation.name'::String        AS db_operation_name,
    attributes.'db.collection.name'::String       AS db_collection_name,
    attributes.'db.namespace'::String             AS db_namespace,
    attributes.'db.response.status_code'::String  AS db_response_status,
    attributes.'server.address'::String           AS server_address,

    attributes.'error.type'::String          AS error_type,

    quantileTimingState(duration_nano / 1000000.0)                                    AS latency_state,
    count()                                                                           AS request_count,
    countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                 AS error_count,
    sum(duration_nano / 1000000.0)                                                    AS duration_ms_sum
FROM observability.spans
GROUP BY
    team_id, ts_bucket, timestamp, fingerprint,
    service, name, kind_string, peer_service, host, pod, environment, is_root,
    http_method, http_route, http_status_bucket, response_status_code, status_code_string,
    service_version,
    db_system, db_operation_name, db_collection_name, db_namespace, db_response_status, server_address,
    error_type;

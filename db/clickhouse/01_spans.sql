-- INVARIANT: bucket values are computed Go-side (internal/infra/timebucket); no CH bucket functions in this file.
CREATE TABLE IF NOT EXISTS optikk.spans (
    ts_bucket                             UInt32          CODEC(DoubleDelta, LZ4),
    team_id                               UInt32          CODEC(T64, ZSTD(1)),
    timestamp                             DateTime64(9)   CODEC(DoubleDelta, LZ4),
    trace_id                              String CODEC(ZSTD(1)),
    span_id                               String CODEC(ZSTD(1)),
    parent_span_id                        String CODEC(ZSTD(1)),
    trace_state                           String          CODEC(ZSTD(1)),
    flags                                 UInt32          CODEC(T64, ZSTD(1)),
    name                                  LowCardinality(String) CODEC(ZSTD(1)),
    kind                                  Int8            CODEC(T64, ZSTD(1)),
    kind_string                           LowCardinality(String) CODEC(ZSTD(1)),
    duration_nano                         UInt64          CODEC(T64, ZSTD(1)),
    has_error                             Bool            CODEC(T64, ZSTD(1)),
    status_code                           Int16           CODEC(T64, ZSTD(1)),
    status_code_string                    LowCardinality(String) CODEC(ZSTD(1)),
    status_message                        String          CODEC(ZSTD(1)),
    http_url                              LowCardinality(String) CODEC(ZSTD(1)),
    http_method                           LowCardinality(String) CODEC(ZSTD(1)),
    http_host                             LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code                  LowCardinality(String) CODEC(ZSTD(1)),

    service                               LowCardinality(String) CODEC(ZSTD(1)),
    host                                  LowCardinality(String) CODEC(ZSTD(1)),
    pod                                   LowCardinality(String) CODEC(ZSTD(1)),
    service_version                       LowCardinality(String) CODEC(ZSTD(1)),
    environment                           LowCardinality(String) CODEC(ZSTD(1)),
    peer_service                          LowCardinality(String) CODEC(ZSTD(1)),
    db_system                             LowCardinality(String) CODEC(ZSTD(1)),
    db_name                               LowCardinality(String) CODEC(ZSTD(1)),
    db_statement                          String                 CODEC(ZSTD(1)),
    http_route                            LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket                    LowCardinality(String) CODEC(ZSTD(1)),

    attributes                            JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),

    fingerprint                           UInt64          CODEC(ZSTD(1)),
    events                                Array(String)   CODEC(ZSTD(2)),
    links                                 String          CODEC(ZSTD(1)),

    exception_type                        LowCardinality(String) CODEC(ZSTD(1)),
    exception_message                     String          CODEC(ZSTD(1)),
    exception_stacktrace                  String          CODEC(ZSTD(1)),
    exception_escaped                     Bool            CODEC(T64, ZSTD(1)),

    -- Group key = (service, name, exception_type, http_status_bucket) — all bounded
    -- dims. status_message is deliberately excluded: messages embed request IDs /
    -- user values, so hashing them explodes one logical error into thousands of
    -- groups. The representative message is fetched per group at read time
    -- (argMax(status_message) over raw spans in services/errors).
    error_group_id                        String          MATERIALIZED lower(hex(halfMD5(concat(service, '|', name, '|', exception_type, '|', http_status_bucket)))) CODEC(ZSTD(1)),

    operation_name           LowCardinality(String) ALIAS name,
    start_time               DateTime64(9)          ALIAS timestamp,
    duration_ms              Float64                ALIAS duration_nano / 1000000.0,
    status                   LowCardinality(String) ALIAS status_code_string,
    http_status_code         UInt16                 ALIAS toUInt16OrZero(response_status_code),
    is_error                 UInt8                  ALIAS if(has_error OR toUInt16OrZero(response_status_code) >= 400, 1, 0),
    is_root                  UInt8                  ALIAS if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/spans', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;

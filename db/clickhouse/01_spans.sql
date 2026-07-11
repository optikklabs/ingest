CREATE TABLE IF NOT EXISTS optikk.spans (
    tenant_id                               UInt32          CODEC(T64, ZSTD(1)),
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
    db_statement_normalized               String                 MATERIALIZED if(db_statement = '', '', normalizeQuery(db_statement)) CODEC(ZSTD(1)),
    query_hash                            String                 MATERIALIZED if(db_statement = '', '', lower(hex(normalizedQueryHash(db_statement)))) CODEC(ZSTD(1)),
    http_route                            LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket                    LowCardinality(String) CODEC(ZSTD(1)),

    attributes                            JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),

    gen_ai_system                         LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_operation                      LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_request_model                  LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_response_model                 LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_input_tokens                   UInt64                 CODEC(T64, ZSTD(1)),
    gen_ai_output_tokens                  UInt64                 CODEC(T64, ZSTD(1)),
    is_gen_ai                             Bool                   CODEC(T64, ZSTD(1)),

    fingerprint                           UInt64          CODEC(ZSTD(1)),
    events                                Array(String)   CODEC(ZSTD(2)),
    links                                 String          CODEC(ZSTD(1)),

    exception_type                        LowCardinality(String) CODEC(ZSTD(1)),
    exception_message                     String          CODEC(ZSTD(1)),
    exception_stacktrace                  String          CODEC(ZSTD(1)),
    exception_escaped                     Bool            CODEC(T64, ZSTD(1)),
    
    error_group_id                        String          MATERIALIZED lower(hex(halfMD5(concat(service, '|', name, '|', exception_type, '|', http_status_bucket)))) CODEC(ZSTD(1)),

    operation_name           LowCardinality(String) ALIAS name,
    start_time               DateTime64(9)          ALIAS timestamp,
    duration_ms              Float64                ALIAS duration_nano / 1000000.0,
    status                   LowCardinality(String) ALIAS status_code_string,
    http_status_code         UInt16                 ALIAS toUInt16OrZero(response_status_code),
    is_error                 UInt8                  ALIAS if(has_error OR toUInt16OrZero(response_status_code) >= 400, 1, 0),
    is_root                  UInt8                  ALIAS if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0),

    INDEX idx_error_group_id error_group_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_query_hash query_hash TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/spans', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, fingerprint, trace_id, span_id)
TTL timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'gcs_only',
    index_granularity = 8192,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;

-- Exact, narrow error-span projection. Error explorer queries keep occurrence,
-- grouping, trace, exception, and arbitrary-attribute semantics without
-- scanning successful spans.

CREATE TABLE IF NOT EXISTS optikk.error_events (
    tenant_id            UInt32                 CODEC(T64, ZSTD(1)),
    timestamp            DateTime64(9)          CODEC(DoubleDelta, LZ4),
    error_group_id       String                 CODEC(ZSTD(1)),
    trace_id             String                 CODEC(ZSTD(1)),
    span_id              String                 CODEC(ZSTD(1)),
    duration_nano        UInt64                 CODEC(T64, ZSTD(1)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    service_version      LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    name                 LowCardinality(String) CODEC(ZSTD(1)),
    kind_string          LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string   LowCardinality(String) CODEC(ZSTD(1)),
    status_message       String                 CODEC(ZSTD(1)),
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    http_route           LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket   LowCardinality(String) CODEC(ZSTD(1)),
    peer_service         LowCardinality(String) CODEC(ZSTD(1)),
    exception_type       LowCardinality(String) CODEC(ZSTD(1)),
    exception_message    String                 CODEC(ZSTD(1)),
    exception_stacktrace String                 CODEC(ZSTD(1)),
    attributes           Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    is_error             UInt8                  CODEC(T64, ZSTD(1)),

    INDEX idx_error_group_id error_group_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/error_events', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, error_group_id, trace_id, span_id)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_error_events
TO optikk.error_events AS
SELECT
    tenant_id, timestamp, error_group_id, trace_id, span_id, duration_nano,
    service, service_version, environment, host, pod, name, kind_string,
    status_code_string, status_message, http_method, http_route,
    response_status_code, http_status_bucket, peer_service, exception_type,
    exception_message, exception_stacktrace, attributes, is_error
FROM optikk.spans
WHERE is_error = 1;

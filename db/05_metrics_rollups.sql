-- Metric pre-aggregation cascade: raw metrics roll up 1m -> 5m -> 1h.
-- Dimensions are carried with each fingerprint so queries do not join the
-- metrics_series metadata table on the read path.

CREATE TABLE IF NOT EXISTS optikk.metrics_1m_v2 (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    metric_name LowCardinality(String),
    fingerprint UInt64 CODEC(ZSTD(1)),
    timestamp   DateTime CODEC(DoubleDelta, LZ4),

    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),

    pod             SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    container       SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_namespace   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    environment     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    temporality     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    attributes      SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    cloud_provider  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_account   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_region    SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_platform  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_node        SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),

    val_last    AggregateFunction(argMax, Float64, DateTime) CODEC(ZSTD(1)),
    val_min     SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max     SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    val_sum     SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count   SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    hist_sum    SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count  SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_1m_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, metric_name, timestamp, service, host, fingerprint)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 7 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_1m_v2_mv
TO optikk.metrics_1m_v2 AS
SELECT
    tenant_id,
    metric_name,
    fingerprint,
    toStartOfMinute(timestamp) AS timestamp,
    service,
    host,
    any(pod)             AS pod,
    any(container)       AS container,
    any(k8s_namespace)   AS k8s_namespace,
    any(environment)     AS environment,
    any(temporality)     AS temporality,
    any(attributes)      AS attributes,
    any(cloud_provider)  AS cloud_provider,
    any(cloud_account)   AS cloud_account,
    any(cloud_region)    AS cloud_region,
    any(cloud_platform)  AS cloud_platform,
    any(k8s_node)        AS k8s_node,
    argMaxState(value, timestamp) AS val_last,
    min(value)     AS val_min,
    max(value)     AS val_max,
    sum(value)     AS val_sum,
    count()        AS val_count,
    sum(hist_sum)   AS hist_sum,
    sum(hist_count) AS hist_count,
    quantilesPrometheusHistogramArrayState(0.5, 0.95, 0.99)(
        if(length(hist_counts) = length(hist_buckets) + 1, arrayPushBack(hist_buckets, inf), emptyArrayFloat64()),
        if(length(hist_counts) = length(hist_buckets) + 1, arrayCumSum(hist_counts), emptyArrayUInt64())
    ) AS latency_state
FROM optikk.metrics
GROUP BY tenant_id, metric_name, fingerprint, timestamp, service, host;

CREATE TABLE IF NOT EXISTS optikk.metrics_5m_v2 (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    metric_name LowCardinality(String),
    fingerprint UInt64 CODEC(ZSTD(1)),
    timestamp   DateTime CODEC(DoubleDelta, LZ4),

    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),

    pod             SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    container       SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_namespace   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    environment     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    temporality     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    attributes      SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    cloud_provider  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_account   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_region    SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_platform  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_node        SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),

    val_last    AggregateFunction(argMax, Float64, DateTime) CODEC(ZSTD(1)),
    val_min     SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max     SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    val_sum     SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count   SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    hist_sum    SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count  SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_5m_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, metric_name, timestamp, service, host, fingerprint)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 14 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_5m_v2_mv
TO optikk.metrics_5m_v2 AS
SELECT
    tenant_id,
    metric_name,
    fingerprint,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    service,
    host,
    any(pod)             AS pod,
    any(container)       AS container,
    any(k8s_namespace)   AS k8s_namespace,
    any(environment)     AS environment,
    any(temporality)     AS temporality,
    any(attributes)      AS attributes,
    any(cloud_provider)  AS cloud_provider,
    any(cloud_account)   AS cloud_account,
    any(cloud_region)    AS cloud_region,
    any(cloud_platform)  AS cloud_platform,
    any(k8s_node)        AS k8s_node,
    argMaxMergeState(val_last) AS val_last,
    min(val_min)      AS val_min,
    max(val_max)      AS val_max,
    sum(val_sum)      AS val_sum,
    sum(val_count)    AS val_count,
    sum(hist_sum)     AS hist_sum,
    sum(hist_count)   AS hist_count,
    quantilesPrometheusHistogramMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.metrics_1m_v2
GROUP BY tenant_id, metric_name, fingerprint, timestamp, service, host;

CREATE TABLE IF NOT EXISTS optikk.metrics_1h_v2 (
    tenant_id     UInt32 CODEC(T64, ZSTD(1)),
    metric_name LowCardinality(String),
    fingerprint UInt64 CODEC(ZSTD(1)),
    timestamp   DateTime CODEC(DoubleDelta, LZ4),

    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),

    pod             SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    container       SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_namespace   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    environment     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    temporality     SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    attributes      SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    cloud_provider  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_account   SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_region    SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    cloud_platform  SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),
    k8s_node        SimpleAggregateFunction(any, LowCardinality(String)) CODEC(ZSTD(1)),

    val_last    AggregateFunction(argMax, Float64, DateTime) CODEC(ZSTD(1)),
    val_min     SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max     SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    val_sum     SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count   SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    hist_sum    SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count  SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/metrics_1h_v2', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, metric_name, timestamp, service, host, fingerprint)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.metrics_1h_v2_mv
TO optikk.metrics_1h_v2 AS
SELECT
    tenant_id,
    metric_name,
    fingerprint,
    toStartOfHour(timestamp) AS timestamp,
    service,
    host,
    any(pod)             AS pod,
    any(container)       AS container,
    any(k8s_namespace)   AS k8s_namespace,
    any(environment)     AS environment,
    any(temporality)     AS temporality,
    any(attributes)      AS attributes,
    any(cloud_provider)  AS cloud_provider,
    any(cloud_account)   AS cloud_account,
    any(cloud_region)    AS cloud_region,
    any(cloud_platform)  AS cloud_platform,
    any(k8s_node)        AS k8s_node,
    argMaxMergeState(val_last) AS val_last,
    min(val_min)      AS val_min,
    max(val_max)      AS val_max,
    sum(val_sum)      AS val_sum,
    sum(val_count)    AS val_count,
    sum(hist_sum)     AS hist_sum,
    sum(hist_count)   AS hist_count,
    quantilesPrometheusHistogramMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.metrics_5m_v2
GROUP BY tenant_id, metric_name, fingerprint, timestamp, service, host;

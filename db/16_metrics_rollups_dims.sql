-- Dimensional metrics rollup cascade, replacing the fingerprint-only tables in
-- 07_metrics_rollups.sql. Row counts are unchanged: every dimension is
-- functionally determined by (metric_name, fingerprint), already in the key.
-- service/host join the sort key to prune; the rest are constant per
-- fingerprint, so SimpleAggregateFunction(any). fingerprint stays as the series
-- identity the cumulative counter-reset window partitions by.

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
    attributes      SimpleAggregateFunction(any, Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
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
    attributes      SimpleAggregateFunction(any, Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
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
    attributes      SimpleAggregateFunction(any, Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
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

-- CUTOVER — one-time, not idempotent. Both cascades dual-write until step 3.
--
-- 1. Backfill per day partition, oldest first. Raw metrics has a 2 DAY TTL, so
--    only 5m/1h need this, sourced from the old rollup joined to metrics_series.
--    Safe only because service/host are in the v2 sort key.
--
--      INSERT INTO optikk.metrics_5m_v2
--      SELECT m.tenant_id, m.metric_name, m.fingerprint, m.timestamp,
--             s.service, s.host, s.pod, s.container, s.k8s_namespace,
--             s.environment, s.temporality, s.attributes,
--             s.resource_attributes['cloud.provider'],
--             s.resource_attributes['cloud.account.id'],
--             if(s.resource_attributes['cloud.region'] != '',
--                s.resource_attributes['cloud.region'],
--                s.resource_attributes['aws.region']),
--             s.resource_attributes['cloud.platform'],
--             s.resource_attributes['k8s.node.name'],
--             m.val_last, m.val_min, m.val_max, m.val_sum, m.val_count,
--             m.hist_sum, m.hist_count, m.latency_state
--      FROM optikk.metrics_5m AS m
--      INNER JOIN (
--          SELECT fingerprint, argMax(service, timestamp) AS service, ...
--          FROM optikk.metrics_series GROUP BY fingerprint
--      ) AS s ON m.fingerprint = s.fingerprint
--      WHERE m.timestamp >= '<DAY>' AND m.timestamp < '<DAY+1>';
--
-- 2. Reconcile per partition; both must match:
--      SELECT count(), sum(val_count) FROM optikk.metrics_5m    WHERE ...;
--      SELECT count(), sum(val_count) FROM optikk.metrics_5m_v2 WHERE ...;
--    And check the k8s.pod.uid fallback lost no labels (uid is not in the
--    fingerprint, so pod is not determined by it):
--      SELECT count() FROM optikk.metrics_5m_v2 WHERE pod = '' AND k8s_node != '';
--
-- 3. Deploy query, which reads the _v2 names (timebucket.RollupTableForGrain).
--    No RENAME — it would orphan the v2 MVs' TO targets. Rollback = revert deploy.
--
-- 4. After a soak, drop optikk.metrics_{1m,5m,1h}_mv then the three old tables.

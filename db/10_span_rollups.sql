-- RED span-stat cascade: spans roll up 1m -> 5m -> 1h with every APM
-- dimension as a real column, replacing the Go spanmetrics aggregator and the
-- fingerprint join against metrics_series (same pattern as llm_stats_1m).
-- Latency is a mergeable tDigest state in milliseconds; error counting stays a
-- read-time predicate on status_code_string / http_status_bucket so readers
-- keep their own error semantics.
--
-- Like the Go aggregator it replaces, the MV aggregates every span (no kind
-- filter); readers narrow by kind_string where needed.
--
-- http_method/rpc_system carry the call's protocol verb so readers can label an
-- endpoint without parsing span_name, which only encodes the method when the
-- instrumentation knew the route.
--
-- peer_name/peer_type carry the client-side call target, which makes this
-- cascade the sole source for the service graph (topology.GetEdges) and
-- replaces the paired CLIENT/SERVER service_graph_edges_1m table. Edge latency
-- and errors are therefore client-observed.

CREATE TABLE IF NOT EXISTS optikk.span_stats_1m (
    tenant_id                UInt32                 CODEC(T64, ZSTD(1)),
    timestamp                DateTime               CODEC(DoubleDelta, LZ4),
    service                  LowCardinality(String) CODEC(ZSTD(1)),
    environment              LowCardinality(String) CODEC(ZSTD(1)),
    host                     LowCardinality(String) CODEC(ZSTD(1)),
    pod                      LowCardinality(String) CODEC(ZSTD(1)),
    span_name                LowCardinality(String) CODEC(ZSTD(1)),
    kind_string              LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string       LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket       LowCardinality(String) CODEC(ZSTD(1)),
    http_route               LowCardinality(String) CODEC(ZSTD(1)),
    http_method              LowCardinality(String) CODEC(ZSTD(1)),
    rpc_system               LowCardinality(String) CODEC(ZSTD(1)),
    db_system                LowCardinality(String) CODEC(ZSTD(1)),
    messaging_system         LowCardinality(String) CODEC(ZSTD(1)),
    messaging_destination    LowCardinality(String) CODEC(ZSTD(1)),
    messaging_consumer_group LowCardinality(String) CODEC(ZSTD(1)),
    cloud_provider           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_platform           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_region             LowCardinality(String) CODEC(ZSTD(1)),
    k8s_node                 LowCardinality(String) CODEC(ZSTD(1)),
    peer_name                LowCardinality(String) CODEC(ZSTD(1)),
    peer_type                LowCardinality(String) CODEC(ZSTD(1)),
    request_count            SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    duration_ms_sum          SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    latency_state            AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/span_stats_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, service, span_name, kind_string, status_code_string,
          http_status_bucket, http_route, db_system, messaging_system,
          messaging_destination, messaging_consumer_group, environment, host, pod,
          cloud_provider, cloud_platform, cloud_region, k8s_node,
          peer_name, peer_type, http_method, rpc_system)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 7 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_span_stats_1m
TO optikk.span_stats_1m AS
SELECT
    tenant_id,
    toStartOfMinute(timestamp) AS timestamp,
    service,
    environment,
    host,
    pod,
    name AS span_name,
    kind_string,
    status_code_string,
    http_status_bucket,
    http_route,
    http_method,
    attributes['rpc.system']                    AS rpc_system,
    db_system,
    attributes['messaging.system']              AS messaging_system,
    attributes['messaging.destination.name']    AS messaging_destination,
    attributes['messaging.consumer.group.name'] AS messaging_consumer_group,
    attributes['cloud.provider']                AS cloud_provider,
    attributes['cloud.platform']                AS cloud_platform,
    if(attributes['cloud.region'] != '', attributes['cloud.region'], attributes['aws.region']) AS cloud_region,
    attributes['k8s.node.name']                 AS k8s_node,
    -- Client-side peer for the service graph; empty on server-side kinds so
    -- SERVER/CONSUMER rows do not fan out by peer.
    if(kind_string IN ('CLIENT', 'PRODUCER'),
       multiIf(peer_service != '',                             peer_service,
               db_system != '',                                db_system,
               db_name != '',                                  db_name,
               attributes['server.address'] != '',             attributes['server.address'],
               attributes['messaging.destination.name'] != '', attributes['messaging.destination.name'],
               ''), '')                             AS peer_name,
    if(kind_string IN ('CLIENT', 'PRODUCER'),
       multiIf(peer_service != '',                             '',
               db_system != '',                                'database',
               db_name != '',                                  'database',
               attributes['server.address'] != '',             '',
               attributes['messaging.destination.name'] != '', 'messaging',
               ''), '')                             AS peer_type,
    count()                                     AS request_count,
    sum(duration_nano / 1000000.0)              AS duration_ms_sum,
    quantilesTDigestState(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS latency_state
FROM optikk.spans
GROUP BY tenant_id, timestamp, service, environment, host, pod, span_name,
         kind_string, status_code_string, http_status_bucket, http_route,
         http_method, rpc_system,
         db_system, messaging_system, messaging_destination, messaging_consumer_group,
         cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type;

CREATE TABLE IF NOT EXISTS optikk.span_stats_5m (
    tenant_id                UInt32                 CODEC(T64, ZSTD(1)),
    timestamp                DateTime               CODEC(DoubleDelta, LZ4),
    service                  LowCardinality(String) CODEC(ZSTD(1)),
    environment              LowCardinality(String) CODEC(ZSTD(1)),
    host                     LowCardinality(String) CODEC(ZSTD(1)),
    pod                      LowCardinality(String) CODEC(ZSTD(1)),
    span_name                LowCardinality(String) CODEC(ZSTD(1)),
    kind_string              LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string       LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket       LowCardinality(String) CODEC(ZSTD(1)),
    http_route               LowCardinality(String) CODEC(ZSTD(1)),
    http_method              LowCardinality(String) CODEC(ZSTD(1)),
    rpc_system               LowCardinality(String) CODEC(ZSTD(1)),
    db_system                LowCardinality(String) CODEC(ZSTD(1)),
    messaging_system         LowCardinality(String) CODEC(ZSTD(1)),
    messaging_destination    LowCardinality(String) CODEC(ZSTD(1)),
    messaging_consumer_group LowCardinality(String) CODEC(ZSTD(1)),
    cloud_provider           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_platform           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_region             LowCardinality(String) CODEC(ZSTD(1)),
    k8s_node                 LowCardinality(String) CODEC(ZSTD(1)),
    peer_name                LowCardinality(String) CODEC(ZSTD(1)),
    peer_type                LowCardinality(String) CODEC(ZSTD(1)),
    request_count            SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    duration_ms_sum          SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    latency_state            AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/span_stats_5m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, service, span_name, kind_string, status_code_string,
          http_status_bucket, http_route, db_system, messaging_system,
          messaging_destination, messaging_consumer_group, environment, host, pod,
          cloud_provider, cloud_platform, cloud_region, k8s_node,
          peer_name, peer_type, http_method, rpc_system)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 14 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.span_stats_5m_mv
TO optikk.span_stats_5m AS
SELECT
    tenant_id,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    service, environment, host, pod, span_name, kind_string, status_code_string,
    http_status_bucket, http_route, http_method, rpc_system, db_system, messaging_system,
    messaging_destination, messaging_consumer_group,
    cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type,
    sum(request_count)   AS request_count,
    sum(duration_ms_sum) AS duration_ms_sum,
    quantilesTDigestMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.span_stats_1m
GROUP BY tenant_id, timestamp, service, environment, host, pod, span_name,
         kind_string, status_code_string, http_status_bucket, http_route,
         http_method, rpc_system,
         db_system, messaging_system, messaging_destination, messaging_consumer_group,
         cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type;

CREATE TABLE IF NOT EXISTS optikk.span_stats_1h (
    tenant_id                UInt32                 CODEC(T64, ZSTD(1)),
    timestamp                DateTime               CODEC(DoubleDelta, LZ4),
    service                  LowCardinality(String) CODEC(ZSTD(1)),
    environment              LowCardinality(String) CODEC(ZSTD(1)),
    host                     LowCardinality(String) CODEC(ZSTD(1)),
    pod                      LowCardinality(String) CODEC(ZSTD(1)),
    span_name                LowCardinality(String) CODEC(ZSTD(1)),
    kind_string              LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string       LowCardinality(String) CODEC(ZSTD(1)),
    http_status_bucket       LowCardinality(String) CODEC(ZSTD(1)),
    http_route               LowCardinality(String) CODEC(ZSTD(1)),
    http_method              LowCardinality(String) CODEC(ZSTD(1)),
    rpc_system               LowCardinality(String) CODEC(ZSTD(1)),
    db_system                LowCardinality(String) CODEC(ZSTD(1)),
    messaging_system         LowCardinality(String) CODEC(ZSTD(1)),
    messaging_destination    LowCardinality(String) CODEC(ZSTD(1)),
    messaging_consumer_group LowCardinality(String) CODEC(ZSTD(1)),
    cloud_provider           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_platform           LowCardinality(String) CODEC(ZSTD(1)),
    cloud_region             LowCardinality(String) CODEC(ZSTD(1)),
    k8s_node                 LowCardinality(String) CODEC(ZSTD(1)),
    peer_name                LowCardinality(String) CODEC(ZSTD(1)),
    peer_type                LowCardinality(String) CODEC(ZSTD(1)),
    request_count            SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    duration_ms_sum          SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    latency_state            AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/span_stats_1h', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, service, span_name, kind_string, status_code_string,
          http_status_bucket, http_route, db_system, messaging_system,
          messaging_destination, messaging_consumer_group, environment, host, pod,
          cloud_provider, cloud_platform, cloud_region, k8s_node,
          peer_name, peer_type, http_method, rpc_system)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.span_stats_1h_mv
TO optikk.span_stats_1h AS
SELECT
    tenant_id,
    toStartOfHour(timestamp) AS timestamp,
    service, environment, host, pod, span_name, kind_string, status_code_string,
    http_status_bucket, http_route, http_method, rpc_system, db_system, messaging_system,
    messaging_destination, messaging_consumer_group,
    cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type,
    sum(request_count)   AS request_count,
    sum(duration_ms_sum) AS duration_ms_sum,
    quantilesTDigestMergeState(0.5, 0.95, 0.99)(latency_state) AS latency_state
FROM optikk.span_stats_5m
GROUP BY tenant_id, timestamp, service, environment, host, pod, span_name,
         kind_string, status_code_string, http_status_bucket, http_route,
         http_method, rpc_system,
         db_system, messaging_system, messaging_destination, messaging_consumer_group,
         cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type;

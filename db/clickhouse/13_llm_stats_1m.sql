-- 1-minute rollup of GenAI spans. Powers LLM Observability aggregates
-- (apps table, KPIs, token/latency/cost charts) without scanning raw spans.
-- Latency kept in ms as tDigest state; cost is derived at query time from tokens.

CREATE TABLE IF NOT EXISTS optikk.llm_stats_1m (
    tenant_id              UInt32 CODEC(T64, ZSTD(1)),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_system        LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_request_model LowCardinality(String) CODEC(ZSTD(1)),
    gen_ai_operation     LowCardinality(String) CODEC(ZSTD(1)),
    span_count           SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    error_count          SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    input_tokens         SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    output_tokens        SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    latency_state        AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float64) CODEC(ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/llm_stats_1m', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, service, environment, gen_ai_system, gen_ai_request_model, gen_ai_operation)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_llm_stats_1m
TO optikk.llm_stats_1m AS
SELECT
    tenant_id,
    toStartOfMinute(timestamp) AS timestamp,
    service,
    environment,
    gen_ai_system,
    gen_ai_request_model,
    gen_ai_operation,
    count() AS span_count,
    countIf(has_error) AS error_count,
    sum(gen_ai_input_tokens) AS input_tokens,
    sum(gen_ai_output_tokens) AS output_tokens,
    quantilesTDigestState(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS latency_state
FROM optikk.spans
WHERE is_gen_ai
GROUP BY tenant_id, timestamp, service, environment, gen_ai_system, gen_ai_request_model, gen_ai_operation;

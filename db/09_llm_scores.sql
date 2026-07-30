-- Evaluation scores attached to LLM traces/spans, populated by the ingest
-- score sink (from gen_ai.evaluation.result / langfuse.score span events) and
-- by the query POST /llm/scores API (human + programmatic scores).
-- One row = one score on one trace (optionally one span).

CREATE TABLE IF NOT EXISTS optikk.llm_scores (
    tenant_id    UInt32          CODEC(T64, ZSTD(1)),
    timestamp    DateTime64(3)   CODEC(DoubleDelta, LZ4),
    trace_id     String          CODEC(ZSTD(1)),
    span_id      String          CODEC(ZSTD(1)),
    session_id   String          CODEC(ZSTD(1)),
    user_id      String          CODEC(ZSTD(1)),
    service      LowCardinality(String) CODEC(ZSTD(1)),
    environment  LowCardinality(String) CODEC(ZSTD(1)),
    name         LowCardinality(String) CODEC(ZSTD(1)),
    -- otel | api | human | eval  (eval == an automated judge run; reserved).
    source       LowCardinality(String) CODEC(ZSTD(1)),
    -- numeric | boolean | categorical.
    data_type    LowCardinality(String) CODEC(ZSTD(1)),
    value        Float64         CODEC(ZSTD(1)),
    string_value String          CODEC(ZSTD(1)),
    comment      String          CODEC(ZSTD(1)),
    evaluator_id UInt64          CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/llm_scores', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, name, timestamp, trace_id)
TTL
    toDateTime(timestamp) + INTERVAL 3 DAY TO VOLUME 'main',
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

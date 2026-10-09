-- GenAI span index: a narrow copy of every GenAI span, powering the LLM
-- trace list, sessions, users and trace counts. Those reads filtered raw
-- spans on is_gen_ai (not in the sort key) and decoded up-to-16KiB prompts;
-- here they scan GenAI rows only, with a short prompt preview.
--
-- Column names match optikk.spans so span-level SQL fragments (e.g. the
-- pricing cost expression) apply unchanged. Full prompt/completion text stays
-- on spans and is fetched by trace_id.

CREATE TABLE IF NOT EXISTS optikk.llm_spans (
    tenant_id             UInt32                        CODEC(T64, ZSTD(1)),
    timestamp             DateTime64(9)                 CODEC(DoubleDelta, LZ4),
    trace_id              String                        CODEC(ZSTD(1)),
    span_id               String                        CODEC(ZSTD(1)),
    duration_nano         UInt64                        CODEC(T64, ZSTD(1)),
    service               LowCardinality(String)        CODEC(ZSTD(1)),
    environment           LowCardinality(String)        CODEC(ZSTD(1)),
    has_error             Bool                          CODEC(T64, ZSTD(1)),
    gen_ai_system         LowCardinality(String)        CODEC(ZSTD(1)),
    gen_ai_operation      LowCardinality(String)        CODEC(ZSTD(1)),
    gen_ai_request_model  LowCardinality(String)        CODEC(ZSTD(1)),
    gen_ai_input_tokens   UInt64                        CODEC(T64, ZSTD(1)),
    gen_ai_output_tokens  UInt64                        CODEC(T64, ZSTD(1)),
    llm_user_id           String                        CODEC(ZSTD(1)),
    llm_session_id        String                        CODEC(ZSTD(1)),
    llm_tags              Array(LowCardinality(String)) CODEC(ZSTD(1)),
    prompt_preview        String                        CODEC(ZSTD(1)),

    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_session llm_session_id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/optikk/llm_spans', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (tenant_id, timestamp, trace_id, span_id)
TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_llm_spans
TO optikk.llm_spans AS
SELECT
    tenant_id,
    timestamp,
    trace_id,
    span_id,
    duration_nano,
    service,
    environment,
    has_error,
    gen_ai_system,
    gen_ai_operation,
    gen_ai_request_model,
    gen_ai_input_tokens,
    gen_ai_output_tokens,
    llm_user_id,
    llm_session_id,
    llm_tags,
    leftUTF8(gen_ai_prompt, 160) AS prompt_preview
FROM optikk.spans
WHERE is_gen_ai;

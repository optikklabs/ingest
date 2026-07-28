                                                                        
                                                                   
                                                                    
                                                                          
                                                                              
                                                

CREATE TABLE IF NOT EXISTS optikk.llm_traces (
    tenant_id     UInt32          CODEC(T64, ZSTD(1)),
    trace_date    Date            CODEC(DoubleDelta, LZ4),
    trace_id      String          CODEC(ZSTD(1)),
    start_time    SimpleAggregateFunction(min, DateTime64(9)) CODEC(DoubleDelta, LZ4),
    end_time      SimpleAggregateFunction(max, DateTime64(9)) CODEC(DoubleDelta, LZ4),
    trace_name    SimpleAggregateFunction(anyLast, String)               CODEC(ZSTD(1)),
    service       SimpleAggregateFunction(anyLast, LowCardinality(String)) CODEC(ZSTD(1)),
    environment   SimpleAggregateFunction(anyLast, LowCardinality(String)) CODEC(ZSTD(1)),
    user_id       SimpleAggregateFunction(anyLast, String)               CODEC(ZSTD(1)),
    session_id    SimpleAggregateFunction(anyLast, String)               CODEC(ZSTD(1)),
    release       SimpleAggregateFunction(anyLast, String)               CODEC(ZSTD(1)),
    tags          SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1)),
    models        SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1)),
    systems       SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1)),
    obs_count     SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    genai_count   SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    error_count   SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    input_tokens  SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    output_tokens SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1))
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/optikk/llm_traces', '{replica}')
PARTITION BY toYYYYMM(trace_date)
ORDER BY (tenant_id, trace_date, trace_id)
TTL
    toDateTime(trace_date) + INTERVAL 90 DAY DELETE
SETTINGS
    storage_policy = 'tiered',
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS optikk.spans_to_llm_traces
TO optikk.llm_traces AS
SELECT
    tenant_id,
    toDate(timestamp) AS trace_date,
    trace_id,
    min(timestamp) AS start_time,
    max(timestamp + toIntervalNanosecond(duration_nano)) AS end_time,
    anyLastIf(name, parent_span_id = '') AS trace_name,
    anyLast(service) AS service,
    anyLast(environment) AS environment,
    anyLastIf(llm_user_id, llm_user_id != '') AS user_id,
    anyLastIf(llm_session_id, llm_session_id != '') AS session_id,
    anyLastIf(llm_release, llm_release != '') AS release,
    groupUniqArrayArray(llm_tags) AS tags,
    groupUniqArrayArrayIf([gen_ai_request_model], gen_ai_request_model != '') AS models,
    groupUniqArrayArrayIf([gen_ai_system], gen_ai_system != '') AS systems,
    count() AS obs_count,
    countIf(is_gen_ai) AS genai_count,
    countIf(has_error) AS error_count,
    sum(gen_ai_input_tokens) AS input_tokens,
    sum(gen_ai_output_tokens) AS output_tokens
FROM optikk.spans
WHERE is_gen_ai OR llm_session_id != '' OR llm_user_id != ''
GROUP BY tenant_id, trace_date, trace_id;

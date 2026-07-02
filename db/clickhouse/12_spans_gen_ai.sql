-- GenAI (LLM) span columns promoted from OTel gen_ai.* semconv attributes.
-- is_gen_ai marks spans carrying gen_ai attrs; feeds the llm_stats_1m rollup.
ALTER TABLE optikk.spans
    ADD COLUMN IF NOT EXISTS gen_ai_system         LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS gen_ai_operation      LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS gen_ai_request_model  LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS gen_ai_response_model LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS gen_ai_input_tokens   UInt64 CODEC(T64, ZSTD(1)),
    ADD COLUMN IF NOT EXISTS gen_ai_output_tokens  UInt64 CODEC(T64, ZSTD(1)),
    ADD COLUMN IF NOT EXISTS is_gen_ai             Bool   CODEC(T64, ZSTD(1));

-- LLM Observability context columns on the raw spans table.
-- Additive only (ADD COLUMN IF NOT EXISTS) so it is safe to re-apply and safe
-- on the live cluster; existing rows read these as empty/zero defaults.
-- Promoted from gen_ai / langfuse span attributes by the ingest mapper and
-- consumed by the llm_traces / llm_scores rollups and the query LLM module.

ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_user_id        String                        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_session_id     String                        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_tags           Array(LowCardinality(String)) CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_release        LowCardinality(String)        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_prompt_name    LowCardinality(String)        CODEC(ZSTD(1));
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS llm_prompt_version UInt32                        CODEC(T64, ZSTD(1));
-- span | generation | event | eval — the Langfuse-style observation kind.
ALTER TABLE optikk.spans ADD COLUMN IF NOT EXISTS gen_ai_span_kind   LowCardinality(String)        CODEC(ZSTD(1));

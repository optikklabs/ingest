-- Promoted LLM content, capped at 16KiB by the ingest mapper.
ALTER TABLE optikk.spans
    ADD COLUMN IF NOT EXISTS gen_ai_prompt     String CODEC(ZSTD(2)),
    ADD COLUMN IF NOT EXISTS gen_ai_completion String CODEC(ZSTD(2));

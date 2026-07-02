-- Stable per-statement identity for the Query Detail page.
-- MATERIALIZED: computed at insert for new parts, on-the-fly for old parts.
ALTER TABLE optikk.spans
    ADD COLUMN IF NOT EXISTS db_statement_normalized String
        MATERIALIZED if(db_statement = '', '', normalizeQuery(db_statement)) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS query_hash String
        MATERIALIZED if(db_statement = '', '', lower(hex(normalizedQueryHash(db_statement)))) CODEC(ZSTD(1));

ALTER TABLE optikk.spans
    ADD INDEX IF NOT EXISTS idx_query_hash query_hash TYPE bloom_filter GRANULARITY 1;

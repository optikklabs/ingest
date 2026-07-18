-- body_search duplicated every log body solely for its text index. Queries
-- already search body, so index it directly and reclaim the duplicate column.
ALTER TABLE optikk.logs DROP INDEX IF EXISTS idx_body_text;

ALTER TABLE optikk.logs ADD INDEX idx_body_text body TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lowerUTF8(body)) GRANULARITY 1;

ALTER TABLE optikk.logs DROP COLUMN IF EXISTS body_search;

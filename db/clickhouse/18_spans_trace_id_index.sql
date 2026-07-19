-- trace_id had no index, so every trace lookup had to be bounded by a caller
-- supplied time window. Index it so a trace can be fetched by identity alone.
ALTER TABLE optikk.spans ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE optikk.spans MATERIALIZE INDEX idx_trace_id;

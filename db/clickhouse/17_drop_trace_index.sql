-- Existing installations may already have the trace index and its writer.
-- Query services no longer read this table, so remove the writer before the
-- destination table. Fresh installations only execute these idempotent drops.
DROP VIEW IF EXISTS optikk.spans_to_trace_index;

DROP TABLE IF EXISTS optikk.trace_index;

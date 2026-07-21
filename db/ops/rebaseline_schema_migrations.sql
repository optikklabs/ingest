-- One-off rebaseline for the 17 -> 10 DDL consolidation.
--
-- The migrator (internal/infra/database/migrate.go) tracks applied migrations
-- by *filename*. Consolidation renamed files, so on an existing cluster the new
-- names are absent from schema_migrations and would re-run. Every consolidated
-- statement is idempotent, so that re-run is a harmless no-op -- but pre-seeding
-- the ledger avoids it entirely and keeps the record clean.
--
-- Run ONCE against each existing cluster BEFORE deploying the consolidated-schema
-- build, while the DB is already at end-state (all old 00..18 applied). Fresh
-- installs must NOT run this -- their migrator populates the ledger normally.
--
-- Idempotent: inserts only names not already recorded, so re-running is safe.
--   clickhouse-client -n < db/ops/rebaseline_schema_migrations.sql

INSERT INTO optikk.schema_migrations (version)
SELECT version
FROM (
    SELECT arrayJoin([
        '00_database.sql',
        '01_spans.sql',
        '02_spans_resource.sql',
        '03_logs.sql',
        '04_logs_resource.sql',
        '05_metrics.sql',
        '06_metrics_series.sql',
        '07_metrics_rollups.sql',
        '08_llm_rollups.sql',
        '09_ingestion_stats.sql'
    ]) AS version
)
WHERE version NOT IN (SELECT version FROM optikk.schema_migrations);

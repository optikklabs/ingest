-- Move telemetry tables from gcs_only (GCS as the hot write path) to the
-- tiered policy: inserts and merges land on the local disk, parts demote to
-- GCS after 3 days. Under gcs_only every insert round-tripped objects to GCS
-- and staged metadata locally, and the gcs disk reports 16 EiB free, so
-- ClickHouse had no back-pressure and the PVC filled to ENOSPC (2026-07-15).
--
-- The tiered policy keeps a volume named 'main' holding gcs_cache because
-- ALTER ... MODIFY SETTING storage_policy requires the new policy to contain
-- the old policy's volumes by name. Existing parts stay on GCS; only new
-- inserts land hot, so this migration moves no data.
--
-- MODIFY TTL replaces the whole TTL expression, so each DELETE rule is
-- restated alongside its move rule. Dropping one here silently disables
-- retention.

ALTER TABLE optikk.spans MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.spans MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE;

ALTER TABLE optikk.logs MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.logs MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 15 DAY DELETE;

ALTER TABLE optikk.logs_resource MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.logs_resource MODIFY TTL
    toDateTime(ts_bucket) + INTERVAL 3 DAY TO VOLUME 'main',
    toDateTime(ts_bucket) + INTERVAL 15 DAY DELETE;

ALTER TABLE optikk.spans_resource MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.spans_resource MODIFY TTL
    last_seen + INTERVAL 3 DAY TO VOLUME 'main',
    last_seen + INTERVAL 15 DAY DELETE;

ALTER TABLE optikk.trace_index MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.trace_index MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE;

ALTER TABLE optikk.metrics_series MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.metrics_series MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE;

ALTER TABLE optikk.metrics_1m MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.metrics_1m MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 7 DAY DELETE;

ALTER TABLE optikk.metrics_5m MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.metrics_5m MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 14 DAY DELETE;

ALTER TABLE optikk.metrics_1h MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.metrics_1h MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 30 DAY DELETE;

ALTER TABLE optikk.llm_stats_1m MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.llm_stats_1m MODIFY TTL
    timestamp + INTERVAL 3 DAY TO VOLUME 'main',
    timestamp + INTERVAL 90 DAY DELETE;

-- Raw metrics expire at 2 days, before any move rule could fire. Hot only.
ALTER TABLE optikk.metrics MODIFY SETTING storage_policy = 'tiered';

-- Monthly partitions, so moves are month-granular. 30 days keeps at most two
-- partitions hot; the rest of the 400-day retention lives on GCS.
ALTER TABLE optikk.ingestion_stats MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE optikk.ingestion_stats MODIFY TTL
    bucket_hour + INTERVAL 30 DAY TO VOLUME 'main',
    bucket_hour + INTERVAL 400 DAY DELETE;

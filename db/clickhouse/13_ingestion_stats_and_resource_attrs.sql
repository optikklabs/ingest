-- Host-scoped resource attributes (os.*, host.*, cloud.*, k8s node/cluster)
-- retained per series for host metadata panels. Allowlisted at ingest.
ALTER TABLE optikk.metrics_series
    ADD COLUMN IF NOT EXISTS resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1));

ALTER TABLE optikk.ingestion_stats
    MODIFY SETTING
        replicated_deduplication_window = 1000,
        replicated_deduplication_window_seconds = 86400;

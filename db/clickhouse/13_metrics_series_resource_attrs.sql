-- Host-scoped resource attributes (os.*, host.*, cloud.*, k8s node/cluster)
-- retained per series for host metadata panels. Allowlisted at ingest.
ALTER TABLE optikk.metrics_series
    ADD COLUMN IF NOT EXISTS resource_attributes JSON(max_dynamic_paths=100) CODEC(ZSTD(1));

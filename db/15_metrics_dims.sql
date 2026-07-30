-- Series dimensions on raw metrics, promoted by the ingest mapper so the rollup
-- MVs can group on real columns instead of joining metrics_series.

ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS service         LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS host            LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS pod             LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS container       LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS k8s_namespace   LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS environment     LowCardinality(String) CODEC(ZSTD(1));

-- Verbatim datapoint attributes, matching metrics_series so arbitrary-label
-- group-by in the metrics explorer keeps working.
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS attributes Map(LowCardinality(String), String) CODEC(ZSTD(1));

-- Resource attributes, not datapoint ones.
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS cloud_provider  LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS cloud_account   LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS cloud_region    LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS cloud_platform  LowCardinality(String) CODEC(ZSTD(1));
ALTER TABLE optikk.metrics ADD COLUMN IF NOT EXISTS k8s_node        LowCardinality(String) CODEC(ZSTD(1));

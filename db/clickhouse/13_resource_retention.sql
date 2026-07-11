-- Existing resource tables retain their current partitioning. Rebuilding
-- logs_resource into date partitions is an operational migration because it
-- cannot be changed in place without interrupting concurrent writers.

ALTER TABLE optikk.logs_resource
    MODIFY TTL toDateTime(ts_bucket) + INTERVAL 15 DAY DELETE;

ALTER TABLE optikk.spans_resource
    ADD COLUMN IF NOT EXISTS last_seen DateTime DEFAULT now() CODEC(DoubleDelta, LZ4);

ALTER TABLE optikk.spans_resource
    MODIFY TTL last_seen + INTERVAL 15 DAY DELETE;

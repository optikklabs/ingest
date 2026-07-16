ALTER TABLE optikk.ingestion_stats
    MODIFY SETTING
        replicated_deduplication_window = 1000,
        replicated_deduplication_window_seconds = 86400;

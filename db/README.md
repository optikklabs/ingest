# ClickHouse schema

Hand-applied, numbered DDL. Every statement is `IF NOT EXISTS` / additive, so
the whole set is safe to re-apply:

```sh
for f in db/*.sql; do clickhouse-client -mn < "$f"; done
```

On the live cluster add `-n optikk` where the client is not already scoped to
the database.

## Files

| File | Objects |
|---|---|
| `00_database.sql` | `optikk` database |
| `01_spans.sql` | `spans` — raw trace spans |
| `02_logs.sql` | `logs` |
| `03_metrics.sql` | `metrics` — raw datapoints with promoted series dimensions |
| `04_metrics_series.sql` | `metrics_series` — metadata keyed by metric fingerprint |
| `05_metrics_rollups.sql` | `metrics_1m_v2/5m_v2/1h_v2` and their materialized views |
| `06_llm_rollups.sql` | `llm_stats_1m` and `spans_to_llm_stats_1m` |
| `07_ingestion_stats.sql` | `ingestion_stats` |
| `08_spans_root.sql` | `spans_root` and `spans_to_root` |
| `09_llm_scores.sql` | `llm_scores` |
| `10_span_rollups.sql` | `span_stats_1m/5m/1h` and their materialized views |
| `11_log_rollups.sql` | `logs_stats_1m` and `logs_to_stats_1m` |
| `12_error_events.sql` | `error_events` and `spans_to_error_events` |

`db_name` and `query_hash` are part of the `span_stats` sorting key. Existing
installations created before those dimensions were added must rebuild the three
`span_stats` tables and their materialized views from `10_span_rollups.sql`;
`CREATE TABLE IF NOT EXISTS` cannot change an existing sorting key. Retained raw
spans can be used to backfill the rebuilt cascade. The destructive, one-time
upgrade is provided separately at
`db/upgrades/20260802_rebuild_span_stats_query_dimensions.sql` so the normal
`db/*.sql` apply loop remains additive and safe to repeat.

`span_stats` is the single source for span-derived reads: RED, per-host/pod
aggregates, database and Kafka RED, and the service-graph edges. There is no
separate edge table — edges are the client-side rows
(`kind_string IN ('CLIENT','PRODUCER')`, non-empty `peer_name`).

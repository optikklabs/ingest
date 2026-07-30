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
| `10_span_stats.sql` | `span_stats_1m/5m/1h` and their materialized views |

`span_stats` is the single source for span-derived reads: RED, per-host/pod
aggregates, database and Kafka RED, and the service-graph edges. There is no
separate edge table — edges are the client-side rows
(`kind_string IN ('CLIENT','PRODUCER')`, non-empty `peer_name`).

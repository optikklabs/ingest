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
| `03_logs.sql` | `logs` |
| `05_metrics.sql` | `metrics` — raw datapoints |
| `06_metrics_series.sql` | `metrics_series` — series metadata keyed by fingerprint; `attributes` / `resource_attributes` are `Map(LowCardinality(String), String)` |
| `07_metrics_rollups.sql` | `metrics_1m/5m/1h` + MVs. Fingerprint-keyed only — no dimensions, so dimensional metric reads join `metrics_series` |
| `08_llm_rollups.sql` | `llm_stats_1m` + `spans_to_llm_stats_1m` |
| `09_ingestion_stats.sql` | `ingestion_stats` |
| `10_spans_root.sql` | `spans_root` + `spans_to_root` (traces explorer) |
| `11_spans_llm_context.sql` | `ALTER`s adding gen_ai/langfuse columns to `spans` |
| `12_llm_scores.sql` | `llm_scores` |
| `13_llm_traces.sql` | `llm_traces` + `spans_to_llm_traces` |
| `14_span_stats.sql` | `span_stats_1m/5m/1h` + MVs — the RED cascade, every APM dimension as a real column, and `peer_name`/`peer_type` for the service graph |

`span_stats` is the single source for span-derived reads: RED, per-host/pod
aggregates, database and Kafka RED, and the service-graph edges. There is no
separate edge table — edges are the client-side rows
(`kind_string IN ('CLIENT','PRODUCER')`, non-empty `peer_name`).

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
| `07_metrics_rollups.sql` | `metrics_1m/5m/1h` + MVs. Fingerprint-keyed only — superseded by `16`, retired after cutover |
| `08_llm_rollups.sql` | `llm_stats_1m` + `spans_to_llm_stats_1m` |
| `09_ingestion_stats.sql` | `ingestion_stats` |
| `10_spans_root.sql` | `spans_root` + `spans_to_root` (traces explorer) |
| `11_spans_llm_context.sql` | `ALTER`s adding gen_ai/langfuse columns to `spans` |
| `12_llm_scores.sql` | `llm_scores` |
| `14_span_stats.sql` | `span_stats_1m/5m/1h` + MVs — the RED cascade, every APM dimension as a real column, and `peer_name`/`peer_type` for the service graph |
| `15_metrics_dims.sql` | `ALTER`s adding series dimensions to `metrics` |
| `16_metrics_rollups_dims.sql` | `metrics_1m/5m/1h_v2` + MVs — the dimensional rollup cascade, plus the cutover runbook |

`span_stats` is the single source for span-derived reads: RED, per-host/pod
aggregates, database and Kafka RED, and the service-graph edges. There is no
separate edge table — edges are the client-side rows
(`kind_string IN ('CLIENT','PRODUCER')`, non-empty `peer_name`).

## Removed files

Numbers are never reused; gaps mark retired objects. `02` (`spans_resource`)
and `04` (`logs_resource`) were dropped with their pipelines. `13`
(`llm_traces` + `spans_to_llm_traces`) was a write-only rollup read by
nothing and was removed. Clusters created before the removal still carry the
objects; drop them by hand (view first, so span inserts stop feeding it):

```sql
DROP VIEW IF EXISTS optikk.spans_to_llm_traces;
DROP TABLE IF EXISTS optikk.llm_traces SYNC;
```

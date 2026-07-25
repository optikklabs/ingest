# span_stats rollout runbook

Ordered steps to cut RED + the service graph over from the Go aggregators to the
ClickHouse MVs. Do not skip the parity gates.

## 1. Apply schema + backfill (both pipelines live)

1. Apply the numbered DDL as usual: `for f in db/clickhouse/*.sql; do clickhouse-client -mn < "$f"; done`
   (creates span_stats_*; all `IF NOT EXISTS`).
2. Backfill history once: `db/clickhouse/backfill/backfill_span_stats.sh`
   (cascades to 5m/1h automatically; safe only to run once).
3. Deploy ingest+query from this branch. The Go spanmetrics/servicegraph
   consumers are REMOVED in this build — so run step 2's parity gate against the
   PREVIOUS build if you want a true side-by-side. If you deploy this build
   directly, the old `traces.span.metrics.duration` /
   `traces_service_graph_*` metrics stop being produced and the parity query
   below only has new data.

   > If you need the side-by-side gate, cherry-pick only `14_span_stats.sql` +
   > the backfill onto the currently-deployed build first, let it fill, run the
   > parity gate, THEN deploy this decommission build.

## 2. Adding peer_name/peer_type to a live cluster

The service graph reads `peer_name`/`peer_type` off this cascade, so an existing
deployment needs them added before query's topology reader is deployed. Per tier
(`span_stats_1m`, `_5m`, `_1h`):

```sql
ALTER TABLE optikk.span_stats_1m
  ADD COLUMN IF NOT EXISTS peer_name LowCardinality(String) CODEC(ZSTD(1)),
  ADD COLUMN IF NOT EXISTS peer_type LowCardinality(String) CODEC(ZSTD(1));

-- Both MUST join the sorting key, or AggregatingMergeTree merges rows that
-- differ only by peer and the distinction is lost. Appending keeps the existing
-- key prefix, so current queries' pruning is unaffected.
ALTER TABLE optikk.span_stats_1m MODIFY ORDER BY (
  tenant_id, timestamp, service, span_name, kind_string, status_code_string,
  http_status_bucket, http_route, db_system, messaging_system,
  messaging_destination, messaging_consumer_group, environment, host, pod,
  cloud_provider, cloud_platform, cloud_region, k8s_node, peer_name, peer_type);
```

Then `DROP` and re-`CREATE` the three MVs from `14_span_stats.sql` so they select
the new columns, and re-run `backfill_span_stats.sh` for the window the MVs were
missing. If `MODIFY ORDER BY` is rejected, rebuild `span_stats_1m` from `spans`
(7-day TTL, cheap) rather than the 1h tier, which holds 30 days.

Verify: `peer_name` non-empty on CLIENT/PRODUCER rows and empty elsewhere; daily
`count()` within a few percent of the pre-change value; 1m→5m→1h
`sum(request_count)` parity for one overlapping hour.

## 3. Parity gate — RED (must pass before trusting new dashboards)

Run `parity_check_span_stats.sql` (bind tenantID/start/end) for several windows
and tenants. Every row must have `ok = 1`: request/error counts identical
old-vs-new. Latency is expected to differ — p99 rises from bucketed to tDigest.

Watch this for a few days across real traffic before considering RED migrated.

## 4. Parity gate — service graph

Edges are derived from client-side spans in the same cascade — there is no
separate edge table and no pairing step. Before trusting topology, compare
against the old counters (only available on a build that still runs the Go
servicegraph consumer):

```sql
SELECT service AS source, peer_name AS target, sum(request_count) AS calls
FROM optikk.span_stats_1m
WHERE tenant_id = {t} AND timestamp BETWEEN {s} AND {e}
  AND kind_string IN ('CLIENT', 'PRODUCER')
  AND peer_name != '' AND peer_name != service
GROUP BY source, target;
```

vs the old `traces_service_graph_request_total` per (client, server). Two
expected differences, both from dropping CLIENT↔SERVER pairing:

- **Latency and errors are client-observed**, so they include network and
  queueing the caller paid. Per-service server-side latency/error rate is
  unaffected — topology's node aggregates read the same cascade's server rows.
- **Targets are addresses where `peer.service` is unset.** A Java
  auto-instrumented caller can yield `orders.default.svc.cluster.local:8080`
  instead of `orders`, which leaves that node unconnected in the graph. If the
  miss rate is material, add a `server.address → service` mapping at read time.

## 5. Alert monitors

APM latency monitors evaluate p99 via the new tDigest state — accurate p99 is
typically HIGHER than the old bucket-interpolated value (which capped at
15000ms). Review every APM monitor with a latency threshold and re-baseline
before/after cutover so accuracy improvements don't page as regressions. Error-
rate and hits monitors are unaffected (counts match exactly per the parity gate).

# optikk ingest

The ingestion layer of the Optikk observability backend. Accepts OTLP (gRPC +
HTTP), processes telemetry through Kafka, and writes spans/logs/metrics to
**ClickHouse**. Owns the ClickHouse schema.

## Schema

The ClickHouse DDL lives in [`db/clickhouse`](db/clickhouse) as numbered `.sql`
files. It is **applied by hand** — the service does not migrate on boot. Apply
it (in lexical order) at least once before starting ingest or query against a
fresh cluster, or they will fail against missing tables. The DDL is idempotent
(`CREATE ... IF NOT EXISTS`), so re-applying is safe.

Local (ClickHouse on `localhost`):

```bash
for f in db/clickhouse/*.sql; do clickhouse-client -mn < "$f"; done
```

In-cluster (e.g. AKS):

```bash
for f in db/clickhouse/*.sql; do
  kubectl -n optikk exec -i chi-optikk-cluster-0-0-0 -c clickhouse -- \
    clickhouse-client -mn < "$f"
done
```

Because schema is now hand-rolled, the old `schema_migrations` tracking table is
an inert leftover on existing clusters — harmless, drop it whenever.

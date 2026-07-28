# CLAUDE.md — ingest

Workspace-wide standards live in `../CLAUDE.md` and apply in full here.
This file covers only what is specific to ingest.

## What This Repo Owns

OTLP ingestion, Kafka produce/consume, and ClickHouse writes. Business logic,
querying, and auth policy belong to query; UI belongs to web.

Data flow: OTLP (gRPC :4317 / HTTP :4318) → per-signal mapper → Kafka →
consumer → ClickHouse batch insert.

## Layout

- `cmd/ingest/` — entrypoint; wiring in `internal/app/`.
- `internal/ingestion/<signal>/` — one package per signal (spans, logs,
  metrics, metricseries, ...): mapper, schema, handler for that signal only.
- `internal/ingestion/core/` — the shared produce/consume/write pipeline.
  Keep it thin; if a type in core has one consumer, move it to that signal.
- `internal/infra/<concern>/` — kafka, database, otlp, metrics, config.
  ClickHouse DDL lives in `db/` migrations.

## Failure Semantics — Accepted Loss

This pipeline drops on failure, by design. A failed publish or insert is
counted (`internal/infra/metrics/ingest.go`), logged once, and the batch is
dropped. Do not add retry writers, re-flush buffers, backoff loops, or retry
config. `IngestionStatsPublishDropped` is the pattern to copy.

Durability comes from the architecture — Kafka retention and consumer
offsets — not from application retry code.

## Hot Path Discipline

The per-row path (mapper → producer → consumer → writer) is the hottest code
in the platform. That is an argument for *less* code on it, not more: no
pooling, no manual zero-alloc formatting, no sharded caches — per workspace
rule 3. Throughput comes from batch size and Kafka partitioning.

## Gotchas

- Unknown API keys are negative-cached (15s) in auth; a "missing API key"
  drop can be cache, not config.

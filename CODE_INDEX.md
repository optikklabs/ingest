# Ingest Service Code Index

Welcome to the `ingest` repository index! This service is the high-throughput ingestion pipeline for Optikk. It receives OpenTelemetry (OTLP) data via gRPC, processes it, routes it through Kafka, and writes the telemetry payload into ClickHouse. **It is the authoritative owner of the ClickHouse schema.**

---

## 🚀 Entry Points & Setup
- **Main Entrypoint**: `cmd/ingest/main.go` (Initializes Kafka consumers, OTLP receivers, and ClickHouse writers).
- **Configuration**: `config.yml` (App configuration).
- **OTLP Config**: `otel-collector-config.yaml` (Defines how the OTEL collector should parse and route incoming traces/metrics/logs).

---

## 🌪️ Ingestion Pipeline (`internal/ingestion/`)
This directory contains the logic for reading from Kafka, mapping/normalizing the data, and batch-inserting into ClickHouse.

- **`core/`**: Shared interfaces and core processing logic.
- **`metrics/` & `metricseries/`**: Consumers and mappers for incoming metrics.
  - Files: `consumer.go`, `handler.go`, `mapper.go`, `normalize.go`.
  - Also handles metric fingerprinting and schema definitions (`schema/`).
- **`logs/` & `logsresource/`**: Consumers and mapping logic for log entries.
- **`spans/` & `spansresource/`**: Consumers and mapping logic for APM traces and spans.

---

## 🏗️ Infrastructure & Transport (`internal/infra/`)
This layer abstracts away the complex integrations with external systems like Kafka and ClickHouse.

- **`otlp/`**: Houses the OTLP receivers. Handles protocol conversions (`protoconv.go`) and attributes (`typed_attrs.go`).
- **`kafka/`**: Kafka producer and consumer wrappers.
- **`database/`**: ClickHouse connection pooling, batch insert mechanics, and **ClickHouse Schema Migrations**.
- **`fingerprint/`**: Deduplication logic. Hashes incoming time-series metadata to assign unique fingerprints.
- **`timebucket/`**: Shared logic for rounding timestamps into ClickHouse partition buckets.
- **`metrics/`**: Internal observability (recording how many spans/logs are ingested).

---

## 🔐 Authentication (`internal/auth/` & `internal/authrepo/`)
- Handles validation of incoming telemetry API keys.
- **`authrepo/`**: Fetches tenant and token data from the database to ensure only valid telemetry is ingested.

---

## 🎯 Quick Navigation
- **Adding a new column to ClickHouse?** Modify the schema in `internal/infra/database/` and update the relevant `schema/` and `mapper.go` in `internal/ingestion/`.
- **Changing how attributes are parsed?** Look in `internal/infra/otlp/typed_attrs.go`.
- **Debugging Kafka lag?** Check the consumers in `internal/ingestion/<type>/consumer.go`.

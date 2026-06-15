# optikk ingest

The ingestion path of the Optikk observability backend. Accepts OTLP over gRPC
(spans/logs/metrics), authenticates each request by `x-api-key`, produces to
Kafka, consumes, and writes to ClickHouse. Owns the **ClickHouse** schema and
migrates it on startup.

The query/API layer lives in the separate
[`query`](https://github.com/optikklabs/query) repo, which owns and migrates the
**MySQL** schema. Ingest connects to MySQL read-only solely to resolve API keys
to teams (`internal/authrepo`).

## Pipeline

```
OTLP gRPC (:4317)  →  Kafka produce  →  Kafka consume  →  ClickHouse write
```

An HTTP listener on `server.port` (default `:18090`) serves `/metrics` and
`/health` only.

## Commands

- **Build**: `make build` or `go build ./cmd/ingest`
- **Run**: `make run` or `go run ./cmd/ingest`
- **Generate Protobuf**: `make proto`
- **Format / Vet**: `make fmt` / `make vet`

## Local development

```bash
docker compose up -d        # Kafka + ClickHouse + MariaDB (+ Prometheus/Grafana)
go run ./cmd/ingest         # applies CH migrations, ensures topics, serves OTLP :4317
```

Configuration is read from `config.yml` (env overrides via the `OPTIKK_` prefix).

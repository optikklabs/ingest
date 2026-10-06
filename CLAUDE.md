# CLAUDE.md — ingest

Conventions for the ingest repo, the write path of Optikk. The query and web
services are separate repositories; this file covers only ingest. See
README.md for running it and applying the schema.

## What This Repo Owns

OTLP intake (gRPC and HTTP), the Kafka pipeline, and the ClickHouse schema
(`db/`, numbered DDL applied by hand, idempotent). Query reads these tables
but never changes them.

## Layout

- `cmd/ingest` — `main`: config, signals, `app.New(cfg).Start(ctx)`.
- `internal/app` — wiring: infra, transports (OTLP gRPC/HTTP, health,
  metrics), Kafka topic reconciliation, consumers, lifecycle.
- `internal/auth`, `internal/authrepo` — API-key → tenant resolution with a
  bounded cache, used by both transports.
- `internal/ingestion/<signal>` — one package per signal (spans, logs,
  metrics, metricseries, llmscores, ingestionstats). Each holds `handler.go`
  (OTLP request → rows), `mapper.go` (OTLP → row proto), `mapping.go` (row →
  ClickHouse columns), `module.go`, and `schema/` with the row `.proto` and
  generated code (`make proto`; never edit `*.pb.go`).
- `internal/ingestion/core` — the shared produce → consume → batch insert
  machinery and the DLQ. Signal packages plug into it; they never talk to
  Kafka or ClickHouse directly.
- `internal/infra` — Kafka, database, fingerprinting, OTLP helpers, metrics.

## Pipeline Rules

- Accept fast, write later. A request is acknowledged once its rows are on
  Kafka. ClickHouse inserts happen in consumers, in batches, never on the
  request path. A batch that fails or panics goes to the DLQ before its
  offsets commit.
- Each batch inserts with a dedup token derived from its Kafka offsets, so
  redelivery cannot double-count. Side-effect rows (usage stats) use the
  best-effort `AsyncPublisher`.
- Topics are reconciled at startup: created when missing, grown when
  configured larger, never shrunk.
- Map OTLP semantic conventions in one place per signal. Missing attributes
  become empty values, never invented ones.
- Schema changes edit the numbered DDL in place (no migrations; nothing
  runs in production yet) and keep `mapping.go` and the row `.proto` in step.

## Go Standards

These follow Effective Go, Go Code Review Comments and the bug-preventing
parts of the Uber Go style guide. `.golangci.yml` enforces most of them;
CI runs `go mod tidy -diff`, `go vet`, golangci-lint, `go test -race` and
govulncheck.

### Tooling

- `make fmt` (gofmt + goimports, imports grouped stdlib / third-party /
  `github.com/optikklabs`), `make lint`, `make test`, `make vulncheck`.
- Fix lint findings; do not suppress them. A `//nolint` must name the
  linter and give the reason (`//nolint:nilnil // a nil cursor is the first
  page`); nolintlint rejects anything vaguer.
- `go.mod` pins the Go patch release CI builds with. Bump it when
  govulncheck reports a standard-library fix.

### Errors

- Return errors; never panic on input. A panic is reserved for a broken
  internal invariant (an unknown constant, a missing budget context).
- Wrap with context using `%w`; compare with `errors.Is` / `errors.As`,
  never `==` or a type switch on a wrapped error.
- No `(nil, nil)` for pointers or interfaces: return a value, a sentinel
  error, or a `(T, bool)` pair. A nil map or slice is a valid empty result.
- Discard an error only when it cannot happen or cannot be acted on
  (`crypto/rand.Read`, hash writes, close-after-failure), and say which.

### Context

- `ctx context.Context` is the first parameter of anything that does I/O,
  and is passed down, never replaced by `context.Background()`.
- Work that must outlive the caller (shutdown drains, background jobs,
  failure bookkeeping) derives from `context.WithoutCancel(ctx)` so it keeps
  the caller's values. Only `main` and long-lived workers start from
  `context.Background()`.
- Log with the `*Context` slog variants and typed attrs:
  `slog.WarnContext(ctx, "msg", slog.String("k", v))`.

### Code shape

- Evaluate before you return: never `return rows, query(&rows)`. Go leaves
  the order of reading `rows` and calling `query` unspecified. Write
  `err := query(&rows); return rows, err`.
- Never append to a slice you do not own and then keep using the original
  (`args := append(base, x)` aliases `base`'s backing array).
- Prefer the standard library: `slices`, `maps`, `strings.Cut`, `min`/`max`,
  `for i := range n`, `errors.Join`, `net.ListenConfig`. Do not hand-roll
  what it provides, and do not shadow builtins (`max`, `new`, `len`).
- Keep functions to five or fewer results; return a struct past that.
- Accept interfaces where a seam is needed and return concrete types.
  Define an interface beside its consumer, not its implementation.
- Names follow Go Code Review Comments: short receivers, `ErrFoo`
  sentinels, `FooError` types, initialisms upper-case (`ID`, `URL`, `HTTP`),
  no `Get` prefix on plain getters, no stutter (`kafka.Client`, not
  `kafka.KafkaClient`).
- A comment explains why, not what. Document an exported symbol when its
  behaviour is not obvious from its name and signature.

### Tests

- Tests are table-driven where cases share a shape. Use the standard
  `testing` package, `t.Context()` and `t.TempDir()`.
- End-to-end behaviour is verified by running the stack and ingesting real
  telemetry, not by mocks of ClickHouse or Kafka.

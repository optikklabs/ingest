# Ingest Scalability — Design Doc (Phases 1–5)

Scope decided: **Phases 1–5 only** (throughput + allocation). Multi-tenancy
quotas (6–7) and DLQ disk-spill (7b) are **out of scope**; the accepted-loss DLQ
contract stays as-is. Phase 0 (baseline + observability) is a prerequisite.

Each phase is a standalone PR, independently revertible, with no Kafka wire or
ClickHouse schema changes. Verify each against the k6 baseline before the next.

Dependency versions confirmed present: `franz-go v1.21.0`, `franz-go/pkg/kadm
v1.18.0`, `cespare/xxhash/v2 v2.3.0`, `hashicorp/golang-lru/v2 v2.0.7`,
`google.golang.org/protobuf v1.35.2`.

---

## Phase 0 — Baseline & observability (prerequisite)

**Goal:** every later phase has a before/after number.

### Files
- `internal/infra/metrics/ingest.go` — add gauges/counters (promauto, same
  `optikk`/`ingest` namespace as existing):
  - `resource_cache_hits_total{signal}`, `_misses_total{signal}`,
    `_evictions_total{signal}` (feeds Phase 4).
  - `aggregator_keys{signal}` gauge, `aggregator_keys_dropped_total{signal}`
    (feeds later cardinality work; harmless to add now).
  - `consumer_batch_insert_duration_seconds{signal}` histogram and
    `consumer_inflight_batches{signal}` gauge (feeds Phase 2).
- `internal/ingestion/core/resource_cache.go` — wire hit/miss/evict counters
  (LRU eviction callback via `lru.NewWithEvict`).

### Baseline capture (no code)
Boot local stack (Podman, ≥8GB VM — see stack runtime notes) + k6 harness.
Record, per signal: throughput (rows/s), Export p50/p99, consumer lag drain
rate, alloc rate + GC pause (`pprof` heap+cpu, 60s), CH `parts` created/s.
Save snapshot to scratchpad; it is the acceptance yardstick for 1–5.

### Verify
All new series scrape on `/metrics`; baseline snapshot archived.

**Risk:** none. **Effort:** low.

---

## Phase 1 — Per-signal partition tuning

**Goal:** remove the fixed 8-way parallelism ceiling per signal.

### Files
- `config.yml` — set per-signal partition counts. Proposed starting point
  (tune from baseline volume ratios, do not blanket-raise):
  - `spans`, `logs`, `spans_tracegraph`: 32
  - `metrics`: 16
  - `metric_series`, `spans_resource`, `logs_resource`, `ingestion_stats`: 8
- No Go change for reads — `config.IngestSignal` already plumbs `Partitions`
  (`internal/config/ingestion.go:18-23,55-57`).

### The repartition gap (must handle explicitly)
`EnsureTopics` (`internal/infra/kafka/topics.go:34-67`) calls
`adm.CreateTopics`, which **no-ops on an existing topic** (`TopicAlreadyExists`
is swallowed). It will **not** grow partitions on a live topic. Two options:

- **1a (recommended):** add `EnsureTopicPartitions` using
  `kadm.Client.UpdatePartitions(ctx, count, topic...)`. In `EnsureTopics`, after
  the create path, if the topic exists and `spec.Partitions >` current count,
  call UpdatePartitions. Guard: never shrink (Kafka forbids it; skip + warn).
  Log old→new. This makes partition growth declarative and idempotent.
- **1b:** leave create-only; document a manual `kafka-topics --alter` runbook.

Prefer 1a — it fits the existing "ensure" idempotency model.

### Correctness note
Topics are keyed by fingerprint / traceId (`core/producer.go:26-31`,
`app/ingest.go:81-83`). Growing partitions reshuffles key→partition mapping.
That is safe here: per-partition ordering is not relied on (CH inserts are
order-independent), and the servicegraph pairing stays correct because a whole
trace still hashes to one partition after the change. Note in the PR that live
repartitioning briefly redistributes in-flight keys.

### Tests / verify
- Unit test `EnsureTopicPartitions`: existing topic with N<target grows to
  target; N==target no-ops; N>target warns and skips.
- Live: consumer group shows `min(pods, partitions)` active members; add pods
  and confirm throughput scales ~linearly to the new partition count.

**Risk:** low. **Effort:** trivial (+ small kadm helper).

---

## Phase 2 — Double-buffer the consume loop (fetch ‖ insert)

**Goal:** ~2× per-consumer throughput by overlapping the next poll with the
current batch's CH insert. Today `Consumer.Run` polls, then unmarshals +
inserts + commits inline (`internal/infra/kafka/consumer.go:38-85`), so CH
latency is dead time on the fetch loop.

### Design (single file: `internal/infra/kafka/consumer.go`)
Split the loop into a **fetcher** (the existing goroutine) and one **worker**
goroutine, connected by a **depth-1 buffered channel** of `[]*kgo.Record`.

```
fetcher:  loop { recs := PollRecords(); ch <- recs }   // blocks when worker busy
worker:   loop { recs := <-ch; if handle(recs)==nil { CommitRecords(recs) } }
```

Invariants preserved from today:
- Commit only after `handle` returns nil; on handler error, **skip commit** so
  the batch redelivers (`consumer.go:73-84`). Unchanged, just moved to worker.
- Depth-1 bounds in-flight to **two batches** (one fetching, one inserting).
  This caps redelivery-on-rebalance exposure and memory, and gives natural
  backpressure: the fetcher blocks on a full channel instead of unbounded
  buffering.
- Commits stay strictly ordered (single worker, FIFO channel) — no offset
  reordering.

Backoff / fetch-error handling (`consumer.go:48-71`) stays in the fetcher.
Shutdown: close the channel when `PollRecords` reports client-closed / ctx done;
worker drains remaining ≤1 batch, commits, returns. Both goroutines join before
`Run` returns.

Emit `consumer_inflight_batches` and `consumer_batch_insert_duration_seconds`
(added in Phase 0).

### Why not a worker pool (yet)
A pool inserting in parallel needs an offset-completion tracker to avoid
committing an offset whose predecessor hasn't landed. Depth-1 double-buffering
gets most of the win with none of that risk. A pool is a future phase if CH
insert latency (not fetch) remains the bottleneck after this.

### Tests / verify
- Unit: table test with a fake `handle` — assert (a) commit happens only after
  success, (b) error skips commit and the same batch is re-handed on redelivery,
  (c) at most 2 batches outstanding, (d) clean shutdown drains and joins.
- Race detector (`go test -race`) on the new goroutine handoff.
- Live: kill-9 mid-insert, confirm the uncommitted batch redelivers with **no
  gap and no double-count** — reconcile exact row counts, not aggregate totals.
- Throughput: lag drain rate up ~2× at fixed offered load.

**Risk:** medium (offset-commit correctness). Gate on the race + redelivery
tests. **Effort:** low–medium.

---

## Phase 3 — Async side-topic publishes

**Goal:** cut span-Export p99 2–3×. Each Export today blocks on the main
publish **and** the tracegraph publish (`internal/ingestion/spans/handler.go:
79-95`) **and** the resource publish — up to 3 serial AllISR round-trips. The
main publish stays synchronous (durability). Tracegraph + resource are already
best-effort (warn-only) and belong off the request path.

### New component: `internal/ingestion/core/async_publisher.go`
A bounded fire-and-forget publisher:

```
type AsyncPublisher[T Row] struct { in chan asyncJob[T]; ... }   // buffered, size from config
// Enqueue(rows, onDrop) -> bool   // false + metric if queue full; never blocks the caller
// N worker goroutines drain the queue, call the underlying Producer.Publish
```

- Queue-full → drop + `sidepublish_dropped_total{signal,topic}`. Acceptable:
  these signals are already best-effort.
- Graceful drain on shutdown (close + wait, bounded timeout).

### Handler changes (`spans/handler.go`, `logs/handler.go`)
- Replace the inline `tracegraphProducer.Publish` (currently
  `context.Background()` + 5s timeout, `handler.go:89-95`) with
  `asyncTracegraph.Enqueue(tgRows)`.
- **Resource-cache rollback interaction (critical):** today `PublishResources`
  rolls the cache back on publish failure (`core/resource_publish.go:27-36`) so
  a failed publish re-emits next time. With async, the publish result is not
  known at Export time. Design: **add to cache only on successful enqueue**; if
  the async publish later fails, the worker calls `cache.Remove(keys)` in its
  drop/err path. Keeps the "publish failed ⇒ retry next window" property. The
  `newKeys`/`resourceRows` pairing (`handler.go:56-76`) moves into the async job
  so rollback keys travel with the rows.

### Tests / verify
- Unit: enqueue accepted ⇒ cache retains key; worker publish-fail ⇒ key removed
  (next call re-emits). Queue-full ⇒ dropped + counter, caller not blocked.
- Live: Export p99 drops; servicegraph/resource output unchanged under steady
  load; drop counter ~0 except under deliberate overload.

**Risk:** medium (cache/rollback semantics). **Effort:** low–medium.

---

## Phase 4 — Shard the ResourceCache

**Goal:** kill the global-LRU lock that serializes the span/log hot path. Every
span calls `CheckAndUpdateBucket` → Peek + Get/Add under one mutex
(`internal/ingestion/core/resource_cache.go`); up to 10k concurrent gRPC streams
(`app/transport.go:62-65`) contend on it.

### Files
- `internal/ingestion/core/resource_cache.go` — shard internally, **public API
  unchanged** (`Add`, `Remove`, `CheckAndUpdateBucket`):
  - `shards [N]*lru.Cache[ResourceKey, uint32]`, N a power of two (default 32),
    each with `maxSize/N` capacity and its own mutex (golang-lru is
    per-instance locked).
  - Shard select: `key.Fingerprint & (N-1)`. Fingerprint is already a
    well-distributed xxhash, so low bits are uniform.
  - Wire the Phase-0 hit/miss/evict counters per shard (aggregate label).
- `internal/app/ingest.go:226-227` — replace hardcoded `NewResourceCache(100000)`
  with a config-driven size (below).
- `internal/config/*` — add `ingestion.resource_cache_size` (default e.g.
  500_000; sized to expected active-series cardinality, not tenant count).

### Cleanup (Finding 6 dead-path)
`ResourceCache` exposes both `Add` (sentinel value 0) and `CheckAndUpdateBucket`
(stores the day bucket). Audit callers: spans/logs handlers use
`CheckAndUpdateBucket`; `PublishResources` uses `Remove`. If `Add` is unused,
remove it (surgical, only if truly orphaned — confirm with grep first).

### Tests / verify
- Unit: sharded cache matches single-cache semantics for Add/Peek/bucket-change;
  keys distribute across shards (statistical check on random fingerprints).
- Bench: `go test -bench` concurrent `CheckAndUpdateBucket` — mutex-profile
  contention down ~N×.
- Live: `pprof` mutex profile shows the cache lock no longer dominates; resource
  re-publish rate does not spike (no eviction thrash — watch the evict counter).

**Risk:** low. **Effort:** low.

---

## Phase 5 — Allocation reduction (mapping + serialization)

**Goal:** cut GC pressure at billions of rows. **Do as independent sub-PRs**,
each gated on `go test -bench -benchmem` showing alloc/op down **and** output
byte-identical to baseline. Highest silent-bug risk phase — pooled buffers must
be fully reset or rows cross-contaminate. Reconcile exact fingerprints/rows,
not aggregates.

### 5a — Pool transient maps (ownership matters)
Only pool maps that are **fully consumed inside `mapRequest`**:
- `spans/mapper.go`: `spanMap` (`mapper.go:55`) is read for promoted fields then
  copied into `merged` (`mergeAndCapAttrs:116-128`) — free after the copy →
  poolable per span. `resMap` (`mapper.go:33`) is shared across a resource's
  spans and copied into row fields + `merged` → poolable per **ResourceSpans**,
  returned after that resource's inner loop.
- **Do NOT pool `merged`**: it is retained as `row.Attributes` and marshaled
  later in the producer — its lifetime escapes the mapper.
- Mechanism: `sync.Pool` of `map[string]string`; `clear(m)` before return (Go
  1.21 builtin). Return in a `defer` at the right scope. Same pattern for the
  metrics datapoint scratch that is not retained.

### 5b — Hoist `SeriesHash` resource merge
`fingerprint.SeriesHash` (`internal/infra/fingerprint/fingerprint.go:21-36`)
rebuilds a merged `resAttrs+dpAttrs` map **per datapoint**. Filter the
high-cardinality-stripped resource attrs **once per ResourceMetrics**
(`metrics/mapper.go:21-40`) and pass the pre-filtered map down; merge only
`dpAttrs` per point. Reuse the `keys` sort buffer in
`fingerprint/hash.go:18-22` via a pooled `[]string`.

### 5c — proto buffer reuse
- Producer (`core/producer.go:39-61`): use
  `proto.MarshalOptions{}.MarshalAppend(buf[:0], r)` with a pooled `[]byte` per
  Publish call instead of `proto.Marshal` per row (fresh alloc each).
- Consumer unmarshal (`spans/consumer.go:32`, and the other per-signal
  consumers): pool the concrete `*schema.Row` — `proto.Reset(row)` before reuse.
  **Ownership vs Phase 2:** rows are consumed by `writer.Insert`; DLQ uses raw
  `recs` bytes, not rows (`core/dlq.go:36-44`), so rows are free once `handle`
  returns. Return to the pool at the end of `handle`, **after** Insert — never
  hold a pooled row across the depth-1 handoff.

### 5d — Fix `AnyValueString` map/array path
`internal/infra/otlp/protoconv.go:31-33` uses `fmt.Sprintf("%v")` for
map/array/kvlist values — slow and non-deterministic (ordering churn inflates
fingerprint cardinality). Replace with explicit handling: JSON-encode
kvlist/array deterministically, or drop with a counter. Deterministic output is
the correctness win; the alloc win is secondary.

### 5e — Servicegraph struct match-key
`servicegraph/consumer.go:112-117` builds `fmt.Sprintf("%s:%s", trace, span)`
per span for the store map key. Replace with a comparable struct key
`{TraceID, SpanID string}` used directly as the `map` key
(`servicegraph/store.go` map type changes `map[string]CachedSpan` →
`map[spanKey]CachedSpan`). Eliminates one string alloc + format per span.

### 5f — Hash the API key once per request
`internal/auth/resolver.go:109-111` recomputes sha256 in `lookupCache`,
`group.Do`, and `cacheSet` — several times per Export, even on cache hits.
Compute the digest once at the top of `ResolveTenantID` and thread it through
the private helpers (`lookupCache(digest)`, `cacheSet(digest, …)`). Public
`ResolveTenantID(ctx, apiKey)` signature unchanged.

### Tests / verify (per sub-PR)
- `-benchmem` before/after showing alloc/op ↓; no regression in ns/op.
- Golden test: a fixed OTLP request → identical rows + fingerprints before/after
  (guards pool contamination and 5d determinism).
- `go test -race` on 5c (pooled rows across the Phase-2 handoff).
- Live: alloc rate + GC pause down vs Phase-0 baseline at fixed load; row/
  fingerprint counts unchanged (atomic reconcile).

**Risk:** medium (pool reset contamination, 5d output change). **Effort:** medium.

---

## Sequencing & acceptance

| Phase | Change | Risk | Gate |
|------|--------|------|------|
| 0 | baseline + metrics | none | series scrape; snapshot saved |
| 1 | partition tuning (+kadm grow) | low | scales with pods to new count |
| 2 | double-buffer consumer | med | race + redelivery tests; ~2× drain |
| 3 | async side-publishes | med | cache-rollback test; p99 ↓ |
| 4 | shard resource cache | low | mutex profile ↓; no evict thrash |
| 5 | alloc reduction (5a–5f) | med | benchmem ↓; golden output identical |

Ship in order. Each PR includes its k6 delta vs the Phase-0 snapshot.

## Cross-cutting
- Update `CODE_INDEX.md` after Phases 2 and 4 (structural).
- Document every new config key in `config.yml` comments and `.env.example`.
- Record two project memories: the Phase-1 live-repartition key-reshuffle
  caveat, and the Phase-5 pool-ownership rule (never pool `merged`/retained
  maps; return pooled rows only after Insert).

## Explicitly out of scope (unchanged trade-offs)
- AllISR synchronous main publish (durability contract).
- Single-attempt best-effort DLQ (accepted-loss — kept per decision).
- Connector accepted-loss on restart/rebalance.
- Per-tenant quotas / cardinality caps (Phases 6–7, deferred).

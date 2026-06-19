# Audit: Concurrent Kafka Consumption in Ingest

**Date:** 2026-06-16
**Scope:** `internal/infra/kafka/consumer.go`, `internal/ingestion/core/consumer.go`, `internal/app/{transport,ingest}.go`
**Trigger:** Per-signal consumption is driven by a single goroutine doing poll → unmarshal → insert → commit serially.

---

## 1. Current flow

Per signal (spans, logs, metrics) the app wires exactly **one** consumer client and starts **one** goroutine for it:

- `App.addConsumerActors` adds one `run.Group` actor per consumer → one goroutine each ([transport.go:123](../../internal/app/transport.go#L123)).
- That goroutine runs `kafka.Consumer.Run`, a single serial loop ([consumer.go:25](../../internal/infra/kafka/consumer.go#L25)):

```
for {
    fetches := PollFetches(ctx)      // blocks up to FetchMaxWait (2s)
    recs := flatten(all partitions)  // every partition's records → one slice
    handle(ctx, recs)                // unmarshal + ONE ClickHouse insert
    CommitRecords(ctx, recs...)      // sync commit
}
```

- `core.Consumer.handle` unmarshals all records serially, then does a **single** `writer.Insert` for the whole batch, with `wait_for_async_insert=1` so it blocks on the ClickHouse round-trip ([writer.go:31](../../internal/ingestion/core/writer.go#L31)).

So there are 3 consumer goroutines total (one per signal), and within each signal everything is sequential.

## 2. The bottleneck

Topics default to **8 partitions** per signal ([ingestion.go:25](../../internal/config/ingestion.go#L25)), but a single goroutine processes all 8 serially. The stages do not overlap:

1. **No poll/insert overlap.** While `writer.Insert` blocks on ClickHouse (`wait_for_async_insert=1`), the goroutine is not polling. Fetch buffers drain and brokers throttle. Effective throughput ≈ `batch_rows / (poll + unmarshal + insert) latency`. CH insert latency dominates and gates everything.
2. **No partition parallelism.** All 8 partitions are flattened into one slice and one insert. Partition-level parallelism that Kafka is designed to give us is collapsed to 1.
3. **No CPU/I/O overlap.** `proto.Unmarshal` (CPU) and Kafka fetch / CH insert (I/O) run on the same goroutine, never concurrently.

Net: a topology sized for 8-way parallelism runs at 1×. Adding partitions or broker capacity does nothing until consumption is parallelized.

## 3. Correctness constraints any fix must keep

The client uses **manual commit** (`DisableAutoCommit`) with the **cooperative-sticky** balancer ([client.go:49](../../internal/infra/kafka/client.go#L49)). Concurrency must respect:

- **Commit-after-durability.** An offset may be committed only after its records are inserted (or DLQ'd). Committing ahead of a still-in-flight insert risks data loss on crash. The current code already follows this; concurrency must not break it.
- **Per-partition offset ordering.** Offsets within a partition must be committed in order. With a naive shared worker pool, partition P's offset 200 could be committed before 150 → silent gap. Concurrency must commit per-partition and only up to the highest **contiguous** completed offset.
- **Rebalance safety.** On a cooperative rebalance, in-flight work for a revoked partition must stop/drain before the partition moves, or two consumers process the same offsets.
- **DLQ semantics.** The "insert fails → publish batch to DLQ → commit anyway" path ([consumer.go:46](../../internal/ingestion/core/consumer.go#L46)) must stay per-partition.

## 4. Options

### Option A — Goroutine per partition (recommended)
Decouple the poll loop from processing. The poll loop walks `fetches.EachPartition` and hands each partition's records to a dedicated worker goroutine (one per topic-partition) over a bounded channel. Each worker runs the existing `handle` and commits **its own** partition's offsets after success.

- **Pros:** Matches the 8-partition topology (up to ~8× in-process), preserves per-partition ordering and DLQ semantics naturally, overlaps poll/unmarshal/insert, idiomatic franz-go (`BlockRebalanceOnPoll` + `AllowRebalance`, stop workers in `OnPartitionsRevoked`). The existing `core.Consumer.handle` is reused unchanged as the per-partition unit of work.
- **Cons:** Need a small partition→worker manager and rebalance hooks. Parallelism caps at partition count per instance.
- **Effort:** Medium. Lives mostly in `infra/kafka/consumer.go`; `core`/`app` largely untouched.

### Option B — Bounded worker pool + contiguous-offset committer
Poll loop feeds a shared pool of N workers; a separate offset tracker commits only contiguous completed offsets per partition.

- **Pros:** Parallelism can exceed partition count (helps if unmarshal is CPU-bound). Fixed worker budget.
- **Cons:** Significantly more complex — must build the per-partition contiguous-offset tracker to avoid commit gaps; reordering and backpressure are subtle. Easy to get wrong.
- **Effort:** High. Recommend only if profiling shows unmarshal (not CH insert) is the true ceiling.

### Option C — Poll/insert pipeline (double buffer)
Keep one processing goroutine but overlap the next `PollFetches` with the current insert via a 1-deep buffer.

- **Pros:** Smallest change, ~2× from hiding insert latency behind the next poll.
- **Cons:** Still no partition parallelism; ceiling is 2×, not 8×.
- **Effort:** Low.

### Orthogonal — horizontal scaling
Running more ingest replicas in the same consumer group already distributes partitions across processes (cooperative-sticky handles this). This is complementary but doesn't improve single-instance utilization, and the stack targets a single-node ClickHouse, so the CH insert path stays shared. In-process concurrency (A) is still needed to use each instance fully.

## 5. Recommendation

**Adopt Option A (goroutine-per-partition).** It directly exploits the existing partition topology, keeps the established commit-after-durability + per-partition DLQ behavior, and reuses `core.Consumer.handle` as-is. Suggested follow-ups when implementing:

1. Add a config knob (e.g. `kafka.max_inflight_per_partition`, default 1) so partition workers can pipeline batches without unbounded memory.
2. Make worker count = assigned partitions; wire `OnPartitionsAssigned` / `OnPartitionsRevoked` to spawn/drain workers and `BlockRebalanceOnPoll` + `AllowRebalance` to close the rebalance race.
3. Before/after, measure rows/s per signal and consumer-group lag (the `LagPoller` already publishes lag every 15s) to confirm the speedup and watch for ClickHouse becoming the new ceiling.

## 6. Open questions

- Is the current ceiling ClickHouse insert latency or proto unmarshal CPU? This decides A vs. B — worth a quick profile first.
- Target throughput per instance? Defaults are tuned for ~150–300K rows/s/instance; concurrency goals should be set against that.
- Is single-node ClickHouse expected to absorb 8 concurrent async inserts per signal, or should per-partition inserts be coalesced before hitting CH?

# Optikk ingest production benchmark

**Date:** 2026-07-26  
**Target:** Optikk production ingest service  
**Protocol:** authenticated OTLP/gRPC  
**Infrastructure evidence:** AKS pod CPU, memory, HPA state, replica count,
restarts, ClickHouse, and Kafka

[Open the interactive HTML visualization](benchmark-2026-07-26.html).

## Result

- Practical multi-client knee: **33k–46k spans/s**
- Black-box saturation ceiling: **approximately 49k spans/s**
- Peak ingest HPA CPU: **132%**
- Maximum observed ingest replicas: **4**
- Export errors: **0**
- Ingest pod restarts: **0**

With eight independent gRPC clients, doubling offered load from 80,000 to
160,000 spans/s increased achieved throughput by only 6.9%, from 45,841 to
49,007 spans/s. This establishes the production black-box plateau.

## Multi-client OTLP/gRPC ramp

| Offered spans/s | Achieved spans/s | Target achieved | Export errors | HPA CPU max | Max replicas |
|---:|---:|---:|---:|---:|---:|
| 20,000 | 18,290 | 91.4% | 0 | 97% | 3 |
| 40,000 | 32,948 | 82.4% | 0 | 132% | 4 |
| 80,000 | 45,841 | 57.3% | 0 | 80% | 4 |
| 160,000 | 49,007 | 30.6% | 0 | 63% | 4 |

The limiting behavior was synchronous exporter backpressure rather than
explicit OTLP rejection. One ingest pod was consistently hotter than the other
replicas, indicating connection distribution contributed to the knee.

## Single-channel result

| Offered spans/s | Achieved spans/s | Target achieved |
|---:|---:|---:|
| 1,000 | 970 | 97.0% |
| 2,500 | 2,388 | 95.5% |
| 5,000 | 4,780 | 95.6% |
| 10,000 | 8,593 | 85.9% |
| 20,000 | 12,018 | 60.1% |
| 40,000 | 13,264 | 33.2% |

A single gRPC channel plateaued near 13k spans/s while cluster CPU remained
low. Multiple clients were therefore required to measure cluster capacity.

## OpenTelemetry Demo signal path

The normal OpenTelemetry Demo generated and exported traces, logs, and metrics.
In the corrected fast workload, 100 VUs produced approximately:

- 917 spans/s
- 284 log records/s
- 91 metric points/s
- exporter queue depth of 4

At 200 VUs the local demo API error rate reached 19.9%, p99 reached 10 seconds,
and span throughput fell to 245/s while Optikk ingest HPA CPU was only 22%.
Those higher levels measure the local demo ceiling and are excluded from the
Optikk saturation conclusion.

## Interpretation

The tested deployment can sustain roughly 33k spans/s with useful headroom.
The 46k–49k region is saturation territory and should not be treated as a
steady-state operating target. A production target should include margin for
traffic bursts, uneven gRPC connection distribution, Kafka lag, and query
traffic sharing ClickHouse.

No credentials are stored in this document or its HTML visualization.

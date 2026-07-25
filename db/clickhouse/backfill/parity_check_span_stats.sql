-- Parity gate for the span_stats migration. Run this WHILE BOTH pipelines are
-- live (Go spanmetrics still running AND span_stats MV populated) — i.e. before
-- decommissioning the Go consumer. Compares per-service request/error counts
-- from the OLD fingerprint-join path against the NEW span_stats path over the
-- same window.
--
-- Set @tenantID, @start, @end to a representative window on the 1m grain.
-- request_count / error_count MUST match exactly. Latency is intentionally NOT
-- compared here: p99 moves from bucket-interpolated (capped 15000ms) to tDigest
-- and legitimately rises — verify latency visually on the dashboard instead.
--
-- Any row where old != new is a parity failure; investigate before cutover.

WITH
    old_series AS (
        SELECT fingerprint,
               service,
               attributes['status.code'] AS status_code
        FROM optikk.metrics_series
        PREWHERE tenant_id = {tenantID:UInt32}
             AND timestamp BETWEEN {start:DateTime} AND {end:DateTime}
             AND metric_name = 'traces.span.metrics.duration'
        GROUP BY fingerprint, service, status_code
    ),
    old_agg AS (
        SELECT s.service                                       AS service,
               sum(m.hist_count)                               AS request_count,
               sumIf(m.hist_count, s.status_code = 'STATUS_CODE_ERROR' OR s.status_code = 'ERROR') AS error_count
        FROM optikk.metrics_1m AS m
        INNER JOIN old_series s ON m.fingerprint = s.fingerprint
        PREWHERE m.tenant_id = {tenantID:UInt32}
             AND m.timestamp BETWEEN {start:DateTime} AND {end:DateTime}
             AND m.metric_name = 'traces.span.metrics.duration'
        GROUP BY service
    ),
    new_agg AS (
        SELECT service                                         AS service,
               sum(request_count)                              AS request_count,
               sumIf(request_count, status_code_string = 'ERROR') AS error_count
        FROM optikk.span_stats_1m
        PREWHERE tenant_id = {tenantID:UInt32}
             AND timestamp BETWEEN {start:DateTime} AND {end:DateTime}
        GROUP BY service
    )
SELECT
    coalesce(o.service, n.service)                 AS service,
    o.request_count                                AS old_requests,
    n.request_count                                AS new_requests,
    o.error_count                                  AS old_errors,
    n.error_count                                  AS new_errors,
    o.request_count = n.request_count
        AND o.error_count = n.error_count          AS ok
FROM old_agg o
FULL OUTER JOIN new_agg n ON o.service = n.service
ORDER BY ok ASC, service;

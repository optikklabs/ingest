#!/usr/bin/env bash
# One-time backfill of span_stats_1m from historical spans, for rows that
# predate the spans_to_span_stats_1m MV. Run ONCE, AFTER 14_span_stats.sql is
# applied. Re-running double-counts (AggregatingMergeTree does not dedup), which
# is why this lives outside the idempotent db/clickhouse/*.sql migration glob.
#
# The 5m/1h tiers backfill automatically: inserting into span_stats_1m fires
# span_stats_5m_mv, which fires span_stats_1h_mv. No separate tier backfill.
#
# The upper bound is the MV's creation time so rows the MV already captured are
# not double-inserted. The lower bound matches span_stats_1m's 7-day TTL.
#
# Usage: CH_URL / CH_USER / CH_PASSWORD env, then ./backfill_span_stats.sh
set -euo pipefail

CH() { clickhouse-client --host "${CH_HOST:-localhost}" --port "${CH_PORT:-9000}" \
  --user "${CH_USER:-default}" --password "${CH_PASSWORD:-}" "$@"; }

MV_CREATED=$(CH --query "
  SELECT toString(max(metadata_modification_time))
  FROM system.tables
  WHERE database = 'optikk' AND name = 'spans_to_span_stats_1m'")

if [[ -z "${MV_CREATED}" || "${MV_CREATED}" == "1970-01-01 00:00:00" ]]; then
  echo "ERROR: spans_to_span_stats_1m not found — apply 14_span_stats.sql first." >&2
  exit 1
fi
echo "Backfilling span_stats_1m for spans in [now()-7d, ${MV_CREATED}) ..."

CH --query "
  INSERT INTO optikk.span_stats_1m
  SELECT
      tenant_id,
      toStartOfMinute(timestamp)                   AS timestamp,
      service, environment, host, pod,
      name                                         AS span_name,
      kind_string, status_code_string, http_status_bucket, http_route, db_system,
      attributes['messaging.system']               AS messaging_system,
      attributes['messaging.destination.name']     AS messaging_destination,
      attributes['messaging.consumer.group.name']  AS messaging_consumer_group,
      attributes['cloud.provider']                 AS cloud_provider,
      attributes['cloud.platform']                 AS cloud_platform,
      if(attributes['cloud.region'] != '', attributes['cloud.region'], attributes['aws.region']) AS cloud_region,
      attributes['k8s.node.name']                  AS k8s_node,
      if(kind_string IN ('CLIENT', 'PRODUCER'),
         multiIf(peer_service != '',                             peer_service,
                 db_system != '',                                db_system,
                 db_name != '',                                  db_name,
                 attributes['server.address'] != '',             attributes['server.address'],
                 attributes['messaging.destination.name'] != '', attributes['messaging.destination.name'],
                 ''), '')                                 AS peer_name,
      if(kind_string IN ('CLIENT', 'PRODUCER'),
         multiIf(peer_service != '',                             '',
                 db_system != '',                                'database',
                 db_name != '',                                  'database',
                 attributes['server.address'] != '',             '',
                 attributes['messaging.destination.name'] != '', 'messaging',
                 ''), '')                                 AS peer_type,
      count()                                       AS request_count,
      sum(duration_nano / 1000000.0)                AS duration_ms_sum,
      quantilesTDigestState(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS latency_state
  FROM optikk.spans
  WHERE timestamp >= now() - INTERVAL 7 DAY
    AND timestamp <  '${MV_CREATED}'
  GROUP BY tenant_id, timestamp, service, environment, host, pod, span_name,
           kind_string, status_code_string, http_status_bucket, http_route,
           db_system, messaging_system, messaging_destination, messaging_consumer_group,
           cloud_provider, cloud_platform, cloud_region, k8s_node,
           peer_name, peer_type"

echo "Backfill complete. Verify: SELECT count() FROM optikk.span_stats_1m"

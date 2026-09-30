-- Go SubmitQueue request-feature events, September 1–29, 2026 (complete datestr partitions).
-- Hive table: rawdata_user.kafka_hp_submitqueue_request_feature_event_nodedup
-- QueryBuilder run: mcDHURW0X / PEOkkp0n
-- Data Central execution UUID: 6a86dc71-00db-4c93-a38d-b07cb221e51d
-- One latest event per immutable diff ID; exclude stacks so one observation is one code change.
-- Affected-target proxy = changed + added + removed from feature-event fields;
-- verify producer semantics before assuming the categories cannot overlap. This is not a Tango RPC result.
WITH single_diff_events AS (
  SELECT datestr,
         msg.diffids[1] AS diff_id,
         msg.basesha AS base_sha,
         msg.targetschanged AS targets_changed,
         msg.targetsadded AS targets_added,
         msg.targetsremoved AS targets_removed,
         msg.targetschanged + msg.targetsadded + msg.targetsremoved AS affected_targets,
         ts,
         ROW_NUMBER() OVER (
           PARTITION BY msg.diffids[1]
           ORDER BY ts DESC, datestr DESC
         ) AS latest_event
  FROM rawdata_user.kafka_hp_submitqueue_request_feature_event_nodedup
  WHERE datestr BETWEEN '2026-09-01' AND '2026-09-29'
    AND msg.queueid = 'go'
    AND CARDINALITY(msg.diffids) = 1
    AND msg.stackheight = 1
    AND msg.diffids[1] IS NOT NULL
    AND msg.targetschanged IS NOT NULL
    AND msg.targetsadded IS NOT NULL
    AND msg.targetsremoved IS NOT NULL
    AND (hadoop_isdeleted = false OR hadoop_isdeleted IS NULL)
), combined AS (
  SELECT 'raw_events' AS grain, targets_changed, targets_added, targets_removed, affected_targets
  FROM single_diff_events
  UNION ALL
  SELECT 'unique_diff_latest' AS grain, targets_changed, targets_added, targets_removed, affected_targets
  FROM single_diff_events
  WHERE latest_event = 1
)
SELECT grain,
       COUNT(*) AS observations,
       SUM(affected_targets) AS sum_affected_targets,
       AVG(CAST(affected_targets AS DOUBLE)) AS avg_affected_targets,
       AVG(CAST(targets_changed AS DOUBLE)) AS avg_changed,
       AVG(CAST(targets_added AS DOUBLE)) AS avg_added,
       AVG(CAST(targets_removed AS DOUBLE)) AS avg_removed,
       APPROX_PERCENTILE(affected_targets, ARRAY[0.5, 0.9, 0.95, 0.99, 0.999]) AS p50_p90_p95_p99_p999,
       COUNT_IF(affected_targets = 0) AS zero_target_observations,
       COUNT_IF(affected_targets >= 1000) AS ge_1000,
       COUNT_IF(affected_targets >= 10000) AS ge_10000,
       COUNT_IF(affected_targets >= 100000) AS ge_100000,
       MAX(affected_targets) AS max_affected_targets
FROM combined
GROUP BY grain
ORDER BY grain
LIMIT 10;

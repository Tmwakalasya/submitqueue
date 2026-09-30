-- Cross-check the broad-diff contribution independently of the mean query.
-- Grain and filters intentionally match hive-go-diff-targets-20260930.sql.
WITH single_diff_events AS (
  SELECT msg.diffids[1] AS diff_id,
         msg.targetschanged + msg.targetsadded + msg.targetsremoved AS affected_targets,
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
)
SELECT COUNT(*) AS unique_diffs,
       SUM(affected_targets) AS total_affected_targets,
       COUNT_IF(affected_targets >= 100000) AS broad_diffs,
       SUM(IF(affected_targets >= 100000, affected_targets, 0)) AS broad_affected_targets,
       AVG(IF(affected_targets < 100000, CAST(affected_targets AS DOUBLE), NULL)) AS avg_below_100000
FROM single_diff_events
WHERE latest_event = 1
LIMIT 10;

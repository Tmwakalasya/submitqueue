-- Exact empirical distribution of the per-diff affected-target proxy for go-code single-diff requests.
-- Same population as hive-go-diff-targets-20260930.sql (September 1–29, 2026, latest event per diff ID),
-- aggregated to one row per distinct affected-target count so no individual diff is exported.
-- QueryBuilder report ETX3KWSXp, run xeBwJha1Z; Data Central execution 38adcea7-47e2-4521-957f-e61eb47c5e89.
-- Output: hive-go-diff-target-histogram-20260930.csv (4,953 rows; 58,876 diffs; sum 148,766,796, matching the summary query).
WITH single_diff_events AS (
  SELECT msg.targetschanged + msg.targetsadded + msg.targetsremoved AS affected_targets,
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
SELECT affected_targets, COUNT(*) AS diffs
FROM single_diff_events
WHERE latest_event = 1
GROUP BY affected_targets
ORDER BY affected_targets

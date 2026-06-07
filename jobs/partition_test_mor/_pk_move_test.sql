-- MERGE_ON_READ: same key A, partition changed P -> S on upsert.
-- Global index (RECORD_INDEX) => expect a SINGLE row per table, no duplicate.
-- For MOR we query the _rt (realtime/snapshot) view to see merged base+log state.

-- update.partition.path = true  => record migrates to new partition S
SELECT 'pk_move_mor_true' AS tbl, "_hoodie_partition_path" AS partition, id, part, ts, val
FROM hudi.lake.pk_move_mor_true_rt
ORDER BY part;

-- update.partition.path = false => record stays in original partition P
SELECT 'pk_move_mor_false' AS tbl, "_hoodie_partition_path" AS partition, id, part, ts, val
FROM hudi.lake.pk_move_mor_false_rt
ORDER BY part;

-- Row counts on the realtime view: 1 each confirms no cross-partition duplicate
SELECT 'pk_move_mor_true_rt'  AS tbl, count(*) AS row_count FROM hudi.lake.pk_move_mor_true_rt
UNION ALL
SELECT 'pk_move_mor_false_rt' AS tbl, count(*) AS row_count FROM hudi.lake.pk_move_mor_false_rt;

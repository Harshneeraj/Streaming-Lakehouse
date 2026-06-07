-- Same key A, partition changed P -> S on upsert.
-- Global index (RECORD_INDEX) => expect a SINGLE row in each table, no duplicate.

-- update.partition.path = true  => record migrates to new partition S
SELECT 'pk_move_true' AS tbl, "_hoodie_partition_path" AS partition, id, part, ts, val
FROM hudi.lake.pk_move_true
ORDER BY part;

-- update.partition.path = false => record stays in original partition P
SELECT 'pk_move_false' AS tbl, "_hoodie_partition_path" AS partition, id, part, ts, val
FROM hudi.lake.pk_move_false
ORDER BY part;

-- Row counts: 1 each confirms no cross-partition duplicate
SELECT 'pk_move_true' AS tbl, count(*) AS row_count FROM hudi.lake.pk_move_true
UNION ALL
SELECT 'pk_move_false' AS tbl, count(*) AS row_count FROM hudi.lake.pk_move_false;

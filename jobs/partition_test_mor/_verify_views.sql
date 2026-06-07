-- Compare realtime (_rt: base+logs merged) vs read-optimized (_ro: base only)
-- for the update.partition.path=true MOR table.

SELECT 'TRUE_rt' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_true_rt ORDER BY partition;

SELECT 'TRUE_ro' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_true_ro ORDER BY partition;

SELECT 'FALSE_rt' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_false_rt ORDER BY partition;

SELECT 'FALSE_ro' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_false_ro ORDER BY partition;

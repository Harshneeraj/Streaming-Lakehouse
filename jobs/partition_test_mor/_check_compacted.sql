-- After compaction, both _ro (base-only) and _rt (realtime) should show 1 row in S.
SELECT 'compacted_ro' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_compacted_ro ORDER BY partition;

SELECT 'compacted_rt' AS view, "_hoodie_partition_path" AS partition, id, ts, val
FROM hudi.lake.pk_move_mor_compacted_rt ORDER BY partition;

SELECT 'compacted_ro' AS view, count(*) AS row_count FROM hudi.lake.pk_move_mor_compacted_ro
UNION ALL
SELECT 'compacted_rt' AS view, count(*) AS row_count FROM hudi.lake.pk_move_mor_compacted_rt;

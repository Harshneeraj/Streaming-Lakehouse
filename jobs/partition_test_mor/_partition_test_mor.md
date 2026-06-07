2 separate results with         "hoodie.record.index.update.partition.path": update_partition_path,

Yes with above config false upsert failed and with true it didn't do upsert

_pk_move_test.sql output

pk_move_mor_true   P  A  ts=1  first    <- OLD record still present!
pk_move_mor_true   S  A  ts=2  second   <- new record
pk_move_mor_false  P  A  ts=1  first    <- only original

row counts:
pk_move_mor_true_rt  = 2   <- DUPLICATE across partitions
pk_move_mor_false_rt = 1


_verify_views.sql output
"TRUE_rt","P","A","1","first"
"TRUE_rt","S","A","2","second"
"TRUE_ro","P","A","1","first"
"TRUE_ro","S","A","2","second"
"FALSE_rt","P","A","1","first"
"FALSE_ro","P","A","1","first"
Views also confirm the same thing

Output by reading via a spark job
It was same as the trino.
Spark's snapshot read shows 1 row — exactly like COW. Trino showed 2. So the duplicate is not a real Hudi data issue; it's a difference in how the two query engines read a MOR table that has a cross-partition update.

Why Trino showed 2 rows for the true case:

When the record moves partitions, Hudi implements it as delete from P + insert into S. In MOR, that delete is written as a delete record in a log file in partition P (the base Parquet in P still physically contains the old A). Reading the latest state requires merging the base file with the delete log so the old A is tombstoned.

Spark does this merge correctly across the record's global identity → old A is suppressed → 1 row.
Trino's Hudi MOR reader merges base+logs within each partition independently. The delete log lives in partition P, but here's the catch — let me confirm exactly where the delete landed, because that determines whether this is a Trino limitation or a compaction-timing thing.

Compaction resolves this....
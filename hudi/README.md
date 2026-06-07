HUDI = Transactional Data Lake / Data Lake House - framework that gives core warehouse and database functionality directly to a data lake

concurrency control, indexing, change capture and updates/deletes

Atomicity - Suppose you're upserting 1 million records. If the job crashes after writing 500,000 records, Hudi will not expose those 500,000 records as a valid table state.

Consistency-If a commit updates customer information, queries will either see the data before the update or after the update, but never an inconsistent mixture.

Isolation-A Spark job is writing new data at 10:00 AM. A query running at 10:00:05 AM will still see the snapshot from the most recent completed commit.This is often called Snapshot Isolation.

Durability-After a commit succeeds, even if the Spark cluster crashes immediately afterward, the data remains available.

Metadata Table: 
The Hudi metadata table is NOT a Hive/catalog-registered table. It lives entirely inside the data table's own directory on MinIO, under .hoodie/metadata/.
s3a://lakehouse/orders/orders_cow/
  .hoodie/              <- main table timeline (.commit files)
  .hoodie/metadata/     <- THE METADATA TABLE
    .hoodie/            <- metadata table's own inner timeline
    files/              <- partition 1
    bloom_filters/      <- partition 2
    column_stats/       <- partition 3
    record_index/       <- partition 4
  US/                   <- actual data partition
  IN/                   <- actual data partition
  ...
It's essentially a Hudi MOR table inside a Hudi table — the metadata table has its own .hoodie/ timeline, its own base files (.hfile format) and log files (.log). It's completely self-contained inside the outer table's directory and is read/written only by the Hudi engine internally.

The four partitions and what they store:

Partition	What's in it	Used for
files/	Every partition path + all file groups/slices in it	File listing — avoids expensive LIST calls to S3 on every read/write
bloom_filters/	Bloom filter per data file	Upsert key lookup — quickly eliminates files that can't contain a record key
column_stats/	Min/max + null counts per column per file	Data skipping — query engines push predicates to skip irrelevant files
record_index/	Map of record_key → file group ID + partition	Global index — O(1) upsert tag-location, what makes your RECORD_INDEX fast
Why you can't see it from Trino or the Hive Metastore:

It's never registered with any catalog. There's no CREATE TABLE for it.
It's only accessed via the Hudi table client internally, not via Spark SQL or Trino.
Trino's Hudi connector reads the main table's file listing from this metadata table under the hood, but doesn't expose the metadata table itself as a queryable relation.
Why it's on MinIO and not in Postgres/HMS:

It stores per-file data (file listings, bloom filters, column stats, record-level index). That data is high-volume and grows with the table. Putting it in Postgres would make HMS a bottleneck. By keeping it co-located with the table data on object storage, every reader/writer accesses it directly without a central metadata server hop — which is the whole point of Hudi's metadata layer replacing expensive S3 LIST operations with fast local reads.
# Kafka → Spark Structured Streaming → Apache Hudi Pipeline

A self-contained streaming lakehouse you can run on a single host.

## What's in the box

| Component        | Image                                | Role                                             |
|------------------|--------------------------------------|--------------------------------------------------|
| `kafka`          | `bitnamilegacy/kafka:3.7.1`          | Single-broker Kafka in KRaft mode                |
| `kafka-ui`       | `provectuslabs/kafka-ui:0.7.2`       | Topic browser at http://localhost:8088           |
| `minio`          | `minio/minio`                        | S3-compatible object store                       |
| `postgres`       | `postgres:15-alpine`                 | HMS metadata DB                                  |
| `hive-metastore` | thin layer over `apache/hive:4.0.0`  | Catalog used by Hudi sync, Spark, Trino          |
| `spark-master`   | `bitnamilegacy/spark:3.5.1` + Hudi 0.15 + S3A + Kafka | Driver and master                  |
| `spark-worker`   | same image                           | One executor (3 cores × 3 GB)                    |
| `trino`          | `trinodb/trino:451`                  | Hudi/Hive query engine at http://localhost:8090  |
| `producer`       | Python 3.11 + confluent-kafka        | Synthetic e-commerce event generator             |

## Quick start

The Makefile assumes a Linux/Mac shell with Docker on the path. On Windows
the easiest path is to clone this folder into WSL2 (Docker has much better
filesystem performance there) and run the targets from a WSL bash shell:

```bash
# inside WSL2
make build       # ~3 min (only the first time)
make up          # ~1 min: starts everything
make produce     # streams 20 000 synthetic orders into Kafka
make ingest      # spark-submit the Structured Streaming job (foreground)
make verify      # snapshot, time-travel, incremental queries via Spark
make sql         # the same checks via Trino
make smoke       # one-shot end-to-end check (counts, partitions, timeline)
make down        # stop services (volumes preserved)
make clean       # full reset (drops volumes)
```

UIs: Kafka 8088 · MinIO 9001 (admin / admin12345) · Spark 8080 · Trino 8090.

## What Hudi features are exercised

* COW + MOR tables with automatic Hive Metastore sync (incl. `_ro` and `_rt` projections)
* Upserts on `order_id` ordered by `event_ts_ms` (precombine)
* Soft deletes via `_hoodie_is_deleted`
* Partitioning on `country` with `MultiPartKeysValueExtractor`
* Hudi metadata table with column stats + bloom filter + record-level index
* Async cleaning, async clustering with Z-order on `(country, category)`
* MOR async compaction with `LogFileSizeBasedCompactionStrategy`
* In-process locks (S3-safe alternative to FS lock provider)
* Snapshot, read-optimized, incremental, and time-travel reads
* Trino's `hudi` catalog with predicate pushdown to Hudi metadata column stats

## Documentation

* `docs/PIPELINE.md` — full design, version rationale, tuning notes, ops playbook
* `docs/RUN_LOG.md` — end-to-end first-run transcript + verification output
* `docs/queries.sql` — sample Trino queries

## Verified results

Single end-to-end run: 20 000 events → 13 757 unique orders after upserts, distributed
evenly across 8 country partitions, with the order lifecycle (`PLACED → PAID → SHIPPED → DELIVERED`)
observable in the Hudi snapshot read. Both Spark and Trino return identical row counts and
aggregates. See `docs/RUN_LOG.md` for the full transcript.


# Kafka → Spark → Hudi Pipeline — Design & Operating Guide

A self-contained streaming lakehouse you can run on a single host. This document explains
what is in the box, why each piece is configured the way it is, and how to operate it.

## 1. Components:

| Service          | Image / Build                                | Purpose                                         |
|------------------|----------------------------------------------|-------------------------------------------------|
| `kafka`          | `bitnamilegacy/kafka:3.7.1` (KRaft mode)     | Single-broker Kafka, no ZooKeeper               |
| `kafka-ui`       | `provectuslabs/kafka-ui:0.7.2`               | Topic & message browser                         |
| `minio`          | `minio/minio:RELEASE.2024-08-17`             | S3-compatible object store                      |
| `minio-init`     | `minio/mc`                                   | Creates `lakehouse` and `warehouse` buckets     |
| `postgres`       | `postgres:15-alpine`                         | Metadata DB for Hive Metastore                  |
| `hms-deps`       | `alpine:3.20`                                | One-shot init that pulls the Postgres JDBC jar  |
| `hive-metastore` | thin layer over `apache/hive:4.0.0`          | Catalog used by Hudi sync, Spark, and Trino     |
| `spark-master`   | custom Spark 3.5.1 + Hudi 0.15.0 bundle      | Driver + cluster master                         |
| `spark-worker`   | same image                                   | Executors (3 cores × 3 GB)                       |
| `trino`          | `trinodb/trino:451`                          | Query engine; reads Hudi via the Hudi connector |
| `producer`       | custom Python 3.11 image                     | Synthetic event generator                       |

## 2. Why these versions

* **Spark 3.5.1 + Hudi 0.15.0** — `0.15.0` is the first Hudi release that ships a
  `hudi-spark3.5-bundle_2.12` artifact, so Spark 3.5 / Scala 2.12 works out of the box.
  Newer 1.x lines work too, but `0.15.0` is the most battle-tested baseline today.
* **Hadoop 3.3.4 drivers** — match what Spark 3.5 ships internally; this avoids the
  classic `hadoop-aws` ↔ `hadoop-client-runtime` shading clash.
* **Hive Metastore 4.0.0 (`apache/hive:4.0.0`)** — the official Apache Hive image runs
  the metastore service standalone via `SERVICE_NAME=metastore`. The community-maintained
  3.x standalone-metastore tarballs were removed from `archive.apache.org`, so building
  a custom 3.x image is brittle today. We thin-layer the official image to add the
  Postgres JDBC driver plus symlinks for `hadoop-aws` / `aws-java-sdk-bundle` (which the
  base image already ships under `/opt/hadoop/share/hadoop/tools/lib/`) into Hive's lib
  directory.
* **Trino 451** — current LTS-grade build with a mature Hudi connector that no longer
  needs Hudi's deprecated input format.

## 3. Storage layout

Two Hudi tables, identical schema, different table types:

```
s3a://lakehouse/orders/orders_cow/   COPY_ON_WRITE   read-optimised
s3a://lakehouse/orders/orders_mor/   MERGE_ON_READ   write-optimised + async compaction
s3a://lakehouse/_chk/orders/         streaming checkpoint
s3a://warehouse/                     Hive warehouse root (used by sync)
```

Both tables are partitioned by `country` and registered in HMS as `lake.orders_cow` and
`lake.orders_mor`. The `_hoodie_is_deleted` flag implements soft deletes for events whose
`_op = 'd'` (e.g. `status=CANCELLED`).

## 4. Schema

```python
order_id        STRING   PK / record key
user_id         STRING
country         STRING   partition column
category        STRING
amount          DOUBLE
currency        STRING
status          STRING   PLACED|PAID|SHIPPED|DELIVERED|CANCELLED
payment_method  STRING
items           INT
event_ts_ms     LONG     precombine field (latest wins on upsert)
ingest_ts_ms    LONG
_op             STRING   i / u / d
event_date      DATE     derived in Spark
processed_ts    TIMESTAMP
kafka_partition INT
kafka_offset    LONG
```

## 5. Hudi features exercised

* **Upserts** keyed on `order_id`, ordered by `event_ts_ms`.
* **Soft deletes** via `_hoodie_is_deleted` (no Hudi DeleteSupport needed).
* **Country-based partitioning** with `MultiPartKeysValueExtractor` for HMS sync.
* **Hudi metadata table** (`hoodie.metadata.enable=true`) with:
  * column-stats index (predicate pruning),
  * bloom filter index,
  * record-level index (`hoodie.index.type=RECORD_INDEX`) — O(1) tag-location lookups.
* **Async cleaning** retaining the last 10 commits.
* **Async clustering** every 4 commits, sorted by `country, category` using a Z-order curve
  (`hoodie.layout.optimize.strategy=z-order`).
* **Async compaction** (MOR only) using `LogFileSizeBasedCompactionStrategy`, scheduled
  inline + executed asynchronously; keeps log files ≤ 128 MB.
* **Optimistic concurrency control** with `InProcessLockProvider`. The S3A
  `FileSystemBasedLockProvider` cannot be used here because S3A does not support the
  atomic `create` semantic Hudi expects from a lock provider; in-process locks are
  correct for a single Spark application driver writing both tables.
* **Schema-on-read evolution** is enabled.
* **HMS sync** in `hms` mode using the Thrift endpoint directly (no JDBC, no HiveServer2).
* **Snapshot, incremental, time-travel, and read-optimized** queries are demonstrated by
  `jobs/verify_hudi.py`.

## 6. Tuning notes

Where the choices in `jobs/stream_to_hudi.py` and `conf/spark-defaults.conf` came from.

* **Trigger = 20 s** — matches the cleaning/clustering cadence and amortises the overhead
  of writing two tables per micro-batch.
* **`maxOffsetsPerTrigger = 20000`** — at ~200 events/s the producer keeps the broker hot
  but micro-batches stay small enough that each `foreachBatch` finishes in well under the
  trigger window. Bump it if you raise `EVENTS_PER_SECOND`.
* **`spark.sql.shuffle.partitions = 16` and Hudi `*.shuffle.parallelism = 16`** — keeps
  output Parquet count predictable on a single worker (4 cores × 4 GB).
* **Parquet small/max file size = 64 / 128 MB** — avoids the “tiny file” problem on streaming
  workloads while staying inside Hudi’s default file-group rewrite budget.
* **Snappy** for Parquet — best balance of CPU vs. compression for upserts, where each commit
  reads + rewrites file slices.
* **Kryo + `HoodieSparkKryoRegistrar`** — required for Hudi’s `HoodieRecord` payloads;
  default Java serialization is ~3× slower in practice.
* **Adaptive execution + skew-join** — pays off in clustering and incremental queries that
  shuffle uneven partitions (e.g. `country=US` is much larger than `country=JP`).
* **S3A `committer=directory` + `path.style.access=true`** — required for MinIO; the magic
  committer needs S3 conditional writes which MinIO does not implement.
* **`hadoop-client-runtime` is left untouched** in the Spark image; `hadoop-aws` 3.3.4 is
  matched to it. Mixing 3.3.6+ here would hit the well-known `S3AFileSystem` initialisation
  break.
* **`metastore_db`-backed Hive disabled** — we use Postgres for HMS so multiple Spark drivers
  can share the catalog.

## 7. Running it

Inside WSL or any Linux/Mac shell with Docker available:

```bash
make build       # ~3 min: builds spark + hive-metastore + producer images
make up          # ~1 min: starts kafka, minio, hms, spark, trino
make produce     # streams 20 000 synthetic orders into Kafka (3 partitions)
make ingest-bg   # spark-submit the streaming job; runs detached
make verify      # snapshot, incremental, time-travel queries in Spark
make sql         # the same checks via Trino
make down        # stops containers (keeps volumes)
make clean       # full reset (volumes wiped)
```

Useful URLs (printed by `make ui`):

* Kafka UI — http://localhost:8088
* MinIO console — http://localhost:9001 (admin / admin12345)
* Spark master — http://localhost:8080
* Spark driver UI — http://localhost:4040 while the streaming job runs
* Trino UI — http://localhost:8090

## 8. Verifying correctness

`jobs/verify_hudi.py` runs:

1. `SELECT COUNT(*)` on the COW snapshot, plus per-country / per-status histograms.
2. The same on the MOR snapshot, then again with `query.type=read_optimized` to show the
   delta between log files and base files.
3. An **incremental** query starting from the very first commit timestamp.
4. A **time-travel** read using `as.of.instant`.
5. A `SHOW TABLES IN lake` to prove HMS sync is wired in.
6. An aggregation through Spark SQL on the synced table to prove the catalog plumbs through.

`docs/queries.sql` does the equivalent through Trino.

## 9. Operating playbook

* **Stuck commit** (very rare): delete `.hoodie/.locks/*` under the table path. Hudi will
  re-acquire the FS lock on the next write.
* **Compaction fell behind** on MOR: `make ingest-bg` keeps scheduling inline + executing
  async; you can also manually run `org.apache.hudi.utilities.HoodieCompactor` via
  `spark-submit` against the same path.
* **HMS schema mismatch**: drop the `metastore` Postgres DB and `make up` again — the HMS
  entrypoint will run `schematool -initSchema` automatically.
* **MinIO bucket was wiped**: re-run `docker compose up minio-init`; it is idempotent.

## 10. Where to extend

* Add a **Debezium + Kafka Connect** stack to feed change data instead of synthetic events.
* Replace the producer with a real upstream (kafkacat, faust, etc.) — Spark expects only
  `value` to be JSON; the schema is enforced at parse time.
* Turn on `hoodie.write.markers.type=DIRECT` on cold storage with weak consistency.
* Enable Trino’s **insert/update/delete** support against the Hudi connector (preview in
  Trino 451+) by setting `hudi.commit-time-table-property-name`.

## 11. Verification log

The full first-run output is captured in `docs/RUN_LOG.md`.

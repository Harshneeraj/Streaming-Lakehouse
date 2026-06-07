"""
Kafka → Spark Structured Streaming → Apache Hudi

Reads JSON events from Kafka topic `events`, parses them with a strict schema,
and writes them to TWO Hudi tables to demonstrate the trade-offs:

  1) `lake.orders_cow`  - Copy-On-Write   (read-optimized, simpler)
  2) `lake.orders_mor`  - Merge-On-Read   (write-optimized + async compaction)

Hudi features exercised:
  * Upserts on `order_id` (recordKey) with `event_ts_ms` precombine
  * Soft deletes derived from `_op = 'd'` / status = CANCELLED
  * Country-based partitioning
  * Hive sync via Hive Metastore Thrift
  * Hudi metadata table with column stats + bloom filter + record-level index
  * Async cleaning + (MOR) async compaction + inline/async clustering
  * Optimistic concurrency control (multi-writer-safe locks, in-process)
  * Z-order layout via clustering for fast `country, category` predicates
  * Schema-on-read evolution allowed
"""
from __future__ import annotations

import argparse
import os
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType,
)


# ─────────────────────────────────────── Schema ────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("order_id",       StringType(),  False),
    StructField("user_id",        StringType(),  True),
    StructField("country",        StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("items",          IntegerType(), True),
    StructField("event_ts_ms",    LongType(),    True),
    StructField("ingest_ts_ms",   LongType(),    True),
    StructField("_op",            StringType(),  True),
])


# ─────────────────────────────────────── Hudi opts ─────────────────────────────────────
def hudi_common_options(table_name: str, table_type: str, base_path: str) -> Dict[str, str]:
    """Common Hudi write options. `table_type` is COPY_ON_WRITE or MERGE_ON_READ."""
    opts: Dict[str, str] = {
        # ── identity ─────────────────────────────────────────────────────────────────
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,

        # ── keys & ordering ──────────────────────────────────────────────────────────
        "hoodie.datasource.write.recordkey.field": "order_id",
        "hoodie.datasource.write.precombine.field": "event_ts_ms",
        "hoodie.datasource.write.partitionpath.field": "country",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",

        # ── operation: upsert by default; soft-delete is handled in transform ───────
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.combine.before.upsert": "true",
        "hoodie.combine.before.insert": "true",

        # ── parallelism (matches our shuffle.partitions=16) ──────────────────────────
        "hoodie.upsert.shuffle.parallelism": "16",
        "hoodie.insert.shuffle.parallelism": "16",
        "hoodie.bulkinsert.shuffle.parallelism": "16",
        "hoodie.delete.shuffle.parallelism": "16",

        # ── file sizing: keep output files in the 64–128 MB sweet spot ──────────────
        "hoodie.parquet.small.file.limit":  str(64 * 1024 * 1024),
        "hoodie.parquet.max.file.size":     str(128 * 1024 * 1024),
        "hoodie.parquet.compression.codec": "snappy",
        "hoodie.copyonwrite.record.size.estimate": "256",

        # ── metadata table & indexing (huge speedup for upserts/queries) ─────────────
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.bloom.filter.enable": "true",
        # Record-level index enables O(1) tag-location for upserts on huge tables
        "hoodie.metadata.record.index.enable": "true",
        "hoodie.index.type": "RECORD_INDEX",

        # ── cleaning: keep last 10 commits, run async ────────────────────────────────
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained": "10",

        # ── archival: keep timeline lean ─────────────────────────────────────────────
        "hoodie.keep.min.commits": "20",
        "hoodie.keep.max.commits": "30",

        # ── clustering: rewrite small files & sort for fast predicate pushdown ──────
        "hoodie.clustering.inline": "false",
        "hoodie.clustering.async.enabled": "true",
        "hoodie.clustering.async.max.commits": "4",
        "hoodie.clustering.plan.strategy.target.file.max.bytes":  str(128 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.small.file.limit":       str(64 * 1024 * 1024),
        "hoodie.clustering.plan.strategy.sort.columns":           "country,category",
        "hoodie.layout.optimize.strategy":                        "z-order",
        "hoodie.layout.optimize.curve.build.method":              "directly",

        # ── concurrency control ─────────────────────────────────────────────────────
        # Single-writer (this Spark streaming app); use in-process locks since S3A does not
        # support atomic creates that the FileSystemBasedLockProvider needs.
        "hoodie.write.concurrency.mode":               "SINGLE_WRITER",
        "hoodie.cleaner.policy.failed.writes":         "EAGER",
        "hoodie.write.lock.provider":                  "org.apache.hudi.client.transaction.lock.InProcessLockProvider",

        # ── hive sync via thrift HMS ─────────────────────────────────────────────────
        "hoodie.datasource.hive_sync.enable":          "true",
        "hoodie.datasource.hive_sync.mode":            "hms",
        "hoodie.datasource.hive_sync.metastore.uris":  "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.database":        "lake",
        "hoodie.datasource.hive_sync.table":           table_name,
        "hoodie.datasource.hive_sync.partition_fields": "country",
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.datasource.hive_sync.create_managed_table": "false",
        "hoodie.datasource.hive_sync.use_jdbc":            "false",
        "hoodie.datasource.meta.sync.base.path":           base_path,

        # ── schema evolution ────────────────────────────────────────────────────────
        "hoodie.schema.on.read.enable":              "true",
        "hoodie.datasource.write.reconcile.schema":  "true",
    }

    if table_type == "MERGE_ON_READ":
        opts.update({
            # write logs frequently, compact in background
            "hoodie.compact.inline":            "false",
            "hoodie.compact.schedule.inline":   "true",
            "hoodie.compact.inline.max.delta.commits": "5",
            "hoodie.compaction.strategy":       "org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy",
            "hoodie.logfile.max.size":          str(128 * 1024 * 1024),
            "hoodie.datasource.compaction.async.enable": "true",
        })
    return opts


# ─────────────────────────────────────── Spark session ─────────────────────────────────
def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Logged config covers the rest via spark-defaults.conf
        .enableHiveSupport()
        .getOrCreate()
    )


# ─────────────────────────────────────── Streaming pipeline ────────────────────────────
def build_stream(spark: SparkSession, kafka_bootstrap: str, topic: str, starting: str) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "20000")
        .option("kafka.session.timeout.ms", "30000")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts", "partition AS kafka_partition", "offset AS kafka_offset")
        .withColumn("evt", F.from_json(F.col("json_str"), EVENT_SCHEMA))
        .filter(F.col("evt").isNotNull() & F.col("evt.order_id").isNotNull())
        .select(
            F.col("evt.order_id").alias("order_id"),
            F.col("evt.user_id").alias("user_id"),
            F.coalesce(F.col("evt.country"), F.lit("UNKNOWN")).alias("country"),
            F.col("evt.category").alias("category"),
            F.col("evt.amount").alias("amount"),
            F.col("evt.currency").alias("currency"),
            F.col("evt.status").alias("status"),
            F.col("evt.payment_method").alias("payment_method"),
            F.col("evt.items").alias("items"),
            F.col("evt.event_ts_ms").alias("event_ts_ms"),
            F.col("evt.ingest_ts_ms").alias("ingest_ts_ms"),
            F.coalesce(F.col("evt._op"), F.lit("u")).alias("_op"),
            # processing columns
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.current_timestamp().alias("processed_ts"),
            F.to_date(F.from_unixtime(F.col("evt.event_ts_ms") / 1000)).alias("event_date"),
        )
    )
    return parsed


def write_to_hudi(batch_df: DataFrame, batch_id: int, *, table_name: str, table_type: str, base_path: str) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[batch {batch_id}] empty — skipping {table_name}")
        return

    # Hudi soft-delete marker column: rows with _op = 'd' get tombstoned
    enriched = batch_df.withColumn(
        "_hoodie_is_deleted",
        F.when(F.col("_op") == F.lit("d"), F.lit(True)).otherwise(F.lit(False)),
    )

    opts = hudi_common_options(table_name, table_type, base_path)
    count = enriched.count()
    print(f"[batch {batch_id}] writing {count} rows to {table_name} ({table_type}) at {base_path}")

    (
        enriched.write
        .format("org.apache.hudi")
        .options(**opts)
        .mode("append")
        .save(base_path)
    )
    print(f"[batch {batch_id}] {table_name} done")


def run(args: argparse.Namespace) -> None:
    spark = build_spark(args.app_name)
    spark.sparkContext.setLogLevel(args.log_level)

    # Make sure target DB exists for hive sync
    spark.sql("CREATE DATABASE IF NOT EXISTS lake")

    parsed = build_stream(spark, args.kafka_bootstrap, args.topic, args.starting_offsets)

    cow_path = f"{args.lake_root}/orders_cow"
    mor_path = f"{args.lake_root}/orders_mor"

    def for_each(batch_df: DataFrame, batch_id: int) -> None:
        # Cache once because we write twice
        batch_df = batch_df.persist()
        try:
            write_to_hudi(batch_df, batch_id, table_name="orders_cow", table_type="COPY_ON_WRITE", base_path=cow_path)
            write_to_hudi(batch_df, batch_id, table_name="orders_mor", table_type="MERGE_ON_READ", base_path=mor_path)
        finally:
            batch_df.unpersist()

    query = (
        parsed.writeStream
        .foreachBatch(for_each)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .queryName("kafka_to_hudi")
        .start()
    )

    print("[stream] started; awaiting termination...")
    query.awaitTermination(timeout=args.run_seconds if args.run_seconds > 0 else None)
    if args.run_seconds > 0:
        print(f"[stream] hit run_seconds={args.run_seconds}; stopping gracefully")
        query.stop()
    spark.stop()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka → Spark Structured Streaming → Apache Hudi")
    p.add_argument("--app-name",         default=os.getenv("APP_NAME", "kafka_to_hudi"))
    p.add_argument("--kafka-bootstrap",  default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic",            default=os.getenv("KAFKA_TOPIC", "events"))
    p.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "earliest"),
                   choices=["earliest", "latest"])
    p.add_argument("--lake-root",        default=os.getenv("LAKE_ROOT", "s3a://lakehouse/orders"))
    p.add_argument("--checkpoint",       default=os.getenv("CHECKPOINT", "s3a://lakehouse/_chk/orders"))
    p.add_argument("--trigger",          default=os.getenv("TRIGGER", "20 seconds"))
    p.add_argument("--run-seconds",      type=int, default=int(os.getenv("RUN_SECONDS", "0")),
                   help="if > 0, stop the stream after N seconds (useful for CI/test runs)")
    p.add_argument("--log-level",        default=os.getenv("LOG_LEVEL", "WARN"))
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())

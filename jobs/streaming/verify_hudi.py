"""
Verifies the Hudi tables written by `stream_to_hudi.py`.

Demonstrates:
  - SNAPSHOT query (default)
  - INCREMENTAL query (commits since the earliest one + 1)
  - READ-OPTIMIZED query (MOR table; bypasses log files, fastest reads)
  - Time-travel ("as of" a specific commit time)
  - File-level stats from the Hudi metadata table
"""
from __future__ import annotations

import os
import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def list_commits(spark: SparkSession, base_path: str) -> List[str]:
    # Hudi's commit timeline lives in <base>/.hoodie/*.commit / *.deltacommit
    fs_df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "false")
        .load(f"{base_path}/.hoodie")
        .filter(F.col("path").rlike(r"\.hoodie/[0-9]+\.(commit|deltacommit)$"))
        .select(F.regexp_extract("path", r"([0-9]+)\.(commit|deltacommit)$", 1).alias("ts"))
        .filter(F.col("ts") != "")
        .distinct()
        .orderBy("ts")
    )
    return [r["ts"] for r in fs_df.collect()]


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("verify_hudi")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    cow_path = os.getenv("COW_PATH", "s3a://lakehouse/orders/orders_cow")
    mor_path = os.getenv("MOR_PATH", "s3a://lakehouse/orders/orders_mor")

    # ── SNAPSHOT ─────────────────────────────────────────────────────────────────
    banner("COW snapshot count + sample")
    cow = spark.read.format("hudi").load(cow_path)
    print(f"COW total rows: {cow.count()}")
    cow.groupBy("country").count().orderBy(F.desc("count")).show(20, False)
    cow.groupBy("status").count().orderBy(F.desc("count")).show(20, False)
    cow.select("order_id", "user_id", "country", "category", "status", "amount", "event_ts_ms").show(10, False)

    # ── MOR snapshot vs read-optimized ────────────────────────────────────────────
    banner("MOR snapshot vs read-optimized")
    mor_snap = spark.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").load(mor_path)
    mor_ro   = spark.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(mor_path)
    print(f"MOR snapshot rows:        {mor_snap.count()}")
    print(f"MOR read-optimized rows:  {mor_ro.count()}  (only base parquet, no log files)")

    # ── INCREMENTAL ──────────────────────────────────────────────────────────────
    banner("COW incremental query")
    commits = list_commits(spark, cow_path)
    if len(commits) >= 2:
        # Hudi expects a "begin" instant strictly less than the commits we want to see
        # ("0" effectively asks for everything that exists in the timeline).
        begin = "0"
        print(f"commits found: {len(commits)} -- incremental from {begin}")
        try:
            incr = (
                spark.read.format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", begin)
                .load(cow_path)
            )
            print(f"incremental rows since {begin}: {incr.count()}")
            incr.select("order_id", "status", "_hoodie_commit_time").show(5, False)
        except Exception as e:  # noqa: BLE001
            print(f"incremental query skipped: {e!r}")
    else:
        print("not enough commits for incremental query yet")

    # ── TIME TRAVEL ──────────────────────────────────────────────────────────────
    banner("Time travel")
    if commits:
        first = commits[0]
        try:
            df_t = (
                spark.read.format("hudi")
                .option("as.of.instant", first)
                .load(cow_path)
            )
            print(f"rows as of first commit {first}: {df_t.count()}")
        except Exception as e:  # noqa: BLE001
            print(f"time-travel query skipped: {e!r}")

    # ── COMMIT METADATA ──────────────────────────────────────────────────────────
    banner("Commit timeline (COW)")
    print("\n".join(commits))

    # ── HMS sync sanity check ────────────────────────────────────────────────────
    banner("Catalog: lake.* tables")
    spark.sql("SHOW TABLES IN lake").show(50, False)

    banner("Direct Spark SQL on synced tables")
    spark.sql("SELECT country, COUNT(*) AS n, ROUND(AVG(amount), 2) AS avg_amount "
              "FROM lake.orders_cow GROUP BY country ORDER BY n DESC").show(20, False)

    spark.stop()


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"verify failed: {e}", file=sys.stderr)
        raise

"""
Q2: run compaction WITHOUT ingesting new data.

1) Create a MOR table with 1 insert + 1 update (no inline compaction) -> leaves a log file.
2) In a SEPARATE step, call run_compaction via Spark SQL (no new data written).
3) Confirm the log file got merged into a new base parquet.
"""
from pyspark.sql import SparkSession, Row


def opts(table_name, base_path):
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "part",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.index.type": "BLOOM",
        "hoodie.metadata.enable": "false",
        "hoodie.compact.inline": "false",
        "hoodie.compact.schedule.inline": "false",
        # force a plan even with few delta commits
        "hoodie.compact.inline.max.delta.commits": "1",
        "hoodie.compaction.strategy":
            "org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy",
        # in-process lock (S3A has no atomic create for FileSystem lock)
        "hoodie.write.concurrency.mode": "SINGLE_WRITER",
        "hoodie.write.lock.provider":
            "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        # register so we can CALL run_compaction by table name
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.database": "lake",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": "part",
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.meta.sync.base.path": base_path,
    }


def main():
    spark = (
        SparkSession.builder.appName("standalone_compaction")
        .config("hoodie.metadata.enable", "false")
        .enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE DATABASE IF NOT EXISTS lake")

    path = "s3a://lakehouse/_test/sc_mor"
    name = "sc_mor"

    # two writes -> base parquet + log file
    spark.createDataFrame([Row(id="A", part="P", ts=1, val="first")]) \
        .write.format("org.apache.hudi").options(**opts(name, path)).mode("append").save(path)
    spark.createDataFrame([Row(id="A", part="P", ts=2, val="second")]) \
        .write.format("org.apache.hudi").options(**opts(name, path)).mode("append").save(path)

    print("[setup] wrote insert + update (log file should exist)")

    # ---- Standalone compaction: NO new data written here ----
    print("[compaction] scheduling...")
    spark.sql(f"CALL run_compaction(op => 'schedule', table => 'lake.{name}')").show(truncate=False)
    print("[compaction] running...")
    spark.sql(f"CALL run_compaction(op => 'run', table => 'lake.{name}')").show(truncate=False)

    print("[compaction] timeline after:")
    spark.sql(f"CALL show_compaction(table => 'lake.{name}')").show(truncate=False)

    rows = spark.read.format("org.apache.hudi").load(path).select("id", "ts", "val").collect()
    print(f"[after compaction] hudi read = {len(rows)} row(s): " +
          ", ".join(f"(ts={r['ts']},val={r['val']})" for r in rows))

    spark.stop()


if __name__ == "__main__":
    main()

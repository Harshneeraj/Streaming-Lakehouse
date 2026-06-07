"""
MOR cross-partition move + FORCED COMPACTION.

Same scenario as _test_partition_change.py but with inline compaction enabled
so the delete log in the old partition gets merged into the base file. After
this, Trino's read-optimized/realtime views should agree with Spark (1 row).

  1) INSERT  (id=A, part=P, ts=1)
  2) UPSERT  (id=A, part=S, ts=2)  -> triggers compaction (max.delta.commits=1)
"""
from pyspark.sql import SparkSession, Row


def common_opts(table_name, base_path):
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "part",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.record.index.enable": "true",
        "hoodie.record.index.update.partition.path": "true",
        # ── FORCE inline compaction after every delta commit ──────────────
        "hoodie.compact.inline": "true",
        "hoodie.compact.inline.max.delta.commits": "1",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        # ── hive sync ─────────────────────────────────────────────────────
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
    spark = SparkSession.builder.appName("mor_compaction_test").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE DATABASE IF NOT EXISTS lake")

    table_name = "pk_move_mor_compacted"
    base_path = "s3a://lakehouse/_test/pk_move_mor_compacted"

    df1 = spark.createDataFrame([Row(id="A", part="P", ts=1, val="first")])
    df1.write.format("org.apache.hudi").options(**common_opts(table_name, base_path)) \
        .mode("append").save(base_path)

    df2 = spark.createDataFrame([Row(id="A", part="S", ts=2, val="second")])
    df2.write.format("org.apache.hudi").options(**common_opts(table_name, base_path)) \
        .mode("append").save(base_path)

    print(f"[{table_name}] written + compacted + synced")

    # Spark snapshot read as ground truth
    rows = spark.read.format("org.apache.hudi").load(base_path) \
        .select("_hoodie_partition_path", "id", "ts", "val").collect()
    print(f"[spark-snapshot] row count = {len(rows)}")
    for r in rows:
        print(f"   part={r['_hoodie_partition_path']}  id={r['id']}  ts={r['ts']}  val={r['val']}")

    spark.stop()


if __name__ == "__main__":
    main()

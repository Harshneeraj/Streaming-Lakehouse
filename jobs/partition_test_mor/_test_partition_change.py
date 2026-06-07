"""
Ad-hoc test: same record key, different partition path  (MERGE_ON_READ, via Trino).

Scenario:
  1) INSERT  (id=A, part=P, ts=1)
  2) UPSERT  (id=A, part=S, ts=2)   <- same key, NEW partition

Writes two Hudi MOR tables into the `lake` DB and syncs them to the Hive
Metastore so Trino can query them. For MOR, hive sync registers each as
three catalog entries: <name>, <name>_ro (read-optimized), <name>_rt (realtime):
  * lake.pk_move_mor_true   (hoodie...update.partition.path = true)
  * lake.pk_move_mor_false  (hoodie...update.partition.path = false)
"""
from pyspark.sql import SparkSession, Row


def common_opts(table_name, base_path, update_partition_path):
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "part",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        # Global index (same family as the project's RECORD_INDEX)
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.record.index.enable": "true",
        # The knob under test:
        "hoodie.record.index.update.partition.path": update_partition_path,
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        # ── hive sync so Trino can see it ─────────────────────────────────
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


def run_case(spark, table_name, base_path, update_partition_path):
    df1 = spark.createDataFrame([Row(id="A", part="P", ts=1, val="first")])
    df1.write.format("org.apache.hudi") \
        .options(**common_opts(table_name, base_path, update_partition_path)) \
        .mode("append").save(base_path)

    df2 = spark.createDataFrame([Row(id="A", part="S", ts=2, val="second")])
    df2.write.format("org.apache.hudi") \
        .options(**common_opts(table_name, base_path, update_partition_path)) \
        .mode("append").save(base_path)
    print(f"[{table_name}] written + synced (MOR)")


def main():
    spark = SparkSession.builder.appName("test_partition_change_mor").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE DATABASE IF NOT EXISTS lake")

    run_case(spark, "pk_move_mor_true",  "s3a://lakehouse/_test/pk_move_mor_true",  "true")
    run_case(spark, "pk_move_mor_false", "s3a://lakehouse/_test/pk_move_mor_false", "false")

    spark.stop()


if __name__ == "__main__":
    main()

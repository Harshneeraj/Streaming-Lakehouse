"""
Test: write 1 record, update it in a 2nd batch, then read the RAW parquet
files directly (bypassing Hudi's merge) for both COW and MOR.

Question: do the raw parquet files contain ALL record versions?

We read via spark.read.parquet(...) which ignores the Hudi timeline/log
merge and just returns whatever rows physically live in the parquet files.
"""
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


def opts(table_name, base_path, table_type):
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "part",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.record.index.enable": "true",
        # keep both versions around: don't auto-clean during the test
        "hoodie.clean.automatic": "false",
        # MOR: do NOT compact, so the update stays in a log file
        "hoodie.compact.inline": "false",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.datasource.hive_sync.enable": "false",
    }


def write_two_batches(spark, table_name, base_path, table_type):
    df1 = spark.createDataFrame([Row(id="A", part="P", ts=1, val="first")])
    df1.write.format("org.apache.hudi").options(**opts(table_name, base_path, table_type)) \
        .mode("append").save(base_path)

    df2 = spark.createDataFrame([Row(id="A", part="P", ts=2, val="second")])
    df2.write.format("org.apache.hudi").options(**opts(table_name, base_path, table_type)) \
        .mode("append").save(base_path)


def report(spark, label, base_path):
    print(f"\n========== {label} ==========")

    # 1) Hudi-aware read (merged, correct latest state)
    hudi_rows = spark.read.format("org.apache.hudi").load(base_path) \
        .select("id", "part", "ts", "val").collect()
    print(f"[{label}] HUDI read  -> {len(hudi_rows)} row(s): " +
          ", ".join(f"(id={r['id']},ts={r['ts']},val={r['val']})" for r in hudi_rows))

    # 2) RAW parquet read (physical parquet rows only, ignores logs + timeline).
    # recursiveFileLookup=true disables partition-path inference (avoids the
    # `part` column clash) and just reads every parquet file's actual rows.
    try:
        raw = spark.read.option("recursiveFileLookup", "true") \
            .parquet(f"{base_path}/P") \
            .select("id", "ts", "val").orderBy("ts")
        raw_rows = raw.collect()
        print(f"[{label}] RAW parquet -> {len(raw_rows)} row(s): " +
              ", ".join(f"(id={r['id']},ts={r['ts']},val={r['val']})" for r in raw_rows))
    except Exception as e:
        import traceback
        print(f"[{label}] RAW parquet read error:")
        traceback.print_exc()


def main():
    spark = SparkSession.builder.appName("raw_read_test").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    cow_path = "s3a://lakehouse/_test/raw_cow"
    mor_path = "s3a://lakehouse/_test/raw_mor"

    write_two_batches(spark, "raw_cow", cow_path, "COPY_ON_WRITE")
    write_two_batches(spark, "raw_mor", mor_path, "MERGE_ON_READ")

    report(spark, "COW", cow_path)
    report(spark, "MOR", mor_path)

    spark.stop()


if __name__ == "__main__":
    main()

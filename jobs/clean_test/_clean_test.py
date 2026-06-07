"""
Demonstrate Hudi cleaning on a COW table.

COW keeps a NEW base parquet per update; the old (superseded) base file
lingers until the cleaner removes it. With cleaner.commits.retained=1, after
the 2nd update the 1st base file becomes eligible and gets cleaned.

We write 3 batches and watch the base-file count + raw-parquet rows shrink.
"""
from pyspark.sql import SparkSession, Row


def opts(table_name, base_path, clean_on, retained):
    o = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "part",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.record.index.enable": "true",
        # ── CLEANING ──────────────────────────────────────────────────────
        "hoodie.clean.automatic": clean_on,
        "hoodie.clean.async": "false",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained": str(retained),
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.datasource.hive_sync.enable": "false",
    }
    return o


def count_base_files(spark, base_path):
    # raw parquet rows in partition P (each base file holds 1 version of A)
    raw = spark.read.option("recursiveFileLookup", "true").parquet(f"{base_path}/P")
    return raw.count()


def write_batch(spark, table_name, base_path, ts, val, clean_on, retained):
    df = spark.createDataFrame([Row(id="A", part="P", ts=ts, val=val)])
    df.write.format("org.apache.hudi").options(**opts(table_name, base_path, clean_on, retained)) \
        .mode("append").save(base_path)


def main():
    spark = SparkSession.builder.appName("clean_test").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    path = "s3a://lakehouse/_test/clean_cow"

    # 3 batches with cleaning ON, retain only the latest 1 commit
    write_batch(spark, "clean_cow", path, 1, "first",  "true", 1)
    print(f"[after batch 1] raw parquet rows = {count_base_files(spark, path)}")

    write_batch(spark, "clean_cow", path, 2, "second", "true", 1)
    print(f"[after batch 2] raw parquet rows = {count_base_files(spark, path)}")

    write_batch(spark, "clean_cow", path, 3, "third",  "true", 1)
    print(f"[after batch 3] raw parquet rows = {count_base_files(spark, path)}")

    hudi_rows = spark.read.format("org.apache.hudi").load(path).select("id", "ts", "val").collect()
    print(f"[hudi read] {len(hudi_rows)} row(s): " +
          ", ".join(f"(ts={r['ts']},val={r['val']})" for r in hudi_rows))

    spark.stop()


if __name__ == "__main__":
    main()

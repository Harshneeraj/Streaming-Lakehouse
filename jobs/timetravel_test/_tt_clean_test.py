"""
Two checks on a COW table:

  Q1) After the cleaner deletes the old base file, can we time-travel to it?
  Q2) (separate MOR test in _standalone_compaction.py)

Write 3 batches with cleaner.commits.retained=1 so batch-1 files get cleaned,
capture each commit's instant time, then try time-travel reads at each instant.
"""
from pyspark.sql import SparkSession, Row


def opts(table_name, base_path):
    return {
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
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "false",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained": "1",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.datasource.hive_sync.enable": "false",
    }


def write_batch(spark, path, ts, val):
    df = spark.createDataFrame([Row(id="A", part="P", ts=ts, val=val)])
    df.write.format("org.apache.hudi").options(**opts("tt_cow", path)).mode("append").save(path)


def main():
    spark = SparkSession.builder.appName("tt_clean_test").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    path = "s3a://lakehouse/_test/tt_cow"

    write_batch(spark, path, 1, "first")
    write_batch(spark, path, 2, "second")
    write_batch(spark, path, 3, "third")

    # list commit instants from the timeline
    commits = spark.read.format("org.apache.hudi").load(path) \
        .select("_hoodie_commit_time").distinct().collect()
    instants = sorted(r["_hoodie_commit_time"] for r in commits)
    print(f"[timeline] visible commit instants in current snapshot: {instants}")

    # Pull the FULL commit list from the .hoodie timeline via the metadata
    hpath = spark._jvm.org.apache.hadoop.fs.Path(path + "/.hoodie")
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    all_instants = [f.getPath().getName() for f in fs.listStatus(hpath)
                    if f.getPath().getName().endswith(".commit")]
    all_instants = sorted(i.replace(".commit", "") for i in all_instants)
    print(f"[timeline] all .commit instants on disk: {all_instants}")

    # Try time-travel to the FIRST (oldest) commit -> its base file may be cleaned
    first = all_instants[0]
    print(f"\n[time-travel] attempting read as of FIRST commit {first} ...")
    try:
        df = spark.read.format("org.apache.hudi") \
            .option("as.of.instant", first).load(path) \
            .select("id", "ts", "val")
        rows = df.collect()
        print(f"[time-travel @ {first}] SUCCESS -> {len(rows)} row(s): " +
              ", ".join(f"(ts={r['ts']},val={r['val']})" for r in rows))
    except Exception as e:
        print(f"[time-travel @ {first}] FAILED -> {type(e).__name__}: {str(e)[:160]}")

    # Time-travel to the LATEST commit -> should always work
    last = all_instants[-1]
    print(f"\n[time-travel] attempting read as of LATEST commit {last} ...")
    try:
        df = spark.read.format("org.apache.hudi") \
            .option("as.of.instant", last).load(path) \
            .select("id", "ts", "val")
        rows = df.collect()
        print(f"[time-travel @ {last}] SUCCESS -> {len(rows)} row(s): " +
              ", ".join(f"(ts={r['ts']},val={r['val']})" for r in rows))
    except Exception as e:
        print(f"[time-travel @ {last}] FAILED -> {type(e).__name__}: {str(e)[:160]}")

    spark.stop()


if __name__ == "__main__":
    main()

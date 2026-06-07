"""Cross-check the MOR partition-move result with a Spark snapshot read."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mor_read_check").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for label, path in [
    ("TRUE",  "s3a://lakehouse/_test/pk_move_mor_true"),
    ("FALSE", "s3a://lakehouse/_test/pk_move_mor_false"),
]:
    df = spark.read.format("org.apache.hudi").load(path) \
        .select("_hoodie_partition_path", "id", "ts", "val").orderBy("_hoodie_partition_path")
    rows = df.collect()
    print(f"[{label}] snapshot row count = {len(rows)}")
    for r in rows:
        print(f"   part={r['_hoodie_partition_path']}  id={r['id']}  ts={r['ts']}  val={r['val']}")

spark.stop()

# Spark — How It Connects to Hive Metastore

## The Hive Client inside Spark

Spark has a built-in library called the **Hive client** that knows how to talk to a Hive Metastore. Whenever Spark needs to look up a table, list partitions, or register a new table, it goes through this client.

The call chain:

```
spark.sql("SELECT * FROM lake.orders_cow")
    → Spark needs to resolve "lake.orders_cow"
    → Hive client opens a Thrift connection to hive-metastore:9083
    → Asks: "where is table orders_cow in database lake?"
    → HMS responds: "it's at s3a://lakehouse/orders/orders_cow, partitioned by country"
    → Spark reads the Parquet files directly from MinIO
```

---

## How Hive support is enabled

In `spark/jobs/stream_to_hudi.py`:

```python
SparkSession.builder.enableHiveSupport().getOrCreate()
```

This switches Spark from its default in-memory catalog to `HiveExternalCatalog`, which routes all table lookups through the Hive client → HMS.

---

## How Hudi syncs tables to HMS

When Hudi finishes writing a batch, it calls HMS to register or update the table:

```python
"hoodie.datasource.hive_sync.mode": "hms"
"hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083"
```

After sync, four tables appear in HMS:
- `lake.orders_cow` — COW snapshot
- `lake.orders_mor` — MOR snapshot
- `lake.orders_mor_ro` — MOR read-optimized (base files only)
- `lake.orders_mor_rt` — MOR real-time (base + log files)

---

## Connection behavior

- **One connection per Spark session.** Opened lazily, kept alive for the session's lifetime.
- **Hudi opens its own connection.** Separate from Spark's.
- **No new socket per query.** Both connections are reused.

---

## Key config

In `spark/conf/spark-defaults.conf`:

```properties
spark.hadoop.hive.metastore.uris    thrift://hive-metastore:9083
```

# MinIO — Self-Hosted S3

MinIO is a storage server that speaks the same language (HTTP API) as AWS S3. Any tool that works with S3 works with MinIO — you just change the endpoint URL.

In this project, MinIO replaces AWS S3. Hudi writes Parquet files to it, Spark reads from it, and Trino queries through it.

---

## How Hadoop knows which storage to use

Hadoop picks the storage driver based on the URL scheme:

| Scheme | Driver | Talks to |
|--------|--------|----------|
| `file://` | LocalFileSystem | Your local disk |
| `hdfs://` | DistributedFileSystem | HDFS NameNode |
| `s3a://` | S3AFileSystem | Anything that speaks S3 (AWS, MinIO, etc.) |
| `gs://` | GoogleHadoopFileSystem | Google Cloud Storage |
| `abfs://` | AzureBlobFileSystem | Azure Data Lake Gen2 |

So `s3a://lakehouse/orders/...` means: "use the S3A driver to access bucket `lakehouse`."

---

## Pointing S3A at MinIO instead of AWS

By default, S3A talks to `s3.amazonaws.com`. We override it in `spark/conf/spark-defaults.conf`:

```properties
spark.hadoop.fs.s3a.endpoint              http://minio:9000
spark.hadoop.fs.s3a.access.key            admin
spark.hadoop.fs.s3a.secret.key            admin12345
spark.hadoop.fs.s3a.path.style.access     true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

---

## Why `s3://` works on EMR but not here

On **AWS EMR**, Amazon ships their own driver (EMRFS) registered under the `s3://` scheme. On **plain Spark** (like our Docker setup), only `s3a://` works.

Rule of thumb: if you're not on EMR or Databricks, always use `s3a://`.

---

## MinIO ports

| Port | Purpose |
|------|---------|
| 9000 | S3 API — what Spark, Hudi, Trino, and HMS connect to |
| 9001 | Web console — browse buckets at http://localhost:9001 (login: `admin` / `admin12345`) |

---

## Why the `minio-init` container exists

MinIO starts with zero buckets. The `minio-init` container runs once, creates the required buckets, and exits:

```
mc mb local/lakehouse    → Hudi data
mc mb local/warehouse    → Hive default warehouse
```

This makes the setup reproducible — `make up` always gives you a ready environment.

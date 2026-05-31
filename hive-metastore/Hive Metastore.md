# Hive Metastore — The Table Catalog

## What it does

The Hive Metastore (HMS) is a catalog service. It answers questions like:

- What tables exist in database `lake`?
- Where is the data for `lake.orders_cow`? → `s3a://lakehouse/orders/orders_cow`
- What columns does it have? What types?
- What partitions exist? → `country=US`, `country=IN`, ...

It does **not** read or write your actual data. It only stores metadata.

---

## How clients connect to it

HMS exposes a **Thrift** server on port 9083. Three clients connect in our project:

```
Spark       → thrift://hive-metastore:9083  (spark-defaults.conf)
Hudi sync   → thrift://hive-metastore:9083  (stream_to_hudi.py)
Trino       → thrift://hive-metastore:9083  (hudi.properties)
```

---

## What is Thrift?

A way for programs to call functions on a remote server:

| Protocol | Format |
|----------|--------|
| REST | HTTP + JSON |
| gRPC | HTTP/2 + Protobuf |
| Thrift | TCP + binary encoding |

---

## What's behind HMS

```
Client (Spark/Trino/Hudi)
    │ Thrift (port 9083)
    ▼
Hive Metastore daemon
    │ JDBC (SQL)
    ▼
Postgres database
```

---

## How we built it

Official `apache/hive:4.0.0` image + thin custom layer that adds:
1. Postgres JDBC driver (so HMS can talk to Postgres)
2. S3A jars (so HMS can verify `s3a://` paths exist)

Config files:
- `hive-metastore/conf/hive-site.xml` — Postgres connection + warehouse location
- `hive-metastore/conf/core-site.xml` — MinIO/S3A settings

---

## On AWS EMR with Glue

You can skip HMS entirely. AWS Glue Data Catalog replaces it — no Thrift server, no Postgres, just one config line.

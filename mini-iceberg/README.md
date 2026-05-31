# mini-iceberg

A small, readable implementation of the Apache Iceberg table format. Built to
demonstrate **what Iceberg actually is**: a stack of JSON/Avro pointer files
on top of Parquet that gives you snapshots, time travel, schema evolution,
and ACID writes — without an engine, without a JVM, in ~600 lines of Python.

This stack is **independent** from the main Hudi pipeline at the project root.
It has its own `docker-compose.yml`, its own image, and its own warehouse
bucket. Nothing in the parent project is touched.

## Layout

```
mini-iceberg/
├── miniiceberg/                 ← the library (~600 lines)
│   ├── types.py                 schemas, fields, partition specs, snapshots
│   ├── transforms.py            identity / bucket[N] / truncate[L]
│   ├── stats.py                 row-count + min/max from pyarrow
│   ├── manifest.py              manifest + manifest-list serialisation
│   ├── metadata.py              the table's metadata.json
│   ├── catalog.py               filesystem catalog (version-hint.text pointer)
│   ├── io.py                    fsspec-backed FileIO (local + S3/MinIO)
│   └── table.py                 user-facing Table: append / overwrite / scan
│
├── examples/
│   ├── 01_write_and_read.py     create a table, append, scan, partition filter
│   ├── 02_time_travel.py        read each historical snapshot
│   ├── 03_schema_evolution.py   add a column, write rows that use it
│   ├── 04_partitioned.py        bucket transform with hidden partitioning
│   └── 05_query_with_duckdb.py  let DuckDB query the table format
│
├── tests/test_table.py          black-box tests against a local-fs catalog
├── docker-compose.yml           worker container + optional MinIO
├── Dockerfile                   python:3.11 + pyarrow + fsspec + duckdb
└── Makefile                     build, up, demo, test
```

## What it implements

| Iceberg feature | What we built | Comment |
|-----------------|---------------|---------|
| Table format    | metadata.json + manifest-list.json + manifest.json + Parquet | JSON instead of Avro |
| Catalog         | Per-table `version-hint.text` pointer file | Same approach as Iceberg's HadoopCatalog |
| Append / Overwrite | `Table.append(rows)` / `Table.overwrite(rows)` | Each commit produces a new snapshot |
| Snapshots       | One per write, chained via `parent_id` | Stored in metadata.json |
| Time travel     | `Table.scan(snapshot_id=...)` | Read any historical snapshot |
| Schema evolution | `Table.add_column(name, type)` | New schema_id, snapshots tagged |
| Partitioning    | Identity, bucket[N], truncate[L] | One folder per partition value |
| Hidden partitioning | Yes (bucket / truncate) | Filter on partition column directly |
| Statistics      | row count + per-column min/max + null count | Stored in manifest entries |
| ACID writes     | Optimistic concurrency on the version-hint | `RuntimeError` on conflict |
| File IO         | Local FS + S3/MinIO via fsspec | Same code path |
| External engine integration | DuckDB demo | Catalog returns file list, engine reads Parquet |

## What we deliberately skipped

To keep the code under 1000 lines:

- **Avro** for manifests. We use JSON. The structure is identical; the encoding is simpler.
- **Field IDs** for full schema evolution semantics (rename / drop / promote). Add column works.
- **Position / equality deletes** (Iceberg v2). All writes are full-file. No row-level deletes.
- **REST catalog protocol**, **Glue/Hive/JDBC catalogs**. We have one filesystem catalog.
- **Schema validation** on writes. We trust the caller to send rows matching the schema.
- **Predicate pushdown beyond partition filtering**. We have stats but don't use them yet.

These are all worthy ~200-line additions on top of this base if you want to extend it.

## Quick start

The worker reuses the main project's MinIO by default. If the main `hudi-pipeline`
stack is running, just:

```bash
cd mini-iceberg
make build           # one-time
make up              # starts the worker, attached to both networks
make demo            # runs all 5 examples
make test            # runs library tests
```

If you want this stack fully standalone (no main project running):

```bash
make up-standalone   # spins up its own MinIO too on host ports 9100/9101
```

You can then browse the warehouse at
http://localhost:9001 (main MinIO) or http://localhost:9101 (standalone).

## What the data looks like on disk

After `make ex1`, the bucket layout is:

```
s3://iceberg-warehouse/demo/events/
├── metadata/
│   ├── version-hint.text                          ← contains "3"
│   ├── v1.metadata.json                           ← table created
│   ├── v2.metadata.json                           ← snapshot 1 (3 rows)
│   ├── v3.metadata.json                           ← snapshot 2 (5 rows)
│   ├── snap-1700000001000.manifest-list.json
│   ├── snap-1700000002000.manifest-list.json
│   ├── <hash>.manifest.json                       ← per snapshot
│   └── <hash>.manifest.json
└── data/
    ├── user=alice/<uuid>.parquet
    ├── user=bob/<uuid>.parquet
    └── user=carol/<uuid>.parquet
```

The whole table state is recoverable from `version-hint.text` — read it,
load `v3.metadata.json`, follow `current_snapshot_id` → manifest list →
manifests → data files. Same chain Iceberg's official spec describes.

## Reading the code

In dependency order, shortest to longest:

1. `types.py` (~100 lines) — data classes for Schema, Field, PartitionSpec, Snapshot.
2. `transforms.py` (~80) — pure functions for identity / bucket / truncate.
3. `stats.py` (~30) — pyarrow min/max.
4. `manifest.py` (~80) — read/write manifest and manifest-list JSON.
5. `metadata.py` (~110) — TableMetadata, the table's full state JSON.
6. `io.py` (~80) — fsspec wrapper, atomic-replace for the catalog pointer.
7. `catalog.py` (~110) — filesystem catalog, create / load / drop / commit.
8. `table.py` (~180) — the Table API: append, overwrite, scan, add_column.

Each file is self-contained. Reading them top-to-bottom is the fastest
introduction to the Iceberg spec I know how to write.

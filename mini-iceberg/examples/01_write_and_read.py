"""Example 1: create a table, append rows, read them back."""
from __future__ import annotations

import os

from miniiceberg import Catalog, Schema, Field, PartitionSpec


def main() -> None:
    warehouse = os.environ["ICEBERG_WAREHOUSE"]
    s3_options = {
        "key":           os.environ["ICEBERG_S3_KEY"],
        "secret":        os.environ["ICEBERG_S3_SECRET"],
        "client_kwargs": {"endpoint_url": os.environ["ICEBERG_S3_ENDPOINT"]},
    }
    catalog = Catalog(warehouse, s3_options=s3_options)

    # Drop if it exists (idempotent demo)
    try:
        catalog.drop_table("demo.events", purge=True)
    except FileNotFoundError:
        pass

    schema = Schema([
        Field("id",     "long"),
        Field("user",   "string"),
        Field("action", "string"),
        Field("ts",     "long"),
    ])

    table = catalog.create_table(
        "demo.events",
        schema=schema,
        partition_spec=PartitionSpec.identity("user"),
    )
    print(f"created table: {table.identifier}")
    print(f"  location: {table.metadata.location}")
    print(f"  schema:   {[f.name + ':' + f.type for f in table.schema.fields]}")

    # Append two batches
    table = table.append([
        {"id": 1, "user": "alice", "action": "login",  "ts": 1700000001000},
        {"id": 2, "user": "bob",   "action": "login",  "ts": 1700000002000},
        {"id": 3, "user": "alice", "action": "click",  "ts": 1700000003000},
    ])
    print(f"\nafter first append:  snapshot={table.current_snapshot_id}")

    table = table.append([
        {"id": 4, "user": "carol", "action": "login",  "ts": 1700000004000},
        {"id": 5, "user": "alice", "action": "logout", "ts": 1700000005000},
    ])
    print(f"after second append: snapshot={table.current_snapshot_id}")
    print(f"snapshots so far:    {[s.snapshot_id for s in table.history()]}")

    # Read back
    print("\nfull scan (latest snapshot):")
    rows = table.scan().to_pylist()
    for r in rows:
        print(f"  {r}")

    # Filter by partition
    print("\nfiltered scan (user='alice'):")
    rows = table.scan(filters={"user": "alice"}).to_pylist()
    for r in rows:
        print(f"  {r}")


if __name__ == "__main__":
    main()

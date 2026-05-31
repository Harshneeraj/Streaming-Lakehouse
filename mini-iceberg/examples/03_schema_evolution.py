"""Example 3: add a column, write rows that use it, prove old snapshots still readable."""
from __future__ import annotations

import os

from miniiceberg import Catalog


def main() -> None:
    warehouse = os.environ["ICEBERG_WAREHOUSE"]
    s3_options = {
        "key":           os.environ["ICEBERG_S3_KEY"],
        "secret":        os.environ["ICEBERG_S3_SECRET"],
        "client_kwargs": {"endpoint_url": os.environ["ICEBERG_S3_ENDPOINT"]},
    }
    catalog = Catalog(warehouse, s3_options=s3_options)

    table = catalog.load_table("demo.events")
    pre_snapshot = table.current_snapshot_id
    print(f"before evolution: schema = {[f.name for f in table.schema.fields]}")

    # Add a new column
    table = table.add_column("device", "string")
    print(f"after add_column: schema = {[f.name for f in table.schema.fields]}")
    print(f"schema_id is now {table.schema.schema_id}")

    # Append rows that use the new column
    table = table.append([
        {"id": 6, "user": "alice", "action": "view", "ts": 1700000006000, "device": "iphone"},
        {"id": 7, "user": "bob",   "action": "view", "ts": 1700000007000, "device": "android"},
    ])

    print("\nlatest scan (new column populated):")
    for r in table.scan().to_pylist():
        print(f"  {r}")

    # Old snapshot still works (device will simply not be in the parquet schema there)
    print(f"\ntime-travel back to snapshot {pre_snapshot} (no device column yet):")
    for r in table.scan(snapshot_id=pre_snapshot).to_pylist():
        print(f"  {r}")


if __name__ == "__main__":
    main()

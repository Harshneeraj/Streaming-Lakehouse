"""Example 2: time-travel between snapshots."""
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
    snapshots = table.history()
    print(f"snapshot history ({len(snapshots)} entries):")
    for s in snapshots:
        print(f"  {s.snapshot_id}  op={s.operation}  records={s.summary.get('added-records')}")

    # Read the table at each snapshot
    for s in snapshots:
        rows = table.scan(snapshot_id=s.snapshot_id).to_pylist()
        print(f"\nas of snapshot {s.snapshot_id}: {len(rows)} rows")
        for r in rows:
            print(f"  {r}")


if __name__ == "__main__":
    main()

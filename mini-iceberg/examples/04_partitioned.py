"""Example 4: partitioning with a bucket transform.

Identity partitioning makes one folder per distinct value, which doesn't
scale for high-cardinality columns like user IDs. Iceberg solves this with
*hidden partitioning* via transforms — the user just queries `user_id = 42`
and Iceberg handles the bucket-to-partition mapping under the hood.
"""
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

    try:
        catalog.drop_table("demo.orders", purge=True)
    except FileNotFoundError:
        pass

    table = catalog.create_table(
        "demo.orders",
        schema=Schema([
            Field("order_id", "long"),
            Field("user_id",  "long"),
            Field("amount",   "double"),
        ]),
        partition_spec=PartitionSpec.bucket("user_id", 4),
    )

    table = table.append([
        {"order_id": i, "user_id": i % 100, "amount": float(i) * 1.25}
        for i in range(1, 21)
    ])

    print("data files written and their partitions:")
    for f in table.scan().files():
        print(f"  partition={f.partition}  rows={f.record_count}  path={f.path.split('/data/')[-1]}")

    print("\nfilter by bucket=2 (Iceberg-style bucket pruning):")
    rows = table.scan(filters={"user_id_bucket": 2}).to_pylist()
    print(f"  {len(rows)} rows match the bucket")


if __name__ == "__main__":
    main()

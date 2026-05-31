"""mini-iceberg: a tiny, readable implementation of the Apache Iceberg table format.

Public API:

    from miniiceberg import Catalog, Schema, Field, PartitionSpec

    catalog = Catalog("s3://lakehouse/iceberg")          # or "/tmp/iceberg"
    table = catalog.create_table(
        "lake.events",
        schema=Schema([
            Field("id",   "long"),
            Field("user", "string"),
            Field("ts",   "long"),
        ]),
        partition_spec=PartitionSpec.identity("user"),
    )
    table.append(rows=[{"id": 1, "user": "a", "ts": 100}, ...])

    table = catalog.load_table("lake.events")
    arrow_table = table.scan().to_arrow()                # latest snapshot
    arrow_table = table.scan(snapshot_id=...).to_arrow() # time travel
"""
from .types import Schema, Field, PartitionSpec, Snapshot
from .catalog import Catalog
from .table import Table

__all__ = ["Catalog", "Table", "Schema", "Field", "PartitionSpec", "Snapshot"]
__version__ = "0.1.0"

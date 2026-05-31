"""Sanity tests against a local filesystem catalog (no MinIO needed)."""
from __future__ import annotations

import os
import tempfile

from miniiceberg import Catalog, Schema, Field, PartitionSpec


def test_create_append_scan() -> None:
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t = cat.create_table(
            "ns.t",
            schema=Schema([Field("id", "long"), Field("name", "string")]),
        )
        t = t.append([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        t = t.append([{"id": 3, "name": "c"}])
        rows = t.scan().to_pylist()
        ids = sorted(r["id"] for r in rows)
        assert ids == [1, 2, 3], ids


def test_time_travel() -> None:
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t = cat.create_table("ns.t", schema=Schema([Field("id", "long")]))
        t = t.append([{"id": 1}])
        first = t.current_snapshot_id
        t = t.append([{"id": 2}])
        # latest sees both
        assert {r["id"] for r in t.scan().to_pylist()} == {1, 2}
        # time-travel sees only the first
        assert {r["id"] for r in t.scan(snapshot_id=first).to_pylist()} == {1}


def test_overwrite_marks_old_files_deleted() -> None:
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t = cat.create_table("ns.t", schema=Schema([Field("id", "long")]))
        t = t.append([{"id": 1}, {"id": 2}])
        t = t.overwrite([{"id": 99}])
        assert {r["id"] for r in t.scan().to_pylist()} == {99}


def test_schema_evolution_add_column() -> None:
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t = cat.create_table("ns.t", schema=Schema([Field("id", "long")]))
        t = t.append([{"id": 1}])
        t = t.add_column("name", "string")
        t = t.append([{"id": 2, "name": "two"}])
        names = {f.name for f in t.schema.fields}
        assert names == {"id", "name"}
        rows = t.scan().to_pylist()
        assert len(rows) == 2


def test_identity_partitioning() -> None:
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t = cat.create_table(
            "ns.t",
            schema=Schema([Field("id", "long"), Field("bucket", "string")]),
            partition_spec=PartitionSpec.identity("bucket"),
        )
        t = t.append([
            {"id": 1, "bucket": "a"},
            {"id": 2, "bucket": "a"},
            {"id": 3, "bucket": "b"},
        ])
        files = t.scan().files()
        partitions = {f.partition["bucket"] for f in files}
        assert partitions == {"a", "b"}
        # Filter pushdown
        rows_a = t.scan(filters={"bucket": "a"}).to_pylist()
        assert {r["id"] for r in rows_a} == {1, 2}


def test_concurrent_commit_detection() -> None:
    """Two writers loading the same version should clash."""
    with tempfile.TemporaryDirectory() as wh:
        cat = Catalog(wh)
        t1 = cat.create_table("ns.t", schema=Schema([Field("id", "long")]))
        # Reload — two handles at the same version
        t2 = cat.load_table("ns.t")
        t1 = t1.append([{"id": 1}])
        try:
            t2.append([{"id": 2}])
        except RuntimeError as e:
            assert "concurrent" in str(e)
        else:
            raise AssertionError("expected concurrent commit detection")


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"running {name} ...", end=" ")
            fn()
            print("ok")
    print("\nall tests passed")

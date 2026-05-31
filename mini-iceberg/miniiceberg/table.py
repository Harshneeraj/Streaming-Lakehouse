"""User-facing Table API: append, overwrite, scan, time-travel, schema evolution.

The Table object is immutable per load — operations return updated Table
instances. This makes the lifecycle obvious and avoids accidental sharing.
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import (
    DataFile, Manifest, ManifestEntry, ManifestListEntry,
    read_manifest, read_manifest_list, write_manifest, write_manifest_list,
)
from .metadata import TableMetadata
from .stats import compute_stats
from .transforms import get_transform
from .types import Field, PartitionSpec, Schema, Snapshot


class Scan:
    """Lazy view of a snapshot. Materialise via .to_arrow() or .files()."""

    def __init__(self, table: "Table", snapshot_id: int,
                 filters: Optional[Dict[str, Any]] = None) -> None:
        self.table = table
        self.snapshot_id = snapshot_id
        self.filters = filters or {}

    def files(self) -> List[DataFile]:
        snap = self.table.metadata.snapshot(self.snapshot_id)
        manifests = read_manifest_list(self.table._io, snap.manifest_list)
        files: List[DataFile] = []
        for ml in manifests:
            m = read_manifest(self.table._io, ml.manifest_path)
            for e in m.entries:
                if e.status == "deleted":
                    continue
                if self._partition_matches(e.data_file):
                    files.append(e.data_file)
        return files

    def _partition_matches(self, df: DataFile) -> bool:
        for k, v in self.filters.items():
            if df.partition.get(k) != v:
                return False
        return True

    def to_arrow(self) -> pa.Table:
        files = self.files()
        if not files:
            return _empty_arrow_table(self.table.schema)
        tables = []
        for df in files:
            with self.table._io.fs.open(df.path, "rb") as fp:
                tables.append(pq.read_table(fp))
        return pa.concat_tables(tables, promote_options="default")

    def to_pylist(self) -> List[Dict[str, Any]]:
        return self.to_arrow().to_pylist()


class Table:
    """An Iceberg-style table backed by Parquet on object storage."""

    def __init__(self, catalog, identifier, metadata: TableMetadata, version: int) -> None:
        self.catalog = catalog
        self.identifier = identifier
        self.metadata = metadata
        self.version = version
        self._io = catalog.io
        self._root = metadata.location

    # ── conveniences ──────────────────────────────────────────────────────────────────
    @property
    def schema(self) -> Schema:
        return self.metadata.schema()

    @property
    def partition_spec(self) -> PartitionSpec:
        return self.metadata.partition_spec()

    @property
    def current_snapshot_id(self) -> Optional[int]:
        return self.metadata.current_snapshot_id

    def history(self) -> List[Snapshot]:
        return list(self.metadata.snapshots)

    # ── reads ─────────────────────────────────────────────────────────────────────────
    def scan(self, snapshot_id: Optional[int] = None,
             filters: Optional[Dict[str, Any]] = None) -> Scan:
        sid = snapshot_id or self.current_snapshot_id
        if sid is None:
            raise RuntimeError("table has no snapshots yet")
        return Scan(self, sid, filters or {})

    # ── writes ────────────────────────────────────────────────────────────────────────
    def append(self, rows: Iterable[Dict[str, Any]] = (), arrow_table: Optional[pa.Table] = None
               ) -> "Table":
        return self._write(rows=rows, arrow_table=arrow_table, operation="append")

    def overwrite(self, rows: Iterable[Dict[str, Any]] = (), arrow_table: Optional[pa.Table] = None
                  ) -> "Table":
        return self._write(rows=rows, arrow_table=arrow_table, operation="overwrite")

    def add_column(self, name: str, type_: str, nullable: bool = True) -> "Table":
        new_schema = self.schema.with_added(Field(name=name, type=type_, nullable=nullable))
        new_meta = self.metadata.with_schema(new_schema)
        new_version = self.catalog.commit(self.identifier, self.version, new_meta)
        return Table(self.catalog, self.identifier, new_meta, new_version)

    # ── internals ─────────────────────────────────────────────────────────────────────
    def _write(self, *, rows: Iterable[Dict[str, Any]], arrow_table: Optional[pa.Table],
               operation: str) -> "Table":
        if arrow_table is None:
            arrow_table = _rows_to_arrow(rows, self.schema)
        if arrow_table.num_rows == 0 and operation == "append":
            return self  # nothing to do

        # split by partition values
        groups = self._partition_groups(arrow_table)

        new_files: List[DataFile] = []
        for partition_values, group in groups.items():
            df = self._write_parquet(group, dict(partition_values))
            new_files.append(df)

        # Build manifest entries:
        #   - 'added' for new files
        #   - 'existing' for previously live files (only on append)
        #   - 'deleted' for previously live files (only on overwrite)
        snapshot_id = int(time.time() * 1000)
        previous_live = self._live_files()

        if operation == "append":
            entries = (
                [ManifestEntry(status="added",    data_file=f) for f in new_files]
                + [ManifestEntry(status="existing", data_file=f) for f in previous_live]
            )
        elif operation == "overwrite":
            entries = (
                [ManifestEntry(status="added",   data_file=f) for f in new_files]
                + [ManifestEntry(status="deleted", data_file=f) for f in previous_live]
            )
        else:
            raise ValueError(f"unknown operation: {operation!r}")

        manifest = Manifest(
            entries=entries,
            snapshot_id=snapshot_id,
            schema_id=self.schema.schema_id,
            spec_id=self.partition_spec.spec_id,
        )
        manifest_path = write_manifest(self._io, self._root, manifest)

        ml_entry = ManifestListEntry(
            manifest_path=manifest_path,
            added_files_count=len(new_files),
            existing_files_count=sum(1 for e in entries if e.status == "existing"),
            deleted_files_count=sum(1 for e in entries if e.status == "deleted"),
            added_rows_count=sum(df.record_count for df in new_files),
        )
        ml_path = write_manifest_list(self._io, self._root, snapshot_id, [ml_entry])

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_id=self.current_snapshot_id,
            timestamp_ms=snapshot_id,
            operation=operation,
            manifest_list=ml_path,
            schema_id=self.schema.schema_id,
            summary={
                "operation": operation,
                "added-data-files": str(len(new_files)),
                "added-records": str(sum(df.record_count for df in new_files)),
            },
        )

        new_meta = self.metadata.with_snapshot(snapshot)
        new_version = self.catalog.commit(self.identifier, self.version, new_meta)
        return Table(self.catalog, self.identifier, new_meta, new_version)

    def _live_files(self) -> List[DataFile]:
        """Returns all currently-live files (the input set for the next snapshot)."""
        if self.current_snapshot_id is None:
            return []
        return self.scan().files()

    def _partition_groups(self, arrow_table: pa.Table) -> Dict[Tuple, pa.Table]:
        """Returns {partition_tuple: arrow_subtable}."""
        spec = self.partition_spec
        if spec.is_unpartitioned():
            return {(): arrow_table}

        # compute partition value for each row, then group
        n = arrow_table.num_rows
        py = arrow_table.to_pylist()
        groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        for row in py:
            key = []
            for pf in spec.fields:
                tx = get_transform(pf.transform)
                key.append((pf.name, tx(row.get(pf.source_name))))
            tup = tuple(key)
            groups.setdefault(tup, []).append(row)
        # back to arrow per group, preserving original schema
        out: Dict[Tuple, pa.Table] = {}
        for k, rows in groups.items():
            out[k] = pa.Table.from_pylist(rows, schema=arrow_table.schema)
        return out

    def _write_parquet(self, arrow_table: pa.Table, partition_values: Dict[str, Any]) -> DataFile:
        # path: <root>/data/<part1=v1>/<part2=v2>/<uuid>.parquet
        parts = "/".join(f"{k}={v}" for k, v in partition_values.items())
        rel = f"{parts}/{uuid.uuid4().hex}.parquet" if parts else f"{uuid.uuid4().hex}.parquet"
        full = f"{self._root}/data/{rel}"
        # Ensure parent dirs exist on local FS (S3 has no real directories)
        parent = full.rsplit("/", 1)[0]
        self._io.mkdirs(parent)
        with self._io.fs.open(full, "wb") as fp:
            pq.write_table(arrow_table, fp)
        size = self._io.fs.info(full).get("size", 0)
        return DataFile(
            path=full,
            record_count=arrow_table.num_rows,
            file_size_bytes=size,
            partition=partition_values,
            column_stats=compute_stats(arrow_table)["column_stats"],
        )


# ────────────────────────────────────── helpers ───────────────────────────────────────
_TYPE_MAP = {
    "int": pa.int32(), "long": pa.int64(),
    "float": pa.float32(), "double": pa.float64(),
    "string": pa.string(), "boolean": pa.bool_(),
    "date": pa.date32(), "timestamp": pa.timestamp("us"),
}


def _arrow_schema_from(schema: Schema) -> pa.Schema:
    return pa.schema([(f.name, _TYPE_MAP[f.type]) for f in schema.fields])


def _rows_to_arrow(rows: Iterable[Dict[str, Any]], schema: Schema) -> pa.Table:
    arrow_schema = _arrow_schema_from(schema)
    rows = list(rows)
    if not rows:
        return _empty_arrow_table(schema)
    # fill missing columns with None
    cols = {name: [r.get(name) for r in rows] for name in schema.names()}
    return pa.Table.from_pydict(cols, schema=arrow_schema)


def _empty_arrow_table(schema: Schema) -> pa.Table:
    return pa.Table.from_pydict(
        {name: [] for name in schema.names()},
        schema=_arrow_schema_from(schema),
    )

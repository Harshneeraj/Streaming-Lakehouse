"""Filesystem catalog.

A catalog answers two questions:
    1. Given a table identifier (e.g. "lake.events"), where does its metadata live?
    2. How do we atomically point the table at a new metadata file (the "commit")?

Real Iceberg has Hive, REST, Glue, JDBC, Nessie catalog implementations.
We implement the simplest one: a per-table `version-hint.text` file at
    <warehouse>/<namespace>/<table>/metadata/version-hint.text

containing the integer version of the current metadata file. A commit means
"write v(N+1).metadata.json, then atomically replace version-hint.text with N+1".

This is the same approach Iceberg's HadoopCatalog uses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .io import FileIO
from .metadata import TableMetadata, metadata_path, read_metadata, write_metadata
from .types import PartitionSpec, Schema


@dataclass
class TableIdentifier:
    namespace: str
    name: str

    @classmethod
    def parse(cls, ident: str) -> "TableIdentifier":
        if "." not in ident:
            raise ValueError(f"identifier must be 'namespace.name': {ident!r}")
        ns, n = ident.split(".", 1)
        return cls(ns, n)

    def __str__(self) -> str:
        return f"{self.namespace}.{self.name}"


class Catalog:
    """A flat-on-filesystem catalog rooted at `warehouse`."""

    def __init__(self, warehouse: str, s3_options: Optional[Dict[str, Any]] = None) -> None:
        self.warehouse = warehouse.rstrip("/")
        self.io = FileIO(self.warehouse, s3_options=s3_options)

    # ── paths ─────────────────────────────────────────────────────────────────────────
    def _table_root(self, ident: TableIdentifier) -> str:
        return f"{self.warehouse}/{ident.namespace}/{ident.name}"

    def _version_hint(self, ident: TableIdentifier) -> str:
        return f"{self._table_root(ident)}/metadata/version-hint.text"

    # ── lifecycle ─────────────────────────────────────────────────────────────────────
    def create_table(
        self,
        identifier: str,
        schema: Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> "Table":
        from .table import Table  # circular import dance
        ident = TableIdentifier.parse(identifier)
        root = self._table_root(ident)
        if self.io.exists(self._version_hint(ident)):
            raise FileExistsError(f"table already exists: {identifier}")

        spec = partition_spec or PartitionSpec.unpartitioned()
        meta = TableMetadata(
            location=root,
            schemas=[schema],
            partition_specs=[spec],
            snapshots=[],
            current_schema_id=schema.schema_id,
            default_spec_id=spec.spec_id,
            properties=properties or {},
        )
        write_metadata(self.io, root, version=1, meta=meta)
        self.io.atomic_replace(self._version_hint(ident), b"1")
        return Table(self, ident, meta, version=1)

    def load_table(self, identifier: str) -> "Table":
        from .table import Table
        ident = TableIdentifier.parse(identifier)
        if not self.io.exists(self._version_hint(ident)):
            raise FileNotFoundError(f"table not found: {identifier}")
        version = int(self.io.read_text(self._version_hint(ident)).strip())
        meta = read_metadata(self.io, metadata_path(self._table_root(ident), version))
        return Table(self, ident, meta, version=version)

    def drop_table(self, identifier: str, purge: bool = False) -> None:
        ident = TableIdentifier.parse(identifier)
        if not self.io.exists(self._version_hint(ident)):
            raise FileNotFoundError(f"table not found: {identifier}")
        if purge:
            root = self._table_root(ident)
            try:
                self.io.fs.rm(root, recursive=True)
            except Exception:
                # Best-effort: walk and delete leaves
                try:
                    for p in self.io.fs.find(root):
                        try:
                            self.io.fs.rm(p)
                        except Exception:
                            pass
                except Exception:
                    pass
        else:
            self.io.delete(self._version_hint(ident))

    def list_tables(self, namespace: str) -> list:
        ns_root = f"{self.warehouse}/{namespace}"
        if not self.io.exists(ns_root):
            return []
        out = []
        for entry in self.io.list(ns_root):
            name = entry.rstrip("/").rsplit("/", 1)[-1]
            ident = TableIdentifier(namespace, name)
            if self.io.exists(self._version_hint(ident)):
                out.append(str(ident))
        return out

    # ── commit ────────────────────────────────────────────────────────────────────────
    def commit(self, ident: TableIdentifier, current_version: int,
               new_meta: TableMetadata) -> int:
        """Atomically advance the table's version pointer.

        We do an optimistic check: read version-hint, if it still equals
        current_version we write v(N+1) and replace the pointer. Otherwise we
        raise — caller must reload + retry.
        """
        existing = int(self.io.read_text(self._version_hint(ident)).strip())
        if existing != current_version:
            raise RuntimeError(
                f"concurrent commit detected: expected v{current_version}, found v{existing}"
            )
        new_version = current_version + 1
        write_metadata(self.io, self._table_root(ident), new_version, new_meta)
        self.io.atomic_replace(self._version_hint(ident), str(new_version).encode())
        return new_version

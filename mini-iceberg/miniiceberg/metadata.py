"""TableMetadata: the JSON file that describes the whole table.

Contents:
    - format_version
    - table_uuid                 (created at first write, never changes)
    - location                   (root path of the table)
    - last_updated_ms
    - last_sequence_number       (monotonic counter)
    - schemas                    (history of schemas)
    - current_schema_id
    - partition_specs
    - default_spec_id
    - snapshots                  (history of all snapshots)
    - current_snapshot_id
    - snapshot_log               (audit log of snapshot transitions)
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from .io import FileIO
from .types import PartitionSpec, Schema, Snapshot


@dataclass
class TableMetadata:
    location: str
    schemas: List[Schema]
    partition_specs: List[PartitionSpec]
    snapshots: List[Snapshot]
    current_schema_id: int = 0
    default_spec_id: int = 0
    current_snapshot_id: Optional[int] = None
    last_updated_ms: int = 0
    last_sequence_number: int = 0
    table_uuid: str = field(default_factory=lambda: uuid.uuid4().hex)
    snapshot_log: List[Dict[str, Any]] = field(default_factory=list)
    format_version: int = 1
    properties: Dict[str, str] = field(default_factory=dict)

    # ── lookups ───────────────────────────────────────────────────────────────────────
    def schema(self) -> Schema:
        for s in self.schemas:
            if s.schema_id == self.current_schema_id:
                return s
        return self.schemas[-1]

    def partition_spec(self) -> PartitionSpec:
        for p in self.partition_specs:
            if p.spec_id == self.default_spec_id:
                return p
        return self.partition_specs[-1]

    def snapshot(self, snapshot_id: int) -> Snapshot:
        for s in self.snapshots:
            if s.snapshot_id == snapshot_id:
                return s
        raise KeyError(f"unknown snapshot id {snapshot_id}")

    def current_snapshot(self) -> Optional[Snapshot]:
        if self.current_snapshot_id is None:
            return None
        return self.snapshot(self.current_snapshot_id)

    # ── mutators (return a new metadata; caller persists it) ─────────────────────────
    def with_snapshot(self, snap: Snapshot) -> "TableMetadata":
        new = TableMetadata(**self.__dict__)
        new.snapshots = self.snapshots + [snap]
        new.current_snapshot_id = snap.snapshot_id
        new.last_updated_ms = snap.timestamp_ms
        new.last_sequence_number = self.last_sequence_number + 1
        new.snapshot_log = self.snapshot_log + [
            {"snapshot_id": snap.snapshot_id, "timestamp_ms": snap.timestamp_ms}
        ]
        return new

    def with_schema(self, schema: Schema) -> "TableMetadata":
        new = TableMetadata(**self.__dict__)
        # ensure schema_id is unique
        next_id = max((s.schema_id for s in self.schemas), default=-1) + 1
        schema.schema_id = next_id
        new.schemas = self.schemas + [schema]
        new.current_schema_id = next_id
        new.last_updated_ms = int(time.time() * 1000)
        return new

    # ── serialise ─────────────────────────────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        return {
            "format_version": self.format_version,
            "table_uuid": self.table_uuid,
            "location": self.location,
            "last_updated_ms": self.last_updated_ms,
            "last_sequence_number": self.last_sequence_number,
            "schemas": [s.to_dict() for s in self.schemas],
            "current_schema_id": self.current_schema_id,
            "partition_specs": [p.to_dict() for p in self.partition_specs],
            "default_spec_id": self.default_spec_id,
            "snapshots": [s.to_dict() for s in self.snapshots],
            "current_snapshot_id": self.current_snapshot_id,
            "snapshot_log": self.snapshot_log,
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TableMetadata":
        return cls(
            location=d["location"],
            schemas=[Schema.from_dict(s) for s in d["schemas"]],
            partition_specs=[PartitionSpec.from_dict(p) for p in d["partition_specs"]],
            snapshots=[Snapshot.from_dict(s) for s in d.get("snapshots", [])],
            current_schema_id=d.get("current_schema_id", 0),
            default_spec_id=d.get("default_spec_id", 0),
            current_snapshot_id=d.get("current_snapshot_id"),
            last_updated_ms=d.get("last_updated_ms", 0),
            last_sequence_number=d.get("last_sequence_number", 0),
            table_uuid=d.get("table_uuid", uuid.uuid4().hex),
            snapshot_log=d.get("snapshot_log", []),
            format_version=d.get("format_version", 1),
            properties=d.get("properties", {}),
        )


# ─────────────────────────────────────── IO helpers ────────────────────────────────────
def metadata_path(table_root: str, version: int) -> str:
    return f"{table_root}/metadata/v{version}.metadata.json"


def write_metadata(io: FileIO, table_root: str, version: int, meta: TableMetadata) -> str:
    path = metadata_path(table_root, version)
    io.write_json(path, meta.to_dict())
    return path


def read_metadata(io: FileIO, path: str) -> TableMetadata:
    return TableMetadata.from_dict(io.read_json(path))

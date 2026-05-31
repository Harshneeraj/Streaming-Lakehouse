"""Core data classes that model the Iceberg table spec.

We keep them as plain dataclasses serialised to JSON. Real Iceberg uses
schema IDs, field IDs, and Avro for manifests; we use names + JSON to keep
the implementation readable.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────── Schema ────────────────────────────────────────
PRIMITIVE_TYPES = {"int", "long", "float", "double", "string", "boolean", "date", "timestamp"}


@dataclass
class Field:
    """A single column in a table schema."""
    name: str
    type: str                            # one of PRIMITIVE_TYPES
    nullable: bool = True
    field_id: int = 0                    # filled by Schema

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Field":
        return cls(**d)


@dataclass
class Schema:
    """Ordered list of Fields. Field IDs are assigned at creation time."""
    fields: List[Field]
    schema_id: int = 0

    def __post_init__(self) -> None:
        for i, f in enumerate(self.fields, start=1):
            if f.type not in PRIMITIVE_TYPES:
                raise ValueError(f"unsupported type {f.type!r}")
            if f.field_id == 0:
                f.field_id = i

    def names(self) -> List[str]:
        return [f.name for f in self.fields]

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)

    def with_added(self, new_field: Field) -> "Schema":
        next_id = max(f.field_id for f in self.fields) + 1
        new_field.field_id = next_id
        return Schema(self.fields + [new_field], schema_id=self.schema_id + 1)

    def to_dict(self) -> Dict[str, Any]:
        return {"schema_id": self.schema_id, "fields": [f.to_dict() for f in self.fields]}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Schema":
        return cls(fields=[Field.from_dict(x) for x in d["fields"]], schema_id=d.get("schema_id", 0))


# ────────────────────────────── Partitioning (transforms) ──────────────────────────────
@dataclass
class PartitionField:
    source_name: str                     # input column
    transform: str                       # 'identity' | 'bucket[N]' | 'truncate[N]'
    name: str                            # partition column name as written to disk

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PartitionField":
        return cls(**d)


@dataclass
class PartitionSpec:
    """Defines how rows are partitioned. Empty = unpartitioned."""
    fields: List[PartitionField] = field(default_factory=list)
    spec_id: int = 0

    @classmethod
    def unpartitioned(cls) -> "PartitionSpec":
        return cls(fields=[])

    @classmethod
    def identity(cls, *names: str) -> "PartitionSpec":
        return cls(fields=[PartitionField(n, "identity", n) for n in names])

    @classmethod
    def bucket(cls, source: str, n_buckets: int, name: Optional[str] = None) -> "PartitionSpec":
        return cls(fields=[PartitionField(source, f"bucket[{n_buckets}]", name or f"{source}_bucket")])

    def is_unpartitioned(self) -> bool:
        return not self.fields

    def to_dict(self) -> Dict[str, Any]:
        return {"spec_id": self.spec_id, "fields": [pf.to_dict() for pf in self.fields]}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PartitionSpec":
        return cls(
            fields=[PartitionField.from_dict(x) for x in d.get("fields", [])],
            spec_id=d.get("spec_id", 0),
        )


# ─────────────────────────────────────── Snapshot ──────────────────────────────────────
@dataclass
class Snapshot:
    """An immutable view of the table at a point in time.

    `manifest_list` is the path to the manifest-list file that enumerates all
    manifest files for this snapshot. `parent_id` chains snapshots together.
    """
    snapshot_id: int                     # epoch millis
    parent_id: Optional[int]
    timestamp_ms: int
    operation: str                       # 'append' | 'overwrite'
    manifest_list: str
    summary: Dict[str, Any] = field(default_factory=dict)
    schema_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Snapshot":
        return cls(**d)

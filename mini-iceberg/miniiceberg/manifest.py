"""Manifests describe which Parquet files exist at a snapshot.

Real Iceberg has two layers:
    1. manifest_list.avro      - list of manifest files for one snapshot
    2. manifest.avro           - list of data files (with stats) for one manifest

We use JSON instead of Avro to keep things readable; the structure is
otherwise identical.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List

from .io import FileIO


@dataclass
class DataFile:
    """One Parquet file, plus its partition values and column stats."""
    path: str                            # full path (s3://... or /tmp/...)
    file_format: str = "parquet"
    record_count: int = 0
    file_size_bytes: int = 0
    partition: Dict[str, Any] = field(default_factory=dict)
    column_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DataFile":
        return cls(**d)


@dataclass
class ManifestEntry:
    """An entry in a manifest: status + data file."""
    status: str                          # 'added' | 'existing' | 'deleted'
    data_file: DataFile

    def to_dict(self) -> Dict[str, Any]:
        return {"status": self.status, "data_file": self.data_file.to_dict()}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ManifestEntry":
        return cls(status=d["status"], data_file=DataFile.from_dict(d["data_file"]))


@dataclass
class Manifest:
    """One manifest file. Lists data files added/existing/deleted in this batch."""
    entries: List[ManifestEntry]
    snapshot_id: int
    schema_id: int = 0
    spec_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "schema_id": self.schema_id,
            "spec_id": self.spec_id,
            "entries": [e.to_dict() for e in self.entries],
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Manifest":
        return cls(
            entries=[ManifestEntry.from_dict(e) for e in d["entries"]],
            snapshot_id=d["snapshot_id"],
            schema_id=d.get("schema_id", 0),
            spec_id=d.get("spec_id", 0),
        )


@dataclass
class ManifestListEntry:
    """A pointer from snapshot → one manifest file, with summary stats."""
    manifest_path: str
    added_files_count: int = 0
    existing_files_count: int = 0
    deleted_files_count: int = 0
    added_rows_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ManifestListEntry":
        return cls(**d)


# ─────────────────────────────────────── IO helpers ────────────────────────────────────
def write_manifest(io: FileIO, table_root: str, manifest: Manifest) -> str:
    """Write a manifest, return its path."""
    name = f"{uuid.uuid4().hex}.manifest.json"
    path = f"{table_root}/metadata/{name}"
    io.write_json(path, manifest.to_dict())
    return path


def write_manifest_list(io: FileIO, table_root: str, snapshot_id: int,
                        entries: List[ManifestListEntry]) -> str:
    """Write the snapshot's manifest list, return its path."""
    name = f"snap-{snapshot_id}.manifest-list.json"
    path = f"{table_root}/metadata/{name}"
    io.write_json(path, {"snapshot_id": snapshot_id,
                         "entries": [e.to_dict() for e in entries]})
    return path


def read_manifest(io: FileIO, path: str) -> Manifest:
    return Manifest.from_dict(io.read_json(path))


def read_manifest_list(io: FileIO, path: str) -> List[ManifestListEntry]:
    d = io.read_json(path)
    return [ManifestListEntry.from_dict(e) for e in d["entries"]]

"""FileIO abstraction.

Real Iceberg has a `FileIO` interface so the same library can target HDFS,
S3, GCS, local disk, etc. We delegate to fsspec which already implements
all of them.

Two URI schemes are supported here:
    /local/path             → local filesystem
    s3://bucket/key         → S3 / MinIO via s3fs
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlparse

import fsspec


class FileIO:
    """Wraps fsspec with a tiny convenience API."""

    def __init__(self, root: str, s3_options: Optional[Dict[str, Any]] = None) -> None:
        self.root = root.rstrip("/")
        parsed = urlparse(self.root)
        if parsed.scheme == "s3":
            self._fs = fsspec.filesystem("s3", **(s3_options or {}))
            self._is_s3 = True
        elif parsed.scheme in ("", "file"):
            self._fs = fsspec.filesystem("file")
            self._is_s3 = False
        else:
            raise ValueError(f"unsupported scheme: {parsed.scheme!r}")

    @property
    def fs(self):
        return self._fs

    def exists(self, path: str) -> bool:
        return self._fs.exists(path)

    def mkdirs(self, path: str) -> None:
        if not self._is_s3:
            os.makedirs(path, exist_ok=True)
        # S3 has no real directories; objects with key prefixes are enough.

    def list(self, path: str) -> Iterable[str]:
        try:
            return self._fs.ls(path, detail=False)
        except FileNotFoundError:
            return []

    def read_bytes(self, path: str) -> bytes:
        with self._fs.open(path, "rb") as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> None:
        if not self._is_s3:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with self._fs.open(path, "wb") as f:
            f.write(data)

    def read_json(self, path: str) -> Dict[str, Any]:
        return json.loads(self.read_bytes(path).decode("utf-8"))

    def write_json(self, path: str, obj: Dict[str, Any]) -> None:
        self.write_bytes(path, json.dumps(obj, indent=2, sort_keys=True).encode("utf-8"))

    def write_text(self, path: str, text: str) -> None:
        self.write_bytes(path, text.encode("utf-8"))

    def read_text(self, path: str) -> str:
        return self.read_bytes(path).decode("utf-8")

    def delete(self, path: str) -> None:
        try:
            self._fs.rm(path)
        except FileNotFoundError:
            pass

    def atomic_replace(self, path: str, data: bytes) -> None:
        """Atomically replace `path` with the given bytes.

        On a local filesystem this uses os.rename, which is atomic on POSIX.
        On S3, PUT is itself atomic per object, so we just write directly.
        Real Iceberg uses CAS via an external catalog for stronger guarantees;
        single-writer is fine here.
        """
        if self._is_s3:
            self.write_bytes(path, data)
        else:
            tmp = f"{path}.tmp.{os.getpid()}"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)

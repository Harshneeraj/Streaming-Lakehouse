"""Partition transforms.

Iceberg supports identity, bucket[N], truncate[L], year, month, day, hour.
We implement the first three; the rest can be added with the same pattern.

Each transform is a pure function (column value → partition value).
"""
from __future__ import annotations

import hashlib
import re
import struct
from typing import Any, Callable

# transform name → callable that takes a value and returns the partition key
_BUCKET_RE = re.compile(r"^bucket\[(\d+)\]$")
_TRUNCATE_RE = re.compile(r"^truncate\[(\d+)\]$")


def _murmur3_x86_32(data: bytes, seed: int = 0) -> int:
    """Iceberg uses MurmurHash3_x86_32 for bucket partitioning. Pure-Python impl."""
    length = len(data)
    nblocks = length // 4
    h1 = seed
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    for i in range(nblocks):
        k1 = struct.unpack_from("<i", data, i * 4)[0] & 0xFFFFFFFF
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
        h1 = (h1 * 5 + 0xe6546b64) & 0xFFFFFFFF
    tail_index = nblocks * 4
    k1 = 0
    tail_size = length & 3
    if tail_size >= 3:
        k1 ^= data[tail_index + 2] << 16
    if tail_size >= 2:
        k1 ^= data[tail_index + 1] << 8
    if tail_size >= 1:
        k1 ^= data[tail_index]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1
    h1 ^= length
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85ebca6b) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xc2b2ae35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    # Iceberg spec: positive 32-bit int, so mask off sign
    return h1 & 0x7FFFFFFF


def _to_bytes(value: Any) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bool):
        return struct.pack("<i", 1 if value else 0)
    if isinstance(value, int):
        return struct.pack("<q", value)
    if isinstance(value, float):
        return struct.pack("<d", value)
    if value is None:
        return b""
    return str(value).encode("utf-8")


def get_transform(name: str) -> Callable[[Any], Any]:
    if name == "identity":
        return lambda v: v
    m = _BUCKET_RE.match(name)
    if m:
        n = int(m.group(1))
        return lambda v: _murmur3_x86_32(_to_bytes(v)) % n if v is not None else None
    m = _TRUNCATE_RE.match(name)
    if m:
        w = int(m.group(1))
        def _trunc(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, str):
                return v[:w]
            if isinstance(v, int):
                return (v // w) * w
            raise TypeError(f"truncate not supported for {type(v).__name__}")
        return _trunc
    raise ValueError(f"unknown transform: {name!r}")

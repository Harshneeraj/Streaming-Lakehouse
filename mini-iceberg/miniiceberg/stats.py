"""Per-file statistics: row count and per-column min/max.

Real Iceberg stores these as binary-encoded values in manifest entries so
predicate pushdown can skip files. We store them as JSON-friendly Python
primitives in our manifest entries.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.compute as pc


def compute_stats(arrow_table: pa.Table) -> Dict[str, Any]:
    """Returns a dict of {row_count, column_stats: {col: {min, max, null_count}}}."""
    column_stats: Dict[str, Dict[str, Any]] = {}
    for name in arrow_table.column_names:
        col = arrow_table[name]
        # null count
        null_count = col.null_count
        # min / max via pyarrow.compute
        try:
            mm = pc.min_max(col)
            min_val = _scalar_to_py(mm["min"]) if mm["min"].is_valid else None
            max_val = _scalar_to_py(mm["max"]) if mm["max"].is_valid else None
        except (pa.ArrowNotImplementedError, KeyError):
            min_val = max_val = None
        column_stats[name] = {"min": min_val, "max": max_val, "null_count": null_count}
    return {"row_count": arrow_table.num_rows, "column_stats": column_stats}


def _scalar_to_py(scalar: pa.Scalar) -> Any:
    v = scalar.as_py()
    # pyarrow returns datetime / date objects; convert to ISO strings for JSON
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v

"""
Hive-partitioned Parquet writer for 1s features.

Output layout:
  data/derived/burst_features_v1/features_1s/
    market=<market>/date=<YYYY-MM-DD>/
      data-XXX.parquet  (append via write_to_dataset)
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from .config import DERIVED_DIR, FEATURES_1S_DIR, CURSOR_FILE, FEATURE_1S_SCHEMA


# ── Cursor state ─────────────────────────────────────────────────────────

def load_cursor() -> Dict[str, str]:
    """
    Load processing cursor: per-market latest processed block_start_ms.
    Values stored as strings to avoid JSON int/str ambiguity.
    """
    path = Path(DERIVED_DIR) / CURSOR_FILE
    if path.exists():
        with open(path) as f:
            raw = json.load(f)
            return {k: str(v) for k, v in raw.items()}
    return {}


def save_cursor(cursor: Dict[str, str]):
    """Persist cursor atomically."""
    path = Path(DERIVED_DIR) / CURSOR_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(cursor, f)
    tmp.rename(path)


def get_last_processed(cursor: Dict[str, str], market: str) -> Optional[int]:
    """Get last processed block_start_ms for a market, or None."""
    val = cursor.get(market)
    if val is not None:
        return int(val)
    return None


def set_last_processed(cursor: Dict[str, str], market: str, block_start_ms: int):
    cursor[market] = str(block_start_ms)


# ── Parquet writer ───────────────────────────────────────────────────────

def _date_from_ts(ts_ms: int) -> str:
    """Convert epoch ms to YYYY-MM-DD string (UTC)."""
    dt = datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%d")


def write_features_1s(rows: List[Dict], market: str) -> int:
    """
    Write 1s feature rows to Hive-partitioned Parquet dataset.

    Returns number of rows written.
    """
    if not rows:
        return 0

    table = pa.Table.from_pylist(rows, schema=FEATURE_1S_SCHEMA)

    # Add date partition column (market already in schema)
    ts_ms = rows[0]["ts"]
    date_str = _date_from_ts(ts_ms)
    date_array = pa.array([date_str] * len(rows), type=pa.utf8())
    table = table.append_column("date", date_array)

    root_path = str(Path(DERIVED_DIR) / FEATURES_1S_DIR)
    Path(root_path).mkdir(parents=True, exist_ok=True)

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["market", "date"],
        existing_data_behavior="overwrite_or_ignore",
        compression="zstd",
    )
    return len(rows)

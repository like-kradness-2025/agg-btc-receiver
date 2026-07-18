"""
Hive-partitioned Parquet writer for 1s features.

Output layout:
  data/derived/burst_features_v1/features_1s/
    market=<market>/date=<YYYY-MM-DD>/
      block-{start_ts}-{hash12}.parquet  (deterministic, idempotent)

Idempotency: deterministic batch filenames (based on sorted ts set per partition)
enable O(1) dedup via file existence check instead of scanning entire partition.
"""

import hashlib
import json
import os
from collections import defaultdict
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


def _features_batch_filename(rows: List[Dict]) -> str:
    """Generate deterministic filename from sorted ts set for idempotency.

    Same set of ts values → same filename (retry dedup).
    """
    ts_list = sorted(r["ts"] for r in rows)
    hash_input = ",".join(str(t) for t in ts_list)
    content_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"block-{ts_list[0]}-{content_hash}.parquet"


def write_features_1s(rows: List[Dict], market: str) -> int:
    """
    Write 1s feature rows to Hive-partitioned Parquet dataset.

    Returns number of rows written (0 if idempotent duplicate).

    Idempotent via deterministic batch filenames — same as book_snapshots.
    """
    if not rows:
        return 0

    # Deduplicate within the batch itself (keep first occurrence of each ts)
    seen_ts = set()
    deduped_rows = []
    for row in rows:
        if row["ts"] not in seen_ts:
            deduped_rows.append(row)
            seen_ts.add(row["ts"])

    # Group rows by their own UTC date
    rows_by_date = defaultdict(list)
    for row in deduped_rows:
        date_str = _date_from_ts(row["ts"])
        rows_by_date[date_str].append(row)

    root_path = Path(DERIVED_DIR) / FEATURES_1S_DIR
    root_path.mkdir(parents=True, exist_ok=True)

    total_written = 0
    for date_str, date_rows in rows_by_date.items():
        partition_dir = root_path / f"market={market}" / f"date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic batch filename from sorted ts set
        batch_file = partition_dir / _features_batch_filename(date_rows)

        # O(1) existence check — no partition scan
        if batch_file.exists():
            continue  # Idempotent: same batch already written

        table = pa.Table.from_pylist(date_rows, schema=FEATURE_1S_SCHEMA)
        # Partition columns (market, date) are encoded in directory structure
        # NOT in data — PyArrow discovers them via Hive partitioning on read.

        pq.write_table(table, str(batch_file), compression="zstd")
        total_written += len(date_rows)

    return total_written

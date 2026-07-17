"""
Hive-partitioned Parquet writer for $1-binned full book snapshots.

Output layout:
  data/derived/burst_features_v1/book_snapshots/
    market=<market>/date=<YYYY-MM-DD>/
      block-{min_ts}-{hash12}.parquet

Idempotency: deterministic batch filenames (based on sorted ts set per partition)
enable O(1) dedup via file existence check instead of scanning entire partition.
Memory/IO proportional to batch size, NOT partition size.
"""

import hashlib
from collections import defaultdict
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from .config import DERIVED_DIR
from .parquet_writer import _date_from_ts

BOOK_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("seeded", pa.bool_(), nullable=False),
    pa.field("bid_prices", pa.list_(pa.float64()), nullable=False),
    pa.field("bid_qtys", pa.list_(pa.float64()), nullable=False),
    pa.field("ask_prices", pa.list_(pa.float64()), nullable=False),
    pa.field("ask_qtys", pa.list_(pa.float64()), nullable=False),
])

SNAPSHOT_ROOT = "book_snapshots"


def _batch_filename(rows: List[dict]) -> str:
    """Generate deterministic filename from sorted ts set for idempotency.

    Same set of ts values → same filename (retry dedup).
    Different ts set → different filename (preserve all).
    Only hashes ts (int64), not book data — O(batch_size) not O(data_size).
    """
    ts_list = sorted(r["ts"] for r in rows)
    hash_input = ",".join(str(t) for t in ts_list)
    content_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"block-{ts_list[0]}-{content_hash}.parquet"


def write_book_snapshots(snapshots: List[dict], market: str) -> int:
    """Write multiple book snapshot rows as Hive-partitioned Parquet. Returns count written.

    Idempotent via deterministic batch filenames:
    - Same batch written twice → same filename → file exists → skipped (0 rows)
    - Different batch → different filename → both preserved
    - No partition scan required (O(1) stat check per date partition)
    - Memory/IO proportional to batch size, not partition size
    """
    if not snapshots:
        return 0

    # Build rows from input
    rows = []
    for snap in snapshots:
        rows.append({
            "ts": snap["ts"],
            "seeded": snap["seeded"],
            "bid_prices": snap.get("bid_prices", []),
            "bid_qtys": snap.get("bid_qtys", []),
            "ask_prices": snap.get("ask_prices", []),
            "ask_qtys": snap.get("ask_qtys", []),
        })

    # Deduplicate within the batch itself (keep first occurrence of each ts)
    seen_ts = set()
    deduped_rows = []
    for row in rows:
        if row["ts"] not in seen_ts:
            deduped_rows.append(row)
            seen_ts.add(row["ts"])

    # Group rows by their own UTC date (each row's ts determines its partition)
    rows_by_date = defaultdict(list)
    for row in deduped_rows:
        date_str = _date_from_ts(row["ts"])
        rows_by_date[date_str].append(row)

    root_path = Path(DERIVED_DIR) / SNAPSHOT_ROOT
    root_path.mkdir(parents=True, exist_ok=True)

    total_written = 0
    for date_str, date_rows in rows_by_date.items():
        partition_dir = root_path / f"market={market}" / f"date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic batch filename from sorted ts set
        batch_file = partition_dir / _batch_filename(date_rows)

        # O(1) existence check — no partition scan
        if batch_file.exists():
            continue  # Idempotent: same batch already written

        # Build table and write directly (no write_to_dataset)
        # Partition columns are encoded in directory structure — NOT in data.
        # PyArrow discovers them via Hive partitioning on read.
        table = pa.Table.from_pylist(date_rows, schema=BOOK_SNAPSHOT_SCHEMA)

        pq.write_table(table, str(batch_file), compression="zstd")
        total_written += len(date_rows)

    return total_written

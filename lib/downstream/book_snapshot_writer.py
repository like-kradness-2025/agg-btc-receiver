"""
Hive-partitioned Parquet writer for $1-binned full book snapshots.

Output layout:
  data/derived/burst_features_v1/book_snapshots/
    market=<market>/date=<YYYY-MM-DD>/
      data-XXX.parquet
"""

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


def _read_existing_ts(root_path: str, market: str, date_str: str) -> set:
    """Read existing ts values from a partition directory for deduplication."""
    partition_dir = Path(root_path) / f"market={market}" / f"date={date_str}"
    if not partition_dir.exists():
        return set()

    existing_ts = set()
    for parquet_file in partition_dir.glob("*.parquet"):
        try:
            table = pq.read_table(parquet_file, columns=["ts"])
            existing_ts.update(table["ts"].to_pylist())
        except Exception:
            # Skip corrupted files
            continue
    return existing_ts


def write_book_snapshots(snapshots: List[dict], market: str) -> int:
    """Write multiple book snapshot rows as a single Parquet batch. Returns count written.

    Idempotent: rows with the same (market, date, ts) are deduplicated.
    Writing the same batch twice produces 1 row; different batches are all preserved.
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
    from collections import defaultdict
    rows_by_date = defaultdict(list)
    for row in deduped_rows:
        date_str = _date_from_ts(row["ts"])
        rows_by_date[date_str].append(row)

    root_path = str(Path(DERIVED_DIR) / SNAPSHOT_ROOT)
    Path(root_path).mkdir(parents=True, exist_ok=True)

    total_written = 0
    for date_str, date_rows in rows_by_date.items():
        # Read existing ts from target partition to avoid duplicates
        existing_ts = _read_existing_ts(root_path, market, date_str)

        # Filter out rows that already exist
        new_rows = [row for row in date_rows if row["ts"] not in existing_ts]

        if not new_rows:
            continue

        table = pa.Table.from_pylist(new_rows, schema=BOOK_SNAPSHOT_SCHEMA)

        # Add partition columns
        market_arr = pa.array([market] * len(new_rows), type=pa.utf8())
        date_arr = pa.array([date_str] * len(new_rows), type=pa.utf8())
        table = table.append_column("market", market_arr)
        table = table.append_column("date", date_arr)

        pq.write_to_dataset(
            table,
            root_path=root_path,
            partition_cols=["market", "date"],
            existing_data_behavior="overwrite_or_ignore",
            compression="zstd",
        )
        total_written += len(new_rows)

    return total_written

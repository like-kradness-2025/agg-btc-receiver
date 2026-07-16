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


def write_book_snapshots(snapshots: List[dict], market: str) -> int:
    """Write multiple book snapshot rows as a single Parquet batch. Returns count written."""
    if not snapshots:
        return 0

    # Build a single table with all rows
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

    table = pa.Table.from_pylist(rows, schema=BOOK_SNAPSHOT_SCHEMA)

    # Add partition columns
    ts = snapshots[0]["ts"]
    date_str = _date_from_ts(ts)
    market_arr = pa.array([market] * len(rows), type=pa.utf8())
    date_arr = pa.array([date_str] * len(rows), type=pa.utf8())
    table = table.append_column("market", market_arr)
    table = table.append_column("date", date_arr)

    root_path = str(Path(DERIVED_DIR) / SNAPSHOT_ROOT)
    Path(root_path).mkdir(parents=True, exist_ok=True)

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["market", "date"],
        existing_data_behavior="overwrite_or_ignore",
        compression="zstd",
    )
    return len(rows)

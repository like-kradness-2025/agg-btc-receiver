#!/usr/bin/env python3
"""Migration: book_snapshots Parquet → deterministic batch filenames.

Before: data-XXX.parquet (PyArrow-generated random names)
After:  block-{min_ts}-{hash12}.parquet (deterministic per batch ts set)

This script:
1. Reads all existing partition data
2. Groups rows by their original batch (inferred from ts contiguity)
3. Writes each batch as block-{min_ts}-{hash}.parquet
4. Removes old data-*.parquet files

Run once per market/date partition that has old-format files.
Safe to re-run (idempotent via deterministic filenames).
"""

import hashlib
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def infer_batches_from_ts(ts_list: list, gap_threshold_ms: int = 60000) -> list:
    """Group consecutive ts values into batches.

    Assumes rows within a batch are < 60s apart, and batches are > 60s apart.
    This matches typical downstream.py behavior (30s blocks, ~1 row/sec).
    """
    if not ts_list:
        return []

    sorted_ts = sorted(ts_list)
    batches = []
    current_batch = [sorted_ts[0]]

    for ts in sorted_ts[1:]:
        if ts - current_batch[-1] > gap_threshold_ms:
            batches.append(current_batch)
            current_batch = [ts]
        else:
            current_batch.append(ts)

    batches.append(current_batch)
    return batches


def batch_filename(ts_list: list) -> str:
    """Generate deterministic filename matching write_book_snapshots logic."""
    ts_sorted = sorted(ts_list)
    hash_input = ",".join(str(t) for t in ts_sorted)
    content_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]
    return f"block-{ts_sorted[0]}-{content_hash}.parquet"


def migrate_partition(partition_dir: Path, dry_run: bool = False) -> dict:
    """Migrate one market/date partition directory.

    Returns: {old_files: N, new_files: M, rows_migrated: K}
    """
    old_files = sorted(partition_dir.glob("data-*.parquet"))
    if not old_files:
        return {"old_files": 0, "new_files": 0, "rows_migrated": 0}

    # Read all rows
    tables = []
    for f in old_files:
        try:
            tables.append(pq.read_table(str(f)))
        except Exception as e:
            print(f"  WARN: skip corrupted {f.name}: {e}", file=sys.stderr)

    if not tables:
        return {"old_files": len(old_files), "new_files": 0, "rows_migrated": 0}

    combined = pa.concat_tables(tables)
    ts_list = combined.column("ts").to_pylist()

    # Infer batches
    batch_groups = infer_batches_from_ts(ts_list, gap_threshold_ms=60000)

    if dry_run:
        print(f"  DRY RUN: {len(old_files)} old files, {len(ts_list)} rows, {len(batch_groups)} inferred batches")
        for i, batch_ts in enumerate(batch_groups):
            print(f"    Batch {i+1}: min_ts={batch_ts[0]}, count={len(batch_ts)}")
        return {"old_files": len(old_files), "new_files": len(batch_groups), "rows_migrated": len(ts_list)}

    # Write new batch files
    ts_set = set(ts_list)
    rows_by_ts = {row["ts"]: row for row in combined.to_pylist()}

    for batch_ts in batch_groups:
        batch_rows = [rows_by_ts[ts] for ts in batch_ts]
        filename = batch_filename(batch_ts)
        output_path = partition_dir / filename

        # Build table without partition columns (they're in directory structure)
        table = pa.Table.from_pylist(batch_rows, schema=combined.schema.remove_metadata()
                                     .delete_field(combined.schema.get_field_index("market"))
                                     .delete_field(combined.schema.get_field_index("date") - 1))
        pq.write_table(table, str(output_path), compression="zstd")

    # Remove old files
    for f in old_files:
        f.unlink()

    return {"old_files": len(old_files), "new_files": len(batch_groups), "rows_migrated": len(ts_list)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Migrate book_snapshots to deterministic batch filenames")
    parser.add_argument("--root", type=str, required=True, help="Path to book_snapshots root (e.g. data/derived/burst_features_v1/book_snapshots)")
    parser.add_argument("--market", type=str, help="Migrate only this market (e.g. bitfinex_spot)")
    parser.add_argument("--date", type=str, help="Migrate only this date (e.g. 2026-07-16)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without modifying files")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"ERROR: {root} does not exist", file=sys.stderr)
        sys.exit(1)

    # Find all partition directories
    pattern = "market=*"
    if args.market:
        pattern = f"market={args.market}"

    partitions = []
    for market_dir in root.glob(pattern):
        if not market_dir.is_dir():
            continue
        date_pattern = "date=*"
        if args.date:
            date_pattern = f"date={args.date}"
        for date_dir in market_dir.glob(date_pattern):
            if date_dir.is_dir():
                partitions.append(date_dir)

    if not partitions:
        print(f"No partitions found matching market={args.market or '*'} date={args.date or '*'}")
        sys.exit(0)

    print(f"Found {len(partitions)} partition(s) to migrate")

    total_stats = {"old_files": 0, "new_files": 0, "rows_migrated": 0}
    for partition in sorted(partitions):
        print(f"\n{partition.relative_to(root)}:")
        stats = migrate_partition(partition, dry_run=args.dry_run)
        for k in total_stats:
            total_stats[k] += stats[k]
        if stats["old_files"] > 0:
            print(f"  {stats['old_files']} old → {stats['new_files']} new, {stats['rows_migrated']} rows")

    print(f"\nTotal: {total_stats['old_files']} old files → {total_stats['new_files']} new, {total_stats['rows_migrated']} rows")
    if args.dry_run:
        print("(DRY RUN — no files modified)")


if __name__ == "__main__":
    main()

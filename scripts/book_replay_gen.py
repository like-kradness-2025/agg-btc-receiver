#!/usr/bin/env python3
"""
Generate pre/post snapshot Parquet from book_updates raw data.

For each market and each 30s block file:
  - Read all updates chronologically
  - For each update, capture pre (before applying) and post (after applying) state
  - Save as 1 row per update with keys: market, block_start_ts, update_idx

Output: data/derived/burst_features_v1/book_replay/market=<market>/date=<YYYY-MM-DD>/block-<ts>-<hash>.parquet

Existing parquet files are skipped (no regeneration).
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import glob
import hashlib
import json
import logging
from typing import List, Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from lib.downstream.book_replay import BookReplay, get_block_start_from_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("book_replay_gen")

DERIVED_DIR = Path("data/derived/burst_features_v1/book_replay")


def get_existing_blocks(market: str, date_str: str) -> set:
    """Return set of block_start_ts that already have parquet files."""
    pattern = f"{DERIVED_DIR}/market={market}/date={date_str}/block-*.parquet"
    existing = set()
    for f in glob.glob(pattern):
        # Extract block_start_ts from filename: block-<ts>-<hash>.parquet
        fname = Path(f).name
        try:
            parts = fname.replace(".parquet", "").split("-")
            if len(parts) >= 2:
                ts = int(parts[1])
                existing.add(ts)
        except (ValueError, IndexError):
            continue
    return existing


def process_block_file(
    file_path: Path,
    market: str,
    force: bool = False,
) -> Optional[Path]:
    """
    Process a single book_updates JSONL file.

    Returns output parquet path, or None if skipped.
    """
    block_start_ts = get_block_start_from_path(str(file_path))
    if block_start_ts is None:
        log.warning(f"Could not extract block_start_ts from {file_path}")
        return None

    date_str = file_path.parent.name  # YYYY-MM-DD
    output_dir = DERIVED_DIR / f"market={market}" / f"date={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if already exists
    if not force:
        existing = get_existing_blocks(market, date_str)
        if block_start_ts in existing:
            log.info(f"  [{market}] {file_path.name} — already exists, skipping")
            return None

    # Read all updates
    updates = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                updates.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not updates:
        log.info(f"  [{market}] {file_path.name} — empty, skipping")
        return None

    # Sort by timestamp
    updates.sort(key=lambda u: int(u.get("ts", 0)))

    # Process each update
    book = BookReplay()
    rows = []

    for idx, update in enumerate(updates):
        update_ts = int(update.get("ts", 0))

        # Pre snapshot (state before applying this update)
        pre_snap = book.get_binned_snapshot(update_ts)
        pre_seeded = pre_snap.get("seeded", False)
        pre_bid_prices = pre_snap.get("bid_prices", [])
        pre_bid_qtys = pre_snap.get("bid_qtys", [])
        pre_ask_prices = pre_snap.get("ask_prices", [])
        pre_ask_qtys = pre_snap.get("ask_qtys", [])

        # Compute pre mid
        if pre_seeded and pre_bid_prices and pre_ask_prices:
            pre_mid = (pre_bid_prices[0] + pre_ask_prices[0]) / 2.0
        else:
            pre_mid = None

        # Apply update
        book.apply_json(update)

        # Post snapshot (state after applying this update)
        post_snap = book.get_binned_snapshot(update_ts)
        post_seeded = post_snap.get("seeded", False)
        post_bid_prices = post_snap.get("bid_prices", [])
        post_bid_qtys = post_snap.get("bid_qtys", [])
        post_ask_prices = post_snap.get("ask_prices", [])
        post_ask_qtys = post_snap.get("ask_qtys", [])

        # Compute post mid
        if post_seeded and post_bid_prices and post_ask_prices:
            post_mid = (post_bid_prices[0] + post_ask_prices[0]) / 2.0
        else:
            post_mid = None

        # Store row
        rows.append({
            "market": market,
            "block_start_ts": block_start_ts,
            "update_idx": idx,
            "update_ts": update_ts,
            "pre_seeded": pre_seeded,
            "pre_bid_prices": pre_bid_prices,
            "pre_bid_qtys": pre_bid_qtys,
            "pre_ask_prices": pre_ask_prices,
            "pre_ask_qtys": pre_ask_qtys,
            "pre_mid": pre_mid,
            "post_seeded": post_seeded,
            "post_bid_prices": post_bid_prices,
            "post_bid_qtys": post_bid_qtys,
            "post_ask_prices": post_ask_prices,
            "post_ask_qtys": post_ask_qtys,
            "post_mid": post_mid,
        })

    if not rows:
        return None

    # Write to parquet
    table = pa.Table.from_pylist(rows)
    hash_suffix = hashlib.md5(file_path.name.encode()).hexdigest()[:8]
    output_file = output_dir / f"block-{block_start_ts}-{hash_suffix}.parquet"

    pq.write_table(table, output_file)
    log.info(f"  [{market}] {file_path.name} — wrote {len(rows)} updates to {output_file.name}")

    return output_file


def process_market(market: str, force: bool = False) -> int:
    """
    Process all book_updates files for a single market.

    Returns number of blocks processed.
    """
    input_dir = Path("data/live_v3/book_updates") / market
    if not input_dir.exists():
        log.warning(f"Market {market}: input dir not found")
        return 0

    block_files = sorted(input_dir.rglob("*.jsonl"))
    if not block_files:
        log.warning(f"Market {market}: no block files found")
        return 0

    log.info(f"[{market}] Processing {len(block_files)} block files")

    processed = 0
    for block_file in block_files:
        try:
            result = process_block_file(block_file, market, force=force)
            if result:
                processed += 1
        except Exception as e:
            log.error(f"  [{market}] Error processing {block_file.name}: {e}")
            continue

    log.info(f"[{market}] Completed: {processed}/{len(block_files)} blocks processed")
    return processed


def main():
    parser = argparse.ArgumentParser(description="Generate pre/post snapshot Parquet from book_updates")
    parser.add_argument("--market", type=str, help="Process single market")
    parser.add_argument("--force", action="store_true", help="Regenerate even if exists")
    args = parser.parse_args()

    if args.market:
        markets = [args.market]
    else:
        # Discover all markets
        input_dir = Path("data/live_v3/book_updates")
        markets = sorted([d.name for d in input_dir.iterdir() if d.is_dir()])

    log.info(f"Processing {len(markets)} markets: {markets}")

    total_processed = 0
    for market in markets:
        processed = process_market(market, force=args.force)
        total_processed += processed

    log.info(f"All done: {total_processed} blocks processed across {len(markets)} markets")


if __name__ == "__main__":
    main()

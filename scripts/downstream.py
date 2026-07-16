#!/usr/bin/env python3
"""
downstream.py — BTC Receiver downstream pipeline (zero-bias rebuild).

Watchdog polling loop:
  1. Scan data/live_v3/trades/ for unprocessed 30s blocks
  2. For each new block:
     a. Read JSONL trades
     b. Detect bursts (same-side, gap <= 50ms, duration <= 5000ms)
     c. Compute 1s features (22 columns)
     d. Write to Hive-partitioned Parquet (features_1s/)
  3. Track processed blocks via .cursor.json

Usage:
  python3 scripts/downstream.py                  # one-shot: process all pending
  python3 scripts/downstream.py --watch          # continuous polling (every 10s)
  python3 scripts/downstream.py --market binance_spot  # single market
  python3 scripts/downstream.py --from-ms 1784195166000  # override cursor
  python3 scripts/downstream.py --data data/live_v3     # custom data dir
  python3 scripts/downstream.py --watch --interval 5    # 5s polling interval
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.downstream.config import (
    LIVE_DATA_DIR, DERIVED_DIR, BLOCK_DURATION_MS, SECOND_MS,
    GAP_THRESHOLD_MS,
    MAX_BURST_DURATION_MS, get_tick_size,
)
from lib.downstream.burst_detector import detect_bursts
from lib.downstream.book_replay import BookReplay, read_book_updates
from lib.downstream.feature_compiler import compute_1s_features
from lib.downstream.parquet_writer import (
    load_cursor, save_cursor, get_last_processed, set_last_processed,
    write_features_1s,
)
from lib.downstream.watchdog import (
    discover_markets, discover_block_files, is_file_stable,
    read_trades_from_block,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("downstream")

# Keep one book state per market across 30s blocks.  Resetting per block
# silently discarded far/static levels that were not present in that block.
# The receiver emits incremental diffs, so the state must be continuous.
_BOOK_STATES: Dict[str, BookReplay] = {}


def process_block(
    file_path: str,
    block_start_ms: int,
    market: str,
    tick_size: float,
    lookback_trades: Optional[List[dict]] = None,
    book_updates_path: Optional[str] = None,
) -> int:
    """
    Process a single 30s block: read trades → detect bursts → compute features → write Parquet.

    Also captures $1-binned full book snapshots when book_updates available.

    Args:
        file_path: Path to the 30s trades JSONL block.
        block_start_ms: Block start timestamp (epoch ms).
        market: Market identifier.
        tick_size: Tick size for span calculation.
        lookback_trades: Trades from prior blocks for 30s lookback window (#12).
        book_updates_path: Path to matching book_updates JSONL block (optional).

    Returns:
        Number of feature rows written (30 on success, 0 on empty/skip).
    """
    trades = read_trades_from_block(file_path)
    if not trades:
        log.info("  [%s] %s — empty block, skipping", market, Path(file_path).name)
        return 0

    # Sort by ts just in case
    trades.sort(key=lambda t: int(t.get("ts", 0)))

    # Combine with lookback trades for rolling 30s traded notional
    all_trades = list(trades)
    if lookback_trades:
        cutoff = block_start_ms - BLOCK_DURATION_MS
        filtered_lookback = [t for t in lookback_trades if int(t.get("ts", 0)) >= cutoff]
        all_trades = filtered_lookback + all_trades

    # Burst detection
    bursts = detect_bursts(trades, tick_size)
    log.info("  [%s] %s — %d trades, %d bursts",
             market, Path(file_path).name, len(trades), len(bursts))

    # Book replay (optional)
    book_replay = None
    book_snapshots = []
    if book_updates_path:
        updates = read_book_updates(book_updates_path)
        if updates:
            # Reuse the continuous per-market state; do not reset each block.
            br = _BOOK_STATES.setdefault(market, BookReplay())

            # Apply WS updates first (chronological order)
            br.apply_updates(updates)

            # Then upsert REST snapshot to fill static levels not covered by WS diffs
            from lib.downstream.rest_book import fetch_rest_book
            rest_bids, rest_asks = fetch_rest_book(market) or ({}, {})
            if rest_bids or rest_asks:
                # Merge as regular update — adds new levels, updates existing
                merge_update = {
                    "bids": [[str(p), str(q)] for p, q in rest_bids.items()],
                    "asks": [[str(p), str(q)] for p, q in rest_asks.items()],
                    "ts": 0,
                }
                br.apply_json(merge_update)
                log.info("  [%s] book — %d WS updates + REST upsert (%d/%d levels)",
                         market, len(updates), len(rest_bids), len(rest_asks))
            else:
                log.info("  [%s] book — %d WS updates only (no REST)", market, len(updates))

            book_replay = br

            # Capture $1-binned snapshots at each second boundary
            for sec_offset in range(0, BLOCK_DURATION_MS, SECOND_MS):
                sec_ts = block_start_ms + sec_offset
                snap = br.get_binned_snapshot(sec_ts, bin_size=1.0)
                book_snapshots.append(snap)

            mid_snap = br.snapshot_at(block_start_ms + 15000)
            log.info("  [%s] book — %d updates, %d bid bins, %d ask bins%s",
                     market, len(updates),
                     len(book_snapshots[0].get("bid_prices", [])) if book_snapshots else 0,
                     len(book_snapshots[0].get("ask_prices", [])) if book_snapshots else 0,
                     ", seeded" if mid_snap.seeded else ", NOT seeded")

    # Compute features (with book if available)
    rows = compute_1s_features(
        block_start_ms=block_start_ms,
        market=market,
        all_bursts=bursts,
        all_trades=all_trades,
        book_replay=book_replay,
    )

    n = write_features_1s(rows, market)

    # Write book snapshots
    if book_snapshots:
        from lib.downstream.book_snapshot_writer import write_book_snapshots
        ns = write_book_snapshots(book_snapshots, market)
        log.debug("  [%s] wrote %d book snapshots", market, ns)

    return n


def _book_path_for_trade(trades_path: str, data_dir: str) -> Optional[str]:
    """Compute matching book_updates path from a trades block path."""
    # trades/<market>/<date>/<file>.jsonl → book_updates/<market>/<date>/<file>.jsonl
    rel = Path(trades_path).relative_to(Path(data_dir) / "trades")
    book = Path(data_dir) / "book_updates" / rel
    if book.exists():
        return str(book)
    return None


def run_once(data_dir: str, market_filter: Optional[str] = None,
             from_override: Optional[int] = None) -> int:
    """
    One-shot: process all pending blocks.

    Returns total rows written.
    """
    cursor = load_cursor()

    markets = [market_filter] if market_filter else discover_markets(data_dir)
    if not markets:
        log.info("No markets found in %s/trades/", data_dir)
        return 0

    total_rows = 0

    for market in sorted(markets):
        tick_size = get_tick_size(market) or 0.01
        blocks = discover_block_files(market, data_dir)
        if not blocks:
            continue

        last_processed = from_override or get_last_processed(cursor, market)
        pending = [
            (fp, bms) for fp, bms in blocks
            if last_processed is None or bms > last_processed
        ]

        if not pending:
            continue

        log.info("[%s] %d pending blocks", market, len(pending))

        # Track lookback trades for 30s traded notional
        lookback_trades = []

        for file_path, block_start_ms in pending:
            if not is_file_stable(file_path, min_age_s=2.0):
                log.debug("  [%s] %s — not stable yet, skipping", market, Path(file_path).name)
                continue

            try:
                # Find matching book_updates block
                book_path = _book_path_for_trade(file_path, data_dir)

                n = process_block(
                    file_path, block_start_ms, market, tick_size,
                    lookback_trades, book_updates_path=book_path,
                )
                if n > 0:
                    set_last_processed(cursor, market, block_start_ms)
                    total_rows += n

                    # Accumulate trades for 30s lookback (keep last 30s)
                    block_trades = read_trades_from_block(file_path)
                    lookback_trades.extend(block_trades)
                    cutoff = block_start_ms - BLOCK_DURATION_MS
                    lookback_trades = [t for t in lookback_trades
                                       if int(t.get("ts", 0)) >= cutoff]
            except Exception as e:
                log.error("  [%s] %s — ERROR: %s", market, Path(file_path).name, e)
                continue

        # Save cursor after each market
        save_cursor(cursor)

    return total_rows


def run_watch(data_dir: str, interval_s: int = 10,
              market_filter: Optional[str] = None):
    """
    Continuous watchdog: poll every interval_s seconds for new blocks.
    """
    log.info("Watchdog started — polling %s/trades/ every %ds", data_dir, interval_s)
    log.info("Parameters: gap=%dms, max_duration=%dms, block=%dms",
             GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS, BLOCK_DURATION_MS)
    log.info("Output: %s/features_1s/ (Hive-partitioned Parquet, zstd)", DERIVED_DIR)

    while True:
        try:
            n = run_once(data_dir, market_filter)
            if n > 0:
                log.info("Processed %d rows in this tick", n)
        except Exception as e:
            log.error("Watchdog tick error: %s", e)
        time.sleep(interval_s)


def main():
    parser = argparse.ArgumentParser(
        description="BTC Receiver downstream pipeline — burst features → Parquet"
    )
    parser.add_argument("--watch", action="store_true",
                        help="Continuous polling mode")
    parser.add_argument("--interval", type=int, default=10,
                        help="Polling interval in seconds (default: 10)")
    parser.add_argument("--market", type=str, default=None,
                        help="Single market filter")
    parser.add_argument("--data", type=str, default=LIVE_DATA_DIR,
                        help=f"Receiver data directory (default: {LIVE_DATA_DIR})")
    parser.add_argument("--from-ms", type=int, default=None,
                        help="Override cursor: process from this timestamp")
    parser.add_argument("--verbose", action="store_true",
                        help="Debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Ensure output directory exists
    Path(DERIVED_DIR).mkdir(parents=True, exist_ok=True)

    if args.watch:
        run_watch(args.data, args.interval, args.market)
    else:
        n = run_once(args.data, args.market, args.from_ms)
        log.info("Done — %d rows written", n)


if __name__ == "__main__":
    main()

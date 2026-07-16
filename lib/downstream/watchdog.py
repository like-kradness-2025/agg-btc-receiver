"""
Watchdog: scan Receiver output directory for new 30s blocks and process them.

Polling-based: scans data/live_v3/trades/<market>/<date>/ for new JSONL files.
Processes only blocks that are "stable" (no modification for 5+ seconds) to
avoid reading a partial write.

Tracks processed blocks via cursor (per-market last processed block_start_ms).
"""

import os
import time
import json
from pathlib import Path
from typing import List, Optional, Tuple


def discover_markets(data_dir: str) -> List[str]:
    """List market directories under data/live_v3/trades/."""
    trades_dir = Path(data_dir) / "trades"
    if not trades_dir.exists():
        return []
    return sorted([
        d.name for d in trades_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ])


def discover_block_files(market: str, data_dir: str) -> List[Tuple[str, int]]:
    """
    Discover all 30s block JSONL files for a market.

    Returns list of (file_path, block_start_ms) sorted by time ascending.
    """
    trades_dir = Path(data_dir) / "trades" / market
    if not trades_dir.exists():
        return []

    blocks = []
    for date_dir in sorted(trades_dir.iterdir()):
        if not date_dir.is_dir() or date_dir.name.startswith("."):
            continue
        for f in sorted(date_dir.iterdir()):
            if not f.name.endswith(".jsonl") or f.name.startswith("."):
                continue
            # Parse HH-MM-SS.jsonl → block_start_ms
            try:
                time_part = f.name.replace(".jsonl", "")
                hh, mm, ss = time_part.split("-")
                date_str = date_dir.name
                parts = date_str.split("-")
                import datetime
                dt = datetime.datetime(
                    int(parts[0]), int(parts[1]), int(parts[2]),
                    int(hh), int(mm), int(ss),
                    tzinfo=datetime.timezone.utc,
                )
                block_start_ms = int(dt.timestamp() * 1000)
                blocks.append((str(f), block_start_ms))
            except (ValueError, IndexError):
                continue

    return blocks


def is_file_stable(file_path: str, min_age_s: float = 3.0) -> bool:
    """
    Check if a file hasn't been modified for min_age_s seconds.
    Avoids reading during active write.
    """
    try:
        mtime = os.path.getmtime(file_path)
        return (time.time() - mtime) >= min_age_s
    except OSError:
        return False


def read_trades_from_block(file_path: str) -> List[dict]:
    """Read trades from a 30s block JSONL file (one JSON object per line)."""
    trades = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                trade = json.loads(line)
                trades.append(trade)
            except json.JSONDecodeError:
                continue
    return trades

"""
Orderbook replay from book_updates incremental diffs.

Maintains bid/ask price→qty maps from sequential updates.
Provides snapshot at any timestamp: best bid/ask, top depth, mid price.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# Maximum distance from fair/mid price to retain a price level (each side).
# Applies to both snapshot_at and binned snapshots. Seedless books are empty.
#
# MAX_PRICE_DISTANCE: float = 10000.0
#
# Unit: USD (United States Dollar)
# Meaning: The maximum absolute price difference from the fair price (mid price)
#          that a price level can have and still be included in snapshots.
# Boundary: Inclusive — levels at exactly ±10000 USD from fair are retained.
# Example: If fair price is $100,000, levels from $90,000 to $110,000 are kept.
#
# This filter prevents outlier prices (e.g., flash crashes, fat-finger orders)
# from distorting aggregate statistics while retaining realistic market depth.
MAX_PRICE_DISTANCE: float = 10000.0

# Upper bound on the number of price levels scanned during fallback searches
# (_best_bid_within_window, _best_ask_within_window).  Exchange orderbooks are
# naturally bounded (typically < 5000 levels), but a misbehaving feed could
# inject unbounded levels.  This cap prevents unbounded CPU in pathological
# cases while still being large enough that legitimate books are never truncated.
MAX_FALLBACK_SCAN_LEVELS: int = 100_000


@dataclass
class BookSnapshot:
    """Book state at a point in time."""
    ts: int                     # epoch ms
    best_bid_price: float = 0.0
    best_bid_qty: float = 0.0
    best_ask_price: float = 0.0
    best_ask_qty: float = 0.0
    mid_price: float = 0.0
    top_depth_notional: float = 0.0  # best_bid_notional + best_ask_notional
    seeded: bool = False        # both sides have at least one level

    def is_valid(self) -> bool:
        return self.seeded


class BookReplay:
    """
    Incremental orderbook replay from book_updates JSONL lines.

    Usage:
        book = BookReplay()
        for line in book_updates_lines:
            book.apply_update(line)
        snap = book.snapshot_at(ts)
    """

    def __init__(self):
        self._bids: Dict[float, float] = {}   # price → qty
        self._asks: Dict[float, float] = {}   # price → qty
        self._last_ts: int = 0
        self._seeded: bool = False
        self._best_bid: Tuple[float, float] = (0.0, 0.0)
        self._best_ask: Tuple[float, float] = (0.0, 0.0)

    def reset(self):
        """Reset book state (for new block)."""
        self._bids.clear()
        self._asks.clear()
        self._last_ts = 0
        self._seeded = False
        self._best_bid = (0.0, 0.0)
        self._best_ask = (0.0, 0.0)

    def _fair_price(self) -> Optional[float]:
        """Return current fair/mid price, or None if not seeded."""
        if not self._seeded:
            return None
        bid_price, _ = self._best_bid
        ask_price, _ = self._best_ask
        if bid_price > 0 and ask_price > 0 and bid_price < ask_price:
            return (bid_price + ask_price) / 2.0
        return None

    def apply_json(self, obj: dict):
        """
        Apply a single book update from a parsed JSON object.

        Expected format:
        {
            "market": "binance_spot",
            "type": "update",
            "bids": [["price", "qty"], ...],
            "asks": [["price", "qty"], ...],
            "ts": 1234567890123,
            "seq": 12345
        }
        """
        ts = int(obj.get("ts", 0))
        if ts > 0:
            self._last_ts = ts

        bids_raw = obj.get("bids", [])
        asks_raw = obj.get("asks", [])

        for price_str, qty_str in bids_raw:
            price = float(price_str)
            try:
                qty = float(qty_str)
            except (ValueError, TypeError):
                qty = 0.0
            if qty <= 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty

        for price_str, qty_str in asks_raw:
            price = float(price_str)
            try:
                qty = float(qty_str)
            except (ValueError, TypeError):
                qty = 0.0
            if qty <= 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty

        # Recompute best bid/ask
        self._recompute_best()

    def _recompute_best(self):
        """Recompute best bid/ask from currently stored levels only.

        Uses single-pass max/min over the bid/ask dictionaries.  Best is
        recomputed after every apply so cached values are always fresh.
        """
        if self._bids:
            best_bid_price = max(self._bids.keys())
            self._best_bid = (best_bid_price, self._bids[best_bid_price])
        else:
            self._best_bid = (0.0, 0.0)

        if self._asks:
            best_ask_price = min(self._asks.keys())
            self._best_ask = (best_ask_price, self._asks[best_ask_price])
        else:
            self._best_ask = (0.0, 0.0)

        # Seed only when both sides are present and non-crossed.
        self._seeded = (
            len(self._bids) > 0
            and len(self._asks) > 0
            and self._best_bid[0] < self._best_ask[0]
        )

    def apply_updates(self, updates: List[dict]):
        """Apply multiple updates in sequence."""
        for u in updates:
            self.apply_json(u)

    def snapshot_at(self, ts: int) -> BookSnapshot:
        """
        Get book state at a specific timestamp.

        Returns the current book state (which reflects all updates applied so far).
        The book is updated incrementally, so the state at ts is the state after
        all updates with update_ts <= ts have been applied.

        Note: we assume updates are applied in chronological order.
        The caller must feed updates in order for correct state.
        """
        snap = BookSnapshot(ts=ts)
        if not self._seeded:
            return snap

        fair = self._fair_price()
        if fair is None:
            return snap

        # Filter best levels to the fair-price window and recompute if needed.
        bid_price, bid_qty = self._best_bid
        ask_price, ask_qty = self._best_ask
        if not self._within_price_window(bid_price, fair):
            bid_price = self._best_bid_within_window(fair)
            bid_qty = self._bids.get(bid_price, 0.0)
        if not self._within_price_window(ask_price, fair):
            ask_price = self._best_ask_within_window(fair)
            ask_qty = self._asks.get(ask_price, 0.0)

        if bid_price <= 0 or ask_price <= 0 or bid_price >= ask_price:
            return snap

        snap.best_bid_price = bid_price
        snap.best_bid_qty = bid_qty
        snap.best_ask_price = ask_price
        snap.best_ask_qty = ask_qty
        snap.seeded = True

        snap.mid_price = (bid_price + ask_price) / 2.0
        snap.top_depth_notional = (bid_price * bid_qty) + (ask_price * ask_qty)

        return snap

    def _best_bid_within_window(self, fair: float) -> float:
        """Return the best bid inside the fair-price window, or 0.0 if none.

        Performance: O(n) scan over all stored bid levels, capped at
        MAX_FALLBACK_SCAN_LEVELS iterations to prevent unbounded CPU in
        pathological cases (e.g., feed injecting millions of spurious levels).
        Normal exchange orderbooks (< 5000 levels) are never affected.

        The MAX_PRICE_DISTANCE filter prunes levels outside ±$10k from fair,
        but this scan visits ALL stored levels (not just those in the window)
        to find the best one that qualifies.
        """
        best = 0.0
        scanned = 0
        for price in self._bids:
            if self._within_price_window(price, fair) and price > best:
                best = price
            scanned += 1
            if scanned >= MAX_FALLBACK_SCAN_LEVELS:
                break
        return best

    def _best_ask_within_window(self, fair: float) -> float:
        """Return the best ask inside the fair-price window, or 0.0 if none.

        See _best_bid_within_window for performance notes.
        """
        best = 0.0
        scanned = 0
        for price in self._asks:
            if self._within_price_window(price, fair) and (best == 0.0 or price < best):
                best = price
            scanned += 1
            if scanned >= MAX_FALLBACK_SCAN_LEVELS:
                break
        return best

    def _within_price_window(self, price: float, fair: float) -> bool:
        """Check if price is within +/- MAX_PRICE_DISTANCE of fair price."""
        return abs(price - fair) <= MAX_PRICE_DISTANCE

    def get_binned_snapshot(self, ts: int, bin_size: float = 1.0) -> dict:
        """
        Get full book snapshot with prices binned to bin_size intervals.

        Bids sorted descending (best first), asks sorted ascending.
        Each bin sums qty for [bin_floor, bin_floor + bin_size).

        Only retains price levels within +/- MAX_PRICE_DISTANCE of the fair
        price. If the book is not seeded (fair price unknown), returns an
        empty seedless snapshot.

        Returns dict:
        {ts, seeded,
         bid_prices: list[float], bid_qtys: list[float],
         ask_prices: list[float], ask_qtys: list[float]}
        """
        import math
        result = {"ts": ts, "seeded": self._seeded}

        fair = self._fair_price()
        if fair is None:
            result["bid_prices"] = []
            result["bid_qtys"] = []
            result["ask_prices"] = []
            result["ask_qtys"] = []
            return result

        def _bin(levels: Dict[float, float], ascending: bool) -> tuple:
            """Bin price→qty levels into bin_size buckets, filtered by fair-price window."""
            bins: Dict[float, float] = {}
            for price, qty in levels.items():
                if qty <= 0:
                    continue
                if not self._within_price_window(price, fair):
                    continue
                bin_key = math.floor(price / bin_size) * bin_size
                bins[bin_key] = bins.get(bin_key, 0.0) + qty

            sorted_keys = sorted(bins.keys(), reverse=not ascending)
            prices = []
            qtys = []
            for k in sorted_keys:
                prices.append(k)
                qtys.append(round(bins[k], 8))
            return prices, qtys

        result["bid_prices"], result["bid_qtys"] = _bin(self._bids, ascending=False)
        result["ask_prices"], result["ask_qtys"] = _bin(self._asks, ascending=True)
        return result


def read_book_updates(file_path: str) -> List[dict]:
    """Read book_updates from a 30s block JSONL file (one JSON object per line)."""
    import json
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
    return updates


def get_block_start_from_path(file_path: str) -> Optional[int]:
    """
    Extract block start timestamp (epoch ms) from a path like
    .../YYYY-MM-DD/HH-MM-SS.jsonl

    Returns None if parsing fails.
    """
    import datetime
    import re
    from pathlib import Path

    fname = Path(file_path).name.replace(".jsonl", "")
    try:
        parts = fname.split("-")
        hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
        date_dir = Path(file_path).parent.name  # YYYY-MM-DD
        dp = date_dir.split("-")
        dt = datetime.datetime(
            int(dp[0]), int(dp[1]), int(dp[2]),
            hh, mm, ss,
            tzinfo=datetime.timezone.utc,
        )
        return int(dt.timestamp() * 1000)
    except (ValueError, IndexError):
        return None

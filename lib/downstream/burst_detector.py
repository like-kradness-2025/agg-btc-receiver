"""
Burst detection from raw trades.

Burst definition:
  - Same market, same side (buy/sell), consecutive trades
  - Adjacent print-to-print gap <= GAP_THRESHOLD_MS (50ms)
  - Burst duration <= MAX_BURST_DURATION_MS (5000ms)
  - Maximal contiguous run

Trade input: list of dicts with ts, price, qty, side, tradeId
"""

from dataclasses import dataclass, field
from typing import List, Optional

from .config import GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS


@dataclass
class Burst:
    """A single burst — maximal contiguous same-side trade sequence."""
    market: str
    side: str                       # 'buy' | 'sell'
    burst_notional: float = 0.0     # sum(price * qty)
    burst_print_count: int = 0
    burst_duration_ms: int = 0
    burst_start_ts: int = 0
    burst_end_ts: int = 0
    min_price: float = 0.0
    max_price: float = 0.0
    distinct_price_count: int = 0
    span_ticks: int = 0             # (max_price - min_price) / tick_size
    same_price_runs: int = 0        # max consecutive same-price sub-run length
    # internal tracking
    _prices: set = field(default_factory=set)
    _current_run_price: Optional[float] = None
    _current_run_len: int = 0
    _max_run_len: int = 0

    def add_trade(self, trade: dict, tick_size: float):
        """Add a trade to this burst, updating all metrics."""
        price = float(trade['price'])
        qty = float(trade['qty'])
        ts = int(trade['ts'])
        notional = price * qty

        if self.burst_print_count == 0:
            self.burst_start_ts = ts
            self._current_run_price = price
            self._current_run_len = 1
            self._max_run_len = 1
        else:
            # Same-price run tracking
            if abs(price - self._current_run_price) < 1e-10:
                self._current_run_len += 1
                if self._current_run_len > self._max_run_len:
                    self._max_run_len = self._current_run_len
            else:
                self._current_run_price = price
                self._current_run_len = 1

        self.burst_notional += notional
        self.burst_print_count += 1
        self.burst_end_ts = ts
        self.burst_duration_ms = ts - self.burst_start_ts

        if price < self.min_price:
            self.min_price = price
        if price > self.max_price:
            self.max_price = price
        self._prices.add(price)
        self.distinct_price_count = len(self._prices)

    def finalize(self, tick_size: float):
        """Call after all trades added. Computes derived fields."""
        if self.distinct_price_count > 1:
            price_span = self.max_price - self.min_price
            if tick_size and tick_size > 0:
                self.span_ticks = round(price_span / tick_size)
            else:
                self.span_ticks = 0
        self.same_price_runs = self._max_run_len


def detect_bursts(trades: List[dict], tick_size: float) -> List[Burst]:
    """
    Detect bursts from a sorted (by ts) list of raw trades.

    Args:
        trades: List of trade dicts, each with at least:
                ts (int), price (float/str), qty (float/str), side (str), tradeId (str)
                Should already be sorted by ts ascending.
        tick_size: Market tick size for span calculation.

    Returns:
        List of Burst objects in temporal order.
    """
    if not trades:
        return []

    bursts: List[Burst] = []
    current: Optional[Burst] = None

    for trade in trades:
        side = trade['side']
        ts = int(trade['ts'])

        if current is None:
            current = Burst(market=trade.get('market', ''), side=side)
            current.add_trade(trade, tick_size)
            continue

        same_side = (side == current.side)
        gap = ts - current.burst_end_ts
        duration_if_added = ts - current.burst_start_ts

        if same_side and gap <= GAP_THRESHOLD_MS and duration_if_added <= MAX_BURST_DURATION_MS:
            # Continue current burst
            current.add_trade(trade, tick_size)
        else:
            # Finalize current, start new
            current.finalize(tick_size)
            bursts.append(current)
            current = Burst(market=trade.get('market', ''), side=side)
            current.add_trade(trade, tick_size)

    if current is not None:
        current.finalize(tick_size)
        bursts.append(current)

    return bursts


def get_closed_bursts_overlapping(bursts: List[Burst], second_ts: int) -> List[Burst]:
    """
    Filter bursts that overlap a given 1s bucket [second_ts, second_ts + 1000).

    Overlap condition:
      burst.burst_start_ts < second_ts + 1000 AND burst.burst_end_ts >= second_ts
    """
    bucket_end = second_ts + 1000
    return [
        b for b in bursts
        if b.burst_start_ts < bucket_end and b.burst_end_ts >= second_ts
    ]

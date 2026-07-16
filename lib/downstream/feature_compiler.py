"""
1-second feature computation from bursts + book state.

For each second in a 30s block, collect overlapping bursts and compute
all 22 feature columns. P1 contract: #1-#12 computed, #13=null, #14=0, #15-#22=0.

When book_replay is provided, #13 and #14 are computed from live book state.
"""

from typing import List, Dict, Optional, TYPE_CHECKING

from .config import BLOCK_DURATION_MS, SECOND_MS
from .burst_detector import Burst, get_closed_bursts_overlapping

if TYPE_CHECKING:
    from .book_replay import BookReplay


def _traded_notional_in_window(trades: List[dict], window_start: int, window_end: int) -> float:
    """Sum of price*qty for trades in [window_start, window_end)."""
    total = 0.0
    for t in trades:
        ts = int(t.get("ts", 0))
        if window_start <= ts < window_end:
            price = float(t.get("price", 0))
            qty = float(t.get("qty", 0))
            total += price * qty
    return total


def compute_1s_features(
    block_start_ms: int,
    market: str,
    all_bursts: List[Burst],
    all_trades: List[dict],
    book_replay: Optional['BookReplay'] = None,
) -> List[Dict]:
    """
    Compute 22 feature columns for each second in a 30s block.

    Args:
        block_start_ms: Block start timestamp (epoch ms, 30s-aligned)
        market: Market identifier
        all_bursts: All closed bursts detected in this block
        all_trades: All trades for lookback (current block + prior blocks)
        book_replay: BookReplay instance seeded with book_updates for this block.
                     If None, book features default to P1 placeholder values.

    Returns:
        List of 30 dicts (one per second), with ts, market, and all 22 features.
    """
    rows = []

    for second_offset in range(0, BLOCK_DURATION_MS, SECOND_MS):
        second_ts = block_start_ms + second_offset
        overlapping = get_closed_bursts_overlapping(all_bursts, second_ts)

        # ── Burst features ──
        burst_count = len(overlapping)

        total_notional = sum(b.burst_notional for b in overlapping)
        max_notional = max((b.burst_notional for b in overlapping), default=0.0)
        max_prints = max((b.burst_print_count for b in overlapping), default=0)
        max_duration = max((b.burst_duration_ms for b in overlapping), default=0)

        buy_notional = sum(b.burst_notional for b in overlapping if b.side == 'buy')
        sell_notional = sum(b.burst_notional for b in overlapping if b.side == 'sell')

        imbalance_ratio = (buy_notional - sell_notional) / max(total_notional, 1e-10)
        largest_share = (max_notional / max(total_notional, 1e-10)) if burst_count > 0 else 0.0

        same_price_count = sum(1 for b in overlapping if b.distinct_price_count == 1)
        multilevel_count = sum(1 for b in overlapping if b.distinct_price_count >= 2)

        # ── #12: Burst vs 30s traded notional ──
        lookback_start = second_ts - BLOCK_DURATION_MS
        traded_30s = _traded_notional_in_window(all_trades, lookback_start, second_ts)
        vs_traded = total_notional / max(traded_30s, 1e-10)
        if traded_30s < 1e-10:
            vs_traded = 0.0

        # ── Book-dependent features #13, #14 ──
        book_snap = None
        if book_replay is not None:
            book_snap = book_replay.snapshot_at(second_ts)

        if book_snap is not None and book_snap.seeded:
            vs_top_depth = total_notional / max(book_snap.top_depth_notional, 1e-10)
            # #14: mid move — compares current mid to mid at block start
            # For a simple version: use mid at second boundary vs previous second
            if book_snap.mid_price > 0:
                mid_move_bps = 0.0  # P1: still 0 until we track per-second mid history
            else:
                mid_move_bps = 0.0
        else:
            vs_top_depth = None
            mid_move_bps = 0.0

        # ── Research features #15-#21 ──
        same_price_max_len = max(
            (b.same_price_runs for b in overlapping if b.distinct_price_count == 1),
            default=0
        )
        same_price_notional = sum(
            b.burst_notional for b in overlapping if b.distinct_price_count == 1
        )
        multilevel_max_span_ticks = max(
            (b.span_ticks for b in overlapping if b.distinct_price_count >= 2),
            default=0
        )
        multilevel_max_span_bps = 0.0
        multilevel_notional = sum(
            b.burst_notional for b in overlapping if b.distinct_price_count >= 2
        )
        absorption_ratio = same_price_notional / max(total_notional, 1e-10)
        delta_notional = buy_notional - sell_notional

        for b in overlapping:
            if b.distinct_price_count >= 2 and b.min_price > 0:
                span_bps = (b.max_price - b.min_price) / b.min_price * 10000
                if span_bps > multilevel_max_span_bps:
                    multilevel_max_span_bps = span_bps

        outlier_flag = 0

        row = {
            "ts": second_ts,
            "market": market,
            "burst_count_1s": burst_count,
            "total_burst_notional_1s": round(total_notional, 2),
            "max_burst_notional_1s": round(max_notional, 2),
            "max_burst_prints_1s": max_prints,
            "max_burst_duration_ms_1s": max_duration,
            "buy_burst_notional_1s": round(buy_notional, 2),
            "sell_burst_notional_1s": round(sell_notional, 2),
            "burst_imbalance_ratio_1s": round(imbalance_ratio, 6),
            "largest_burst_share_notional_1s": round(largest_share, 6),
            "same_price_burst_count_1s": same_price_count,
            "multilevel_burst_count_1s": multilevel_count,
            "burst_notional_vs_30s_traded_notional": round(vs_traded, 6),
            "burst_notional_vs_top_depth": round(vs_top_depth, 6) if vs_top_depth is not None else None,
            "burst_mid_move_bps_1s": round(mid_move_bps, 4),
            "same_price_burst_max_len_1s": same_price_max_len,
            "same_price_burst_notional_1s": round(same_price_notional, 2),
            "multilevel_burst_max_span_ticks_1s": round(multilevel_max_span_ticks, 2),
            "multilevel_burst_max_span_bps_1s": round(multilevel_max_span_bps, 4),
            "multilevel_burst_notional_1s": round(multilevel_notional, 2),
            "same_price_absorption_ratio_1s": round(absorption_ratio, 6),
            "burst_delta_notional_1s": round(delta_notional, 2),
            "outlier_trade_flag_1s": outlier_flag,
        }
        rows.append(row)

    return rows

"""
Incremental per-second feature computer.

Processes trades chronologically, accumulating per-second aggregates.
When second boundary crossed, computes burst features for that second
and emits to features_1s.

Stateful across 30s block boundaries (continuous processing).
"""

import math
from typing import List, Dict, Optional, Tuple
from .burst_detector import Burst
from .config import BLOCK_DURATION_MS, SECOND_MS, get_tick_size


def _compute_realized_vol_from_series(
    price_series: List[Tuple[int, float]],
    at_ts: int,
    window_ms: int,
) -> Optional[float]:
    """Compute realized volatility from a pre-built (ts, price) series.

    Window: [at_ts - window_ms, at_ts) — strict past, no lookahead.
    Returns None if fewer than 3 prices (i.e. fewer than 2 log-returns).

    This is the incremental-path equivalent of
    ``feature_compiler._compute_realized_vol(trades, at_ts, window_ms)``.
    """
    window_start = at_ts - window_ms

    # Binary search for first entry >= window_start
    lo, hi = 0, len(price_series)
    while lo < hi:
        mid = (lo + hi) // 2
        if price_series[mid][0] < window_start:
            lo = mid + 1
        else:
            hi = mid

    # Collect prices in [window_start, at_ts)
    prices = []
    for i in range(lo, len(price_series)):
        ts_i, p_i = price_series[i]
        if ts_i >= at_ts:
            break
        if p_i > 0:
            prices.append(p_i)

    if len(prices) < 3:
        return None

    # Log-returns
    log_returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            log_returns.append(math.log(prices[i] / prices[i - 1]))

    if len(log_returns) < 2:
        return None

    mean = sum(log_returns) / len(log_returns)
    variance = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
    return math.sqrt(variance)


class IncrementalFeatureComputer:
    """
    Stateful per-second feature accumulator.

    Usage:
        computer = IncrementalFeatureComputer(market, tick_size)
        for trade in trades_chronological:
            features_row = computer.process_trade(trade)
            if features_row is not None:
                # Second boundary crossed, emit features_row to features_1s
                write_features_1s([features_row], market)

        # Flush final second at end of stream
        final_row = computer.flush()
        if final_row is not None:
            write_features_1s([final_row], market)
    """

    def __init__(self, market: str, tick_size: float, book_replay=None):
        self.market = market
        self.tick_size = tick_size
        self.book_replay = book_replay

        # Current second state
        self.current_second_ts: Optional[int] = None
        self.current_second_trades: List[dict] = []
        self.completed_bursts_in_second: List[Burst] = []
        self.current_burst: Optional[Burst] = None

        # Cross-second price series for RV rolling window.
        # Stores (ts_ms, price) for every trade processed.
        # Pruned to keep only entries within the last 60s window.
        # RV windows use [at_ts - window_ms, at_ts) (strict past, no lookahead).
        self._price_series: List[tuple] = []
        self._max_window_ms = 60_000  # largest RV window

        # P1: cross-second book snapshot state (for spread/depth/imbalance delta)
        # Key: second_ts (epoch ms, 1s aligned)
        # Value: dict with spread_bps, top_depth, imbalance_100
        self._p1_book_snapshots: Dict[int, Dict] = {}
        self._p1_max_lookback_ms = 30_000  # depth_delta_30s needs 30s lookback

        # Cross-second state for mid_move and 30s traded notional
        # Stores (ts_ms, mid_price) for mid price change tracking
        self._mid_price_series: List[tuple] = []
        # Stores (ts_ms, notional, ts_second) for 30s rolling traded notional
        self._trade_notional_series: List[tuple] = []

    def _compute_p1_features(self, second_ts: int) -> Dict:
        """Compute P1 OrderFlow features for a given second.

        Returns dict with keys matching schema P1 columns:
        - ofi: Order Flow Imbalance (from book_replay bucket)
        - spread_delta_1s: spread change vs 1s ago
        - depth_delta_1s, depth_delta_30s: top depth change vs 1s/30s ago
        - imbalance_delta_1s: imbalance_100 change vs 1s ago
        - bid_add_qty, bid_cancel_qty, ask_add_qty, ask_cancel_qty: add/cancel flow
        - replenishment, pulling: best level qty changes

        Null semantics (no speculation/zero-fill):
        - ofi, add/cancel, replenishment/pulling: 0.0 when book_replay is None or bucket missing
        - spread/depth/imbalance delta: None when prev snapshot missing
        """
        # Aggregate P1 change events from book_replay for this second
        # BookReplay records raw change events; IncrementalFeatureComputer aggregates them
        bucket_ofi = 0.0
        bucket_bid_add = 0.0
        bucket_bid_cancel = 0.0
        bucket_ask_add = 0.0
        bucket_ask_cancel = 0.0
        bucket_replenishment = 0.0
        bucket_pulling = 0.0

        if self.book_replay is not None:
            change_events = self.book_replay.get_p1_change_events_in_window(second_ts)
            for event in change_events:
                bucket_ofi += event["ofi"]
                bucket_bid_add += event["bid_add_qty"]
                bucket_bid_cancel += event["bid_cancel_qty"]
                bucket_ask_add += event["ask_add_qty"]
                bucket_ask_cancel += event["ask_cancel_qty"]
                bucket_replenishment += event["replenishment"]
                bucket_pulling += event["pulling"]

        # Compute book snapshot at second_ts for spread/depth/imbalance delta
        # We need the book state at the END of second_ts (after all updates in that second)
        # Since BookReplay is stateful and apply_json is called chronologically,
        # we need to get snapshot at second_ts + 1000 (start of next second) to capture
        # all updates within [second_ts, second_ts + 1000)
        # However, for lookahead prevention, we should use the state at second_ts
        # (i.e., the state BEFORE any updates in second_ts are applied)
        # But that would give us the previous second's state...
        # Actually, the correct interpretation: we want the book state AFTER all updates
        # in [second_ts, second_ts + 1000) have been applied, which is the state at
        # second_ts + 1000 (exclusive). But since we're computing features for second_ts
        # when the next second starts, the book state is already at second_ts + 1000.
        # So we can call compute_book_features with any ts in (second_ts, second_ts + 1000].

        spread_t = None
        top_depth_t = None
        imbalance_100_t = None

        if self.book_replay is not None:
            # Get book features at the end of second_ts (use second_ts + 999 to avoid
            # crossing into next second's updates if any)
            book_feats = self.book_replay.compute_book_features(second_ts + 999)
            if book_feats["book_spread_bps"] is not None:
                spread_t = book_feats["book_spread_bps"]
            if book_feats["book_bid_depth_100"] is not None and book_feats["book_ask_depth_100"] is not None:
                top_depth_t = book_feats["book_bid_depth_100"] + book_feats["book_ask_depth_100"]
            if book_feats["book_imbalance_100"] is not None:
                imbalance_100_t = book_feats["book_imbalance_100"]

            # Store snapshot for future delta calculations
            if spread_t is not None or top_depth_t is not None or imbalance_100_t is not None:
                self._p1_book_snapshots[second_ts] = {
                    "spread_bps": spread_t,
                    "top_depth": top_depth_t,
                    "imbalance_100": imbalance_100_t,
                }

        # Compute deltas
        spread_delta_1s = None
        depth_delta_1s = None
        depth_delta_30s = None
        imbalance_delta_1s = None

        if spread_t is not None:
            # 1s delta
            prev_1s = second_ts - 1000
            if prev_1s in self._p1_book_snapshots:
                prev_spread = self._p1_book_snapshots[prev_1s]["spread_bps"]
                if prev_spread is not None:
                    spread_delta_1s = spread_t - prev_spread

        if top_depth_t is not None:
            # 1s delta
            prev_1s = second_ts - 1000
            if prev_1s in self._p1_book_snapshots:
                prev_depth = self._p1_book_snapshots[prev_1s]["top_depth"]
                if prev_depth is not None:
                    depth_delta_1s = top_depth_t - prev_depth

            # 30s delta
            prev_30s = second_ts - 30_000
            if prev_30s in self._p1_book_snapshots:
                prev_depth = self._p1_book_snapshots[prev_30s]["top_depth"]
                if prev_depth is not None:
                    depth_delta_30s = top_depth_t - prev_depth

        if imbalance_100_t is not None:
            # 1s delta only (30s not in spec)
            prev_1s = second_ts - 1000
            if prev_1s in self._p1_book_snapshots:
                prev_imbalance = self._p1_book_snapshots[prev_1s]["imbalance_100"]
                if prev_imbalance is not None:
                    imbalance_delta_1s = imbalance_100_t - prev_imbalance

        # Prune old snapshots to avoid memory buildup
        cutoff = second_ts - self._p1_max_lookback_ms
        old_keys = [k for k in self._p1_book_snapshots if k < cutoff]
        for k in old_keys:
            del self._p1_book_snapshots[k]

        return {
            "ofi": bucket_ofi,
            "spread_delta_1s": spread_delta_1s,
            "depth_delta_1s": depth_delta_1s,
            "depth_delta_30s": depth_delta_30s,
            "imbalance_delta_1s": imbalance_delta_1s,
            "bid_add_qty": bucket_bid_add,
            "bid_cancel_qty": bucket_bid_cancel,
            "ask_add_qty": bucket_ask_add,
            "ask_cancel_qty": bucket_ask_cancel,
            "replenishment": bucket_replenishment,
            "pulling": bucket_pulling,
        }

    def process_trade(self, trade: dict) -> Optional[dict]:
        """
        Process a single trade chronologically.

        Returns features dict when second boundary crossed (emit to features_1s),
        or None if still accumulating within current second.
        """
        trade_ts = int(trade.get("ts", 0))
        trade_second = (trade_ts // SECOND_MS) * SECOND_MS

        # Record price for RV rolling window (strict past, so it's available
        # for the *next* second's RV computation, not the current one).
        price = float(trade.get("price", 0))
        qty = float(trade.get("qty", 0))
        if price > 0:
            self._price_series.append((trade_ts, price))
            # Record trade notional for 30s rolling window
            notional = price * qty
            self._trade_notional_series.append((trade_ts, notional, trade_second))

        # First trade ever
        if self.current_second_ts is None:
            self.current_second_ts = trade_second
            self.current_second_trades.append(trade)
            self._update_burst(trade)
            return None

        # Second boundary crossed
        if trade_second > self.current_second_ts:
            # Finalize current burst
            if self.current_burst is not None:
                self.current_burst.finalize(self.tick_size)
                self.completed_bursts_in_second.append(self.current_burst)
                self.current_burst = None

            # Compute features for the just-completed second
            features_row = self._compute_features_for_second()

            # Reset state for new second
            self.current_second_ts = trade_second
            self.current_second_trades = []
            self.completed_bursts_in_second = []

            # Add this trade to new second
            self.current_second_trades.append(trade)
            self._update_burst(trade)

            return features_row

        # Still within current second
        self.current_second_trades.append(trade)
        self._update_burst(trade)
        return None

    def _update_burst(self, trade: dict):
        """Update or start burst based on trade (same logic as detect_bursts)."""
        from .burst_detector import GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS

        side = trade.get("side", "")
        ts = int(trade.get("ts", 0))

        if self.current_burst is None:
            # Start new burst
            self.current_burst = Burst(market=self.market, side=side)
            self.current_burst.add_trade(trade, self.tick_size)
            return

        same_side = (side == self.current_burst.side)
        gap = ts - self.current_burst.burst_end_ts
        duration_if_added = ts - self.current_burst.burst_start_ts

        if (same_side and gap <= GAP_THRESHOLD_MS
                and duration_if_added <= MAX_BURST_DURATION_MS):
            # Continue current burst
            self.current_burst.add_trade(trade, self.tick_size)
        else:
            # Finalize current, start new
            self.current_burst.finalize(self.tick_size)
            self.completed_bursts_in_second.append(self.current_burst)
            self.current_burst = Burst(market=self.market, side=side)
            self.current_burst.add_trade(trade, self.tick_size)

    def _compute_features_for_second(self) -> dict:
        """Compute all features for the just-completed second."""
        from .feature_compiler import (
            get_closed_bursts_overlapping,
            _traded_notional_in_window,
        )

        assert self.current_second_ts is not None
        second_ts = self.current_second_ts
        overlapping = self.completed_bursts_in_second

        # ── Burst features ──
        burst_count = len(overlapping)
        total_notional = sum(b.burst_notional for b in overlapping)
        max_notional = max((b.burst_notional for b in overlapping), default=0.0)
        max_prints = max((b.burst_print_count for b in overlapping), default=0)
        max_duration = max((b.burst_duration_ms for b in overlapping), default=0)

        buy_notional = sum(b.burst_notional for b in overlapping if b.side == "buy")
        sell_notional = sum(b.burst_notional for b in overlapping if b.side == "sell")

        imbalance_ratio = (buy_notional - sell_notional) / max(total_notional, 1e-10)
        largest_share = (max_notional / max(total_notional, 1e-10)) if burst_count > 0 else 0.0

        same_price_count = sum(1 for b in overlapping if b.distinct_price_count == 1)
        multilevel_count = sum(1 for b in overlapping if b.distinct_price_count >= 2)

        # ── #12: Burst vs 30s traded notional (rolling 30s window) ──
        # Uses cross-second _trade_notional_series for proper lookback
        window_start = second_ts - 30_000
        vs_traded = sum(
            n for ts, n, _ in self._trade_notional_series
            if window_start <= ts < second_ts
        )
        # Prune old entries (>30s)
        cutoff = second_ts - 60_000  # keep extra 30s for safety
        self._trade_notional_series = \
            [(ts, n, s) for ts, n, s in self._trade_notional_series if ts >= cutoff]

        # ── Book-dependent features #13-#14 ──
        vs_top_depth = None
        mid_move_bps = None

        if self.book_replay is not None:
            book_feats_mid = self.book_replay.compute_book_features(second_ts)
            mid_t = book_feats_mid.get("book_mid_price")

            # #13: burst_notional_vs_top_depth — book top depth ratio
            if mid_t is not None and book_feats_mid.get("book_spread_bps") is not None:
                top_bid = book_feats_mid.get("book_bid_depth_100")
                top_ask = book_feats_mid.get("book_ask_depth_100")
                if top_bid is not None and top_ask is not None and (top_bid + top_ask) > 0:
                    vs_top_depth = total_notional / (top_bid + top_ask)

            # #14: burst_mid_move_bps_1s — mid price change over 1s
            self._mid_price_series.append((second_ts, mid_t))
            # Find the mid price at second_ts - 1000 (previous second boundary)
            prev_mid = None
            for ts, mp in reversed(self._mid_price_series):
                if ts <= second_ts - 1000:
                    prev_mid = mp
                    break
            if mid_t is not None and prev_mid is not None and prev_mid > 0:
                mid_move_bps = (mid_t - prev_mid) / prev_mid * 10000
            # Prune old mid price entries
            cutoff_mid = second_ts - 60_000
            self._mid_price_series = [(ts, mp) for ts, mp in self._mid_price_series if ts >= cutoff_mid]

        # ── Book features B1-B9 (not computed here, requires book state) ──
        book_features = {
            "book_mid_price": None,
            "book_spread_bps": None,
            "book_bid_depth_100": None,
            "book_ask_depth_100": None,
            "book_bid_depth_1000": None,
            "book_ask_depth_1000": None,
            "book_imbalance_100": None,
            "book_imbalance_1000": None,
            "book_microprice": None,
        }

        # ── P1: OrderFlow features (from book_replay) ──
        # Get P1 features from book_replay for this second
        p1_features = self._compute_p1_features(second_ts)

        # Extract P1 variables for clarity
        ofi = p1_features["ofi"]
        spread_delta_1s = p1_features["spread_delta_1s"]
        depth_delta_1s = p1_features["depth_delta_1s"]
        depth_delta_30s = p1_features["depth_delta_30s"]
        imbalance_delta_1s = p1_features["imbalance_delta_1s"]
        bid_add_qty = p1_features["bid_add_qty"]
        bid_cancel_qty = p1_features["bid_cancel_qty"]
        ask_add_qty = p1_features["ask_add_qty"]
        ask_cancel_qty = p1_features["ask_cancel_qty"]
        replenishment = p1_features["replenishment"]
        pulling = p1_features["pulling"]

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

        # ── outlier_trade_flag (#22) — flag seconds with anomalous price action ──
        # Rule: any trade price deviates >0.5% from rolling mean of recent trades
        # (within the last 30s window). Avoids lookahead by using strict-past window.
        outlier_flag = 0
        current_trades = self.current_second_trades
        if current_trades:
            # Build mean price from _price_series in [second_ts - 30s, second_ts)
            outlier_window_start = second_ts - 30_000
            recent_prices = [p for ts, p in self._price_series
                             if outlier_window_start <= ts < second_ts and p > 0]
            if recent_prices:
                mean_price = sum(recent_prices) / len(recent_prices)
                threshold = mean_price * 0.005  # 0.5%
                for t in current_trades:
                    tp = float(t.get("price", 0))
                    if tp > 0 and abs(tp - mean_price) > threshold:
                        outlier_flag = 1
                        break

        # ── OrderFlow P0 features ──
        bucket_trades = self.current_second_trades
        trade_count = len(bucket_trades)
        traded_notional = sum(
            float(t.get("price", 0)) * float(t.get("qty", 0))
            for t in bucket_trades
        )

        buy_qty = sum(float(t.get("qty", 0)) for t in bucket_trades if t.get("side") == "buy")
        sell_qty = sum(float(t.get("qty", 0)) for t in bucket_trades if t.get("side") == "sell")
        signed_volume = buy_qty - sell_qty

        total_qty = buy_qty + sell_qty
        trade_imbalance_qty = (buy_qty - sell_qty) / total_qty if total_qty > 0 else 0.0

        # RV uses trades from prior seconds (rolling window, strict past).
        # Window is [second_ts - window_ms, second_ts). No lookahead.
        # _compute_realized_vol returns None when < 3 prices / < 2 returns.
        realized_vol_10s = _compute_realized_vol_from_series(
            self._price_series, second_ts, window_ms=10_000
        )
        realized_vol_60s = _compute_realized_vol_from_series(
            self._price_series, second_ts, window_ms=60_000
        )

        # Prune price series: drop entries older than max window from second_ts.
        # Keeps memory bounded across long-running processes.
        cutoff_ts = second_ts - self._max_window_ms
        if self._price_series and self._price_series[0][0] < cutoff_ts:
            # Find first index >= cutoff_ts
            lo, hi = 0, len(self._price_series)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._price_series[mid][0] < cutoff_ts:
                    lo = mid + 1
                else:
                    hi = mid
            self._price_series = self._price_series[lo:]

        return {
            "ts": second_ts,
            "market": self.market,
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
            "burst_mid_move_bps_1s": round(mid_move_bps, 4) if mid_move_bps is not None else None,
            "same_price_burst_max_len_1s": same_price_max_len,
            "same_price_burst_notional_1s": round(same_price_notional, 2),
            "multilevel_burst_max_span_ticks_1s": round(multilevel_max_span_ticks, 2),
            "multilevel_burst_max_span_bps_1s": round(multilevel_max_span_bps, 4),
            "multilevel_burst_notional_1s": round(multilevel_notional, 2),
            "same_price_absorption_ratio_1s": round(absorption_ratio, 6),
            "burst_delta_notional_1s": round(delta_notional, 2),
            "outlier_trade_flag_1s": outlier_flag,
            "book_mid_price": round(book_features["book_mid_price"], 4) if book_features["book_mid_price"] is not None else None,
            "book_spread_bps": round(book_features["book_spread_bps"], 4) if book_features["book_spread_bps"] is not None else None,
            "book_bid_depth_100": round(book_features["book_bid_depth_100"], 2) if book_features["book_bid_depth_100"] is not None else None,
            "book_ask_depth_100": round(book_features["book_ask_depth_100"], 2) if book_features["book_ask_depth_100"] is not None else None,
            "book_bid_depth_1000": round(book_features["book_bid_depth_1000"], 2) if book_features["book_bid_depth_1000"] is not None else None,
            "book_ask_depth_1000": round(book_features["book_ask_depth_1000"], 2) if book_features["book_ask_depth_1000"] is not None else None,
            "book_imbalance_100": round(book_features["book_imbalance_100"], 6) if book_features["book_imbalance_100"] is not None else None,
            "book_imbalance_1000": round(book_features["book_imbalance_1000"], 6) if book_features["book_imbalance_1000"] is not None else None,
            "book_microprice": round(book_features["book_microprice"], 4) if book_features["book_microprice"] is not None else None,
            "trade_count_1s": trade_count,
            "traded_notional_1s": round(traded_notional, 2),
            "signed_volume_1s": round(signed_volume, 8),
            "trade_imbalance_qty_1s": round(trade_imbalance_qty, 6),
            "realized_vol_10s": round(realized_vol_10s, 10) if realized_vol_10s is not None else None,
            "realized_vol_60s": round(realized_vol_60s, 10) if realized_vol_60s is not None else None,
            # P1 features
            "ofi_1s": round(ofi, 8),
            "spread_delta_1s": round(spread_delta_1s, 6) if spread_delta_1s is not None else None,
            "depth_delta_1s": round(depth_delta_1s, 2) if depth_delta_1s is not None else None,
            "depth_delta_30s": round(depth_delta_30s, 2) if depth_delta_30s is not None else None,
            "imbalance_delta_1s": round(imbalance_delta_1s, 6) if imbalance_delta_1s is not None else None,
            "bid_add_qty_1s": round(bid_add_qty, 8),
            "bid_cancel_qty_1s": round(bid_cancel_qty, 8),
            "ask_add_qty_1s": round(ask_add_qty, 8),
            "ask_cancel_qty_1s": round(ask_cancel_qty, 8),
            "replenishment_qty_1s": round(replenishment, 8),
            "pulling_qty_1s": round(pulling, 8),
        }

    def flush(self) -> Optional[dict]:
        """Flush final second (call at end of stream). Returns features or None."""
        if self.current_second_ts is None or not self.current_second_trades:
            return None

        # Finalize current burst
        if self.current_burst is not None:
            self.current_burst.finalize(self.tick_size)
            self.completed_bursts_in_second.append(self.current_burst)
            self.current_burst = None

        row = self._compute_features_for_second()

        # Reset per-second state so the next block starts clean.
        # Note: _price_series (RV rolling window) is intentionally NOT reset —
        # it must persist across blocks for cross-second RV computation.
        self.current_second_ts = None
        self.current_second_trades = []
        self.completed_bursts_in_second = []

        return row

    def flush_block(self, block_start_ms: int, block_end_ms: int) -> List[dict]:
        """
        Flush all seconds in [block_start_ms, block_end_ms) that have not been emitted.
        Emits empty rows (zero counts, zero notional) for seconds with no trades.
        Returns list of feature rows (may include empty-second rows).
        """
        from .config import SECOND_MS
        rows = []
        # Flush the currently accumulating second (if any trades present)
        final_row = self.flush()
        if final_row is not None:
            rows.append(final_row)
            last_emitted_ts = final_row["ts"] + SECOND_MS
        else:
            # No trades at all in this block yet — emit empty rows from block start
            last_emitted_ts = block_start_ms

        # Backfill empty seconds up to block_end_ms
        # Only emit rows for seconds that had no trades (ts > last trade second)
        while last_emitted_ts < block_end_ms:
            # Empty row: all counters zero, nullable fields null
            row = self._empty_row(last_emitted_ts)
            rows.append(row)
            last_emitted_ts += SECOND_MS

        return rows

    def _empty_row(self, ts: int) -> dict:
        """Zero-filled row for a second with no trades."""
        return {
            "ts": ts,
            "market": self.market,
            "burst_count_1s": 0,
            "total_burst_notional_1s": 0.0,
            "max_burst_notional_1s": 0.0,
            "max_burst_prints_1s": 0,
            "max_burst_duration_ms_1s": 0,
            "buy_burst_notional_1s": 0.0,
            "sell_burst_notional_1s": 0.0,
            "burst_imbalance_ratio_1s": 0.0,
            "largest_burst_share_notional_1s": 0.0,
            "same_price_burst_count_1s": 0,
            "multilevel_burst_count_1s": 0,
            "burst_notional_vs_30s_traded_notional": 0.0,
            "burst_notional_vs_top_depth": None,
            "burst_mid_move_bps_1s": 0.0,
            "same_price_burst_max_len_1s": 0,
            "same_price_burst_notional_1s": 0.0,
            "multilevel_burst_max_span_ticks_1s": 0.0,
            "multilevel_burst_max_span_bps_1s": 0.0,
            "multilevel_burst_notional_1s": 0.0,
            "same_price_absorption_ratio_1s": 0.0,
            "burst_delta_notional_1s": 0.0,
            "outlier_trade_flag_1s": 0,
            "book_mid_price": None,
            "book_spread_bps": None,
            "book_bid_depth_100": None,
            "book_ask_depth_100": None,
            "book_bid_depth_1000": None,
            "book_ask_depth_1000": None,
            "book_imbalance_100": None,
            "book_imbalance_1000": None,
            "book_microprice": None,
            "trade_count_1s": 0,
            "traded_notional_1s": 0.0,
            "signed_volume_1s": 0.0,
            "trade_imbalance_qty_1s": 0.0,
            "realized_vol_10s": None,
            "realized_vol_60s": None,
            # P1 features
            "ofi_1s": 0.0,
            "spread_delta_1s": None,
            "depth_delta_1s": None,
            "depth_delta_30s": None,
            "imbalance_delta_1s": None,
            "bid_add_qty_1s": 0.0,
            "bid_cancel_qty_1s": 0.0,
            "ask_add_qty_1s": 0.0,
            "ask_cancel_qty_1s": 0.0,
            "replenishment_qty_1s": 0.0,
            "pulling_qty_1s": 0.0,
        }

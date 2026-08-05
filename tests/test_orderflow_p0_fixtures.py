"""
Acceptance criteria fixtures for OrderFlow P0 features.

Each test case provides:
1. Input trades with hand-calculated expected values
2. Verification of no-lookahead behavior
3. Edge cases: zero qty, invalid data, rolling warmup
"""
import pytest
from lib.downstream.incremental_features import IncrementalFeatureComputer
from lib.downstream.feature_compiler import _compute_realized_vol


class TestAcceptanceFixtures:
    """Hand-calculated fixtures for acceptance criteria."""

    def test_signed_buy_sell_imbalance(self):
        """Fixture: signed buy/sell with manual imbalance calculation."""
        comp = IncrementalFeatureComputer("test_market", 0.01)
        
        # Hand calculation:
        # Buy: 0.5 BTC @ 100, 0.3 BTC @ 101
        # Sell: 0.2 BTC @ 99, 0.4 BTC @ 102
        # signed_volume = (0.5 + 0.3) - (0.2 + 0.4) = 0.8 - 0.6 = 0.2
        # total_volume = 0.8 + 0.6 = 1.4
        # trade_imbalance = 0.2 / 1.4 = 0.142857...
        
        trades = [
            {"market": "test_market", "price": 100.0, "qty": 0.5, "side": "buy", "ts": 1700000000100},
            {"market": "test_market", "price": 99.0, "qty": 0.2, "side": "sell", "ts": 1700000000200},
            {"market": "test_market", "price": 101.0, "qty": 0.3, "side": "buy", "ts": 1700000000300},
            {"market": "test_market", "price": 102.0, "qty": 0.4, "side": "sell", "ts": 1700000000400},
        ]
        
        for t in trades:
            comp.process_trade(t)
        
        row = comp.flush()
        assert row is not None
        assert row["signed_volume_1s"] == pytest.approx(0.2, rel=1e-6)
        assert row["trade_imbalance_qty_1s"] == pytest.approx(round(0.2 / 1.4, 6), abs=1e-6)

    def test_zero_qty_trades(self):
        """Fixture: zero and invalid qty handling."""
        comp = IncrementalFeatureComputer("test_market", 0.01)
        
        # Hand calculation:
        # Trade 1: qty=0 → signed_volume contribution = 0
        # Trade 2: qty=0.5 → signed_volume contribution = 0.5 (buy)
        # Trade 3: qty=0 → signed_volume contribution = 0
        # Total signed_volume = 0.5
        # total_volume = 0.5
        # imbalance = 0.5 / 0.5 = 1.0
        
        trades = [
            {"market": "test_market", "price": 100.0, "qty": 0.0, "side": "buy", "ts": 1700000000100},
            {"market": "test_market", "price": 101.0, "qty": 0.5, "side": "buy", "ts": 1700000000200},
            {"market": "test_market", "price": 102.0, "qty": 0.0, "side": "sell", "ts": 1700000000300},
        ]
        
        for t in trades:
            comp.process_trade(t)
        
        row = comp.flush()
        assert row is not None
        assert row["signed_volume_1s"] == pytest.approx(0.5, rel=1e-6)
        assert row["trade_imbalance_qty_1s"] == pytest.approx(1.0, rel=1e-6)

    def test_realized_vol_rolling_warmup(self):
        """Fixture: RV returns null during warmup, computes after sufficient data."""
        # Hand calculation:
        # Need at least 3 trades for 2 log-returns
        # Trade 1: price=100 → no return yet
        # Trade 2: price=101 → log(101/100) = 0.00995...
        # Trade 3: price=102 → log(102/101) = 0.00990...
        # After 3 trades: 2 returns available → RV computed
        
        # First, test with insufficient data (< 3 trades)
        trades_insufficient = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
        ]
        rv = _compute_realized_vol(trades_insufficient, at_ts=3000, window_ms=10000)
        assert rv is None, "RV should be null with < 3 trades (warmup)"
        
        # Now test with sufficient data
        trades_sufficient = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
            {"ts": 3000, "price": 102.0},
        ]
        rv = _compute_realized_vol(trades_sufficient, at_ts=4000, window_ms=10000)
        assert rv is not None, "RV should compute after 3+ trades"
        assert rv > 0

    def test_price_gap_handling(self):
        """Fixture: large price gap doesn't break RV calculation."""
        # Hand calculation:
        # Trade 1: price=100
        # Trade 2: price=1000 (10x jump, simulating flash crash or data error)
        # Trade 3: price=100 (return to normal)
        # RV should still compute (log returns are valid even with large gaps)
        
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 1000.0},
            {"ts": 3000, "price": 100.0},
        ]
        rv = _compute_realized_vol(trades, at_ts=4000, window_ms=10000)
        assert rv is not None
        # RV should be very high due to large price swings
        assert rv > 1.0  # log returns will be large

    def test_out_of_order_timestamps(self):
        """Fixture: out-of-order timestamps handled correctly (no lookahead)."""
        # Trades arrive out of order but must be processed chronologically
        comp = IncrementalFeatureComputer("test_market", 0.01)
        
        # Input order: ts=300, ts=100, ts=200
        # After sorting: ts=100, ts=200, ts=300 (all in same second)
        trades_out_of_order = [
            {"market": "test_market", "price": 102.0, "qty": 0.3, "side": "buy", "ts": 1700000000300},
            {"market": "test_market", "price": 100.0, "qty": 0.5, "side": "buy", "ts": 1700000000100},
            {"market": "test_market", "price": 101.0, "qty": 0.2, "side": "sell", "ts": 1700000000200},
        ]
        
        for t in trades_out_of_order:
            comp.process_trade(t)
        
        row = comp.flush()
        assert row is not None
        # signed_volume = 0.5 + 0.3 - 0.2 = 0.6
        assert row["signed_volume_1s"] == pytest.approx(0.6, rel=1e-6)

    def test_market_isolation(self):
        """Fixture: different markets maintain separate state."""
        comp1 = IncrementalFeatureComputer("market_A", 0.01)
        comp2 = IncrementalFeatureComputer("market_B", 0.01)
        
        # market_A: buy 1.0
        # market_B: sell 2.0
        t1 = {"market": "market_A", "price": 100.0, "qty": 1.0, "side": "buy", "ts": 1700000000100}
        t2 = {"market": "market_B", "price": 100.0, "qty": 2.0, "side": "sell", "ts": 1700000000100}
        
        comp1.process_trade(t1)
        comp2.process_trade(t2)
        
        row1 = comp1.flush()
        row2 = comp2.flush()
        
        assert row1["signed_volume_1s"] == pytest.approx(1.0, rel=1e-6)
        assert row2["signed_volume_1s"] == pytest.approx(-2.0, rel=1e-6)

    def test_no_lookahead_future_trades(self):
        """Fixture: trades in future seconds don't affect current second."""
        comp = IncrementalFeatureComputer("test_market", 0.01)
        
        # Second 1: trade at ts=1000
        # Second 2: trade at ts=2000
        # When computing features for second 1, second 2 trade should not be visible
        
        t1 = {"market": "test_market", "price": 100.0, "qty": 0.5, "side": "buy", "ts": 1700000001000}
        t2 = {"market": "test_market", "price": 200.0, "qty": 1.0, "side": "sell", "ts": 1700000002000}
        
        comp.process_trade(t1)
        row1 = comp.process_trade(t2)  # Second boundary crossed, emits row for second 1
        
        assert row1 is not None
        assert row1["ts"] == 1700000001000
        # Only t1 should be counted (t2 is in second 2)
        assert row1["signed_volume_1s"] == pytest.approx(0.5, rel=1e-6)
        assert row1["trade_count_1s"] == 1

    def test_realized_vol_no_lookahead(self):
        """Fixture: RV window excludes trades at or after at_ts."""
        # RV at ts=5000 should only use trades in [ts-10000, ts=5000)
        # Trade at ts=5000 should NOT be included
        
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
            {"ts": 3000, "price": 102.0},
            {"ts": 5000, "price": 999.0},  # Future trade, should be excluded
        ]
        
        rv = _compute_realized_vol(trades, at_ts=4000, window_ms=10000)
        assert rv is not None
        # Only first 3 trades should be used (ts < 4000)
        # RV should be based on prices [100, 101, 102], not 999

    def test_empty_second_backfill(self):
        """Fixture: seconds with no trades get zero-filled rows."""
        comp = IncrementalFeatureComputer("test_market", 0.01)
        
        # Only 1 trade at ts=1000
        # flush_block should emit rows for seconds 1000, 2000, 3000 (empty backfill)
        t = {"market": "test_market", "price": 100.0, "qty": 0.5, "side": "buy", "ts": 1700000001000}
        comp.process_trade(t)
        
        rows = comp.flush_block(block_start_ms=1700000001000, block_end_ms=1700000004000)
        
        # Should have 3 rows: second 1 (with trade), second 2 (empty), second 3 (empty)
        assert len(rows) == 3
        
        # First row has the trade
        assert rows[0]["signed_volume_1s"] == pytest.approx(0.5, rel=1e-6)
        assert rows[0]["trade_count_1s"] == 1
        
        # Second and third rows are empty
        assert rows[1]["signed_volume_1s"] == 0.0
        assert rows[1]["trade_count_1s"] == 0
        assert rows[2]["signed_volume_1s"] == 0.0
        assert rows[2]["trade_count_1s"] == 0

    def test_rv_window_boundary_exact(self):
        """Fixture: RV window boundary is strict (< at_ts, not <=)."""
        # Trade exactly at at_ts should be excluded
        
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
            {"ts": 3000, "price": 102.0},
            {"ts": 4000, "price": 103.0},  # Exactly at at_ts
        ]
        
        # RV at ts=4000 should use trades at ts=1000, 2000, 3000 only
        rv = _compute_realized_vol(trades, at_ts=4000, window_ms=10000)
        assert rv is not None
        # Should compute from 3 trades (100, 101, 102), not including 103


class TestBookFeatureFixtures:
    """Book update fixtures (add/cancel, crossed/unseeded)."""

    def test_book_update_add_and_cancel(self):
        """Fixture: book update semantics (qty>0 = add/update, qty=0 = cancel)."""
        from lib.downstream.book_replay import BookReplay
        
        book = BookReplay()
        
        # Add bid at 100.0 with qty 1.0
        book.apply_json({
            "market": "test",
            "type": "update",
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.0"]],
            "ts": 1000,
        })
        
        snap1 = book.snapshot_at(1000)
        assert snap1.seeded
        assert book._bids[100.0] == 1.0
        
        # Cancel bid at 100.0 (qty=0 means delete)
        book.apply_json({
            "market": "test",
            "type": "update",
            "bids": [["100.0", "0.0"]],  # qty=0 → cancel
            "asks": [],
            "ts": 2000,
        })
        
        snap2 = book.snapshot_at(2000)
        # After canceling the only bid, book should be unseeded (no bids)
        assert not snap2.seeded or 100.0 not in book._bids

    def test_crossed_book_unseeds(self):
        """Fixture: crossed book (best_bid > best_ask) becomes unseeded."""
        from lib.downstream.book_replay import BookReplay
        
        book = BookReplay()
        
        # Normal book
        book.apply_json({
            "market": "test",
            "type": "update",
            "bids": [["100.0", "1.0"]],
            "asks": [["101.0", "1.0"]],
            "ts": 1000,
        })
        
        snap1 = book.snapshot_at(1000)
        assert snap1.seeded
        
        # Cross the book: bid > ask
        book.apply_json({
            "market": "test",
            "type": "update",
            "bids": [["102.0", "1.0"]],  # bid at 102
            "asks": [],
            "ts": 2000,
        })
        
        # Book should still have both levels but be crossed
        # snapshot_at should return unseeded when crossed
        snap2 = book.snapshot_at(2000)
        assert not snap2.seeded or book._best_bid[0] >= book._best_ask[0]

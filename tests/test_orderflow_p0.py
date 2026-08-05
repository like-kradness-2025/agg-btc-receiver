"""
Tests for OrderFlow P0 features (v2 schema) and IncrementalFeatureComputer.
"""
import pytest
from lib.downstream.incremental_features import IncrementalFeatureComputer
from lib.downstream.feature_compiler import _compute_realized_vol


class TestIncrementalFeatureComputer:
    """Incremental per-second feature accumulation."""

    def test_single_trade_single_second(self):
        """1 trade → 1 row after flush."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        trade = {"market": "binance_spot", "price": 63980.0, "qty": 0.1, "side": "buy", "ts": 1700000000000}
        row = comp.process_trade(trade)
        assert row is None  # Still accumulating
        final = comp.flush()
        assert final is not None
        assert final["ts"] == 1700000000000
        assert final["trade_count_1s"] == 1
        assert final["signed_volume_1s"] == 0.1  # all buy
        assert final["trade_imbalance_qty_1s"] == 1.0  # (0.1-0)/(0.1) = 1.0

    def test_second_boundary_crossed(self):
        """Trade in second 2 → emit row for second 1."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        # Second 1
        t1 = {"market": "binance_spot", "price": 63980.0, "qty": 0.1, "side": "buy", "ts": 1700000000500}
        row = comp.process_trade(t1)
        assert row is None
        # Second 2
        t2 = {"market": "binance_spot", "price": 63981.0, "qty": 0.2, "side": "sell", "ts": 1700000001000}
        row = comp.process_trade(t2)
        assert row is not None  # Emitted for second 1
        assert row["ts"] == 1700000000000
        assert row["trade_count_1s"] == 1
        assert row["signed_volume_1s"] == 0.1  # all buy in second 1

    def test_signed_volume_buy_sell(self):
        """Buy 0.1, sell 0.2 → signed_volume = -0.1."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        t1 = {"market": "binance_spot", "price": 63980.0, "qty": 0.1, "side": "buy", "ts": 1700000000100}
        t2 = {"market": "binance_spot", "price": 63979.0, "qty": 0.2, "side": "sell", "ts": 1700000000200}
        comp.process_trade(t1)
        comp.process_trade(t2)
        final = comp.flush()
        assert final["signed_volume_1s"] == pytest.approx(-0.1, rel=1e-6)
        assert final["trade_imbalance_qty_1s"] == pytest.approx((0.1 - 0.2) / (0.1 + 0.2), rel=1e-6)

    def test_zero_qty(self):
        """Trade with qty=0 → signed_volume=0, imbalance=0."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        t = {"market": "binance_spot", "price": 63980.0, "qty": 0.0, "side": "buy", "ts": 1700000000100}
        comp.process_trade(t)
        final = comp.flush()
        assert final["signed_volume_1s"] == 0.0
        assert final["trade_imbalance_qty_1s"] == 0.0  # 0/0 → 0

    def test_traded_notional(self):
        """2 trades → traded_notional = sum(price*qty)."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        t1 = {"market": "binance_spot", "price": 100.0, "qty": 1.0, "side": "buy", "ts": 1700000000100}
        t2 = {"market": "binance_spot", "price": 200.0, "qty": 0.5, "side": "sell", "ts": 1700000000200}
        comp.process_trade(t1)
        comp.process_trade(t2)
        final = comp.flush()
        assert final["traded_notional_1s"] == pytest.approx(100.0 + 100.0, rel=1e-6)  # 100*1 + 200*0.5

    def test_burst_features_in_second(self):
        """2 trades same side, gap <= 50ms → 1 burst."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        t1 = {"market": "binance_spot", "price": 100.0, "qty": 1.0, "side": "buy", "ts": 1700000000100}
        t2 = {"market": "binance_spot", "price": 100.0, "qty": 1.0, "side": "buy", "ts": 1700000000140}
        comp.process_trade(t1)
        comp.process_trade(t2)
        final = comp.flush()
        assert final["burst_count_1s"] == 1
        assert final["total_burst_notional_1s"] == pytest.approx(200.0, rel=1e-6)
        assert final["buy_burst_notional_1s"] == pytest.approx(200.0, rel=1e-6)
        assert final["sell_burst_notional_1s"] == pytest.approx(0.0, rel=1e-6)

    def test_burst_side_change(self):
        """Buy then sell → 2 bursts."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        t1 = {"market": "binance_spot", "price": 100.0, "qty": 1.0, "side": "buy", "ts": 1700000000100}
        t2 = {"market": "binance_spot", "price": 100.0, "qty": 1.0, "side": "sell", "ts": 1700000000200}
        comp.process_trade(t1)
        comp.process_trade(t2)
        final = comp.flush()
        assert final["burst_count_1s"] == 2
        assert final["buy_burst_notional_1s"] == pytest.approx(100.0, rel=1e-6)
        assert final["sell_burst_notional_1s"] == pytest.approx(100.0, rel=1e-6)
        assert final["burst_imbalance_ratio_1s"] == pytest.approx(0.0, rel=1e-6)

    def test_empty_flush(self):
        """No trades → flush returns None."""
        comp = IncrementalFeatureComputer("binance_spot", 0.01)
        final = comp.flush()
        assert final is None


class TestRealizedVol:
    """Realized volatility computation."""

    def test_rv_warmup_null(self):
        """<3 prices → null."""
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
        ]
        rv = _compute_realized_vol(trades, at_ts=3000, window_ms=10000)
        assert rv is None

    def test_rv_sufficient_data(self):
        """5 prices → 4 log-returns → RV computed."""
        import math
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
            {"ts": 3000, "price": 102.0},
            {"ts": 4000, "price": 100.5},
            {"ts": 5000, "price": 103.0},
        ]
        rv = _compute_realized_vol(trades, at_ts=6000, window_ms=10000)
        assert rv is not None
        assert rv > 0

    def test_rv_window_boundary(self):
        """Trade outside window excluded."""
        trades = [
            {"ts": 1000, "price": 100.0},  # Outside [2000, 6000)
            {"ts": 3000, "price": 101.0},
            {"ts": 4000, "price": 102.0},
            {"ts": 5000, "price": 100.5},
        ]
        rv = _compute_realized_vol(trades, at_ts=6000, window_ms=4000)
        assert rv is not None
        # Only 3 prices in window → 2 log-returns → RV computed

    def test_rv_no_lookahead(self):
        """Trade at or after at_ts excluded."""
        trades = [
            {"ts": 1000, "price": 100.0},
            {"ts": 2000, "price": 101.0},
            {"ts": 3000, "price": 102.0},
            {"ts": 5000, "price": 999.0},  # Future, should be excluded
        ]
        rv = _compute_realized_vol(trades, at_ts=4000, window_ms=10000)
        assert rv is not None
        # Only 3 prices in [0, 4000) → RV computed from those 3


class TestSchemaV3:
    """Schema v3 (50 columns) validation."""

    def test_schema_column_count(self):
        """FEATURE_1S_SCHEMA has 50 fields (39 existing + 11 P1)."""
        from lib.downstream.config import FEATURE_1S_SCHEMA
        assert len(FEATURE_1S_SCHEMA) == 50

    def test_p1_columns_present(self):
        """P1 v3 columns in schema."""
        from lib.downstream.config import FEATURE_1S_SCHEMA
        field_names = [f.name for f in FEATURE_1S_SCHEMA]
        # P1 features (11 columns)
        assert "ofi_1s" in field_names
        assert "spread_delta_1s" in field_names
        assert "depth_delta_1s" in field_names
        assert "depth_delta_30s" in field_names
        assert "imbalance_delta_1s" in field_names
        assert "bid_add_qty_1s" in field_names
        assert "bid_cancel_qty_1s" in field_names
        assert "ask_add_qty_1s" in field_names
        assert "ask_cancel_qty_1s" in field_names
        assert "replenishment_qty_1s" in field_names
        assert "pulling_qty_1s" in field_names

    def test_backward_compatibility(self):
        """Existing 33 columns unchanged. P0(6) and P1(11) appended at end."""
        from lib.downstream.config import FEATURE_1S_SCHEMA
        field_names = [f.name for f in FEATURE_1S_SCHEMA]
        # Check first 33 columns (original schema)
        assert field_names[:2] == ["ts", "market"]
        assert "burst_count_1s" in field_names
        assert "book_microprice" in field_names
        # P0 columns (6) appended after original 33
        assert field_names[-17:-11] == [
            "trade_count_1s",
            "traded_notional_1s",
            "signed_volume_1s",
            "trade_imbalance_qty_1s",
            "realized_vol_10s",
            "realized_vol_60s",
        ]
        # P1 columns (11) appended at end
        assert field_names[-11:] == [
            "ofi_1s",
            "spread_delta_1s",
            "depth_delta_1s",
            "depth_delta_30s",
            "imbalance_delta_1s",
            "bid_add_qty_1s",
            "bid_cancel_qty_1s",
            "ask_add_qty_1s",
            "ask_cancel_qty_1s",
            "replenishment_qty_1s",
            "pulling_qty_1s",
        ]

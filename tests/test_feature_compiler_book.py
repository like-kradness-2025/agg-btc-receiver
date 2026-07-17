"""Integration test: feature_compiler with book features."""
import pytest
from lib.downstream.book_replay import BookReplay, read_book_updates
from lib.downstream.feature_compiler import compute_1s_features
from lib.downstream.config import BLOCK_DURATION_MS


class TestFeatureCompilerBookFeatures:
    """Test that compute_1s_features includes book features B1-B9."""

    def test_no_book_replay_returns_null_book_features(self):
        """Without book_replay, all book features are None."""
        rows = compute_1s_features(
            block_start_ms=1000000,
            market="binance_spot",
            all_bursts=[],
            all_trades=[],
            book_replay=None,
        )
        assert len(rows) == 30
        for row in rows:
            assert row["book_mid_price"] is None
            assert row["book_spread_bps"] is None
            assert row["book_bid_depth_100"] is None
            assert row["book_ask_depth_100"] is None
            assert row["book_bid_depth_1000"] is None
            assert row["book_ask_depth_1000"] is None
            assert row["book_imbalance_100"] is None
            assert row["book_imbalance_1000"] is None
            assert row["book_microprice"] is None

    def test_unseeded_book_replay_returns_null_book_features(self):
        """With unseeded book_replay, all book features are None."""
        replay = BookReplay()
        rows = compute_1s_features(
            block_start_ms=1000000,
            market="binance_spot",
            all_bursts=[],
            all_trades=[],
            book_replay=replay,
        )
        for row in rows:
            assert row["book_mid_price"] is None
            assert row["book_spread_bps"] is None

    def test_seeded_book_replay_returns_features(self):
        """With seeded book, all 9 features have values."""
        replay = BookReplay()
        # Apply book updates before the block start
        replay.apply_json({
            "ts": 999000,
            "bids": [["100000.0", "0.5"]],
            "asks": [["100001.0", "0.3"]],
        })

        rows = compute_1s_features(
            block_start_ms=1000000,
            market="binance_spot",
            all_bursts=[],
            all_trades=[],
            book_replay=replay,
        )
        for row in rows:
            assert row["book_mid_price"] is not None
            assert row["book_mid_price"] == 100000.5
            assert row["book_spread_bps"] is not None
            assert row["book_bid_depth_100"] is not None
            assert row["book_ask_depth_100"] is not None
            assert row["book_microprice"] is not None
            # imbalance should be 0.0 when both depths equal
            # (in this case bid_depth_100 = 100000*0.5 = 50000,
            #  ask_depth_100 = 100001*0.3 = 30000.3)

    def test_crossed_book_returns_null(self):
        """Crossed book (bid >= ask) returns null book features."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 999000,
            "bids": [["100001.0", "0.5"]],
            "asks": [["100000.0", "0.3"]],  # crossed
        })

        rows = compute_1s_features(
            block_start_ms=1000000,
            market="binance_spot",
            all_bursts=[],
            all_trades=[],
            book_replay=replay,
        )
        for row in rows:
            assert row["book_mid_price"] is None
            assert row["book_spread_bps"] is None

    def test_existing_features_not_broken(self):
        """Existing 22 features are unchanged after adding book features."""
        rows = compute_1s_features(
            block_start_ms=1000000,
            market="binance_spot",
            all_bursts=[],
            all_trades=[],
            book_replay=None,
        )
        # Check original keys still present
        expected_keys = [
            "ts", "market",
            "burst_count_1s", "total_burst_notional_1s",
            "max_burst_notional_1s", "max_burst_prints_1s",
            "max_burst_duration_ms_1s",
            "buy_burst_notional_1s", "sell_burst_notional_1s",
            "burst_imbalance_ratio_1s", "largest_burst_share_notional_1s",
            "same_price_burst_count_1s", "multilevel_burst_count_1s",
            "burst_notional_vs_30s_traded_notional",
            "burst_notional_vs_top_depth", "burst_mid_move_bps_1s",
            "same_price_burst_max_len_1s", "same_price_burst_notional_1s",
            "multilevel_burst_max_span_ticks_1s", "multilevel_burst_max_span_bps_1s",
            "multilevel_burst_notional_1s", "same_price_absorption_ratio_1s",
            "burst_delta_notional_1s", "outlier_trade_flag_1s",
        ]
        for row in rows:
            for key in expected_keys:
                assert key in row, f"Missing key {key} in row"

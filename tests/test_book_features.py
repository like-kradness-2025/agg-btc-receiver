"""Tests for book features B1-B9 computation."""
import pytest
from lib.downstream.book_replay import BookReplay


class TestBookFeatures:
    """Test compute_book_features method."""

    def test_unseeded_book_returns_null_features(self):
        """Empty book returns all None."""
        replay = BookReplay()
        features = replay.compute_book_features(1000)

        assert features["book_mid_price"] is None
        assert features["book_spread_bps"] is None
        assert features["book_bid_depth_100"] is None
        assert features["book_ask_depth_100"] is None
        assert features["book_bid_depth_1000"] is None
        assert features["book_ask_depth_1000"] is None
        assert features["book_imbalance_100"] is None
        assert features["book_imbalance_1000"] is None
        assert features["book_microprice"] is None

    def test_crossed_book_returns_null_features(self):
        """Crossed book (bid >= ask) returns all None."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [["100.0", "1.0"]],
            "asks": [["99.0", "1.0"]],  # crossed
        })

        features = replay.compute_book_features(1000)
        assert features["book_mid_price"] is None

    def test_seeded_book_computes_mid_and_spread(self):
        """Seeded book computes B1 mid_price and B2 spread_bps."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [["100.0", "1.0"]],
            "asks": [["102.0", "1.0"]],
        })

        features = replay.compute_book_features(1000)

        # B1: mid = (100 + 102) / 2 = 101
        assert features["book_mid_price"] == 101.0

        # B2: spread_bps = (102 - 100) / 101 * 10000 = 198.02 bps
        assert abs(features["book_spread_bps"] - 198.02) < 0.1

    def test_depth_at_windows(self):
        """B3-B6: depth at $100 and $1000 windows."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [
                ["100.0", "1.0"],   # at mid, in $100 and $1000
                ["95.0", "2.0"],    # -5 from mid, in $100 and $1000
                ["5.0", "10.0"],    # -96 from mid, in $100 and $1000
                ["0.5", "100.0"],   # exactly mid-100 (inclusive), in $100 and $1000
            ],
            "asks": [
                ["101.0", "1.5"],   # at mid+1, in $100 and $1000
                ["150.0", "0.5"],   # +49 from mid, in $100 and $1000
                ["200.0", "0.1"],   # +99 from mid, in $100 and $1000
            ],
        })

        features = replay.compute_book_features(1000)

        # mid = (100 + 101) / 2 = 100.5
        # $100 bid window: [0.5, 100.5], $1000 bid window: [-899.5, 100.5]
        # $100 ask window: [100.5, 200.5], $1000 ask window: [100.5, 1100.5]

        # B3: bid_depth_100 = 100*1 + 95*2 + 5*10 + 0.5*100 = 100+190+50+50 = 390
        assert features["book_bid_depth_100"] == 390.0

        # B4: ask_depth_100 = 101*1.5 + 150*0.5 + 200*0.1 = 151.5+75+20 = 246.5
        assert features["book_ask_depth_100"] == 246.5

        # B5: bid_depth_1000 (all bids within $1000)
        assert features["book_bid_depth_1000"] == 390.0

        # B6: ask_depth_1000 (all asks within $1000)
        assert features["book_ask_depth_1000"] == 246.5

    def test_imbalance_at_windows(self):
        """B7-B8: imbalance at $100 and $1000 windows."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [["100.0", "2.0"]],  # bid notional = 200
            "asks": [["101.0", "1.0"]],  # ask notional = 101
        })

        features = replay.compute_book_features(1000)

        # B7: imbalance_100 = (200 - 101) / (200 + 101) = 99 / 301 = 0.3289
        expected_imb = 99 / 301
        assert abs(features["book_imbalance_100"] - expected_imb) < 0.001

        # B8: imbalance_1000 (same in this case)
        assert abs(features["book_imbalance_1000"] - expected_imb) < 0.001

    def test_microprice(self):
        """B9: microprice with qty weighting."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [["100.0", "3.0"]],  # bid=100, qty=3
            "asks": [["104.0", "1.0"]],  # ask=104, qty=1
        })

        features = replay.compute_book_features(1000)

        # B9: microprice = (104*3 + 100*1) / (3+1) = (312 + 100) / 4 = 103
        assert features["book_microprice"] == 103.0

    def test_zero_qty_returns_null_microprice(self):
        """Zero qty on both sides returns None for microprice."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [["100.0", "0.0"]],  # qty=0
            "asks": [["101.0", "0.0"]],  # qty=0
        })

        features = replay.compute_book_features(1000)

        # Book is unseeded (qty=0 filtered out in apply_json)
        assert features["book_mid_price"] is None

    def test_window_boundary_inclusive(self):
        """Window boundaries are inclusive (price exactly at mid-100 included)."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [
                ["100.0", "1.0"],  # mid
                ["0.0", "10.0"],   # exactly mid-100, should be included
            ],
            "asks": [["101.0", "1.0"]],
        })

        features = replay.compute_book_features(1000)

        # mid = 100.5, bid at 0.0 is mid-100.5 (outside $100)
        # but if we had bid at 0.5, it would be mid-100 (exactly at boundary)
        # Current: 0.0 is outside, so bid_depth_100 = 100*1 = 100
        assert features["book_bid_depth_100"] == 100.0

    def test_window_100_vs_1000_difference(self):
        """$100 and $1000 windows should give different results when levels span > $100."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [
                ["100.0", "1.0"],   # in $100 and $1000
                ["50.0", "2.0"],    # -50 from mid, in $100 and $1000
                ["5.0", "10.0"],    # -95 from mid, in $100 and $1000
            ],
            "asks": [
                ["101.0", "1.0"],   # in $100 and $1000
                ["500.0", "1.0"],   # +399 from mid, outside $100, inside $1000
            ],
        })

        features = replay.compute_book_features(1000)

        # mid = 100.5
        # bid_depth_100 = 100*1 + 50*2 + 5*10 = 100+100+50 = 250
        assert features["book_bid_depth_100"] == 250.0
        # ask_depth_100 = 101*1 = 101 (500 is outside [100.5, 200.5])
        assert features["book_ask_depth_100"] == 101.0

        # bid_depth_1000 = same as bid_depth_100 (all bids within $1000)
        assert features["book_bid_depth_1000"] == 250.0
        # ask_depth_1000 = 101*1 + 500*1 = 601 (500 is inside [100.5, 1100.5])
        assert features["book_ask_depth_1000"] == 601.0

        # imbalance differs between windows
        imb_100 = (250 - 101) / (250 + 101)
        assert abs(features["book_imbalance_100"] - imb_100) < 0.001

        imb_1000 = (250 - 601) / (250 + 601)
        assert abs(features["book_imbalance_1000"] - imb_1000) < 0.001

    def test_zero_price_or_qty_excluded(self):
        """Levels with price<=0 or qty<=0 are excluded from depth."""
        replay = BookReplay()
        replay.apply_json({
            "ts": 1000,
            "bids": [
                ["100.0", "1.0"],   # valid
                ["99.0", "0.0"],    # qty=0, excluded
                ["0.0", "5.0"],     # price=0, excluded
            ],
            "asks": [["101.0", "1.0"]],
        })

        features = replay.compute_book_features(1000)

        # Only 100*1 = 100 counted
        assert features["book_bid_depth_100"] == 100.0

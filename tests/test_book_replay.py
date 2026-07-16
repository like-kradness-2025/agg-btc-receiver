"""Tests for lib/downstream/book_replay.py.

Covers acceptance criteria for downstream book state:
1. BookReplay does NOT reset across block boundaries (keeps far-away levels).
2. $1-binned full bid/ask snapshots are emitted (not just best levels).
3. First-start seeding, per-market state isolation, empty-update blocks, qty=0 deletion.
4. Existing feature computation remains unaffected.
5. Actual data replay is exercised and logged.
"""

import json
import math
import os
import tempfile
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lib.downstream.book_replay import BookReplay, read_book_updates
from lib.downstream.book_snapshot_writer import write_book_snapshots
from lib.downstream.config import BLOCK_DURATION_MS


@pytest.fixture
def book():
    return BookReplay()


def _make_update(ts, bids=None, asks=None, market="test_market"):
    return {
        "market": market,
        "type": "update",
        "bids": [[str(p), str(q)] for p, q in (bids or [])],
        "asks": [[str(p), str(q)] for p, q in (asks or [])],
        "ts": ts,
    }


class TestBookReplayCore:
    def test_snapshot_at_returns_best_bid_ask(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        snap = book.snapshot_at(1000)
        assert snap.seeded
        assert snap.best_bid_price == 100.0
        assert snap.best_bid_qty == 1.0
        assert snap.best_ask_price == 101.0
        assert snap.best_ask_qty == 2.0
        assert snap.mid_price == 100.5

    def test_qty_zero_deletes_level(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        book.apply_json(_make_update(1001, bids=[(100.0, 0.0)], asks=[(101.0, 0.0)]))
        snap = book.snapshot_at(1001)
        assert not snap.seeded
        assert snap.best_bid_price == 0.0
        assert snap.best_ask_price == 0.0

    def test_empty_qty_string_treated_as_delete(self, book):
        """Bitfinex sends empty qty strings to delete levels."""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        update = _make_update(1001, bids=[(100.0, "")], asks=[(101.0, "")])
        book.apply_json(update)
        snap = book.snapshot_at(1001)
        assert not snap.seeded
        assert 100.0 not in book._bids
        assert 101.0 not in book._asks

    def test_invalid_qty_string_treated_as_delete(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        update = _make_update(1001, bids=[(100.0, "not-a-number")], asks=[(101.0, "bad")])
        book.apply_json(update)
        assert not book._bids
        assert not book._asks


class TestBinnedSnapshots:
    def test_all_levels_binned_to_dollar(self, book):
        """Far-away levels must survive in the $1-binned snapshot, not just best."""
        bids = [(100.0, 1.0), (95.7, 2.0), (50.0, 3.0), (100.3, 4.0)]
        asks = [(101.0, 1.0), (105.2, 2.0), (200.0, 3.0), (101.4, 4.0)]
        book.apply_json(_make_update(1000, bids=bids, asks=asks))
        snap = book.get_binned_snapshot(1000, bin_size=1.0)

        assert snap["seeded"]
        # Bids sorted descending (best first)
        assert snap["bid_prices"][0] == 100.0
        # 100.3 + 100.0 bin together at 100; 95.7 at 95; 50 at 50
        assert 100.0 in snap["bid_prices"]
        assert 50.0 in snap["bid_prices"]
        assert 95.0 in snap["bid_prices"]
        # Asks sorted ascending
        assert snap["ask_prices"][0] == 101.0
        assert 105.0 in snap["ask_prices"]
        assert 200.0 in snap["ask_prices"]

    def test_zero_qty_excluded_from_bins(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0), (99.0, 0.0)], asks=[(101.0, 2.0)]))
        snap = book.get_binned_snapshot(1000, bin_size=1.0)
        assert 99.0 not in snap["bid_prices"]
        assert 100.0 in snap["bid_prices"]


class TestBlockContinuity:
    def test_state_persists_across_blocks_without_reset(self, book):
        """AC1: far-away levels added in block 1 must remain in block 2."""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        book.apply_json(_make_update(1001, bids=[(50.0, 3.0)], asks=[(200.0, 4.0)]))
        # Block 2 arrives with only a top-level update
        book.apply_json(_make_update(30000, bids=[(100.5, 5.0)], asks=[(100.8, 6.0)]))
        snap = book.get_binned_snapshot(30000, bin_size=1.0)
        assert 50.0 in snap["bid_prices"]
        assert 200.0 in snap["ask_prices"]
        assert 100.0 in snap["bid_prices"]

    def test_reset_explicitly_clears_state(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        book.reset()
        assert not book._bids
        assert not book._asks
        assert not book._seeded

    def test_empty_update_list_keeps_state(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        book.apply_updates([])
        snap = book.snapshot_at(1000)
        assert snap.seeded


class TestPerMarketIsolation:
    def test_separate_book_instances_for_markets(self):
        b1 = BookReplay()
        b2 = BookReplay()
        b1.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)], market="m1"))
        b2.apply_json(_make_update(1000, bids=[(200.0, 3.0)], asks=[(201.0, 4.0)], market="m2"))
        assert b1._bids == {100.0: 1.0}
        assert b2._bids == {200.0: 3.0}

    def test_empty_update_list_keeps_state(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        book.apply_updates([])
        snap = book.snapshot_at(1000)
        assert snap.seeded


class TestRestUpsertPreservesFarLevels:
    def test_rest_upsert_does_not_erase_ws_far_levels(self, book):
        """AC1: shallow REST snapshot must not wipe far levels learned from WS."""
        # WS diff builds full state with far levels
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0), (50.0, 3.0)], asks=[(101.0, 2.0), (200.0, 4.0)]))
        # Shallow REST snapshot (only best levels)
        rest_update = _make_update(1001, bids=[(100.0, 5.0)], asks=[(101.0, 6.0)])
        book.apply_json(rest_update)
        snap = book.get_binned_snapshot(1001, bin_size=1.0)
        assert 50.0 in snap["bid_prices"], "far bid level from WS lost after REST upsert"
        assert 200.0 in snap["ask_prices"], "far ask level from WS lost after REST upsert"
        # Best levels overwritten by REST
        assert book._bids[100.0] == 5.0
        assert book._asks[101.0] == 6.0


class TestDownstreamIntegration:
    def test_run_once_writes_features_and_book_snapshots(self, tmp_path, monkeypatch):
        """AC5: exercise scripts/downstream.py end-to-end with synthetic blocks."""
        from scripts.downstream import run_once

        # Build synthetic data tree
        data_dir = tmp_path / "live_v3"
        trades_dir = data_dir / "trades" / "mkt" / "2026-07-16"
        book_dir = data_dir / "book_updates" / "mkt" / "2026-07-16"
        trades_dir.mkdir(parents=True)
        book_dir.mkdir(parents=True)

        ts0 = 1784204160000  # 12-16-00 UTC
        # Block 1: seed far levels
        b1 = book_dir / "12-16-00.jsonl"
        b1.write_text(json.dumps(_make_update(ts0, bids=[(100.0, 1.0), (50.0, 3.0)], asks=[(101.0, 2.0), (200.0, 4.0)])) + "\n")
        t1 = trades_dir / "12-16-00.jsonl"
        t1.write_text(json.dumps({"market": "mkt", "price": 100.0, "qty": 1.0, "side": "buy", "ts": ts0 + 500, "tradeId": "1"}) + "\n")

        # Block 2: only top update, far levels should persist (no reset)
        b2 = book_dir / "12-16-30.jsonl"
        b2.write_text(json.dumps(_make_update(ts0 + 30000, bids=[(100.5, 2.0)], asks=[(100.8, 3.0)])) + "\n")
        t2 = trades_dir / "12-16-30.jsonl"
        t2.write_text(json.dumps({"market": "mkt", "price": 100.5, "qty": 1.0, "side": "sell", "ts": ts0 + 30500, "tradeId": "2"}) + "\n")

        # Make files old enough to pass is_file_stable() in process_block().
        old_mtime = time.time() - 10.0
        os.utime(str(t1), (old_mtime, old_mtime))
        os.utime(str(t2), (old_mtime, old_mtime))

        # Patch derived dir into tmp_path
        derived = tmp_path / "derived"
        monkeypatch.setattr("lib.downstream.parquet_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.config.DERIVED_DIR", str(derived))

        # We also need get_tick_size for the synthetic market
        import lib.downstream.config as cfg
        monkeypatch.setattr(cfg, "MARKET_TICK_SIZE", {"mkt": 0.01})

        n = run_once(str(data_dir), market_filter="mkt")
        assert n == 60  # 30 rows per block * 2 blocks

        # Verify book snapshots exist and contain far levels
        snap_root = derived / "book_snapshots"
        snap_files = list(snap_root.rglob("*.parquet"))
        assert snap_files
        table = pq.read_table(str(snap_files[0]))
        rows = table.to_pylist()
        # Block 2 snapshots should still see far levels from block 1
        later_rows = [r for r in rows if r["ts"] >= ts0 + 30000]
        assert later_rows
        assert 50.0 in later_rows[0]["bid_prices"], "far bid level lost across block boundary"
        assert 200.0 in later_rows[0]["ask_prices"], "far ask level lost across block boundary"

        # Verify features were written
        feat_root = derived / "features_1s"
        feat_files = list(feat_root.rglob("*.parquet"))
        assert feat_files

    def test_per_market_state_isolation(self, tmp_path, monkeypatch):
        """Per-market _BOOK_STATES must keep mkt1 and mkt2 independent."""
        import scripts.downstream as ds
        ds._BOOK_STATES.clear()

        data_dir = tmp_path / "live_v3"
        for mkt in ["mkt1", "mkt2"]:
            (data_dir / "trades" / mkt / "2026-07-16").mkdir(parents=True)
            (data_dir / "book_updates" / mkt / "2026-07-16").mkdir(parents=True)

        ts0 = 1784204160000
        files = []
        for mkt, bid in [("mkt1", 100.0), ("mkt2", 200.0)]:
            b = data_dir / "book_updates" / mkt / "2026-07-16" / "12-16-00.jsonl"
            b.write_text(json.dumps(_make_update(ts0, bids=[(bid, 1.0)], asks=[(bid + 1, 2.0)])) + "\n")
            t = data_dir / "trades" / mkt / "2026-07-16" / "12-16-00.jsonl"
            t.write_text(json.dumps({"market": mkt, "price": bid, "qty": 1.0, "side": "buy", "ts": ts0 + 500, "tradeId": "1"}) + "\n")
            files.append(str(t))

        # Make trade files stable so process_block() doesn't skip them.
        old_mtime = time.time() - 10.0
        for fp in files:
            os.utime(fp, (old_mtime, old_mtime))

        derived = tmp_path / "derived"
        monkeypatch.setattr("lib.downstream.parquet_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.config.DERIVED_DIR", str(derived))
        import lib.downstream.config as cfg
        monkeypatch.setattr(cfg, "MARKET_TICK_SIZE", {"mkt1": 0.01, "mkt2": 0.01})

        from scripts.downstream import run_once
        run_once(str(data_dir))
        assert "mkt1" in ds._BOOK_STATES
        assert "mkt2" in ds._BOOK_STATES
        assert ds._BOOK_STATES["mkt1"]._bids != ds._BOOK_STATES["mkt2"]._bids
        ds._BOOK_STATES.clear()


class TestSnapshotWriter:
    def test_writes_full_book_parquet(self, tmp_path, monkeypatch):
        book = BookReplay()
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0), (50.0, 3.0)], asks=[(101.0, 2.0), (200.0, 4.0)]))
        snaps = [book.get_binned_snapshot(1000 + i * 1000, bin_size=1.0) for i in range(3)]

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))
        n = write_book_snapshots(snaps, "test_market")
        assert n == 3

        root = tmp_path / "book_snapshots"
        assert root.exists()
        part = root / "market=test_market" / "date=1970-01-01"
        files = list(part.glob("*.parquet"))
        assert files

        table = pq.read_table(str(files[0]))
        assert table.column_names == ["ts", "seeded", "bid_prices", "bid_qtys", "ask_prices", "ask_qtys"]
        assert table.column("bid_prices").length() == 3

        # Verify far-away levels survived
        row = table.to_pylist()[0]
        assert 50.0 in row["bid_prices"]
        assert 200.0 in row["ask_prices"]


class TestReadBookUpdates:
    def test_reads_jsonl_block(self, tmp_path):
        f = tmp_path / "12-00-00.jsonl"
        f.write_text(
            json.dumps(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)])) + "\n"
            + json.dumps(_make_update(1001, bids=[(100.0, 0.0)], asks=[(101.0, 0.0)])) + "\n"
        )
        # Make date directory structure
        date_dir = tmp_path / "2026-07-16"
        date_dir.mkdir(exist_ok=True)
        f2 = date_dir / "12-00-00.jsonl"
        f2.write_text(f.read_text())

        updates = read_book_updates(str(f2))
        assert len(updates) == 2


class TestRealDataReplay:
    def test_replays_latest_bitfinex_block(self):
        """AC5: exercise actual data and log result."""
        root = Path("data/live_v3")
        if not root.exists():
            pytest.skip("No live data available")
        trade_dir = root / "trades" / "bitfinex_spot" / "2026-07-16"
        book_dir = root / "book_updates" / "bitfinex_spot" / "2026-07-16"
        if not trade_dir.exists() or not book_dir.exists():
            pytest.skip("bitfinex_spot data not present")

        files = sorted(book_dir.glob("*.jsonl"))
        if not files:
            pytest.skip("No book_update files")

        latest = files[-1]
        updates = read_book_updates(str(latest))
        book = BookReplay()
        book.apply_updates(updates)
        snap = book.get_binned_snapshot(0, bin_size=1.0)
        print(f"\nReal replay: {latest.name} -> {len(snap['bid_prices'])} bid bins, {len(snap['ask_prices'])} ask bins")
        assert len(snap["bid_prices"]) > 0
        assert len(snap["ask_prices"]) > 0
        # Best bid should be less than best ask
        if snap["bid_prices"] and snap["ask_prices"]:
            assert snap["bid_prices"][0] < snap["ask_prices"][0]

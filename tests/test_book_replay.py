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
        """Far-away levels within ±$10k of fair price must survive in the $1-binned snapshot."""
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

    def test_price_filter_excludes_levels_beyond_10000(self, book):
        """Levels more than $10,000 away from fair price must be dropped."""
        fair = 100_000.0
        bids = [(99_900.0, 1.0), (90_000.0, 2.0), (90_000.0, 0.0)]
        asks = [(100_100.0, 1.0), (110_500.0, 2.0)]
        book.apply_json(_make_update(1000, bids=bids, asks=asks))
        snap = book.get_binned_snapshot(1000, bin_size=1.0)
        assert 90_000.0 not in snap["bid_prices"], "level > $10k below fair must be filtered"
        assert 99_900.0 in snap["bid_prices"]
        assert 110_500.0 not in snap["ask_prices"], "level > $10k above fair must be filtered"
        assert 100_100.0 in snap["ask_prices"]

    def test_price_filter_empty_when_no_fair_price(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[]))
        snap = book.get_binned_snapshot(1000, bin_size=1.0)
        assert not snap["seeded"]
        assert snap["bid_prices"] == []
        assert snap["ask_prices"] == []


class TestBestRecomputation:
    def test_best_recomputed_after_level_deletion(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0), (99.0, 2.0)], asks=[(101.0, 1.0), (102.0, 2.0)]))
        snap1 = book.snapshot_at(1000)
        assert snap1.best_bid_price == 100.0
        assert snap1.best_ask_price == 101.0
        # Delete best levels
        book.apply_json(_make_update(1001, bids=[(100.0, 0.0)], asks=[(101.0, 0.0)]))
        snap2 = book.snapshot_at(1001)
        assert snap2.best_bid_price == 99.0
        assert snap2.best_ask_price == 102.0
        assert snap2.mid_price == (99.0 + 102.0) / 2.0

    def test_crossed_book_unseeds(self, book):
        book.apply_json(_make_update(1000, bids=[(101.0, 1.0)], asks=[(100.0, 1.0)]))
        assert not book._seeded
        snap = book.snapshot_at(1000)
        assert not snap.seeded
        assert snap.mid_price == 0.0

    def test_crossed_book_after_updates_unseeds(self, book):
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
        assert book._seeded
        book.apply_json(_make_update(1001, bids=[(102.0, 1.0)], asks=[(99.0, 1.0)]))
        assert not book._seeded
        snap = book.snapshot_at(1001)
        assert not snap.seeded
        assert snap.mid_price == 0.0


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


# ── 残課題6点の追加テスト ───────────────────────────────────────────────────

class TestRestBookApiClarity:
    """課題1: fetch_rest_book force/allow_once API仕様を明確化し、矛盾した組み合わせを防止。"""

    def test_force_and_allow_once_raises_value_error(self, monkeypatch):
        """force=True と allow_once=True の両方を渡すとValueErrorが発生する。"""
        from lib.downstream.rest_book import fetch_rest_book, clear_rest_cache
        clear_rest_cache()
        # モック: config読み込みをスキップ
        monkeypatch.setattr("lib.downstream.rest_book._load_rest_config", lambda *a, **k: None)
        monkeypatch.setattr("lib.downstream.rest_book.REST_CONFIG", {})

        with pytest.raises(ValueError, match="mutually exclusive"):
            fetch_rest_book("test_market", force=True, allow_once=True)

    def test_force_only_bypasses_cache(self, monkeypatch):
        """force=True だけではキャッシュをバイパスしてフェッチする。"""
        from lib.downstream.rest_book import fetch_rest_book, clear_rest_cache, CACHE_TTL_SECONDS
        import lib.downstream.rest_book as rb
        clear_rest_cache()

        call_count = {"n": 0}

        def fake_urlopen(req, timeout=None):
            call_count["n"] += 1
            import io
            class FakeResp:
                def read(self):
                    return b'{"bids": [["100", "1"]], "asks": [["101", "1"]]}'
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return FakeResp()

        monkeypatch.setattr("lib.downstream.rest_book._load_rest_config", lambda *a, **k: None)
        monkeypatch.setattr("lib.downstream.rest_book.REST_CONFIG", {"test_mkt": ("http://x", rb._parse_binance)})
        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

        # allow_once=False, force=True → 毎回フェッチ
        r1 = fetch_rest_book("test_mkt", force=True, allow_once=False)
        r2 = fetch_rest_book("test_mkt", force=True, allow_once=False)
        assert call_count["n"] == 2
        assert r1 is not None
        assert r2 is not None
        clear_rest_cache()

    def test_allow_once_only_fetches_once(self, monkeypatch):
        """allow_once=True (デフォルト) では最初のフェッチのみ実行される。"""
        from lib.downstream.rest_book import fetch_rest_book, clear_rest_cache
        import lib.downstream.rest_book as rb
        clear_rest_cache()

        call_count = {"n": 0}

        def fake_urlopen(req, timeout=None):
            call_count["n"] += 1
            class FakeResp:
                def read(self):
                    return b'{"bids": [["100", "1"]], "asks": [["101", "1"]]}'
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            return FakeResp()

        monkeypatch.setattr("lib.downstream.rest_book._load_rest_config", lambda *a, **k: None)
        monkeypatch.setattr("lib.downstream.rest_book.REST_CONFIG", {"test_mkt": ("http://x", rb._parse_binance)})
        monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

        # allow_once=True (デフォルト) → 1回だけフェッチ
        r1 = fetch_rest_book("test_mkt", allow_once=True)
        r2 = fetch_rest_book("test_mkt", allow_once=True)
        r3 = fetch_rest_book("test_mkt", allow_once=True)
        assert call_count["n"] == 1
        assert r1 == r2 == r3
        clear_rest_cache()


class TestMaxPriceDistanceDocumentation:
    """課題2: MAX_PRICE_DISTANCE=10000 を設定値として明示し、単位・境界を文書化。"""

    def test_max_price_distance_is_10000_usd(self):
        """MAX_PRICE_DISTANCE が 10000.0 USD に設定されていることを確認。"""
        from lib.downstream.book_replay import MAX_PRICE_DISTANCE
        assert MAX_PRICE_DISTANCE == 10000.0

    def test_boundary_inclusive_at_exactly_10000(self, book):
        """境界: fair価格からちょうど±10000 USDのレベルは保持される(包括的)。"""
        fair = 100000.0
        bids = [(90000.0, 1.0), (89999.0, 1.0)]  # 90000はfair-10000=境界、89999は範囲外
        asks = [(110000.0, 1.0), (110001.0, 1.0)]  # 110000はfair+10000=境界、110001は範囲外
        book.apply_json(_make_update(1000, bids=bids, asks=asks))
        snap = book.get_binned_snapshot(1000, bin_size=1.0)
        assert snap["seeded"]
        assert 90000.0 in snap["bid_prices"], "境界(90000.0)は保持されるべき"
        assert 89999.0 not in snap["bid_prices"], "境界外(89999.0)は除外されるべき"
        assert 110000.0 in snap["ask_prices"], "境界(110000.0)は保持されるべき"
        assert 110001.0 not in snap["ask_prices"], "境界外(110001.0)は除外されるべき"


class TestParquetOverwriteOrIgnore:
    """課題3: Parquet overwrite_or_ignore の同一market/date複数batch書き込みを安全にする。"""

    def test_multiple_batches_no_duplicates_no_missing(self, tmp_path, monkeypatch):
        """同一market/dateに対して複数batchを書き込んだ場合、重複・欠落がないことを確認。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        ts0 = 1784204160000  # 2026-07-16 12:16:00 UTC
        date_str = "2026-07-16"

        # Batch 1: 3スナップショット
        batch1 = [
            {"ts": ts0 + i * 1000, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]}
            for i in range(3)
        ]
        n1 = write_book_snapshots(batch1, "test_mkt")
        assert n1 == 3

        # Batch 2: 別の3スナップショット
        batch2 = [
            {"ts": ts0 + (i + 3) * 1000, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]}
            for i in range(3)
        ]
        n2 = write_book_snapshots(batch2, "test_mkt")
        assert n2 == 3

        # 読み込んで全6行あることを確認(重複なし、欠落なし)
        root = tmp_path / "book_snapshots"
        files = list(root.rglob("*.parquet"))
        assert files, "parquetファイルが作成されている"

        table = pq.read_table(str(root))
        rows = table.to_pylist()
        assert len(rows) == 6, f"6行あるはず(actual: {len(rows)})"

        # 重複チェック: tsのセットが一意
        ts_values = [r["ts"] for r in rows]
        assert len(set(ts_values)) == 6, "tsに重複がない"

    def test_cross_midnight_partition(self, tmp_path, monkeypatch):
        """同一batchがUTC日を跨ぐ場合、各rowが自身のts由来のdate partitionに入る。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots, BOOK_SNAPSHOT_SCHEMA

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        # 2026-07-15 23:59:59 UTC と 2026-07-16 00:00:01 UTC
        ts_day1 = 1784159999000  # 2026-07-15 23:59:59
        ts_day2 = 1784160001000  # 2026-07-16 00:00:01

        batch = [
            {"ts": ts_day1, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
            {"ts": ts_day2, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
        ]
        n = write_book_snapshots(batch, "m")
        assert n == 2

        root = tmp_path / "book_snapshots"
        # 各date partitionに1行ずつ
        t1 = pq.read_table(str(root / "market=m" / "date=2026-07-15"))
        assert len(t1) == 1
        assert t1["ts"][0].as_py() == ts_day1

        t2 = pq.read_table(str(root / "market=m" / "date=2026-07-16"))
        assert len(t2) == 1
        assert t2["ts"][0].as_py() == ts_day2

        # schema保持
        full = pq.read_table(str(root))
        expected_cols = set(BOOK_SNAPSHOT_SCHEMA.names) | {"market", "date"}
        assert set(full.column_names) == expected_cols

    def test_cross_midnight_retry_and_distinct_batch(self, tmp_path, monkeypatch):
        """跨日batchのretryは重複せず、distinct batchは欠落しない。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        ts_day1 = 1784159999000
        ts_day2 = 1784160001000

        batch = [
            {"ts": ts_day1, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
            {"ts": ts_day2, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
        ]
        write_book_snapshots(batch, "m")

        # Retry same batch -> 0 rows (deduplicated)
        n_retry = write_book_snapshots(batch, "m")
        assert n_retry == 0

        # Distinct batch with overlapping ts_day1 and a new ts
        ts_day3 = 1784160005000  # 2026-07-16 00:00:05
        batch2 = [
            {"ts": ts_day1, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
            {"ts": ts_day3, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]},
        ]
        n2 = write_book_snapshots(batch2, "m")
        assert n2 == 1  # ts_day1 deduped, ts_day3 new

        root = tmp_path / "book_snapshots"
        full = pq.read_table(str(root))
        rows = full.to_pylist()
        ts_set = {r["ts"] for r in rows}
        assert ts_set == {ts_day1, ts_day2, ts_day3}

        # date partition整合
        for r in rows:
            from lib.downstream.parquet_writer import _date_from_ts
            assert r["date"] == _date_from_ts(r["ts"])

    def test_overwrite_or_ignore_does_not_corrupt_schema(self, tmp_path, monkeypatch):
        """overwrite_or_ignoreでスキーマが変わらないことを確認。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots, BOOK_SNAPSHOT_SCHEMA

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))
        ts0 = 1784204160000

        for batch_idx in range(3):
            batch = [
                {"ts": ts0 + batch_idx * 1000 + i, "seeded": True,
                 "bid_prices": [100.0 + batch_idx], "bid_qtys": [1.0],
                 "ask_prices": [101.0 + batch_idx], "ask_qtys": [2.0]}
                for i in range(2)
            ]
            write_book_snapshots(batch, "test_mkt")

        root = tmp_path / "book_snapshots"
        table = pq.read_table(str(root))
        # スキーマが一致することを確認
        expected_cols = set(BOOK_SNAPSHOT_SCHEMA.names) | {"market", "date"}
        assert set(table.column_names) == expected_cols


class TestUnseededPersistence:
    """課題4: seed_rest=False / REST失敗時のunseeded永続化を明確化。"""

    def test_ws_only_seed_succeeds(self, book):
        """WSだけでseed可能なmarket: bids+asksの両方があればseedされる。"""
        assert not book._seeded
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)]))
        assert book._seeded

    def test_ws_only_one_side_remains_unseeded(self, book):
        """WSで片側しか来ない場合、unseededのまま永続化する。"""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)]))
        assert not book._seeded
        # 追加のupdateが来ても、片側だけではunseededのまま
        book.apply_json(_make_update(2000, bids=[(99.0, 1.0)]))
        assert not book._seeded
        # スナップショットもunseeded
        snap = book.snapshot_at(2000)
        assert not snap.seeded
        assert snap.mid_price == 0.0

    def test_unseeded_state_persists_across_blocks(self, book):
        """REST失敗時: unseeded状態がブロック境界を越えて永続化する。"""
        # ブロック1: 片側だけ
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)]))
        assert not book._seeded
        # ブロック2: 別の片側だけ(asksが来ない)
        book.apply_json(_make_update(31000, bids=[(99.0, 1.0)]))
        assert not book._seeded
        # バイナティースナップショット
        snap = book.get_binned_snapshot(31000, bin_size=1.0)
        assert not snap["seeded"]
        assert snap["bid_prices"] == []
        assert snap["ask_prices"] == []

    def test_seed_rest_false_in_process_block(self, tmp_path, monkeypatch):
        """scripts/downstream.py の process_block(seed_rest=False) でRESTが呼ばれないことを確認。"""
        from scripts.downstream import process_block, _BOOK_STATES
        _BOOK_STATES.clear()

        data_dir = tmp_path / "live_v3"
        trades_dir = data_dir / "trades" / "mkt" / "2026-07-16"
        book_dir = data_dir / "book_updates" / "mkt" / "2026-07-16"
        trades_dir.mkdir(parents=True)
        book_dir.mkdir(parents=True)

        ts0 = 1784204160000
        b1 = book_dir / "12-16-00.jsonl"
        b1.write_text(json.dumps(_make_update(ts0, bids=[(100.0, 1.0)], asks=[(101.0, 2.0)])) + "\n")
        t1 = trades_dir / "12-16-00.jsonl"
        t1.write_text(json.dumps({"market": "mkt", "price": 100.0, "qty": 1.0, "side": "buy", "ts": ts0 + 500, "tradeId": "1"}) + "\n")

        old_mtime = time.time() - 10.0
        os.utime(str(t1), (old_mtime, old_mtime))

        derived = tmp_path / "derived"
        monkeypatch.setattr("lib.downstream.parquet_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(derived))
        monkeypatch.setattr("lib.downstream.config.DERIVED_DIR", str(derived))
        import lib.downstream.config as cfg
        monkeypatch.setattr(cfg, "MARKET_TICK_SIZE", {"mkt": 0.01})

        # RESTフェッチをモック: 呼ばれたら失敗
        call_count = {"n": 0}
        def fail_fetch(*a, **k):
            call_count["n"] += 1
            return None
        monkeypatch.setattr("lib.downstream.rest_book.fetch_rest_book", fail_fetch)

        n = process_block(str(t1), ts0, "mkt", 0.01, book_updates_path=str(b1), seed_rest=False)
        assert n == 30
        assert call_count["n"] == 0, "seed_rest=False では REST が呼ばれない"
        _BOOK_STATES.clear()


class TestFallbackScanCap:
    """課題5: fair window外のfallback O(n)走査に上限を追加。"""

    def test_max_fallback_scan_levels_constant(self):
        """MAX_FALLBACK_SCAN_LEVELS が 100000 に設定されていることを確認。"""
        from lib.downstream.book_replay import MAX_FALLBACK_SCAN_LEVELS
        assert MAX_FALLBACK_SCAN_LEVELS == 100_000

    def test_fallback_scan_respects_cap(self, book, monkeypatch):
        """走査が MAX_FALLBACK_SCAN_LEVELS で打ち切られることを確認。"""
        from lib.downstream.book_replay import MAX_FALLBACK_SCAN_LEVELS
        # 小さな上限でテスト
        monkeypatch.setattr("lib.downstream.book_replay.MAX_FALLBACK_SCAN_LEVELS", 3)

        # fair=100.5 に対して、window外のbidが5個、window内のbidが1個
        # 上限=3なので、先頭3個(50,49,48)しか見ない → 100.0 が見つからない
        bids = [(50.0, 1.0), (49.0, 1.0), (48.0, 1.0), (47.0, 1.0), (46.0, 1.0),
                (100.0, 1.0)]  # window内(6番目)
        asks = [(101.0, 1.0)]
        book.apply_json(_make_update(1000, bids=bids, asks=asks))

        # _best_bid_within_window は fair=100.5 から ±10000 の範囲で走査
        # 上限=3なので、先頭3個しか見ない → 100.0 が見つからない
        fair = book._fair_price()
        assert fair is not None
        result = book._best_bid_within_window(fair)
        # 上限に達した場合、100.0 が見つかる前に打ち切られる
        # dictのiteration順序は挿入順: 50, 49, 48, 47, 46, 100
        # 上限=3なので先頭3個=50,49,48 → これらは全てwindow内(fair±10000)
        # → best=50.0 が見つかる
        assert result == 50.0, "上限=3の場合、先頭3個(50,49,48)のbest=50.0が見つかる"

    def test_normal_book_within_cap(self, book):
        """通常のorderbook(< 1000 levels)は上限の影響を受けない。"""
        from lib.downstream.book_replay import MAX_FALLBACK_SCAN_LEVELS
        assert MAX_FALLBACK_SCAN_LEVELS >= 1000

        # 500レベルのbid/ask
        bids = [(100.0 - i * 0.1, 1.0) for i in range(500)]
        asks = [(101.0 + i * 0.1, 1.0) for i in range(500)]
        book.apply_json(_make_update(1000, bids=bids, asks=asks))

        snap = book.snapshot_at(1000)
        assert snap.seeded
        # best bid/askが正しく計算されている
        assert snap.best_bid_price == 100.0
        assert snap.best_ask_price == 101.0


class TestCrossedBookRecovery:
    """課題6: crossed book (best_bid >= best_ask) 検知後の復旧仕様を明確化。"""

    def test_crossed_book_unseeds_immediately(self, book):
        """crossed book検知後、即座にunseededになる。"""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
        assert book._seeded

        # crossed: bid > ask
        book.apply_json(_make_update(1001, bids=[(102.0, 1.0)], asks=[(99.0, 1.0)]))
        assert not book._seeded
        snap = book.snapshot_at(1001)
        assert not snap.seeded
        assert snap.mid_price == 0.0

    def test_crossed_book_recovers_on_normal_update(self, book):
        """crossed book後、正常なupdateとqty=0で古いレベルを削除して復旧する。"""
        # 初期状態
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
        assert book._seeded

        # crossed状態に
        book.apply_json(_make_update(1001, bids=[(102.0, 1.0)], asks=[(99.0, 1.0)]))
        assert not book._seeded

        # 復旧: crossedレベルをqty=0で削除し、正常なレベルを追加
        # asksからは101.0(初期)、99.0(crossed)を削除し、101.5を追加
        book.apply_json(_make_update(1002,
            bids=[(102.0, 0.0), (100.0, 0.0), (100.5, 1.0)],
            asks=[(99.0, 0.0), (101.0, 0.0), (101.5, 1.0)]))
        assert book._seeded
        snap = book.snapshot_at(1002)
        assert snap.seeded
        assert snap.best_bid_price == 100.5
        assert snap.best_ask_price == 101.5
        assert snap.mid_price == (100.5 + 101.5) / 2.0

    def test_crossed_book_recovers_after_multiple_bad_updates(self, book):
        """複数のcrossed update後でも、qty=0で古いレベルを削除して復旧する。"""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
        assert book._seeded

        # 複数のcrossed update
        book.apply_json(_make_update(1001, bids=[(103.0, 1.0)], asks=[(98.0, 1.0)]))
        assert not book._seeded
        book.apply_json(_make_update(1002, bids=[(104.0, 1.0)], asks=[(97.0, 1.0)]))
        assert not book._seeded

        # 復旧: crossedレベルをqty=0で削除し、正常なレベルを追加
        book.apply_json(_make_update(1003, bids=[(104.0, 0.0), (103.0, 0.0), (100.0, 0.0), (100.0, 1.0)],
                                      asks=[(97.0, 0.0), (98.0, 0.0), (101.0, 0.0), (101.0, 1.0)]))
        assert book._seeded
        snap = book.snapshot_at(1003)
        assert snap.seeded
        assert snap.best_bid_price == 100.0
        assert snap.best_ask_price == 101.0

    def test_crossed_book_via_qty_deletion_recovers(self, book):
        """qty=0によるレベル削除でunseededになり、その後復旧する。"""
        book.apply_json(_make_update(1000, bids=[(100.0, 1.0), (99.0, 1.0)],
                                      asks=[(101.0, 1.0), (102.0, 1.0)]))
        assert book._seeded
        assert book._best_bid[0] == 100.0

        # best bid を削除 → best が 99.0 に変わる
        book.apply_json(_make_update(1001, bids=[(100.0, 0.0)]))
        assert book._seeded
        assert book._best_bid[0] == 99.0

        # best ask を削除 → best が 102.0 に変わる
        book.apply_json(_make_update(1002, asks=[(101.0, 0.0)]))
        assert book._seeded
        assert book._best_ask[0] == 102.0

        # 99.0 と 102.0 はまだ seeded (99.0 < 102.0)
        snap = book.snapshot_at(1002)
        assert snap.seeded
        assert snap.best_bid_price == 99.0
        assert snap.best_ask_price == 102.0

        # 復旧: 新しい正常なレベルを追加
        book.apply_json(_make_update(1003, bids=[(100.0, 1.0)], asks=[(101.0, 1.0)]))
        assert book._seeded
        snap = book.snapshot_at(1003)
        assert snap.seeded
        assert snap.best_bid_price == 100.0
        assert snap.best_ask_price == 101.0

    def test_crossed_book_snapshot_returns_unseeded(self, book):
        """crossed状態でのsnapshotはunseededを返す。"""
        book.apply_json(_make_update(1000, bids=[(105.0, 1.0)], asks=[(100.0, 1.0)]))
        assert not book._seeded
        snap = book.snapshot_at(1000)
        assert not snap.seeded
        assert snap.best_bid_price == 0.0
        assert snap.best_ask_price == 0.0
        assert snap.mid_price == 0.0

    def test_crossed_book_binned_snapshot_empty(self, book):
        """crossed状態でのbinned snapshotは空を返す。"""
        book.apply_json(_make_update(1000, bids=[(105.0, 1.0), (100.0, 1.0)],
                                      asks=[(100.0, 1.0), (110.0, 1.0)]))
        assert not book._seeded
        snap = book.get_binned_snapshot(1000, bin_size=1.0)
        assert not snap["seeded"]
        assert snap["bid_prices"] == []
        assert snap["ask_prices"] == []


class TestNoPartitionScan:
    """partition全体走査が発生しないことを検証。"""

    def test_read_existing_ts_not_called(self, tmp_path, monkeypatch):
        """_read_existing_tsが呼ばれないことを確認（partition走査ゼロ）。"""
        from lib.downstream import book_snapshot_writer
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        # _read_existing_tsが存在しないことを確認（削除済み）
        assert not hasattr(book_snapshot_writer, "_read_existing_ts")

        # 大量データを書いてもpq.read_*が呼ばれない
        read_count = {"n": 0}
        original_read = pq.read_table

        def counting_read(*args, **kwargs):
            read_count["n"] += 1
            return original_read(*args, **kwargs)

        monkeypatch.setattr(pq, "read_table", counting_read)

        # 10バッチ、各100行
        ts_base = 1784204160000
        for batch_idx in range(10):
            batch = [
                {"ts": ts_base + batch_idx * 100000 + i, "seeded": True,
                 "bid_prices": [100.0], "bid_qtys": [1.0],
                 "ask_prices": [101.0], "ask_qtys": [2.0]}
                for i in range(100)
            ]
            write_book_snapshots(batch, "test_mkt")

        # 書き込み時のreadは0回（存在チェックはfile statのみ）
        assert read_count["n"] == 0, f"pq.read_table が {read_count['n']} 回呼ばれた（期待: 0）"

    def test_batch_deterministic_filename(self, tmp_path, monkeypatch):
        """同じbatchは同じファイル名を生成する（retryで重複しない）。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        ts0 = 1784204160000
        batch = [
            {"ts": ts0 + i, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]}
            for i in range(5)
        ]

        # 1回目
        n1 = write_book_snapshots(batch, "mkt1")
        files1 = list((tmp_path / "book_snapshots").rglob("*.parquet"))
        assert len(files1) == 1

        # 2回目（同じbatch）
        n2 = write_book_snapshots(batch, "mkt1")
        files2 = list((tmp_path / "book_snapshots").rglob("*.parquet"))
        assert len(files2) == 1, "同じbatchは同じファイルに上書きされない"
        assert files1[0].name == files2[0].name, "ファイル名が同一"

        # 3回目（異なるbatch）
        batch2 = [{"ts": ts0 + 1000 + i, "seeded": True,
                   "bid_prices": [100.0], "bid_qtys": [1.0],
                   "ask_prices": [101.0], "ask_qtys": [2.0]}
                  for i in range(5)]
        n3 = write_book_snapshots(batch2, "mkt1")
        files3 = list((tmp_path / "book_snapshots").rglob("*.parquet"))
        assert len(files3) == 2, "異なるbatchは別ファイル"
        assert n1 == 5 and n2 == 0 and n3 == 5

    def test_memory_scales_with_batch_not_partition(self, tmp_path, monkeypatch):
        """メモリ使用量がpartitionサイズでなくbatchサイズに比例することを確認。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        # 事前に1000行のpartitionを作成
        ts_base = 1784204160000
        for i in range(10):
            batch = [
                {"ts": ts_base + i * 1000 + j, "seeded": True,
                 "bid_prices": [100.0], "bid_qtys": [1.0],
                 "ask_prices": [101.0], "ask_qtys": [2.0]}
                for j in range(100)
            ]
            write_book_snapshots(batch, "mkt")

        # 1行だけのbatchを書き込む
        small_batch = [{"ts": ts_base + 100000, "seeded": True,
                        "bid_prices": [100.0], "bid_qtys": [1.0],
                        "ask_prices": [101.0], "ask_qtys": [2.0]}]

        read_count = {"n": 0}
        original_read = pq.read_table

        def counting_read(*args, **kwargs):
            read_count["n"] += 1
            return original_read(*args, **kwargs)

        monkeypatch.setattr(pq, "read_table", counting_read)

        write_book_snapshots(small_batch, "mkt")

        # 1000行のpartitionがあっても、1行batchの書き込みでreadは0回
        assert read_count["n"] == 0, "partition走査が発生した"

    def test_multiple_markets_isolated(self, tmp_path, monkeypatch):
        """複数marketが独立して管理されることを確認。"""
        from lib.downstream.book_snapshot_writer import write_book_snapshots

        monkeypatch.setattr("lib.downstream.book_snapshot_writer.DERIVED_DIR", str(tmp_path))

        ts0 = 1784204160000
        batch_m1 = [
            {"ts": ts0 + i, "seeded": True,
             "bid_prices": [100.0], "bid_qtys": [1.0],
             "ask_prices": [101.0], "ask_qtys": [2.0]}
            for i in range(3)
        ]
        batch_m2 = [
            {"ts": ts0 + i, "seeded": True,
             "bid_prices": [200.0], "bid_qtys": [1.0],
             "ask_prices": [201.0], "ask_qtys": [2.0]}
            for i in range(3)
        ]

        n1 = write_book_snapshots(batch_m1, "market1")
        n2 = write_book_snapshots(batch_m2, "market2")

        assert n1 == 3 and n2 == 3

        # 各marketのファイル数
        files_m1 = list((tmp_path / "book_snapshots" / "market=market1").rglob("*.parquet"))
        files_m2 = list((tmp_path / "book_snapshots" / "market=market2").rglob("*.parquet"))
        assert len(files_m1) == 1 and len(files_m2) == 1

        # データが独立していることを確認
        import pyarrow.parquet as pq
        t1 = pq.read_table(str(files_m1[0]))
        t2 = pq.read_table(str(files_m2[0]))
        assert t1["bid_prices"][0].as_py() == [100.0]
        assert t2["bid_prices"][0].as_py() == [200.0]

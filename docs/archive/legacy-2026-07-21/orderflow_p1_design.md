# OrderFlow P1 設計書: OFI・板変化フロー特徴量

> 作成日: 2026-07-18
> 作成者: coder (t_fea6d367)
> 背景: 前回run 363で設計を飛ばして実装し、責任分離違反で失敗。本設計書で先に仕様を固める。

## 1. 問題定義（前回失敗の教訓）

### 前回の実装（run 363, 18:50 ブロック）
- BookReplay.apply_json() 内で直接P1 accumulatorを計算
- `_p1_ofi`, `_p1_bid_add_qty`, `_p1_replenishment_qty` 等をBookReplay内に保持
- 1秒境界検出時に `_p1_snapshots[second_ts]` へ保存

### なぜ失敗したか
- **責任分離違反**: BookReplayは「板状態のreplay」責務、IncrementalFeatureComputerは「1秒集約」責務。BookReplayにP1集約ロジックを混ぜると責務が混乱
- **設計方針**: BookReplayはchange eventを記録するだけ、IncrementalFeatureComputerが秒境界で集約

### 正しい設計
- BookReplay: 各updateのchange eventを `_p1_change_events` リストに記録
- `collect_p1_features(second_ts)` メソッドで、IncrementalFeatureComputerが秒境界で集約
- BookReplayはchange eventの記録に徹する

## 2. 責任分離

### BookReplay（lib/downstream/book_replay.py）
- **責務**: book_updates JSONLを適用し、板状態をreplayする
- **責務外**: P1特徴量の集約（IncrementalFeatureComputerに委任）
- **P1関連責務**: change eventを記録する（OFI, add/cancel, replenishment/pulling用のraw data）

### IncrementalFeatureComputer（lib/downstream/incremental_features.py）
- **責務**: tradesを逐次処理し、1秒境界でfeatures_1sを生成
- **P1追加責務**: book_replayからchange eventsを集約し、P1列を生成
- **依存**: `book_replay: Optional[BookReplay]` を保持

### feature_compiler（lib/downstream/feature_compiler.py）
- **責務**: burst-based featuresの計算（P1非依存）
- **P1非依存**: 既存ロジック維持、P1列はIncrementalFeatureComputer側で追加

## 3. P1 特徴量の定義

### 優先候補（spec.md Phase 1棚卸しに基づく）

| 特徴量 | 列名 | 定義 | 単位 | null semantics | Exchange依存 |
|---|---|---|---|---|---|
| OFI (Order Flow Imbalance) | `ofi_1s` | ΔQ_bid * I(ΔP_bid >= 0) - ΔQ_ask * I(ΔP_ask <= 0) | qty | 0 (seeded=false) | book_updates best price/qty change |
| spread delta | `spread_delta_1s` | spread_bps(t) - spread_bps(t-1) | bps | null (t-1なし) | book_updates best bid/ask |
| depth delta 1s | `depth_delta_1s` | top_depth(t) - top_depth(t-1) | USD | null (t-1なし) | book_updates best bid/ask |
| depth delta 30s | `depth_delta_30s` | top_depth(t) - top_depth(t-30s) | USD | null (t-30sなし) | book_updates best bid/ask |
| imbalance delta | `imbalance_delta_1s` | imbalance(t) - imbalance(t-1) | ratio | null (t-1なし) | book_updates depth $100 |
| bid add flow | `bid_add_qty_1s` | sum(delta_qty where qty increased) in 1s | qty | 0 | book_updates qty diff |
| bid cancel flow | `bid_cancel_qty_1s` | sum(-delta_qty where qty decreased) in 1s | qty | 0 | book_updates qty diff |
| ask add flow | `ask_add_qty_1s` | sum(delta_qty where qty increased) in 1s | qty | 0 | book_updates qty diff |
| ask cancel flow | `ask_cancel_qty_1s` | sum(-delta_qty where qty decreased) in 1s | qty | 0 | book_updates qty diff |
| replenishment | `replenishment_qty_1s` | sum(best_level_qty_increase) in 1s | qty | 0 | book_updates best level |
| pulling | `pulling_qty_1s` | sum(-best_level_qty_decrease) in 1s | qty | 0 | book_updates best level |

### Exchange capability matrix（raw payload確認済み）

| market | trades | book_updates | seq # | OFI適用 | add/cancel flow | replenishment |
|---|---|---|---|---|---|---|
| binance_spot | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| binance_perp | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| binance_perp_btcusdc | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| binance_spot_usdc | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| bybit_spot | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| bybit_perp | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| okx_spot | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| okx_perp | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| coinbase_spot | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| kraken_spot | ✓ | ✓ diff | ✓ | ✓ | ✓ | ✓ |
| crypto_com_spot | ✓ | ✓ diff | ✓ | partial | partial | partial |
| bitfinex_spot | ✓ | ✓ diff | ✓ | partial | partial | partial |
| hyperliquid_perp | ✓ | ✓ diff | ✓ | partial | partial | partial |
| bitmex_perp | ✓ | ✓ diff | ✓ | partial | partial | partial |
| bitstamp_spot | ✓ | ✓ diff | ✓ | trade_only | trade_only | trade_only |

**注**: book_updates のdiff semantics (qty=0で削除、qty>0でupsert) は全market共通。add/cancel識別はraw payloadから可能。ただしreplenishment（部分cancel）の定義はambiguous → P1ではbest levelのみ対象とする。

## 4. データ辞書（schema v2 → P1列追加）

### 既存schema（39列）
```
ts, market
burst_count_1s, total_burst_notional_1s, max_burst_notional_1s, max_burst_prints_1s, max_burst_duration_ms_1s
buy_burst_notional_1s, sell_burst_notional_1s, burst_imbalance_ratio_1s, largest_burst_share_notional_1s
same_price_burst_count_1s, multilevel_burst_count_1s
burst_notional_vs_30s_traded_notional, burst_notional_vs_top_depth, burst_mid_move_bps_1s
same_price_burst_max_len_1s, same_price_burst_notional_1s
multilevel_burst_max_span_ticks_1s, multilevel_burst_max_span_bps_1s, multilevel_burst_notional_1s
same_price_absorption_ratio_1s, burst_delta_notional_1s, outlier_trade_flag_1s
book_mid_price, book_spread_bps, book_bid_depth_100, book_ask_depth_100, book_bid_depth_1000, book_ask_depth_1000
book_imbalance_100, book_imbalance_1000, book_microprice
trade_count_1s, traded_notional_1s, signed_volume_1s, trade_imbalance_qty_1s
realized_vol_10s, realized_vol_60s
```

### P1追加列（11列、末尾にappend）
```
# OrderFlow P1 features (v3, book-dependent)
ofi_1s                    f64   NOT NULL   OFI = ΔQ_bid * I(ΔP_bid>=0) - ΔQ_ask * I(ΔP_ask<=0)
spread_delta_1s           f64   NULLABLE   spread_bps(t) - spread_bps(t-1), null=t-1なし
depth_delta_1s            f64   NULLABLE   top_depth(t) - top_depth(t-1), null=t-1なし
depth_delta_30s           f64   NULLABLE   top_depth(t) - top_depth(t-30s), null=t-30sなし
imbalance_delta_1s        f64   NULLABLE   imbalance_100(t) - imbalance_100(t-1), null=t-1なし
bid_add_qty_1s            f64   NOT NULL   sum(delta_qty where qty increased)
bid_cancel_qty_1s         f64   NOT NULL   sum(-delta_qty where qty decreased)
ask_add_qty_1s            f64   NOT NULL   sum(delta_qty where qty increased)
ask_cancel_qty_1s         f64   NOT NULL   sum(-delta_qty where qty decreased)
replenishment_qty_1s      f64   NOT NULL   sum(best_level_qty_increase)
pulling_qty_1s            f64   NOT NULL   sum(-best_level_qty_decrease)
```

### 後方互換性
- 既存39列はすべて維持（型変更なし）
- P1列11列を末尾に追加。PyArrow schemaは既存Parquetと互換（旧ファイルに新列は存在しないが、読み出し時にoptional fieldとして扱える）
- schema v3 = 39列 + 11列 = 50列

## 5. 設計詳細

### OFI (Order Flow Imbalance, Cont-style)
- 定義: `OFI = ΔQ_bid * I(ΔP_bid >= 0) - ΔQ_ask * I(ΔP_ask <= 0)`
- 各book_update apply時に:
  - `old_best_bid_price, old_best_bid_qty = book._best_bid`
  - update適用後: `new_best_bid_price, new_best_bid_qty = book._best_bid`
  - `ΔP_bid = new_best_bid_price - old_best_bid_price`
  - `ΔQ_bid = new_best_bid_qty - old_best_bid_qty`
  - `ofi_bid = ΔQ_bid if ΔP_bid >= 0 else 0`
  - 同様にask側
- 集約: IncrementalFeatureComputerが1秒内の全ofi_eventをsum

### spread/depth/imbalance delta
- 定義: `delta = feature(t) - feature(t-1)`
- BookReplayはchange eventを記録しない（snapshot_at(t)で都度計算）
- IncrementalFeatureComputerが1秒境界で:
  - `spread_t = book.compute_book_features(second_ts)["book_spread_bps"]`
  - `spread_prev = prev_second_spread[second_ts - 1000]` (cross-second state)
  - `spread_delta = spread_t - spread_prev`
- 30s delta: `depth_t - depth_{t-30s}` → rolling window state必要

### add/cancel flow
- 各book_update apply時に:
  - `bid_add_qty = sum(delta_qty for delta_qty > 0)`
  - `bid_cancel_qty = sum(-delta_qty for delta_qty < 0)`
  - 同様にask側
- BookReplayはchange eventとして `(bid_add_qty, bid_cancel_qty, ask_add_qty, ask_cancel_qty)` を記録
- IncrementalFeatureComputerが1秒内の全eventをsum

### replenishment/pulling
- 定義: best level (top-of-book) のquantity変化
- 各book_update apply時に:
  - `best_bid_qty_delta = new_best_bid_qty - old_best_bid_qty`
  - `replenishment = best_bid_qty_delta if best_bid_qty_delta > 0 else 0`
  - `pulling = -best_bid_qty_delta if best_bid_qty_delta < 0 else 0`
  - 同様にask側
- BookReplayはchange eventとして `(replenishment, pulling)` を記録
- IncrementalFeatureComputerが1秒内の全eventをsum

## 6. Lookahead防止

- `ofi_1s`: 1秒bucket `[ts, ts+1000)` のbook_updatesのみ
- `spread_delta_1s`: `[ts-1000, ts)` のsnapshotと `[ts, ts+1000)` のsnapshot
- `depth_delta_1s`: 同上
- `depth_delta_30s`: `[ts-30000, ts-29000)` のsnapshotと `[ts, ts+1000)` のsnapshot
- `imbalance_delta_1s`: 同上
- `bid/ask_add/cancel_qty`: 1秒bucketのbook_updatesのみ
- `replenishment/pulling`: 1秒bucketのbook_updatesのみ

## 7. 未実装・保留の明示

### 保留 P2
- queue depletion（best level消失時の次のlevelへの移行）
- trade intensity（prints/sec, interarrival mean/std）→ burst_prints_1sが近似
- order flow toxicity（VPIN系）

### データ不足時の処理
- OFI: book_unseeded → 0.0（spec.md指示）
- spread_delta: prevなし → null（推測/0埋めしない）
- depth_delta: prevなし → null
- imbalance_delta: prevなし → null
- add/cancel flow: book_updates存在しないmarket → 0.0
- replenishment/pulling: book_updates存在しないmarket → 0.0

## 8. 実装ステップ

1. BookReplayに `_p1_change_events: List[Dict]` 追加
2. `apply_json` 内でchange eventを記録（OFI, add/cancel, replenishment/pulling）
3. `collect_p1_features(second_ts)` メソッド追加（IncrementalFeatureComputerから呼ばれる）
4. IncrementalFeatureComputerに `book_replay: Optional[BookReplay]` 追加
5. `_compute_features_for_second` 内で `book_replay.collect_p1_features(second_ts)` を呼ぶ
6. cross-second state追加（prev_second_spread, prev_second_depth, prev_second_imbalance）
7. config.pyにP1列11列append（schema v3, 50列）
8. test追加（BookReplay change events, IncrementalFeatureComputer P1集約）
9. 15market real smoke（null rates, CPU/RAM）
10. Parquet dedup確認

## 9. 既存watch・REST semantics保持

- `_BOOK_STATES: Dict[str, BookReplay]` はper-marketで継続（block間でresetしない）
- REST upsert: WS seeded後はskip（downstream.py:141の既存ロジック維持）
- `_FEATURE_COMPUTERS: Dict[str, IncrementalFeatureComputer]` はper-marketで継続（RV state保持）
- P1追加で既存watchと競合しない（process_block内の既存ロジック維持）

## 10. 検証手順

1. `pytest tests/test_book_replay_p1.py` - BookReplay change events記録テスト
2. `pytest tests/test_incremental_p1.py` - IncrementalFeatureComputer P1集約テスト
3. `python3 scripts/downstream.py --market binance_spot --from-ms <ts>` - 1market real smoke
4. `python3 scripts/_check_p1_features.py` - 全15market null rates確認
5. `python3 -c "import pyarrow.parquet as pq; t=pq.read_table(...); print(t.schema)"` - schema v3確認

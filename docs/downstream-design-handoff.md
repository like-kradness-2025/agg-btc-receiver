# 後段集約パイプライン 設計引き継ぎ

## 1. 入力

Receiver が `data/live_v3/` に30秒ブロックで保存する全ファイル:
- `trades/<market>/<date>/<HH-MM-SS>.jsonl` — raw trades
- `agg_trades/<market>/<date>/<HH-MM-SS>.jsonl` — 1秒集約OHLCV
- `book_updates/<market>/<date>/<HH-MM-SS>.jsonl` — raw depth diff
- `snapshots/<market>/<date>/<HH-MM-SS>.jsonl` — full book snapshot (30sごと)
- `book_snapshots/<market>/<date>/<HH-MM-SS>.jsonl` — book snapshot（agg版）
- `liquidations/<market>/<date>/<HH-MM-SS>.jsonl` — liquidation events

## 2. 後段パイプラインの責務

未処理の30秒ブロックを古い順に読み込み、特徴量を計算し、追記DB/ファイルに出力する。
処理済みブロックは削除する。

## 3. 引き継ぐ特徴量（burst features 全14項目）

### 3.1 バースト基本
- burst_count_1s — 1秒あたりのバースト数
- max_burst_notional_1s — 最大バーストのnotional
- max_burst_prints_1s — 最大バーストのprint数
- max_burst_duration_ms_1s — 最大バーストの継続時間(ms)

### 3.2 Same-price（同一価格バースト）
- same_price_burst_count_1s
- same_price_burst_max_len_1s
- same_price_burst_notional_1s

### 3.3 Multilevel（複数価格バースト）
- multilevel_burst_count_1s
- multilevel_burst_max_span_ticks_1s
- multilevel_burst_notional_1s

### 3.4 方向性
- buy_burst_notional_1s
- sell_burst_notional_1s
- burst_delta_notional_1s

### 3.5 集中度
- largest_burst_share_notional_1s

## 4. Book-aware 特徴量（burst book validation）

Phase 5 で設計された book 検証特徴量は burst 文脈の場合は含めるが、独立した book 特徴量は別扱い:
- trade_at_touch_qty / trade_through_qty
- best_deplete_count / best_replenish_count
- burst_at_touch_ratio_1s, burst_through_ratio_1s
- burst_depletion_count_1s, burst_replenish_after_touch_count_1s

## 5. Book depth 特徴量

ring depth + imbalance:
- bid/ask 各バケット別 add/cancel/trade qty (0-1, 1-2, 2-5, 5-25, 25-100 bps)
- imbalance (5bps, 25bps)
- crr, tmr, rvz, realized_vol, cvd

## 6. 既存の後段実装（burst-agg）

`burst-agg.mjs` の実装:
- raw trades から30秒ウィンドウごとに burst 検出
- 出力: `summary/<market>.jsonl`（30秒単位の burst サマリ）
- 処理方式: 一定範囲（300秒/5分刻み）をバッチ処理
- `--delete-processed` で処理済み raw 削除

## 7. CVD チャート関連

burst-cvd-analysis リポジトリ内のスクリプト:
- 入力: burst_agg の summary または 1s_features
- 出力: 3段チャート（価格 + spot CVD + perp CVD）
- サイズ閾値: spot $10K/$100K, perp $100K/$1M

## 8. 設計方針（提案）

1. 30秒ブロックを基本処理単位とする
2. burst 検出は raw trades から行う（agg_trades ではない）
3. 出力は market 別の追記ファイル（`features/<market>/<date>.jsonl`）
4. book state は snapshot または book_updates の再現から取得
5. 処理済みブロックは削除してReceiverのディスク使用量を管理

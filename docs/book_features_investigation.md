# 板特徴量の精査レポート

> 作成日: 2026-07-17
> タスク: t_b46c10b9 — 板特徴量の精査・設計・実装

## 1. 現状整理

### 既存features_1s（22特徴量）
- #1-#12: trade-only（burst系）
- #13: burst_notional_vs_top_depth（板依存、null=未計算）
- #14: burst_mid_move_bps_1s（板依存、現状0固定）
- #15-#21: research系（burst microstructure）
- #22: outlier_trade_flag（監視系）

### book_snapshots（別Parquet）
- $1 bin刻み、bid/ask prices+qtys配列
- MAX_PRICE_DISTANCE = ±$10,000 window
- seeded=true のみ有効データ

### BookReplay（Python）
- `_bids`/`_asks`: price→qty dict（内部状態）
- `snapshot_at(ts)`: best bid/ask + mid + top_depth のみ返却
- `get_binned_snapshot(ts, 1.0)`: 全板を$1 binで返却
- seeded判定: 両サイド存在 + best_bid < best_ask（crossed除外）

---

## 2. 候補特徴量の精査

### 2.1 採用候補

| # | 名称 | 数式 | 単位 | 研究価値 | 計算量 | 採用 |
|---|------|------|------|----------|--------|------|
| B1 | book_mid_price | (best_bid + best_ask) / 2 | USD | ★★☆ | O(1) | ✅ |
| B2 | book_spread_bps | (ask - bid) / mid × 10000 | bps | ★★★ | O(1) | ✅ |
| B3 | book_bid_depth_100 | Σ bid_price×bid_qty for bid ∈ [mid-100, mid] | USD | ★★★ | O(n_bid) | ✅ |
| B4 | book_ask_depth_100 | Σ ask_price×ask_qty for ask ∈ [mid, mid+100] | USD | ★★★ | O(n_ask) | ✅ |
| B5 | book_bid_depth_1000 | same, $1000 window | USD | ★★☆ | O(n_bid) | ✅ |
| B6 | book_ask_depth_1000 | same, $1000 window | USD | ★★☆ | O(n_ask) | ✅ |
| B7 | book_imbalance_100 | (bid_depth_100 - ask_depth_100) / (bid_depth_100 + ask_depth_100) | [-1,1] | ★★★ | 派生 | ✅ |
| B8 | book_imbalance_1000 | same at $1000 | [-1,1] | ★★★ | 派生 | ✅ |
| B9 | book_microprice | (ask×bid_qty + bid×ask_qty) / (bid_qty + ask_qty) | USD | ★★★ | O(1) | ✅ |

### 2.2 不採用候補

| 名称 | 不採用理由 |
|------|-----------|
| book_bid_depth_10 | top_depth（#13既存）とほぼ同値。$10 windowはbest level数tick分であり追加情報量が少ない |
| book_bid_depth_10000 | MAX_PRICE_DISTANCE = $10000 と同一 = 全板total。コスト対効果低い。必要時にB3-B6から計算可能 |
| book_slope | 板の傾き（価格vs累積qtyの回帰）。bin構造が必要で$1 bin Parquetから事後計算が適切。1s featureには重い |
| book_convexity | slopeの微分。slope不採用のため連鎖不採用 |
| book_weighted_mid（別定義） | microprice（B9）と実質同一のため不採用 |
| book_pressure | (Σbid_qty×w_bid - Σask_qty×w_ask) / total。imbalanceの重み付き版だが、window分割で代替可能 |
| book_efficiency | traded_volume / total_depth。1s trade featuresとbook depthのcross featureであり、別レイヤー |

### 2.3 重複・リーク評価

- B1 (mid_price): 既存snapshot.mid_priceと同一。feature rowに含めることでML消費時のjoin不要
- B7/B8 (imbalance): B3-B6から派生可能だが、正規化済み指標として直接保存が有用
- B9 (microprice): mid_priceとは異なりqty重み付き。info ratio = (microprice - mid) / (spread/2) は事後計算可能

---

## 3. 設計決定

### 3.1 features_1s vs book_features_1s 分離判断

**判断: features_1sに追加（9列）**

理由:
1. 1s/30s契約の維持 — 既存features_1sは1秒1行。book featuresも同じ時間粒度
2. Hive partition schema — 末尾にnullable列追加は既存Parquetと後方互換
3. ML消費時のjoin不要 — 1 row = 全特徴量
4. book_snapshotは生データ、features_1sは集約特徴量 — 役割が明確

### 3.2 欠損semantics

| 状態 | 値 | 意味 |
|------|----|----|
| seeded=true, crossed=false | float値 | 正常計算結果 |
| seeded=false (片サイド空) | null | 板情報なし（計算不能） |
| crossed book (bid >= ask) | null | 板情報不正（計算不能） |
| 空板 (both sides empty) | null | 板情報なし |
| price=0, qty=0 | null経由 | snapshot_atがunseeded返却 |

### 3.3 計算効率

- BookReplayに `compute_book_features(ts)` を追加
- 1パスでbid/ask全level走査 → depth at windows全計算
- snapshot_atのbest bid/ask結果を再利用（二重走査なし）
- CPU: O(n_bid + n_ask) per second = 典型5000level × 2 = ~10K iteration/sec
- Parquet: 9列 × 30行/block = 270 cells/block追加（zstd圧縮で軽微）

### 3.4 market間比較可能性

- spread_bps: 正規化済み（bps）、market間直接比較可
- imbalance: [-1,1]正規化済み、比較可
- depth_usd: USD notional、market間比較可（ただしtick size差異は注意）
- mid_price: 絶対値、market間比較には正規化必要

---

## 4. 追加schema（FEATURE_1S_SCHEMA末尾に追記）

```python
# ── Book features B1-B9 (nullable: null when book unseeded/crossed) ──
pa.field("book_mid_price", pa.float64(), nullable=True),
pa.field("book_spread_bps", pa.float64(), nullable=True),
pa.field("book_bid_depth_100", pa.float64(), nullable=True),
pa.field("book_ask_depth_100", pa.float64(), nullable=True),
pa.field("book_bid_depth_1000", pa.float64(), nullable=True),
pa.field("book_ask_depth_1000", pa.float64(), nullable=True),
pa.field("book_imbalance_100", pa.float64(), nullable=True),
pa.field("book_imbalance_1000", pa.float64(), nullable=True),
pa.field("book_microprice", pa.float64(), nullable=True),
```

全9列をnullable=Trueで追加。既存22列は変更なし。

---

## 5. 変更ファイル一覧

| ファイル | 変更内容 |
|---------|---------|
| lib/downstream/book_replay.py | compute_book_features(ts)メソッド追加 |
| lib/downstream/feature_compiler.py | book features計算呼び出し追加 |
| lib/downstream/config.py | FEATURE_1S_SCHEMAに9列追加 |
| lib/burst-reducer/schema.mjs | FEATURE_1S_FIELDS/BOOK_FEATURE_FIELDS追加 |
| lib/burst-reducer/feature-computer-1s.mjs | Node側book features計算追加 |
| tests/test_book_replay.py | book featuresテスト追加 |
| docs/book_features_data_dictionary.md | データ辞書（日本語） |

---

## 6. Schema migration / 再起動

- **Schema migration必要**: FEATURE_1S_SCHEMAに9列追加（末尾、nullable）
- **既存Parquet影響なし**: 旧ファイルは新列欠落、読み取り時にnull補完
- **再起動必要**: downstream pipeline再起動で新schema有効化
- **Node pipeline**: schema.mjs更新後、受信側で新列対応必要

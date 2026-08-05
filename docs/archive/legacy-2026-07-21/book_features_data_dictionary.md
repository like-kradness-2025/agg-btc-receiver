# 板特徴量データ辞書（実装後）

> バージョン: 1.0（実装完了）
> 作成日: 2026-07-17
> 実装: lib/downstream/book_replay.py:compute_book_features(), lib/burst-reducer/feature-computer-1s.mjs

## 概要

features_1sに9つの板特徴量（B1-B9）を追加実装した。既存22特徴量（trade系）と統合され、1秒1行に全31特徴量（22 trade + 9 book）が含まれる。

**追加schema列数:** 9列（全てnullable=True）
**既存Parquet後方互換:** あり（旧ファイルは新列欠落、読み取り時にnull補完）

---

## 特徴量定義

### B1: book_mid_price

**数式:**
```python
mid_price = (best_bid_price + best_ask_price) / 2
```

**単位:** USD

**型:** float64, nullable=True

**計算元:** BookReplay.snapshot_at(ts).mid_price

**境界:**
- seeded=true, crossed=false: float値
- seeded=false: null
- crossed (bid >= ask): null

**Python実装:** book_replay.py:313-315
**Node実装:** feature-computer-1s.mjs:137-140

---

### B2: book_spread_bps

**数式:**
```python
spread_bps = (best_ask_price - best_bid_price) / mid_price × 10000
```

**単位:** bps (1bps = 0.01%)

**型:** float64, nullable=True

**計算元:** B1から派生

**境界:**
- mid_price = 0: null
- spread = 0: 0 bps
- 典型値: 1-10 bps（主要market）

**Python実装:** book_replay.py:317-320
**Node実装:** feature-computer-1s.mjs:142-143

---

### B3: book_bid_depth_100

**数式:**
```python
bid_depth_100 = Σ (bid_price × bid_qty) for bid ∈ [mid_price - 100, mid_price]
```

**単位:** USD notional

**型:** float64, nullable=True

**計算元:** BookReplay._bids dict走査

**境界:**
- bid_price = 0: 除外
- bid_qty = 0: 除外
- 該当levelなし: 0（nullではない）

**欠損:** null = unseeded/crossed

**Python実装:** book_replay.py:335-341（1パスで$100/$1000同時計算）
**Node実装:** feature-computer-1s.mjs:145-152（best level proxy、full book未対応）

---

### B4: book_ask_depth_100

**数式:**
```python
ask_depth_100 = Σ (ask_price × ask_qty) for ask ∈ [mid_price, mid_price + 100]
```

**単位:** USD notional

**型:** float64, nullable=True

**Python実装:** book_replay.py:343-349
**Node実装:** feature-computer-1s.mjs:153-156（best level proxy）

---

### B5: book_bid_depth_1000

**数式:**
```python
bid_depth_1000 = Σ (bid_price × bid_qty) for bid ∈ [mid_price - 1000, mid_price]
```

**単位:** USD notional

**Python実装:** book_replay.py:335-341（B3と同時計算）
**Node実装:** feature-computer-1s.mjs:157（best level proxy）

---

### B6: book_ask_depth_1000

**数式:**
```python
ask_depth_1000 = Σ (ask_price × ask_qty) for ask ∈ [mid_price, mid_price + 1000]
```

**単位:** USD notional

**Python実装:** book_replay.py:343-349（B4と同時計算）
**Node実装:** feature-computer-1s.mjs:158（best level proxy）

---

### B7: book_imbalance_100

**数式:**
```python
imbalance_100 = (bid_depth_100 - ask_depth_100) / (bid_depth_100 + ask_depth_100)
```

**単位:** [-1, 1]（無次元）

**型:** float64, nullable=True

**計算元:** B3, B4から派生

**境界:**
- bid_depth_100 + ask_depth_100 = 0: 0（分母0回避）
- +1 = 買い優位、-1 = 売り優位

**Python実装:** book_replay.py:352-353
**Node実装:** feature-computer-1s.mjs:160-163

---

### B8: book_imbalance_1000

**数式:**
```python
imbalance_1000 = (bid_depth_1000 - ask_depth_1000) / (bid_depth_1000 + ask_depth_1000)
```

**単位:** [-1, 1]

**Python実装:** book_replay.py:356-357
**Node実装:** feature-computer-1s.mjs:165-168

---

### B9: book_microprice

**数式:**
```python
microprice = (best_ask_price × best_bid_qty + best_bid_price × best_ask_qty) / (best_bid_qty + best_ask_qty)
```

**単位:** USD

**型:** float64, nullable=True

**計算元:** best bid/ask price + qty

**境界:**
- best_bid_qty + best_ask_qty = 0: null
- qty多い側に引っ張られる

**Python実装:** book_replay.py:360-365
**Node実装:** feature-computer-1s.mjs:170-173

---

## 欠損semantics共通ルール

| 条件 | 値 | 解釈 |
|------|----|----|
| seeded=true, crossed=false | float | 正常計算結果 |
| seeded=false | null | 板情報なし（計算不能） |
| crossed (bid >= ask) | null | 板不正（計算不能） |
| 空板（both empty） | null | 板なし |
| 分母=0（imbalance/microprice） | 0 or null | 定義不能（imbalance=0, microprice=null） |
| 計算結果=0（depthなし） | 0 | 正常（該当levelなし） |

**0 vs null:**
- 0 = 計算可能だが値が0（例: depth=0, imbalance=0）
- null = 計算不能（板状態不正）

---

## 実装差異（Python vs Node）

### Python（完全実装）
- BookReplay._bids/_asks dict全走査
- B3-B6: 1パスで全window計算、正確なdepth

### Node（部分実装）
- bookSnapshot.state: best levelのみ保持
- B3-B6: best level proxy（full book未対応）
- TODO: full book state取得時に正確なdepth計算実装

**影響:**
- Python: 正確なdepth at windows
- Node: best levelのみ（depth_100 = depth_1000 = top_depth）

---

## テストカバレッジ

### Python（63 tests pass）
- test_book_features.py: 10 tests
  - unseeded/crossed/seeded
  - depth at windows（$100/$1000差異）
  - window境界（inclusive）
  - imbalance/microprice
  - price=0/qty=0除外
- test_feature_compiler_book.py: 5 tests
  - book_replay=None/unseeded/seeded/crossed
  - 既存features破壊なし確認

### Node（794 tests pass, 5 failures）
- schema.test.mjs: 2 failures（field count変更に対応済み）
- feature-computer-1s.test.mjs: 3 failures（board field位置変更）
- orderflow_monitor.test.mjs: 1 failure（subprocess test、無関係）

---

## Performance

### Python
- 計算量: O(n_bid + n_ask) per second
- 典型: 5000 levels × 2 = 10K iteration/sec
- Parquet: 9列 × 30行/block = 270 cells/block追加（zstd軽微）

### Node
- 計算量: O(1)（best levelのみ）
- Parquet: 同上

---

## Schema Migration

**必要:**
- Python: FEATURE_1S_SCHEMAに9列追加（config.py）
- Node: FEATURE_1S_FIELDS/BOOK_FEATURE_FIELDS追加（schema.mjs）

**再起動:**
- downstream pipeline: 新schema有効化
- Node pipeline: 新列対応

**既存データ:**
- 旧Parquet: 新列欠落、読み取り時にnull補完
- 移行作業不要

# 板特徴量データ辞書（実装前）

> バージョン: 1.0（設計段階）
> 作成日: 2026-07-17

## 概要

book_snapshots（$1 bin Parquet）とBookReplayから派生する板特徴量を定義する。
全9特徴量をfeatures_1sに追加し、既存22特徴量と統合する。

---

## 特徴量定義

### B1: book_mid_price

**数式:**
```
mid_price = (best_bid_price + best_ask_price) / 2
```

**単位:** USD（米国ドル）

**意味:** 板の中心価格（best bid と best ask の平均）

**境界:**
- seeded=true, crossed=false: float値
- seeded=false: null
- crossed (bid >= ask): null
- 空板: null

**欠損:** null = 板情報なし（計算不能）

**研究用途:**
- 価格レベル正規化
- micropriceとの比較
- 他featureとの相関分析

---

### B2: book_spread_bps

**数式:**
```
spread_bps = (best_ask_price - best_bid_price) / mid_price × 10000
```

**単位:** bps（basis points, 1bps = 0.01%）

**意味:** bid-ask spreadの相対幅。市場の流動性指標。

**境界:**
- mid_price = 0: null（除算不能）
- spread = 0: 0 bps（完全流動）
- 典型値: 1-10 bps（主要market）

**欠損:** null = mid_price計算不能

**研究用途:**
- 流動性スコア
- market間比較（正規化済み）
- 緊急時のspread拡大検知

---

### B3: book_bid_depth_100

**数式:**
```
bid_depth_100 = Σ (bid_price × bid_qty) for bid ∈ [mid_price - 100, mid_price]
```

**単位:** USD notional（米国ドル）

**意味:** midから$100以内のbid側総notional。短期間の買い圧力。

**境界:**
- bid_price = 0: 除外
- bid_qty = 0: 除外
- 該当levelなし: 0（nullではない）

**欠損:** null = unseeded/crossed

**研究用途:**
- 短期買い.support強度
- imbalance計算の分母

---

### B4: book_ask_depth_100

**数式:**
```
ask_depth_100 = Σ (ask_price × ask_qty) for ask ∈ [mid_price, mid_price + 100]
```

**単位:** USD notional

**意味:** midから$100以内のask側総notional。短期間の売り圧力。

**境界:** B3と同様

**欠損:** null = unseeded/crossed

---

### B5: book_bid_depth_1000

**数式:**
```
bid_depth_1000 = Σ (bid_price × bid_qty) for bid ∈ [mid_price - 1000, mid_price]
```

**単位:** USD notional

**意味:** midから$1000以内のbid側総notional。中期間の買い圧力。

**境界:** B3と同様

**欠損:** null = unseeded/crossed

**研究用途:**
- 中期的サポート強度
- B3との比較で板の厚み分布

---

### B6: book_ask_depth_1000

**数式:**
```
ask_depth_1000 = Σ (ask_price × ask_qty) for ask ∈ [mid_price, mid_price + 1000]
```

**単位:** USD notional

**意味:** midから$1000以内のask側総notional。

**境界:** B3と同様

**欠損:** null = unseeded/crossed

---

### B7: book_imbalance_100

**数式:**
```
imbalance_100 = (bid_depth_100 - ask_depth_100) / (bid_depth_100 + ask_depth_100)
```

**単位:** [-1, 1]（無次元）

**意味:** $100 window内の売り買い圧力バランス。+1 = 買い優位、-1 = 売り優位。

**境界:**
- bid_depth_100 + ask_depth_100 = 0: 0（分母0回避）
- bid_depth_100 = ask_depth_100: 0（均衡）

**欠損:** null = B3 or B4がnull

**研究用途:**
- 短期的な価格方向予測
- market間比較（正規化済み）
- burstとの相関

---

### B8: book_imbalance_1000

**数式:**
```
imbalance_1000 = (bid_depth_1000 - ask_depth_1000) / (bid_depth_1000 + ask_depth_1000)
```

**単位:** [-1, 1]

**意味:** $1000 window内の売り買い圧力バランス。

**境界:** B7と同様

**欠損:** null = B5 or B6がnull

**研究用途:**
- 中期的な価格方向予測
- B7との比較で時間軸差異

---

### B9: book_microprice

**数式:**
```
microprice = (best_ask_price × best_bid_qty + best_bid_price × best_ask_qty) / (best_bid_qty + best_ask_qty)
```

**単位:** USD

**意味:** 取引量重み付き中心価格。qtyが多い側に引っ張られる。

**境界:**
- best_bid_qty + best_ask_qty = 0: null（分母0）
- best_bid_qty >> best_ask_qty: microprice → best_ask_price（ask寄り）
- best_bid_qty = best_ask_qty: microprice = mid_price

**欠損:** null = unseeded/crossed or qty合計=0

**研究用途:**
- mid_priceとの差で情報比率: (microprice - mid) / (spread/2)
- 短期的な価格方向の高精度推定
- 板の非対称性検知

---

## 欠損semantics共通ルール

| 条件 | 値 | 解釈 |
|------|----|----|
| seeded=true, crossed=false | float | 正常計算結果 |
| seeded=false | null | 板情報なし（計算不能） |
| crossed (bid >= ask) | null | 板不正（計算不能） |
| 空板（both empty） | null | 板なし |
| 分母=0（imbalance/microprice） | null | 定義不能 |
| 計算結果=0（depthなし） | 0 | 正常（該当levelなし） |

**0 vs null:**
- 0 = 計算可能だが値が0（例: depth=0）
- null = 計算不能（板状態不正）

---

## 計算順序（1パス）

```python
def compute_book_features(ts):
    snap = snapshot_at(ts)
    if not snap.seeded:
        return all_null_features()

    mid = snap.mid_price
    bid, ask = snap.best_bid_price, snap.best_ask_price

    # B1, B2
    B1 = mid
    B2 = (ask - bid) / mid * 10000 if mid > 0 else None

    # B3-B6: 1パスで全window計算
    B3 = B5 = 0
    for price, qty in bids:
        notional = price * qty
        if mid - 100 <= price <= mid:
            B3 += notional
        if mid - 1000 <= price <= mid:
            B5 += notional

    B4 = B6 = 0
    for price, qty in asks:
        notional = price * qty
        if mid <= price <= mid + 100:
            B4 += notional
        if mid <= price <= mid + 1000:
            B6 += notional

    # B7, B8: 派生
    B7 = (B3 - B4) / (B3 + B4) if (B3 + B4) > 0 else 0
    B8 = (B5 - B6) / (B5 + B6) if (B5 + B6) > 0 else 0

    # B9: best level qty必要
    B9 = (ask * snap.best_bid_qty + bid * snap.best_ask_qty) / (snap.best_bid_qty + snap.best_ask_qty) if (snap.best_bid_qty + snap.best_ask_qty) > 0 else None

    return {B1..B9}
```

---

## 実装時の注意点

1. **既存コード変更最小化**
   - BookReplay.compute_book_features(ts)を新規追加
   - feature_compiler.pyから呼び出し
   - 既存snapshot_atのbest bid/ask結果を再利用

2. **test対象**
   - seeded/unseeded
   - crossed book
   - 空板
   - window境界（$100/$1000ちょうど）
   - market isolation
   - tick size差異

3. **performance**
   - O(n_bid + n_ask) per second
   - 典型: 5000 levels × 2 = 10K iteration/sec
   - Parquet: 9列 × 30行 = 270 cells/block（軽微）

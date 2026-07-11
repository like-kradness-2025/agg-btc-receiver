# BTC Burst Feature Specification v1

**Date:** 2026-07-09
**Scope:** BTC only, v1 — 15 markets (spot + perp)
**Author:** agg-btc-reducer pipeline
**Dependencies:** agg-btc-receiver (30s blocks), BurstBuilder (burst formation)

---

## 目次

1. [概要](#1-概要)
2. [バースト基本定義](#2-バースト基本定義)
3. [MVP 14 特徴量](#3-mvp-14-特徴量)
   - 3.1 burst_count_1s
   - 3.2 total_burst_notional_1s
   - 3.3 max_burst_notional_1s
   - 3.4 max_burst_prints_1s
   - 3.5 max_burst_duration_ms_1s
   - 3.6 same_price_burst_count_1s
   - 3.7 multilevel_burst_count_1s
   - 3.8 buy_burst_notional_1s
   - 3.9 sell_burst_notional_1s
   - 3.10 burst_imbalance_ratio_1s
   - 3.11 largest_burst_share_notional_1s
   - 3.12 burst_notional_vs_30s_traded_notional
   - 3.13 burst_notional_vs_top_depth
   - 3.14 burst_mid_move_bps_1s
4. [研究 7 特徴量](#4-研究-7-特徴量)
5. [監視 1 特徴量](#5-監視-1-特徴量)
6. [時間スケール仕様](#6-時間スケール仕様)
   - 6.1 1s Layer: Raw Features
   - 6.2 30s Layer: Aggregation Statistics
   - 6.3 5min Layer: Cross-Market / Z-Score / Regime
7. [データフロー](#7-データフロー)
8. [出力形式](#8-出力形式)
9. [品質・ガードレール](#9-品質ガードレール)
10. [決定理由](#10-決定理由)
11. [よくある誤解の Pitfalls](#11-よくある誤解の-pitfalls)
12. [マーケット一覧](#12-マーケット一覧)
13. [参照実装](#13-参照実装)

---

## 1. 概要

本仕様書は BTC burst 特徴量パイプライン（`agg-btc-reducer`）が生成する 22 個の特徴量を定義する。

**v1 制約:**
- BTC マーケットのみ（15 markets: 6 perp + 9 spot）
- バースト検出は raw trades のみで行う（深度情報は検出に使わない）
- `burst_count_1s`, `total_burst_notional_1s`, `max_burst_notional_1s` は全分析に必須のコア3項目

**必須ソースコード理解:**
- `lib/burst-builder.mjs`（バースト形成ロジック — gap_threshold_ms + max_burst_duration_ms）
- `lib/trade-aggregator.mjs`（OHLCV 集約 + サイズバケット定義）
- `scripts/burst-agg.mjs`（burst-agg の処理フロー全体）

---

## 2. バースト基本定義

### 2.1 バーストとは

**Definition:** 同一 market、同一 side（buy/sell）の連続 trade print で構成される塊。

```text
burst = 同一market・同一sideの連続trade
条件:
  1. 隣接 print-to-print gap <= gap_threshold_ms（default: 50ms）
  2. バースト全体の継続時間 <= max_burst_duration_ms（default: 5000ms）
  → 条件を満たす最大連続区間（maximal contiguous run）
```

**参照実装:** `lib/burst-builder.mjs` `BurstBuilder` クラス
- `_gapThreshold`（default: 5ms — 実際の burst-agg.mjs では 100ms が使われる）
- `_maxDuration`（default: 500ms — 実際は 3000ms が使われる）

### 2.2 バースト構造体（Burst object）

```typescript
interface Burst {
  burst_id: string;          // "{market}-{seq_number}"
  market: string;
  side: 'buy' | 'sell';
  burst_notional: number;    // sum(price * qty) of all prints
  burst_print_count: number; // number of trade prints
  burst_duration_ms: number; // burst_end_ts - burst_start_ts
  burst_start_ts: number;    // ms timestamp of first print
  burst_end_ts: number;      // ms timestamp of last print
  min_price: number;         // minimum price in burst
  max_price: number;         // maximum price in burst
  distinct_price_count: number; // number of unique price levels
  span_ticks: number;        // (max_price - min_price) / tick_size (0 if single-price)
  same_price_runs: SamePriceRun[];
  prints: TradePrint[];      // raw trade prints (includes _idx, tradeId for traceability)
}
```

### 2.3 Overlap 判定（バースト間隔 vs 1s バケット）

```text
overlap(burst, bucket_ts) :=
  burst.burst_start_ts < bucket_ts + 1000  AND
  burst.burst_end_ts >= bucket_ts
```

**参照実装:** `burst-builder.mjs` `getClosedBurstsOverlapping(secondTs)` L189-195

**重要な意味:** バーストの interval [start_ts, end_ts] と 1s bucket [bucket_start, bucket_start+1000) が少しでも重なればカウント対象。

**Truth table（端点ルール）:**

| burst_start | burst_end | bucket_start | overlap? | 理由 |
|---|---|---|---|---|
| 1000 | 1500 | 1000 | ✅ Yes | `1000 < 2000 ∧ 1500 >= 1000` → start が bucket 内、end も bucket 内 |
| 500 | 1500 | 1000 | ✅ Yes | `500 < 2000 ∧ 1500 >= 1000` → end が bucket 内であれば start が左にあっても overlap |
| 500 | 1000 | 1000 | ✅ Yes | `500 < 2000 ∧ 1000 >= 1000` → end がぴったり bucket 境界と等しい場合も overlap（`>=`） |
| 500 | 950 | 1000 | ❌ No | `500 < 2000` だが `950 < 1000` → burst 全体が bucket 開始より前 |
| 2000 | 2500 | 1000 | ❌ No | `2000 >= 2000` → start が bucket 終了と等しくても `start < bucket_end` を満たさない |
| 1500 | 1800 | 1000 | ✅ Yes | `1500 < 2000 ∧ 1800 >= 1000` → 完全に bucket 内 |
| 1000 | 16000 | 1000 | ✅ Yes | `1000 < 2000 ∧ 16000 >= 1000` → 長時間バースト、bucket と重なる限り全秒でカウント |

**具体例（1秒 bucket = 0:59:45.000 からの各秒）:**

- バーストが **0:59:45.000 開始、1:00:00.000 終了**（15秒の長時間バースト）:
  - bucket `0:59:45.000` ～ `1:00:00.000` の **16個すべての 1s bucket に重なる**
  - 理由: `start=45000 < 46000` (最初の bucket_end) かつ `end=60000 >= 45000` (最初の bucket_start)。同様に全 bucket で条件成立
- バーストが **0:59:45.500 開始、0:59:45.750 終了**:
  - bucket `0:59:45.000` のみに重なる
  - 理由: `start=45500 < 46000 ∧ end=45750 >= 45000` は成立。`start=45500 < 47000 ∧ end=45750 >= 46000` は不成立（end < next bucket_start）

**注意:** 1秒未満のバーストが 2 つの bucket にまたがる可能性もある（例: 0:59:45.800 開始、0:59:46.200 終了 → bucket 45 と 46 の両方に重なる）。

### 2.4 Same-price run

```text
same_price_run = バースト内の連続最大区間（max_price == min_price）
  → 単一価格で取引された部分区間
  → バーストが multilevel の場合も内部に same-price run を持つ可能性あり
```

**分類:**
- `same_price_burst` = `distinct_price_count == 1`（max_price == min_price）
- `multilevel_burst` = `distinct_price_count >= 2`

---

## 3. MVP 14 特徴量

### 3.1 burst_count_1s

| Property | Value |
|---|---|
| **Column name** | `burst_count_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap するバーストの個数 |
| **Formula** | `count(bursts where overlap(burst, second_ts))` |
| **Overlap** | burst interval と 1s bucket の overlap（§2.3） |
| **Null/Zero** | 0（バーストなし秒）= 0。NULL は不可 |
| **Edge cases** | バーストが複数秒にまたがる場合、各秒で別個にカウントされる。長時間バースト（上限: 最大5秒。`max_burst_duration_ms=5000ms`）は、その間隔が重なる各 1s bucket で1件として数えられる |
| **Market comparability** | 直接比較可能。マーケット間の取引頻度差で値域が異なるため、比較時は z-score 推奨 |

**コア3項目の1つ:** 全 downstream 分析で必須。

### 3.2 total_burst_notional_1s

| Property | Value |
|---|---|
| **Column name** | `total_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する全バーストの notional（USD）合計 |
| **Formula** | `sum(burst.burst_notional for burst in overlapping_bursts)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0 = バーストなし。NULL 不可 |
| **Edge cases** | 長時間バーストが複数秒にまたがる場合、**全 notional が各秒で重複計上される**。これは spec に沿った仕様で、秒単位の「その瞬間にどれだけのバースト資金が動いているか」を表現するための設計 |
| **Market comparability** | マーケット規模で正規化必須。spot vs perp は約定量が桁違い（perp の方が大きい） |

**コア3項目の1つ。**

### 3.3 max_burst_notional_1s

| Property | Value |
|---|---|
| **Column name** | `max_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap するバーストの最大 notional |
| **Formula** | `max(burst.burst_notional for burst in overlapping_bursts)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0 = バーストなし。NULL 不可 |
| **Edge cases** | バーストなし秒は 0。単一バーストのみの場合は total と等しくなる |
| **Market comparability** | 直接比較には向かない。`largest_burst_share_notional_1s` で正規化後の比較推奨 |

**コア3項目の1つ。**

### 3.4 max_burst_prints_1s

| Property | Value |
|---|---|
| **Column name** | `max_burst_prints_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap するバーストの最大 print 数 |
| **Formula** | `max(burst.burst_print_count for burst in overlapping_bursts)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0 = バーストなし。NULL 不可 |
| **Edge cases** | 1 print のみのバーストも有効（burst_print_count == 1） |
| **Market comparability** | マーケットの取引速度に依存。低速マーケット（bitfinex_spot）と高速マーケット（binance_perp）では値域が異なる |

### 3.5 max_burst_duration_ms_1s

| Property | Value |
|---|---|
| **Column name** | `max_burst_duration_ms_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap するバーストの最大継続時間（ms） |
| **Formula** | `max(burst.burst_duration_ms for burst in overlapping_bursts)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0 = バーストなし。NULL 不可 |
| **Edge cases** | `max_burst_duration_ms`（default: 5000ms）が上限。5s を超えるバーストは形成されない |
| **Market comparability** | 直接比較可能だがマーケットの取引密度に依存 |

### 3.6 same_price_burst_count_1s

| Property | Value |
|---|---|
| **Column name** | `same_price_burst_count_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap するバーストのうち、`max_price == min_price`（単一価格）のバースト数 |
| **Formula** | `count(b in overlapping where b.distinct_price_count == 1)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0（同一価格バーストなし、またはバースト自体なし）= 0 |
| **Edge cases** | `burst_count_1s == same_price_burst_count_1s + multilevel_burst_count_1s` が常に成立する |
| **Market comparability** | 同一価格バースト比率 = `same_price / burst_count_1s` で比較 |

### 3.7 multilevel_burst_count_1s

| Property | Value |
|---|---|
| **Column name** | `multilevel_burst_count_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap するバーストのうち、`max_price != min_price`（複数価格）のバースト数 |
| **Formula** | `count(b in overlapping where b.distinct_price_count >= 2)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0 |
| **Edge cases** | `multilevel` は `distinct_price_count >= 2` で判定。価格が2段階でも3段階以上でも区別しない（詳細は研究項目 3.17-3.19 で捕捉） |
| **Market comparability** | 絶対数より same_price との比率で使用 |

### 3.8 buy_burst_notional_1s

| Property | Value |
|---|---|
| **Column name** | `buy_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する buy-side バーストの notional 合計 |
| **Formula** | `sum(b.burst_notional for b in overlapping where b.side == 'buy')` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0（buy バーストなし）= 0 |
| **Edge cases** | `total = buy + sell` |
| **Market comparability** | 符号 ≥ 0。買い優勢/売り優勢は imbalance_ratio (§3.10) で判定 |

### 3.9 sell_burst_notional_1s

| Property | Value |
|---|---|
| **Column name** | `sell_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する sell-side バーストの notional 合計 |
| **Formula** | `sum(b.burst_notional for b in overlapping where b.side == 'sell')` |
| **Overlap** | §2.3 |
| **Null/Zero** | 0（sell バーストなし）= 0 |
| **Edge cases** | `total = buy + sell` |
| **Market comparability** | 符号 ≥ 0 |

### 3.10 burst_imbalance_ratio_1s

| Property | Value |
|---|---|
| **Column name** | `burst_imbalance_ratio_1s` |
| **Data type** | `float64` |
| **Definition** | 買い/売りバーストの不均衡比率 |
| **Formula** | `(buy - sell) / (buy + sell + eps)` where `eps = 1e-10` |
| **Range** | `[-1.0, 1.0]` |
| **Overlap** | §2.3 |
| **Null/Zero** | バーストなし秒は `0 / (0 + eps) = 0.0` |
| **Edge cases** | 完全に売りのみ → `-1.0`。完全に買いのみ → `1.0`。均衡 → `0.0`。バーストなし → `0.0`（`0/eps==0`） |
| **Market comparability** | マーケット間直接比較可能（比率のため）。方向性シグナルとして使用 |

### 3.11 largest_burst_share_notional_1s

| Property | Value |
|---|---|
| **Column name** | `largest_burst_share_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 最大バーストの notional が全バースト notional に占める割合 |
| **Formula** | `max_burst_notional_1s / total_burst_notional_1s` |
| **Range** | `(0.0, 1.0]` |
| **Null/Zero** | バーストなし秒は 0。`total_burst_notional == 0` のとき 0 |
| **Edge cases** | バーストが1個のみ → `1.0`。バーストが複数 → 最大の集中度を示す。`NaN` 回避のため除数に注意 |
| **Market comparability** | 直接比較可能（比率のため）。`1.0` に近い = 単一支配的バースト、`0.0` に近い = 多数の小さなバースト |

### 3.12 burst_notional_vs_30s_traded_notional

| Property | Value |
|---|---|
| **Column name** | `burst_notional_vs_30s_traded_notional` |
| **Data type** | `float64` |
| **Definition** | 1秒間のバースト notional 合計 / 直近30秒の総約定 notional |
| **Formula** | `total_burst_notional_1s / max(traded_notional_30s, eps)` |
| **Overlap** | バーストは §2.3、traded_notional は直近30秒 rolling window: `[second_ts - 30000, second_ts)`（second_ts = 当該秒の開始時刻 ms、左閉右開）。window 内の **authoritative `agg_trades`** の `sum(volume * vwap)` を分母とする。分母が厳密に 0 の場合、値は `0` とする（補完しない）。agg row の存在有無（入力可用性）は単体テストで検証すること |
| **Null/Zero** | traded_notional_30s == 0 → 0。NULL 不可。P1 では必ず計算する（agg_trades 入力必須） |
| **P1 契約** | **P1a/P1b で計算必須。** `agg_trades` を読み取り `[second_ts-30000, second_ts)` の総 traded notional を分母とする。これが完了するまで「MVP14 complete」を名乗らない |
| **Edge cases** | 非流動的な時間帯（取引ゼロ）では 0。トレードが極端に少ないマーケットでは分母が小さく値が大きくなりやすい。30s window は burst-agg の処理ウィンドウに合わせる（`scripts/burst-agg.mjs` の 30s 集約） |
| **Market comparability** | **この正規化がないと cross-market 比較が不可能**。各マーケットの流動性に応じた burst の相対的重要度を示す |

### 3.13 burst_notional_vs_top_depth

| Property | Value |
|---|---|
| **Column name** | `burst_notional_vs_top_depth` |
| **Data type** | `float64`（nullable） |
| **Definition** | バースト notional 合計（USD） / 直近の best bid + best ask の top-of-book notional（USD）。板の厚さに対するバースト資金の相対規模 |
| **Formula** | `total_burst_notional_1s / max(best_bid_price * best_bid_qty + best_ask_price * best_ask_qty, eps)` where `eps = 1e-10` |
| **Overlap** | バーストは §2.3。book depth は直近の book state から取得 |
| **Null/Zero** | **book 欠落時は NULL**（book が使えないマーケット、または book snapshot が未着の時間帯）。book が有効で top-of-book notional == 0 の場合は `eps` で除算されるため非常に大きな値になる（実質的に異常値として扱う）。NULL は不可ではなく許容 |
| **P1 契約** | **P1a/P1b では常に `null` を出力する。** 板リプレイ（P4）の実装まで book state は利用不可のため。`_quality.book_seeded: false` を併記。P1 物理出力の22列中13列目は `null`（book なしの明示） |
| **Edge cases** | book snapshot または book_updates replay が不完全な場合、NULL を許容。これが唯一 NULL を許容する MVP 項目。top-of-book notional が極端に小さい（例: 深度がほぼ空）場合、比率が極大化するため downstream 分析では winsorization 推奨 |
| **Market comparability** | マーケットごとに book coverage tier（§9.4）が異なるため、比較時は tier を共変量として含める。Tier C では深度が過小評価され比率が過大に出る傾向がある |

### 3.14 burst_mid_move_bps_1s

| Property | Value |
|---|---|
| **Column name** | `burst_mid_move_bps_1s` |
| **Data type** | `float64` |
| **Definition** | 各秒に overlap するバースト群について、バースト開始直前と終了直後の mid price 変化を bps で集約した指標。Event-time anchor 方式 |
| **Formula** | 手順: 1) overlapping bursts を特定 2) 各バーストについて `mid_before = bookState(burst_start_ts - 1ms)`, `mid_after = bookState(burst_end_ts + 1ms)` 3) `move_bps = (mid_after - mid_before) / mid_before * 10000` 4) 1秒内のバースト群の `move_bps` を **サイズ加重平均** で集約 |
| **Overlap** | バーストは §2.3。mid 照会は **event-time anchor**（秒境界ではない） |
| **Null/Zero** | バーストなし秒 = 0。book 欠落で mid が取れない場合 = 0（NULL にはしない。次善策として 0） |
| **P1 契約** | **P1a/P1b では常に `0` を出力する（`null` ではない）。** 板リプレイ（P4）の実装まで mid 照会は不可のため。`_quality.book_seeded: false` を併記。P1 物理出力の22列中14列目は `0`（book がないため、観測値なしを意味する。P1 契約上の 0 であり、データ欠損ではない） |
| **Edge cases** | 複数バーストが同一秒に overlap する場合、それぞれの move を burst_notional で加重平均。mid_before/mid_after が取れない場合、そのバーストは集約から除外 |
| **Market comparability** | bps 単位なので cross-market 直接比較可能 |

---

## 4. 研究 7 特徴量

**P1 契約:** P1a/P1b では #15-#21 の全列に `0` を出力する（`null` ではない）。0 は「P1 では観測値なし」を意味し、データ欠損とは異なる。将来的に計算可能になり次第実数に切り替える。P6 で実装予定。

### 4.1 same_price_burst_max_len_1s（#15）

| Property | Value |
|---|---|
| **Column name** | `same_price_burst_max_len_1s` |
| **Data type** | `uint16` |
| **Definition** | 1秒間に overlap する同一価格バーストの最大 print 数 |
| **Formula** | `max(b.burst_print_count for b in overlapping where b.distinct_price_count == 1)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 同一価格バーストがない、またはバースト自体がない → 0 |
| **Edge cases** | 単一 print の同一価格バーストも対象（print_count == 1）。バーストの最大長であり、same_price_run の長さではない（バースト全体の長さ） |
| **Market comparability** | マーケットの取引速度に依存。高速マーケットほど値が大きくなる傾向。cross-market では z-score 推奨 |

### 4.2 same_price_burst_notional_1s（#16）

| Property | Value |
|---|---|
| **Column name** | `same_price_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する同一価格バーストの notional 合計（USD） |
| **Formula** | `sum(b.burst_notional for b in overlapping where b.distinct_price_count == 1)` |
| **Overlap** | §2.3 |
| **Null/Zero** | 同一価格バーストなし → 0 |
| **Edge cases** | `total_burst_notional_1s = same_price_burst_notional_1s + multilevel_burst_notional_1s` が成立する。長時間バーストの重複カウントに注意（§2.3） |
| **Market comparability** | notional 依存のため cross-market では `same_price_absorption_ratio_1s` での正規化推奨 |

### 4.3 multilevel_burst_max_span_ticks_1s（#17）

| Property | Value |
|---|---|
| **Column name** | `multilevel_burst_max_span_ticks_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する multilevel バーストの最大 span_ticks |
| **Formula** | `max(b.span_ticks for b in overlapping where b.distinct_price_count >= 2)` |
| **Overlap** | §2.3 |
| **Null/Zero** | multilevel バーストなし → 0 |
| **Edge cases** | tick size はマーケット依存（BTC は取引所ごとに異なる: binance 0.01, bybit 0.10, bitmex 0.50 など。設計書 §1.3 の `market_tick_size` マップを参照）。同じ価格幅でもマーケットごとに tick 換算値が異なるため cross-market では bps 版（#18）を優先。tick_size が未定義のマーケットでは当該列を `null` とする |
| **Market comparability** | 非推奨。cross-market では `multilevel_burst_max_span_bps_1s`（#18）を使用 |

### 4.4 multilevel_burst_max_span_bps_1s（#18）

| Property | Value |
|---|---|
| **Column name** | `multilevel_burst_max_span_bps_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する multilevel バーストの最大価格幅（bps） |
| **Formula** | `max((b.max_price - b.min_price) / b.min_price * 10000 for b in overlapping where b.distinct_price_count >= 2)` |
| **Overlap** | §2.3 |
| **Null/Zero** | multilevel バーストなし → 0 |
| **Edge cases** | 分母 `b.min_price` が 0 の場合はスキップ（異常データ）。span ゼロ（max_price == min_price）のバーストは distinct_price_count >= 2 の判定で除外済みのため、分子が 0 になることはない |
| **Market comparability** | **bps 単位なので cross-market 直接比較可能。ticks（#17）より優先して使用** |

### 4.5 multilevel_burst_notional_1s（#19）

| Property | Value |
|---|---|
| **Column name** | `multilevel_burst_notional_1s` |
| **Data type** | `float64` |
| **Definition** | 1秒間に overlap する multilevel バーストの notional 合計（USD） |
| **Formula** | `sum(b.burst_notional for b in overlapping where b.distinct_price_count >= 2)` |
| **Overlap** | §2.3 |
| **Null/Zero** | multilevel バーストなし → 0 |
| **Edge cases** | `total_burst_notional_1s = same_price_burst_notional_1s + multilevel_burst_notional_1s` が常に成立。長時間バーストの重複カウントに注意（Pitfall 2） |
| **Market comparability** | notional 依存。`multilevel_burst_notional_1s / total_burst_notional_1s` の比率で cross-market 比較 |

### 4.6 same_price_absorption_ratio_1s（#20）

| Property | Value |
|---|---|
| **Column name** | `same_price_absorption_ratio_1s` |
| **Data type** | `float64` |
| **Definition** | 同一価格バースト notional / 全バースト notional。板の吸収力を示唆する代理指標 |
| **Formula** | `same_price_burst_notional_1s / max(total_burst_notional_1s, eps)` where `eps = 1e-10` |
| **Range** | `[0.0, 1.0]` |
| **Overlap** | 分子・分母とも §2.3 に従うバーストを対象 |
| **Null/Zero** | バーストなし秒は `0 / max(0, eps) = 0.0` |
| **Edge cases** | `1.0` に近い = 全バーストが同一価格で吸収された状態（板が強い or passive execution）。`0.0` に近い = バーストが価格を動かしている（multilevel 優勢、aggressive execution） |
| **Market comparability** | 比率なので直接比較可能。マーケットの板厚・流動性の代理指標として有用 |

### 4.7 burst_delta_notional_1s（#21）

| Property | Value |
|---|---|
| **Column name** | `burst_delta_notional_1s` |
| **Data type** | `float64` |
| **Definition** | バースト notional の符号付きデルタ（買い方向の純資金流入） |
| **Formula** | `sum(b.burst_notional * (b.side == 'buy' ? 1 : -1) for b in overlapping)` |
| **Range** | `(-∞, +∞)` |
| **Overlap** | §2.3 |
| **Null/Zero** | バーストなし = 0。完全均衡（buy == sell）も 0 |
| **Edge cases** | 正 = 買い優勢、負 = 売り優勢。`burst_imbalance_ratio_1s` と異なり絶対量を保持するため、小さなバーストが多い場合と大きなバーストが少数の場合を区別できる。長時間バーストの重複カウントがデルタにも影響する |
| **Market comparability** | notional 依存のため cross-market 比較時は正規化必須（`burst_notional_vs_30s_traded_notional` と同様の手法） |

---

## 5. 監視 1 特徴量

**P1 契約:** P1a/P1b では #22 `outlier_trade_flag_1s` は常に `0` を出力する（`null` ではない）。0 は「P1 では観測値なし」を意味し、データ欠損とは異なる。P6 で実装予定。

### 5.1 outlier_trade_flag_1s（#22）

| Property | Value |
|---|---|
| **Column name** | `outlier_trade_flag_1s` |
| **Data type** | `uint8`（bitmask） |
| **Definition** | 1秒間の取引に異常値が含まれるかどうかのフラグ |
| **Formula** | 以下の条件のいずれかに該当する trade が存在した場合にフラグ ON:<br>Bit 0: `abs(trade.price - mid) / mid > 0.05`（±5% 以上乖離）<br>Bit 1: `trade.price * trade.qty > p99.9_notional`（全 trade の p99.9 超過）<br>Bit 2: `qty > 1000 BTC`（異常大ロット）<br>Bit 3: `trade.price <= 0 or trade.qty <= 0`（不正データ） |
| **Overlap** | 1秒 bucket 内の全 trade を対象（バースト overlap とは独立。バーストに含まれない trade も検査対象） |
| **Null/Zero** | 0 = 異常なし。バースト・取引がない秒も 0（異常 trade が存在しないため） |
| **Edge cases** | book 欠落で mid が取れない場合、Bit 0 の判定はスキップ（判定不能のまま他 bit は評価）。全 bit がクリアな秒を "clean" としてラベル付けし分析の基本セットとする。Bit 0 単独での判定は book の瞬間的乱高下の可能性を考慮 |
| **Market comparability** | 全マーケット共通の閾値で判定。p99.9 notional はマーケットごとに別計算（過去24時間 rolling window）。マーケット間のフラグ発生率の違い自体がデータ品質の指標となる |
| **Impact** | このフラグが立った秒の特徴量は分析から除外すべき。統計量計算時は winsorization との併用を推奨 |

---

## 6. 時間スケール仕様

### 6.1 1s Layer: Raw Features

**責務:** 最も粒度の細かい burst 特徴量を生成する。全22列を1秒単位で出力。P1 では 22 列すべてが物理的に存在し、#1-#12 は計算、#13 は `null`、#14 は `0`、#15-#22 は `0` で出力する（P1 契約上のプレースホルダ値。P4/P6 で実数に切り替え）。

- **解像度:** 1 second
- **特徴量:** 全22項目
- **Null policy:** バーストなし秒は全項目 `0`（ただし `burst_notional_vs_top_depth` のみ book 欠落時に NULL）
- **出力:** `data/derived/burst_features_v1/features_1s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`（block shard: 30行。日次追記方式 禁止。HH-MM-SS は 00 または 30 秒のみ）

**行形式:**
```json
{
  "ts": 1751821200000,
  "market": "binance_perp",
  "burst_count_1s": 3,
  "total_burst_notional_1s": 125000.5,
  "max_burst_notional_1s": 80000.0,
  ...
  "outlier_trade_flag_1s": 0
}
```

### 6.2 30s Layer: Aggregation Statistics

**責務:** 1s 系列から統計量を計算し、分析用の低ノイズなシグナルを提供する。

各特徴量に対する集約オペレーター（全22特徴量）:

| # | 1s Feature | 30s Aggregation Columns | Operators | Notes |
|---|---|---|---|---|
| 1 | `burst_count_1s` | `burst_count_mean_30s`, `burst_count_max_30s` | mean, max | — |
| 2 | `total_burst_notional_1s` | `burst_notional_overlap_sum_30s`, `burst_notional_max_30s`, `burst_notional_p95_30s` | sum, max, p95 | 重複カウント注意（Pitfall 2）。1s レイヤーからの sum は overlap により実勢より過大。重複なしの直接集計は `burst_notional_sum_30s`（追加集約指標）を参照 |
| 3 | `max_burst_notional_1s` | `max_burst_notional_max_30s`, `max_burst_notional_mean_30s` | max, mean | — |
| 4 | `max_burst_prints_1s` | `max_burst_prints_max_30s` | max | — |
| 5 | `max_burst_duration_ms_1s` | `max_burst_duration_max_30s` | max | — |
| 6 | `same_price_burst_count_1s` | `same_price_burst_count_sum_30s` | sum | — |
| 7 | `multilevel_burst_count_1s` | `multilevel_burst_count_sum_30s` | sum | — |
| 8 | `buy_burst_notional_1s` | `buy_burst_notional_sum_30s` | sum | — |
| 9 | `sell_burst_notional_1s` | `sell_burst_notional_sum_30s` | sum | — |
| 10 | `burst_imbalance_ratio_1s` | `burst_imbalance_mean_30s` | mean | 時間加重平均 |
| 11 | `largest_burst_share_notional_1s` | `largest_burst_share_max_30s` | max | — |
| 12 | `burst_notional_vs_30s_traded_notional` | `burst_notional_vs_30s_traded_max_30s` | max | — |
| 13 | `burst_notional_vs_top_depth` | `burst_notional_vs_top_depth_max_30s` | max | NULL 秒をスキップ |
| 14 | `burst_mid_move_bps_1s` | `burst_mid_move_mean_30s`, `burst_mid_move_max_30s` | mean, max | — |
| 15 | `same_price_burst_max_len_1s` | `same_price_burst_max_len_max_30s` | max | 30秒間の最大同価格バースト長 |
| 16 | `same_price_burst_notional_1s` | `same_price_burst_notional_sum_30s`, `same_price_burst_notional_max_30s` | sum, max | — |
| 17 | `multilevel_burst_max_span_ticks_1s` | `multilevel_burst_max_span_ticks_max_30s` | max | — |
| 18 | `multilevel_burst_max_span_bps_1s` | `multilevel_burst_max_span_bps_max_30s`, `multilevel_burst_max_span_bps_mean_30s` | max, mean | cross-market では bps 版が主指標 |
| 19 | `multilevel_burst_notional_1s` | `multilevel_burst_notional_sum_30s`, `multilevel_burst_notional_max_30s` | sum, max | — |
| 20 | `same_price_absorption_ratio_1s` | `same_price_absorption_ratio_mean_30s` | mean | 比率の時間平均。`[0,1]` 範囲 |
| 21 | `burst_delta_notional_1s` | `burst_delta_notional_sum_30s` | sum | 符号付きデルタの30秒累積。正=買い優勢累積 |
| 22 | `outlier_trade_flag_1s` | `outlier_trade_ratio_30s` | active_ratio | `flag > 0` だった秒の割合。`[0,1]` |

**追加集約指標:**
- `burst_active_ratio_30s`: 30秒間で `burst_count_1s > 0` だった秒の割合（範囲 [0, 1]）
- `burst_notional_sum_30s`: 30秒間の全バースト notional 合計（30s window 内の burst を**直接集計**。overlap による重複カウントは発生しない。§6.2 表 行#2 の `burst_notional_overlap_sum_30s` とは異なり、実勢 notional を表す）

### 6.3 5min Layer: Cross-Market / Z-Score / Regime

**責務:** マーケット間比較、異常検知、レジーム判定。

- **時間粒度:** 5 minutes（30s × 10）
- **マーケット数:** 15（全 BTC spot + perp）

**集約対象（30s → 5min）:**

| 30s Feature | 5min Aggregation |
|---|---|
| `burst_notional_sum_30s` | `burst_notional_sum_5min`（30s の合計） |
| `burst_count_mean_30s` | `burst_count_mean_5min`（30s の平均） |
| `burst_active_ratio_30s` | `burst_active_ratio_5min`（30s の平均） |
| `burst_mid_move_mean_30s` | `burst_mid_move_mean_5min`（30s の平均） |
| `burst_imbalance_mean_30s` | `burst_imbalance_mean_5min`（30s の平均） |

**Cross-market 演算:**

各 5min バケットで、15マーケット横断の統計量を計算:
- `burst_notional_zscore_5min`: `(market_value - cross_market_mean) / cross_market_std`（各マーケットの burst notional を cross-sectional z-score 化）
- `burst_notional_percentile_5min`: 15マーケット中のパーセンタイル順位 [0, 100]
- `spot_perp_divergence_5min`: `spot_avg_burst_notional - perp_avg_burst_notional`（spot と perp のバースト活動乖離）

**Regime 判定（5min）:**
- `regime_burst_intensity_5min`: `low / medium / high / extreme`（cross-market z-score に基づく四分位分類）
- `regime_burst_imbalance_5min`: `buy_driven / neutral / sell_driven`（imbalance_mean ±0.3 で分類）

---

## 7. データフロー

```mermaid
flowchart TB
    subgraph Receiver["agg-btc-receiver (30s blocks)"]
        direction LR
        T[trades/<market>/<date>/HH-MM-SS.jsonl]
        AT[agg_trades/<market>/<date>/HH-MM-SS.jsonl]
        BU[book_updates/<market>/<date>/HH-MM-SS.jsonl]
        BS[snapshots/<market>/<date>/HH-MM-SS.jsonl]
    end

    subgraph Reducer["agg-btc-reducer (burst pipeline)"]
        direction TB
        R[read 30s blocks<br/><b>P1 read-only: 削除禁止</b>]
        BD[burst detection<br/>BurstBuilder<br/>gap=50ms, max_dur=5000ms]
        BS2[book state replay<br/>replayBestBookState<br/><b>P4 まで不使用</b>]
        F1[1s feature computation<br/>overlap-based<br/>22 columns (P1: #1-#12 computed,<br/>#13 null, #14=0, #15-#22=0)]
        F30[30s aggregation<br/>mean/sum/max/p95<br/>burst_active_ratio<br/><b>P3 で実装</b>]
        F5[5min cross-market<br/>z-score / percentile<br/>spot-perp divergence<br/>regime classification<br/><b>P5 で実装</b>]
    end

    subgraph Output["data/derived/burst_features_v1/（以下のパスはすべてこのベースからの相対パス）"]
        direction LR
        J1[features_1s/<market>/<YYYY-MM-DD>/HH-MM-SS.jsonl<br/>block shard, 30 rows each]
        J30[features_30s/<market>/<YYYY-MM-DD>/HH-MM-SS.jsonl<br/>block shard]
        J5[features_5min/<market>/<YYYY-MM-DD>/HH-MM-SS.jsonl<br/>block shard]
    end

    T --> R
    AT --> R
    BU -.-> R
    BS -.-> R
    R --> BD
    R -.-> BS2
    BD --> F1
    BS2 -.-> F1
    F1 --> J1
    F1 -.-> F30
    F30 -.-> J30
    F30 -.-> F5
    F5 -.-> J5
```

### 処理順序

1. **Read:** 未処理の30秒ブロックを古い順に読み込む（P1: `trades/` + `agg_trades/` のみ。book 系は P4 まで読まない）。**P1 では raw データを一切削除/移動しない。クリーンアップは P6 で実装。**
2. **Burst detection:** raw trades から `BurstBuilder` で burst 検出（trade-only, depth 不使用）
3. **1s features:** overlap 判定で各秒の burst 特徴量を計算。P1 では全22列を物理出力（#1-#12 計算、#13=`null`、#14=`0`、#15-#22=`0`）
4. **30s aggregation:**（P3 将来）1s 系列から30秒集約統計 → `data/derived/burst_features_v1/features_30s/...`
5. **5min cross-market:**（P5 将来）30s 系列から5分横断指標 → `data/derived/burst_features_v1/features_5min/...`

### ブロック境界をまたぐ burst の扱い

30秒ブロック単位で処理する Receiver からのデータ入力において、burst がブロック境界をまたぐケースに対応するため、**1-block lag + checkpoint 永続化方式**を採用する。

**方式: 1-block lag with commitFinalizedBlock + checkpoint persistence**
- block N+1 の raw trades を読み込み、input SHA256 を計算し、全 trades を検証する
- N+1 の全 trades（完全ソート済みシーケンス）を `BurstDetector.feedTrades()` に投入
- `max_burst_duration=5000 < 30000` の保証により、N-origin の全 burst は N+1 の全 trades 投入後に完全に確定する
- N の rows をメモリ上で**初めて**計算（N+1 投入後の確定済み burst 状態を使用）
- `commitFinalizedBlock(finalizedBlock=N, nextPendingBlock=N+1_info, nextDetectorState, rows, manifestInputs, gen, commitId)` で N を一度だけコミット
- N+1 が次 pending となり、checkpoint に `last_committed=N` + `pending_block=N+1_info` が保存される
- N+1 は N のトランザクション内でコミットされない
- チェックポイントには `trade_input_sha256`, `auxiliary_input_hashes`, `replay_identity`, `open_burst_before_N1` を永続化
- 再起動時に checkpoint から復元し、決定論的に同一出力を得る
- EOF 時は `flushAll()` で最終 pending block を finalize

**絶対ルール（全ブロック共通）:** 同一block内の ts decrease は E004 quarantine/fail（正規化禁止）。同一 ts は許可し、順序は `(ts, hasTradeId ? 0 : 1, normalizedTradeIdOrEmpty, source_file_line_index)` で一意に決める。

### 空ブロックのウォーターマーク処理（empty-next-block watermark）

1-block lag における重要な特殊ケースとして、block N が pending open burst を持ち、block N+1 が空（trade 0 件）の場合:

1. block N+1 の raw trades ファイルが空（0 trades）であることを検証。空でもファイルは存在するため watermark = N+1 block end（= N_start + 60000ms）が有効
2. N+1 の agg_trades lookback（`[N_start-30000, N_end)`）を検証
3. N+1 を detector に投入（空のため open burst に影響なし）
4. `max_burst_duration_ms=5000`、`gap_threshold_ms=50` により N+1 が空 → N の open burst 継続不可能が確定
5. N の rows を初回計算（N+1 投入後の確定済み状態で）→ `commitFinalizedBlock()` でコミット
6. N+1 自身は新 pending として設定（checkpoint に N+1 の identity 情報 + open_burst_before_N1 を保存）
7. N+2 の処理時に N+1 の rows（全ゼロ）を計算 → N+1 をコミット
8. EOF 時は `flushAll()` で最終 pending を finalize

**fixture 要件:** N tail open → N+1 empty → N+2 first trade の 3 ブロックで byte-identical 出力。restart after N before N+1 も同出力。N tail open → N+1 empty → EOF の flushAll ケース。checkpoint/マニフェストが byte-identical であること。`auxiliary_input_hashes` が manifest に含まれていること。

---

## 8. 出力形式

### 8.1 1s features

```
data/derived/burst_features_v1/features_1s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

- 形式: JSONL（1 block shard = 30行 = 30秒）
- block shard（日次追記方式 禁止）
- 各行: `{ts, market, <feature_1>, ..., <feature_N>}`
- `ts`: epoch ms（秒境界、`ts % 1000 == 0`）
- メタデータ: ファイル名から `market`, `date`, `block_start` を復元可能
- consumers は manifest index から block shard 一覧を取得

### 8.2 30s features

```
data/derived/burst_features_v1/features_30s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

- 形式: JSONL（1 block shard = 1行 = 30s window）
- 各行: `{ts, market, burst_count_mean_30s, burst_notional_sum_30s, ..., burst_active_ratio_30s}`
- `ts`: window start epoch ms
- HH-MM-SS は 00 または 30 秒のみ（30s window 開始境界）

### 8.3 5min features

```
data/derived/burst_features_v1/features_5min/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

- 形式: JSONL（1行 = 5分 window × 1 market）
- 各行: `{ts, market, burst_notional_sum_5min, burst_notional_zscore_5min, burst_notional_percentile_5min, ..., regime_*}`
- `ts`: window start epoch ms
- HH-MM-SS: seconds=`00`、UTC minute は 5 の倍数（0,5,10,15,...,55）。00/30 秒制約は 1s/30s layer のみに適用され、5min layer には適用されない

---

## 9. 品質・ガードレール

### 9.1 符号規則

| Feature | Sign Convention |
|---|---|
| `burst_count_1s` | ≥ 0（unsigned） |
| `total_burst_notional_1s` | ≥ 0 |
| `max_burst_notional_1s` | ≥ 0 |
| `buy_burst_notional_1s` | ≥ 0 |
| `sell_burst_notional_1s` | ≥ 0 |
| `burst_imbalance_ratio_1s` | [-1.0, 1.0] |
| `largest_burst_share_notional_1s` | [0.0, 1.0] |
| `burst_delta_notional_1s` | (-∞, +∞); 正 = 買い優勢 |
| `burst_mid_move_bps_1s` | (-∞, +∞); 正 = mid 上昇 |
| `same_price_absorption_ratio_1s` | [0.0, 1.0] |
| その他すべてのカウント/notional | ≥ 0 |

### 9.2 Null vs 0 ルール

**P1 契約での値セマンティクス:**

| 状況 | 値 | 意味 |
|---|---|---|
| バーストなし秒 (#1-#12) | 0 | バーストなし。trade 有無は `trade_count_this_second` で判定 |
| trade なし秒 | 全特徴量 0 | バースト確認不能だがデータ欠損ではない |
| book 欠落 + `burst_notional_vs_top_depth` (#13) | **`null`**（P1 では常に） | book がないことを明示。`_quality.book_seeded: false` |
| book 欠落 + `burst_mid_move_bps_1s` (#14) | **`0`**（P1 では常に。`null` ではない） | P1 契約上の観測値なし。P4 で実数に切り替え |
| 研究項目 (#15-#21) | **`0`**（P1 では常に。`null` ではない） | P1 契約上の観測値なし。P6 で実数に切り替え |
| 監視 (#22) | **`0`**（P1 では常に。`null` ではない） | P1 契約上の観測値なし。P6 で実数に切り替え |
| 計算不能な比率（0/0） | 0（eps 加算で NaN 回避） | |

### 9.2a `_quality` フィールド契約

全 1s 出力行は `_quality` オブジェクトを備える。各フィールドの意味と契約:

| フィールド | 型 | P1 契約 | 意味 |
|---|---|---|---|
| `book_seeded` | `boolean` | P1 では常に `false` | 板リプレイがシード済みか。P4 で `true` に切り替え。 |
| `trade_count_this_second` | `number` | 計算（当該秒の raw trade 数） | 0 は取引なし秒。バーストなしでも trade の有無を判定可能。 |
| `warmup` | `boolean` | checkpoint なし初回ブロックのみ `true` | 再起動で checkpoint 復元できれば `false`。30 blocks ではない（初回 1 ブロックのみ）。 |
| `input_block_ids` | `string[]` | この秒の計算に使われた raw trade ブロック ID | **raw trade ブロック ID のみを含む。** 1s row for block N の場合、常に `[N]`（単一要素）。agg_trades のハッシュは manifest の `auxiliary_input_hashes` にのみ存在し、`input_block_ids` には含めない。 |

**根拠:** `input_block_ids` は当該行の burst 特徴量計算に直接使われた trade 入力ブロックを追跡する。agg_trades は補助入力であり、行レベルの再現性に直接関与しないため manifest レベルの `auxiliary_input_hashes` で管理する。行レベルと manifest レベルで責務を分離することで、`input_block_ids` の解釈が曖昧になるのを防ぐ。

### 9.3 外れ値取引処理

**検出:**
- `outlier_trade_flag_1s`（§5.1）でフラグ
- notional p99.9 は過去 24 時間の全 trade から計算（rolling window）

**Winsorization（外れ値対策）:**
- `total_burst_notional_1s` 計算時: 個々の trade notional を p99.9 で winsorize（capping）
- 生の値と winsorized 値の両方を保持するかは v2 検討（v1 では winsorized のみ出力）

**推奨除外ルール:**
- 分析時: `outlier_trade_flag_1s > 0` の秒を除外
- または: p99.9 notional を超える trade を含む burst 自体をフィルタリング

### 9.4 Book Alignment Confidence

Book coverage tier（historical reference: FeatureAccumulator の `BOOK_COVERAGE_TIERS` 定義を流用）:

| Tier | Markets | Depth Coverage | 影響 |
|---|---|---|---|
| **Tier A** | coinbase_spot, bitmex_perp, binance_spot, binance_spot_usdc, kraken_spot | Full book（1000+ levels） | `burst_notional_vs_top_depth` 信頼性最高。top-of-book notional が正確 |
| **Tier B** | binance_perp, binance_perp_btcusdc, bybit_perp | Snapshot-based, limited mid-depth | 信頼性中。top-of-book は正確だが深度全体が過小評価される可能性あり |
| **Tier C** | okx_perp, okx_spot, bybit_spot, bitstamp_spot, bitfinex_spot, crypto_com_spot, hyperliquid_perp | Bounded depth near book（loosely defined） | 信頼性低。深度が浅く見える可能性あり。top-of-book notional が過小になる傾向 |

**`burst_notional_vs_top_depth` 使用時の注意:** Tier C マーケットは深度が過小評価される可能性があるため、この特徴量を cross-market 比較に使う際は tier を共変量として含める。

### 9.5 イベント順序規則

```text
同一タイムスタンプ内の処理順序:
  1. book snapshot（book_update_snapshot）— その時刻の基準 book state を確定
  2. book_updates — book state を最新化
  3. trades — 最新の book state に対して取引を評価

優先度（参照実装: burst-agg.mjs L243-252）:
  snapshot_file = 0
  book_update_snapshot = 0
  book_update_update = 1
  → 数値が小さい方が先に処理される
```

**Trade vs book の alignment:**
- trade event の ts と book update の ts が同一の場合、book state はその瞬間の最終状態を反映
- burst の mid_before 取得時は `burst_start_ts - 1ms` で直前の book state を照会
- これにより trade 自体が book に与えた影響を含まずに "pre-burst" state を取得

### 9.6 データ整合性

- **duplicate prevention:** `appendJsonlIfNew` 関数（burst-agg.mjs L515-539）により、既存 ts の重複書き込みを防止
- **date rollover:** UTC 0時をまたぐ場合、別ファイルに書き込み。`_ensureDate()` で前日の writer をクローズ
- **partial seconds:** trade が属する秒は `ts - (ts % 1000)` で決定。ミリ秒以下は切り捨て

---

## 10. 決定理由

### 10.1 なぜ trade-only burst detection か

1. **シンプルさと信頼性:** depth-based burst detection は book update の信頼性に依存する。Tier C マーケットでは book が不完全であり、depth を使ったバースト検出は false positive/negative を増やす
2. **計算効率:** `BurstBuilder` は O(N) の単一パスで全 burst を検出可能
3. **再現性:** trade data は最も基本的で普遍的なデータソース。book データが欠落しても burst 検出は可能
4. **book は context として使う:** burst 検出自体には使わないが、mid_move, vs_top_depth などの文脈特徴量で book を使用

### 10.2 なぜ gap_threshold=50ms, max_duration=5000ms か

- **50ms:** 単一 aggressive order の約定間隔が典型的に 10-100ms。50ms は同方向連続取引を単一 burst にまとめる適切な閾値
- **5000ms:** 長時間の大きなアルゴリズム執行を burst として捕捉するが、通常のマーケットメイク活動との分離のための上限
- 旧 burst-agg.mjs では gap=100ms, max_dur=3000ms が使われていた。v1 では以下の理由で変更:
  - gap を短く = より細かい burst 粒度（吸収検知の精度向上）
  - max_dur を長く = 大口執行の捕捉漏れ防止

### 10.3 なぜ 1s / 30s / 5min の3層か

| Layer | 目的 | 出力先 |
|---|---|---|
| **1s** | 最小粒度の生データ。全 downstream 分析の入力 | `data/derived/burst_features_v1/features_1s/` |
| **30s** | ノイズ低減。Receiver の30秒ブロック単位と整合。チャート描画・ダッシュボードに最適 | `data/derived/burst_features_v1/features_30s/` |
| **5min** | Cross-market 比較。マクロな regime 判定。z-score/percentile でマーケット横断の異常検知 | `data/derived/burst_features_v1/features_5min/` |

### 10.4 なぜ `largest_burst_share_notional_1s` と `burst_notional_vs_30s_traded_notional` の両方が必要か

- `largest_burst_share`: 1秒内の集中度（単一支配バーストか分散か）→ microstructure
- `burst_notional_vs_30s`: そのマーケットの通常取引量に対する相対的重要性 → cross-market 比較

### 10.5 なぜ `burst_notional_vs_top_depth` のみ NULL 許容か

この特徴量は book data に依存する唯一の MVP 項目。Tier C マーケットや book update 欠損時間帯では計算不能。0 にすると「深度ゼロの板」と「データ欠損」が区別できなくなるため、NULL を許容。

### 10.6 なぜ research feature は MVP から分離したか

- MVP 14 項目 = 全 downstream 分析の最小必須セット
- Research 7 項目 = 探索的分析用。計算コストが高い、または解釈が実験段階
- 分離することで、本番パイプライン（MVP）と研究パイプライン（research）を独立に実行可能

---

## 11. よくある誤解の Pitfalls

### Pitfall 1: 1秒間にバーストが「ない」ことと「0」の区別

❌ **誤解:** `burst_count_1s = 0` はバーストがない = 取引がない = データ欠損
✅ **正解:** `burst_count_1s = 0` は「その秒に overlap するバーストがない」こと。取引はあってもバースト形成条件（同 side 連続 + gap <= 50ms）を満たさない可能性がある
✅ 取引有無は `trade_count`（別途取得）で判断。データ欠損はファイルの有無で判断

### Pitfall 2: total_burst_notional_1s の重複カウント

❌ **誤解:** 30秒合計を取るとき `sum(total_burst_notional_1s)` で良い
✅ **正解:** 長時間バーストは複数秒で重複カウントされるため、`sum(total_burst_notional_1s over 30s) > actual_30s_burst_notional` になる
✅ 30秒の正しい burst notional を取得するには、30s layer で burst を window 単位で直接集計する

### Pitfall 3: burst_mid_move_bps の秒境界アンカー

❌ **誤解:** 秒境界（bucket start / bucket end）の mid を使えばよい
✅ **正解:** burst の前後 mid は **event-time anchor**（burst_start_ts - 1ms, burst_end_ts + 1ms）で取得する。秒境界の mid はバーストと無関係な price action を含む可能性がある
✅ 参照実装: §3.14 の formula に明記

### Pitfall 4: bursting と non-bursting の分離

❌ **誤解:** burst 中の全 trade が burst print、残りが non-burst print
✅ **正解:** burst は `BurstBuilder` で形成された構造に属する trade のみ。`_idx` ベースで membership を判定
✅ 参照実装: `burst-agg.mjs` L332-337 の `burstPrintIdx` Set

### Pitfall 5: same_price vs multilevel の境界

❌ **誤解:** 価格が動いても tick size 内なら same_price
✅ **正解:** `distinct_price_count` は生の price 値のユニーク数。`99.5, 99.5` → same_price（count=1）。`99.50, 99.51` → multilevel（count=2）
✅ tick size は span_ticks 計算のみに使用。分類には使わない

### Pitfall 6: cross-market 比較に生の notional を使う

❌ **誤解:** binance_perp の burst notional と bitstamp_spot の burst notional を直接比較できる
✅ **正解:** マーケットの取引量が桁違いのため不可。必ず `burst_notional_vs_30s_traded_notional` か z-score で正規化する
✅ v1 では z-score は 5min layer で提供

### Pitfall 7: outlier flag が立った秒のデータを全部捨てる

❌ **誤解:** `outlier_trade_flag_1s > 0` の秒は完全に無視すべき
✅ **正解:** Bit 0（±5%価格乖離）のみが立っていて他がクリアなら、book の瞬間的な乱高下の可能性。分析の目的に応じて選択的に除外する
✅ 全 bit がクリアな秒を "clean" としてラベル付けし、分析の基本セットとする

### Pitfall 8: 30s window 境界での burst 扱い

❌ **誤解:** burst_start_ts が window [ws, we) に含まれていればその window の burst
✅ **正解:** burst-agg.mjs の `computeSummary` は `burst_start_ts >= ws && burst_start_ts < we` で割り当てるが、1s feature の `computeFeatures` は **overlap** を使用する
✅ 両者の基準が異なることに注意（前者は window 割り当て、後者は秒単位の存在検知）

---

## 12. マーケット一覧

v1 対象の 15 BTC マーケット:

| # | Market | Type | Tier |
|---|---|---|---|
| 1 | `binance_spot` | spot (USDT) | A |
| 2 | `binance_spot_usdc` | spot (USDC) | A |
| 3 | `binance_perp` | perp (USDT) | B |
| 4 | `binance_perp_btcusdc` | perp (USDC) | B |
| 5 | `bybit_perp` | perp | B |
| 6 | `bybit_spot` | spot | C |
| 7 | `okx_perp` | perp | C |
| 8 | `okx_spot` | spot | C |
| 9 | `coinbase_spot` | spot (USD) | A |
| 10 | `kraken_spot` | spot (USD) | A |
| 11 | `hyperliquid_perp` | perp | C |
| 12 | `bitmex_perp` | perp | A |
| 13 | `bitstamp_spot` | spot | C |
| 14 | `crypto_com_spot` | spot | C |
| 15 | `bitfinex_spot` | spot | C |

---

## 13. 参照実装

| ファイル | 内容 |
|---|---|
| `agg-btc-receiver/lib/burst-builder.mjs` | BurstBuilder: gap_threshold + max_duration による burst 形成 |
| `agg-btc-receiver/lib/feature-accumulator.mjs` | FeatureAccumulator: 1s 特徴量計算 + JSONL 出力 + book depth 管理<br>**注:** このファイルは Receiver から削除済み。tier 定義のみ historical 参照として使用 |
| `agg-btc-receiver/lib/trade-aggregator.mjs` | TradeAggregator: OHLCV + サイズバケット（small/medium/large） |
| `agg-btc-receiver/lib/trade-size-buckets.mjs` | サイズ閾値定義: small < $1k, medium $1k-$10k, large ≥ $10k |
| `agg-btc-receiver/scripts/burst-agg.mjs` | burst-agg CLI: 30s window 処理 + summary/features 出力 + cleanup |
| `agg-btc-receiver/lib/replay-book-state.mjs` | replayBestBookState: book_updates からの book state 時刻再現 |
| `agg-btc-receiver/docs/downstream-design-handoff.md` | 後段パイプライン設計方針（本仕様の上位設計） |
| `agg-btc-receiver/docs/worklog/2026-07-09-burst-feature-spec.md` | 作業ログ（22項目分類の決定経緯） |

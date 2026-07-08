# CVD Size Bucket Thresholds — 修正設計書

> 作成: 2026-06-30
> 対象: agg-btc-receiver
> 現状課題: Medium ($1k-$10k) と Large (>= $10k) の notional 閾値が近すぎて、
> チャート上で中口と大口の挙動に差が出ない。

---

## 現状

| バケット | 閾値 (USD notional) | BTC換算 (@$100k) |
|---|---|---|
| Small | < $1K | < 0.01 BTC |
| Medium | $1K – $9,999 | 0.01 – 0.1 BTC |
| Large | >= $10K | >= 0.1 BTC |

**問題点:**
- Medium と Large の間が1桁しかない
- BTC $100k において `$10K = 0.1 BTC` は「大口」と呼べない
- Coinalyze 等の業界標準では `$100K` からが「Large」扱い

---

## 業界リファレンス

| プラットフォーム | 分類数 | Large開始 | Whale開始 |
|---|---|---|---|
| **Coinalyze** | 7 | $100K | $1M |
| **Hyblock** | 4+ | $100K | $1M |
| **CoinGlass** | 2 (whale filter) | — | $3M–$5.5M |
| **Kiyotaka** (推奨) | カスタム | $100K+ | — |

業界共通のパターン: **対数スケール（order-of-magnitude）でバケットを区切る。**

---

## パターン案

### 案A: 3バケット調整（撤回済み）

Receiver 側の分類ロジックは3バケット維持し、閾値のみ `$100K` に上げる案。

| バケット | 閾値 | BTC換算 (@$100k) |
|---|---|---|
| Small | < $1K | < 0.01 BTC |
| Medium | $1K – $99,999 | 0.01 – 1 BTC |
| Large | >= $100K | >= 1 BTC |

**撤回理由:**
- 現行3本CVDの Large としては `$100K+` が sparse すぎる。
- `$100K+` は Large ではなく Whale として別bucketにすべき。
- 現行 `1s_features` schema は `$1K/$10K` 契約なので、ラベルだけ変えると誤表示になる。

**実装しない。** v1は `$1K/$10K` 維持、v2候補で `$100K+` を `Whale` として追加する。

---

### 案B: 4バケット（Coinalyze準拠・簡略版）

Small / Medium / Large / Whale の4分類。Coinalyze の7段階をビジュアライズ用に統合。

| バケット | 閾値 | BTC換算 (@$100k) |
|---|---|---|
| Small | < $1K | < 0.01 BTC |
| Medium | $1K – $9,999 | 0.01 – 0.1 BTC |
| Large | $10K – $99,999 | 0.1 – 1 BTC |
| Whale | >= $100K | >= 1 BTC |

**変更範囲（大）:**
- `lib/trade-aggregator.mjs`: 分類関数を `'whale'` を含む4値に拡張、`TradeAggregatedRow` に `whale_volume`, `whale_count` 追加
- `lib/feature-accumulator.mjs`: 出力カラムに `buy_whale_qty`, `sell_whale_qty`, `buy_whale_count`, `sell_whale_count` 追加
- `scripts/cvd_size_buckets.py`: 4バケット対応に全面改修（load_data のカラム、CVD計算、plot関数、legend）

**トレードオフ:** 変更量が大きい。JSONL スキーマ変更あり。一方でCoinalyzeとの比較が容易。

---

### 案C: 4バケット（対数スケール・バランス型）

Small / Medium / Large / Whale。閾値を綺麗な対数間隔に設定。

| バケット | 閾値 | BTC換算 (@$100k) |
|---|---|---|
| Small | < $10K | < 0.1 BTC |
| Medium | $10K – $99,999 | 0.1 – 1 BTC |
| Large | $100K – $999,999 | 1 – 10 BTC |
| Whale | >= $1M | >= 10 BTC |

**特徴:**
- Small の範囲が広がる（`$0-$10K`）。従来の Small + Medium を包含。
- Large が `1–10 BTC`、Whale が `10 BTC+` と心理的に綺麗
- 各バケット間がちょうど1桁ずつ開く

**変更範囲:** 案Bと同じ（4バケット化のため全層修正）

---

## 初期案の比較（記録用・案Aは撤回済み）

| 観点 | 案A (3bucket $100K Large / 撤回) | 案B (4bucket Coinalyze) | 案C (4bucket log) |
|---|---|---|---|
| 変更量 | 小 | 大 | 大 |
| JSONL互換 | ◯（カラム不変） | ✗（カラム追加） | ✗（カラム追加） |
| 過去データ再読込 | △（閾値不整合あり） | △（同左） | △（同左） |
| 業界比較 | △（Coinalyzeと非対応） | ◯（Coinalyze近い） | ◯（論理構造が綺麗） |
| チャートの視認性 | 一見差は出るが sparse | 4本でやや密集 | 4本だがレンジ差大 |
| CVD の解釈性 | **NG: Large が Whale 化する** | Large/Whale の分離が良い | 各 bucket の意味が直感的 |

---

## 現行推奨

- **v1（現行3bucket）は `$1K/$10K` を維持する。**
- **案Aは実装しない。** `$100K+` は3bucketの Large ではなく、v2の `Whale` bucket として扱う。
- Coinalyze比較や Whale 分離を重視するなら、別フェーズで **案Bベースの4bucket** を設計する。
- Burst / participant 目線はさらに別軸として扱い、現行 per-trade USD notional schema に混ぜない。

---

## 再検討結果（2026-06-30）

**結論: 直近の案A（Large `$100K+` への3バケット変更）は撤回。v1 は `$1K/$10K` を維持する。**

理由:

1. **実装経路の不整合が判明**
   - `scripts/cvd_size_buckets.py` が読む `data/1s_features/*/*.jsonl` は `lib/feature-accumulator.mjs` で生成される。
   - `feature-accumulator.mjs` の分類は `$1K/$10K` のまま。
   - `lib/trade-aggregator.mjs` だけ `$100K` にしても、現行チャートの入力には反映されない。
   - したがって `$100K` ラベル表示は **誤表示** になる。

2. **実データ分布では `$100K+` は sparse すぎる**

   2026-06-29 raw trade JSONL（隣接重複除外）で確認:

   | Market type | Trades | `<$1K` count/qty/sec | `$1K-$10K` count/qty/sec | `$10K-$100K` count/qty/sec | `$100K+` count/qty/sec |
   |---|---:|---:|---:|---:|---:|
   | Perp | 13,981,678 | 78.96% / 4.64% / 99.71% | 15.87% / 22.90% / 95.53% | 4.94% / 52.96% / 74.04% | 0.23% / 19.50% / 15.29% |
   | Spot | 8,303,673 | 89.29% / 11.72% / 99.94% | 9.92% / 53.17% / 71.44% | 0.78% / 29.65% / 20.19% | 0.02% / 5.46% / 0.83% |

   - `$10K-$100K` は perp では **74%の秒に出現**し、CVD線として十分な密度がある。
   - `$100K+` は spot では **0.83%の秒**しか出ず、3本しかないCVD線の Large としては薄すぎる。
   - `$100K+` は Large ではなく **Whale** として別扱いすべき。

3. **Codex 独立レビューの結論とも一致**
   - baseline CVD は **per-trade USD notional** が妥当。
   - v1 3バケット維持なら `Small<$1K / Medium $1K-$10K / Large>=$10K` が最良。
   - `$100K+` は whale filter であり、3バケットの Large にすると信号密度が落ちる。
   - long-term は 4バケット `Small<$1K / Medium $1K-$10K / Large $10K-$100K / Whale >=$100K` がよい。

---

## 確定事項（v1）

現行 `1s_features` schema の size bucket は以下で固定する。

| Bucket | USD notional | 用途 |
|---|---:|---|
| Small | `< $1K` | retail / tiny flow |
| Medium | `$1K-$10K` | meaningful small flow |
| Large | `>= $10K` | market-impact flow（whaleではない） |

**重要:** `Large` という名前は「大口/whale」ではなく、**CVD用の高notional flow** を意味する。

**実装契約:**
- 閾値と分類関数は `lib/trade-size-buckets.mjs` に集約する。
- `lib/trade-aggregator.mjs` と `lib/feature-accumulator.mjs` は同じ `classifyTradeNotional()` を使う。
- チャート側 `scripts/cvd_size_buckets.py` は `1s_features` の既存列を読むため、ラベルは `$1K/$10K` schema と一致させる。

---

## v2候補（別フェーズ）

スキーマ変更を許容するなら、次は4バケット化する。

| Bucket | USD notional | 解釈 |
|---|---:|---|
| Small | `< $1K` | tiny / retail |
| Medium | `$1K-$10K` | active retail / small participant |
| Large | `$10K-$100K` | high notional market-impact flow |
| Whale | `>= $100K` | whale / institutional slice |

v2では sideごとに `qty`, `count`, `notional` を保存する。

例:
- `buy_small_qty`, `buy_small_count`, `buy_small_notional`
- `buy_medium_qty`, `buy_medium_count`, `buy_medium_notional`
- `buy_large_qty`, `buy_large_count`, `buy_large_notional`
- `buy_whale_qty`, `buy_whale_count`, `buy_whale_notional`
- sell側も同様

Burst / cluster based (`0.1 BTC / 1 BTC`) は base schema に混ぜず、**別の派生特徴量**として設計する。

---

## 未着手（次フェーズ）

- per-market チャートへの凡例追加
- dpi / キャンバスサイズ調整
- seek 精度 / live file 対策
- v2 4バケット schema 設計
- burst-based participant CVD 設計

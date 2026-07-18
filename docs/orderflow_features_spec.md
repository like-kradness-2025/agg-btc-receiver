# OrderFlow P0/P1/P2 特徴量スペック

> Phase 1 棚卸し + Phase 2 設計書（日本語）
> 作成者: coder (t_1159c01e)

## 1. Phase 1 現状把握

### Raw payload 実態（data/live_v3/ 直接確認）

**trades JSONL — 全15market共通:**
- キー: `market`, `price`(float), `qty`(float), `side`("buy"|"sell"), `ts`(epoch ms), `tradeId`(str)
- side は必ず "buy" / "sell"。qty=0 は理論上あり得るが、実測では未出現
- ts は exchange 刻み（ms精度）。market 間で重複なし（market isolation）

**book_updates JSONL — 全 market 共通:**
- キー: `market`, `type`("update"), `bids`([[price_str, qty_str], ...]), `asks`([[price_str, qty_str], ...]), `ts`(epoch ms), `seq`(int, 市場固有)
- update semantics: **qty diff**（Binance）。qty=0 → そのprice level削除。qty>0 → upsert
- snapshot field なし（REST seed が別途必要、downstream.py が処理）
- Hyperliquid は type="update" のみ、snapshot field なし（部分market）
- Bitstamp: book_updates あるが coverage 低い

### 現行 features_1s schema（33列 → v2 で39列に拡張）

| カテゴリ | 列数 | 内容 |
|---|---|---|
| Envelope | 2 | ts, market |
| Burst #1-#12 | 12 | burst_count, total/max notional, prints, duration, buy/sell notional, imbalance, share, same_price/multilevel counts, vs_30s_traded |
| Book-dep #13-#14 | 2 | vs_top_depth (nullable), mid_move_bps (0固定) |
| Research #15-#21 | 7 | same_price_max_len, same_price_notional, multilevel span ticks/bps, multilevel_notional, absorption_ratio, delta_notional |
| Monitoring #22 | 1 | outlier_trade_flag (0固定) |
| Book B1-B9 | 9 | mid_price, spread_bps, depth 100/1000 bid/ask, imbalance 100/1000, microprice |
| **P0 v2** | **6** | **trade_count, traded_notional, signed_volume, imbalance_qty, RV_10s, RV_60s** |

### Exchange capability matrix

| market | trades | book_updates | seq # | snapshot type | OFI適用 |
|---|---|---|---|---|---|
| binance_spot | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| binance_perp | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| binance_perp_btcusdc | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| binance_spot_usdc | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| bybit_spot | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| bybit_perp | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| okx_spot | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| okx_perp | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| coinbase_spot | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| kraken_spot | ✓ | ✓ diff | ✓ | WS+REST | ✓ |
| crypto_com_spot | ✓ | ✓ diff | ✓ | WS+REST | partial |
| bitfinex_spot | ✓ | ✓ diff | ✓ | WS+REST | partial |
| hyperliquid_perp | ✓ | ✓ diff | ✓ | WS only | partial |
| bitmex_perp | ✓ | ✓ diff | ✓ | WS+REST | partial |
| bitstamp_spot | ✓ | ✓ diff | ✓ | WS+REST | trade_only |

## 2. P0 採用・判断

### 採用 P0（v2 schema、即実装）

| 特徴量 | 列名 | 単位 | window | null semantics | 根拠 |
|---|---|---|---|---|---|
| trade count | `trade_count_1s` | count | 1s | 0（bucket空でも） | 基本統計。burst_countとの比でburst density測れる |
| traded notional | `traded_notional_1s` | USD | 1s | 0.0 | 出来高の絶対値。imbalance正規化に必要 |
| signed volume | `signed_volume_1s` | BTC (qty) | 1s | 0.0 | CVD増分。buy-sellの符号付き数量 |
| trade imbalance (qty) | `trade_imbalance_qty_1s` | ratio [-1,1] | 1s | 0.0 | 売買圧の正規化指標。burst imbalanceとは独立（全trade対象） |
| realized vol 10s | `realized_vol_10s` | log-return std | 10s rolling | **null during warmup** | 価格変動率。3 trade未満でnull |
| realized vol 60s | `realized_vol_60s` | log-return std | 60s rolling | **null during warmup** | 中期変動率。RVZ比で正規化 |

### P0 設計詳細

**signed_volume_1s:**
- 定義: sum(qty where side="buy") - sum(qty where side="sell") in [ts, ts+1000)
- CVD（Cumulative Volume Delta）の増分。積算は不要（downstreamで計算可能）
- lookahead防止: bucket内のtradeのみ使用

**trade_imbalance_qty_1s:**
- 定義: (buy_qty - sell_qty) / (buy_qty + sell_qty) in 1s bucket
- 分母=0（trade無し）→ 0.0（burst_imbalance_ratio_1sと整合）
- burst imbalance（#9）との違い: burstベース vs 全tradeベース。相関高いが非同一

**realized_vol:**
- 定義: window内のtrade price系列のlog-return (log(p_i/p_{i-1})) のpopulation std
- warmup: window内にtrade 3件未満（log-return 2件未満）で null
- lookahead防止: [at_ts - window_ms, at_ts) のみ使用（現在秒を含まない）
- RVZ（realized vol z-score）は別段で計算可能（rolling mean/std against longer baseline）

### 保留 P1（実装せず、列も作らない）

| 候補 | 保留理由 |
|---|---|
| OFI (Order Flow Imbalance, Cont-style) | book_updates がdiff semantics (qty=0で削除)のため、best bid/ask price change の追跡が現状snapshot_atに依存。top-of-book更新が全marketで使えないためP0昇格は見送り |
| spread/depth delta | book_imbalance_100/1000 (B7/B8) が既に静的imbalanceを保持。delta（前秒差分）は後段で計算可能。列重複回避 |
| cancel/add/replenishment | raw update が数量deltaのみ。add vs cancel の識別は可能（qty>0=add, qty=0=cancel）が、replenishment（部分cancel）の定義がambiguous。P2相当 |
| trade intensity (prints/sec, interarrival) | burst_prints_1s, burst_count_1s が近似。interarrival mean/std は1s bucketではsample不足。P2 |

### 未実装明示（0埋めしない）

- `burst_mid_move_bps_1s`: 現状0固定。task body指示通り「別途直すか未実装と明示」
- `outlier_trade_flag_1s`: 現状0固定。同上

## 3. P0 データ辞書（schema v2）

```
ts                           int64   NOT NULL   epoch ms, second boundary (1s aligned)
market                       utf8    NOT NULL   market identifier
burst_count_1s               int32   NOT NULL   burst count overlapping this 1s
total_burst_notional_1s      f64     NOT NULL   sum of burst notionals
max_burst_notional_1s        f64     NOT NULL   max single burst notional
max_burst_prints_1s          int32   NOT NULL   max prints in single burst
max_burst_duration_ms_1s     int32   NOT NULL   max burst duration (ms)
buy_burst_notional_1s        f64     NOT NULL   buy-side burst notional
sell_burst_notional_1s       f64     NOT NULL   sell-side burst notional
burst_imbalance_ratio_1s     f64     NOT NULL   (buy-sell)/(buy+sell) burst notional
largest_burst_share_notional_1s  f64 NOT NULL   max_burst / total_burst
same_price_burst_count_1s    int32   NOT NULL   bursts with 1 distinct price
multilevel_burst_count_1s    int32   NOT NULL   bursts with >=2 distinct prices
burst_notional_vs_30s_traded_notional  f64 NOT NULL  burst_notional / 30s_lookback_traded
burst_notional_vs_top_depth  f64     NULLABLE   burst / book top depth (null=unseeded)
burst_mid_move_bps_1s        f64     NOT NULL   [UNIMPLEMENTED, always 0.0]
same_price_burst_max_len_1s  int32   NOT NULL   max consecutive same-price run
same_price_burst_notional_1s f64     NOT NULL   notional of same-price bursts
multilevel_burst_max_span_ticks_1s  f64 NOT NULL  max price span (ticks)
multilevel_burst_max_span_bps_1s    f64 NOT NULL  max price span (bps)
multilevel_burst_notional_1s f64     NOT NULL   notional of multilevel bursts
same_price_absorption_ratio_1s  f64  NOT NULL   same_price / total burst notional
burst_delta_notional_1s      f64     NOT NULL   buy - sell burst notional
outlier_trade_flag_1s        int32   NOT NULL   [UNIMPLEMENTED, always 0]
book_mid_price               f64     NULLABLE   mid price (null=unseeded)
book_spread_bps              f64     NULLABLE   spread in bps
book_bid_depth_100           f64     NULLABLE   bid depth within $100 of mid
book_ask_depth_100           f64     NULLABLE   ask depth within $100 of mid
book_bid_depth_1000          f64     NULLABLE   bid depth within $1000 of mid
book_ask_depth_1000          f64     NULLABLE   ask depth within $1000 of mid
book_imbalance_100           f64     NULLABLE   (bid-ask)/(bid+ask) depth $100
book_imbalance_1000          f64     NULLABLE   (bid-ask)/(bid+ask) depth $1000
book_microprice              f64     NULLABLE   qty-weighted mid
--- P0 v2 (OrderFlow) ---
trade_count_1s               int32   NOT NULL   total trade prints in [ts, ts+1000)
traded_notional_1s           f64     NOT NULL   sum(price*qty) in 1s bucket
signed_volume_1s             f64     NOT NULL   buy_qty - sell_qty in 1s bucket
trade_imbalance_qty_1s       f64     NOT NULL   (buy-sell)/(buy+sell) qty ratio
realized_vol_10s             f64     NULLABLE   log-return std over 10s (null=warmup)
realized_vol_60s             f64     NULLABLE   log-return std over 60s (null=warmup)
```

## 4. Lookahead防止

- `trade_count_1s`, `traded_notional_1s`, `signed_volume_1s`, `trade_imbalance_qty_1s`: 1s bucket [ts, ts+1000) のみ
- `realized_vol_10s`: [ts-10000, ts) — 現在秒を含まない（strict past）
- `realized_vol_60s`: [ts-60000, ts) — 同上
- burst 特徴量: burst end_ts で判定、overlap window は既存ロジック維持

## 5. Schema backward compatibility

- 既存33列はすべて維持（型変更なし）
- 新規6列を末尾に追加。PyArrow schema は既存Parquetファイルと互換（旧ファイルに新列は存在しないが、読み出し時にoptional fieldとして扱える）
- Parquet writer は `pa.Table.from_pylist(rows, schema=FEATURE_1S_SCHEMA)` で全39列を出力。既存ファイルは上書きされない（dedupにより同一batch skip）

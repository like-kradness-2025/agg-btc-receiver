# 1s Features Schema v2 — Design Proposal

Professional quant review (GPT-5.5) applied. Status: **APPROVED for implementation.**

---

## Required Semantic Contract (fix before columns)

| 観点 | 決定 |
|------|------|
| **Time basis** | 原則 `exchange_event_time` で1秒窓。exchange時刻がないvenueのみ `recv_time` にフォールバック |
| **Boundary state** | `*_open` = その秒の開始時点で有効な最新book state（forward-filled）。`*_close` = その秒の終了時点。`stale_ms` で新鮮さを報告 |
| **NULL vs 0** | book stateがstale/missingなら book-derived columns は `NULL`。イベントフロー系は「観測したイベントがない」場合のみ `0` |
| **Units** | absolute price = native quote, relative = `*_bps`, flow = `*_qty`, depth = `*_qty`（BTC） |
| **Ring bucket reference** | **inner rings (0-1, 1-2, 2-5)**: best bid/ask 基準。**outer rings (5-25, 25-100)**: mid 基準。`imbalance_*` は同一基準で計算 |
| **Microprice formula** | `(best_bid_size × best_ask + best_ask_size × best_bid) / (best_bid_size + best_ask_size)` |
| **Quality flags** | `missing_flag`, `stale_ms` はv2でも維持。追加列がNULLになった理由をdownstreamが判定可能に |
| **Cross-venue ref** | global ref = `binance_spot`。参照marketが同秒でmissingなら `NULL`。premium/basisは `bps` 統一 |
| **Schema evolution** | `schema_version = 2` を保存パスで明示。破壊的変更はdual-write方針を先に決める |

---

## v2 Changes — Final

### Existing columns to modify

| 変更 | 内容 |
|------|------|
| cumulative bps → **ring buckets** | `bid_1bps`(0-1) → `bid_0_1bps`(0-1), `bid_1_2bps`(1-2), `bid_2_5bps`(2-5), `bid_5_25bps`(5-25), `bid_25_100bps`(25-100)。同列数(10)でcollinearity解消 |
| `wps` | 削除（= spread_bps_close） |
| `type` | 削除（常に'1s_feature'） |
| `trade_event_count` | 削除（`trade_count`に統合） |
| `snapshot_reset_count` | 削除（実装なし） |
| `rvz` | → `realized_vol_30s`（rolling realized vol, 30s window）として新設。rvzは同名維持しない |

### New columns to add

#### Phase A — Ship immediately (table stakes)

**Ring buckets（cumulative→ring、列数変わらず）:**
`bid_0_1bps`, `bid_1_2bps`, `bid_2_5bps`, `bid_5_25bps`, `bid_25_100bps`（ask同様）

**Best queue dynamics（13 columns）:**

| Column | Type | Description |
|--------|------|-------------|
| `best_bid_size_open_qty` | DOUBLE | 秒開始時点のbest bid size |
| `best_bid_size_close_qty` | DOUBLE | 秒終了時点のbest bid size |
| `best_ask_size_open_qty` | DOUBLE | 同 ask |
| `best_ask_size_close_qty` | DOUBLE | 同 ask |
| `best_bid_atouch_add_qty` | DOUBLE | ベストbidでのadd total qty |
| `best_bid_atouch_cancel_qty` | DOUBLE | ベストbidでのcancel total qty |
| `best_ask_atouch_add_qty` | DOUBLE | 同 ask |
| `best_ask_atouch_cancel_qty` | DOUBLE | 同 ask |
| `best_bid_atouch_trade_qty` | DOUBLE | ベストbidにhitした約定量 |
| `best_ask_atouch_trade_qty` | DOUBLE | 同 ask |
| `best_bid_price_move_out_count` | BIGINT | best bidがcancel/tradeではなく価格変化で消えた回数 |
| `best_ask_price_move_out_count` | BIGINT | 同 ask |
| `best_replenish_count` | BIGINT | deplete→replenish回数 |

→ **+13 columns**

**Imbalance by ring（5 columns）:**

| Column | Type | Description |
|--------|------|-------------|
| `imbalance_0_1bps` | DOUBLE | `(bid_0_1bps - ask_0_1bps) / (bid_0_1bps + ask_0_1bps)` |
| `imbalance_1_2bps` | DOUBLE | 同 |
| `imbalance_2_5bps` | DOUBLE | 同 |
| `imbalance_5_25bps` | DOUBLE | 同 |
| `imbalance_25_100bps` | DOUBLE | 同 |

→ **+5 columns**

**Microprice（1 column）:**

| Column | Type | Description |
|--------|------|-------------|
| `microprice_close` | DOUBLE | weighted mid = `(bid_size×ask + ask_size×bid) / (bid_size+ask_size)` |

→ **+1 column**

---

#### Phase B — Ship if compute allows

**Cross-venue（2 columns）:**

| Column | Type | Description |
|--------|------|-------------|
| `premium_to_ref_bps` | DOUBLE | `(mid_close / ref_mid_close - 1) * 10000` |
| `basis_to_ref_bps` | DOUBLE | `(perp_mid_close / spot_mid_close - 1) * 10000` |

**Vol / CVD / adverse selection（4 columns）:**

| Column | Type | Description |
|--------|------|-------------|
| `realized_vol_10s` | DOUBLE | rolling realized vol, 10s window |
| `cvd_10s` | DOUBLE | delta_notionalの10秒累積和 |
| `cvd_30s` | DOUBLE | delta_notionalの30秒累積和 |
| `adverse_selection_bps` | DOUBLE | touch約定直後のmid移動bps（正=不利） |

**Trade at-touch / through（2 columns）:**

| Column | Type | Description |
|--------|------|-------------|
| `trade_at_touch_qty` | DOUBLE | touchで成立した約定量 |
| `trade_through_qty` | DOUBLE | touchを貫通した約定量 |

**Event quality（1 column）:**

| Column | Type | Description |
|--------|------|-------------|
| `exchange_to_recv_lag_ms_avg` | DOUBLE | exchange event timeとrecv timeの平均差 |

→ **+9 columns**

---

#### Phase C — Deferred or cut

| 候補 | 判断 | 理由 |
|------|------|------|
| P2 extended rings (100-200/200-500/500-1000bps) | **切る** | BTC $90k時、500bps=$4,500。99%の行で0 |
| `best_replenish_avg_lag_ms` | **切る** | 1秒窓では数サンプル。統計的ノイズ |
| `depth_interarrival_ms_*` | **切る** | 1秒窓のp95は無意味 |
| `trade_sweep_count` | **切る** | public tickからsweep判定不能。誤分類でモデル汚染 |
| `out_of_order_count` | **切る** | ops指標。戦略に使わない |
| ATR bins (P3) | **先送り** | 実装コスト>効果 |
| `crr` / `tmr` | **維持**（再定義） | 価値あるが再定義が必要。後日対応 |

---

## 最終収支

| 項目 | 列数 |
|------|------|
| v1 | 68 |
| -wps, -type, -trade_event_count, -snapshot_reset_count | -4 |
| ring buckets（cumulative→ring） | ±0（置換） |
| +Phase A: best queue(13) + imbalance(5) + microprice(1) | +19 |
| +Phase B: cross-venue(2) + vol/CVD/adverse(4) + at-touch(2) + lag(1) | +9 |
| **v2 total（Phase A+B）** | **~92** |
| -Phase C cuts | -11 |
| 増分（v1→v2実装分） | +13（92-4-68=20→ring置換で13実増） |

データサイズ予測: **30MB/日 → 約35MB/日**（+17%）

---

## 実装優先度

| 優先度 | 作業 | 工数 | 備考 |
|--------|------|------|------|
| 🔴 P0 | ring buckets（cumulative→ring） | 小 | collinearity除去、列数変わらず |
| 🔴 P0 | best queue dynamics（13列） | 中 | MMバックテストに必須 |
| 🔴 P0 | imbalance（5列） | 小 | 計算軽い、即効性あり |
| 🔴 P0 | microprice（1列） | 小 | 加重midは数行 |
| 🟡 P1 | cross-venue premium/basis（2列） | 中 | 17marketの強み |
| 🟡 P1 | realized_vol_10s + CVD + adverse selection（4列） | 中 | rolling window計算。追加パイプラインが必要 |
| 🟡 P1 | trade at-touch / through（2列） | 中 | sub-second book stateが必要かも。設計確認必須 |
| 🟡 P1 | exchange_to_recv_lag_ms_avg（1列） | 小 | 既存のtimestampsから計算可 |
| 🔵 deferred | ATR bins, crr/tmr再定義 | 大 | 後日 |

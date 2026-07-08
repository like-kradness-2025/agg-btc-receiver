# v2 Schema Implementation Plan

Based on `docs/1s-features-schema-v2.md`. Phase A+B, 92 columns total.
各PhaseはSDD + 敵対的レビュー（95点以上）必須。

---

## Phase 0: Semantic Contract + Ring Buckets

**目的**: 前提条件をコードに反映し、collinearity問題を解消する。

**変更:**
- `lib/feature-accumulator.mjs`: cumulative → ring buckets に変更（bid_1bps→bid_0_1bps 等、同列数10）
- ring reference: inner(0-1,1-2,2-5) = best bid/ask基準, outer(5-25,25-100) = mid基準
- docsの"notional"→"qty (BTC)" 修正
- `wps`, `type`, `trade_event_count`, `snapshot_reset_count` 削除

**検証**: npm test + live smokeでregressionなし

---

## Phase A1: Best Queue Dynamics

**目的**: ベスト価格のキュー動態を追跡する。MMバックテストの最大の穴を埋める。

**変更:**
- `lib/feature-accumulator.mjs` に新しいfeed/accumulateロジック:
  - depth diff到着時にベスト価格が変化したか判定
  - add/cancel/tradeをtouch区分（atouch）で集計
  - size open/closeをbook stateから取得
  - price_move_out, replenish_count をflow trackingに追加

**+13 columns**: best_bid/ask_size_open/close_qty, _atouch_add/cancel/trade_qty, _price_move_out_count, _replenish_count

**検証**: live smokeでベスト価格のadd/cancel/tradeが正しくカウントされているか確認

---

## Phase A2: Imbalance

**目的**: 正規化された板不均衡を各ring別に計算する。

**変更:**
- `lib/feature-accumulator.mjs`: ring buckets計算後にimbalance = (bid - ask) / (bid + ask) を各ringで算出
- 既存のring bucket列から導出可能なので計算のみ。新規feed不要

**+5 columns**: imbalance_0_1bps, _1_2bps, _2_5bps, _5_25bps, _25_100bps

**検証**: ring bucketのbid/ask値からimbalanceが正しく計算されていることを確認

---

## Phase A3: Microprice

**目的**: 加重ミッドを提供する。

**変更:**
- `lib/feature-accumulator.mjs`: feedSecond()内で `(bid_size × ask + ask_size × bid) / (bid_size + ask_size)` を計算
- best_bid/askとそのsizeから導出。新規feed不要

**+1 column**: microprice_close

**検証**: 手計算と一致することを確認

---

## Phase B1: Cross-venue

**目的**: 17marketの強みを活かし、market間プレミアム/ベーシスを記録する。

**変更:**
- `lib/fair-price-collector.mjs` または新しいcollector: 全marketのmidを共有状態として保持
- premium = binance_spot基準の各marketのmid差分（bps）
- basis = perpのspotに対するmid差分（bps）
- 同期問題: event-time alignか許容ノイズの文書化

**+2 columns**: premium_to_ref_bps, basis_to_ref_bps

**検証**: 実走でbinance_spot基準のpremiumが正しく計算されることを確認

---

## Phase B2: Vol / CVD / Adverse Selection

**目的**: rolling window計算による短期的リスク指標を追加する。

**変更:**
- `lib/feature-accumulator.mjs` または別モジュール: 
  - `realized_vol_10s`: 10秒間の1s returnの標準偏差
  - `cvd_10s/30s`: delta_notionalのrolling sum
  - `adverse_selection_bps`: 約定直後のmid移動方向
- rolling window計算: FeatureAccumulatorにring buffer追加

**+4 columns**: realized_vol_10s, cvd_10s, cvd_30s, adverse_selection_bps

**検証**: 手計算と一致、rolling windowの境界条件確認

---

## Phase B3: Trade At-Touch / Through

**目的**: 約定の攻撃性を分類する。

**変更:**
- `lib/feature-accumulator.mjs`: trade到着時にbook stateと照合
- 1秒解像度の制約: 秒未満のbook snapshotがない場合、trade_to_qtyの誤分類リスクあり。設計確認必須
- 代案: 実装を見送り、trade_at_touch_qtyのみ実装しtrade_through_qtyは後日

**+2 columns**: trade_at_touch_qty, trade_through_qty

**検証**: live smokeで分類の整合性確認。trade > touch_qty + through_qty にならないことを確認

---

## Phase B4: Exchange Lag

**目的**: データ品質指標を追加する。

**変更:**
- 各connectorからexchange event timeとrecv timeの差を収集
- FeatureAccumulatorで1秒ごとに平均を計算

**+1 column**: exchange_to_recv_lag_ms_avg

**検証**: 実走でラグ値が現実的な範囲（50-500ms）であることを確認

---

## 全体収支

| Phase | 新規列 | 累積 |
|-------|--------|------|
| v1 | 68 | 68 |
| Phase 0（ring化+削除） | -4（削除） | 64 |
| Phase A1 | +13 | 77 |
| Phase A2 | +5 | 82 |
| Phase A3 | +1 | 83 |
| Phase B1 | +2 | 85 |
| Phase B2 | +4 | 89 |
| Phase B3 | +2 | 91 |
| Phase B4 | +1 | 92 |

データサイズ: 30MB/日 → ~35MB/日（Phase Aのみなら~33MB）

---

## 依存関係

```
Phase 0 ← Phase A1 ← Phase A2 ← Phase A3
  ↓                      ↕
Phase B1                Phase B2
  ↓                      ↓
Phase B3 ←────────── Phase B4
```

- Phase 0→A1→A2→A3: 直列（ring bucketsを先に直さないとimbalance/micropriceが狂う）
- Phase B1: Phase 0の後に独立して実行可能
- Phase B2: A1のbest queue + A2のimbalance の後に（rolling windowで使うため）
- Phase B3: B1のcross-venue + A1のbest queue の後に（参照midとbook stateが必要）
- Phase B4: どのconnectorからも独立

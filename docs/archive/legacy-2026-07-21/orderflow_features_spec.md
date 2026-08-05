# OrderFlow 特徴量 — 現行実装と移行状況

> **Status index（2026-07-19）**
>
> 目標仕様、計算契約、Phase 合格条件の正本は
> [`downstream-design-handoff.md`](downstream-design-handoff.md)。
> 本書は「現在どこまで実装済みか」を追跡するための文書であり、
> 目標列を実装済みと宣言するものではない。

## 1. 結論

現行後工程は、raw trade から既存 burst 特徴量を作り、
1秒→30秒→5分に集約する経路までは動作している。

一方、Bookを含む完全なOrderFlow分析に必要な次の中核は未完成である。

- `book_updates` 内 snapshot を使った seed と full book replay
- multi-level depth の実合計
- add/cancel、OFI、depletion/replenishment
- trade と直前 book の結合
- liquidation、derivatives、cross-market 層

したがって、現状を「OrderFlow 特徴量が網羅済み」とは判定しない。

## 2. 実装状況

| 層 | 状況 | 利用判断 |
|---|---|---|
| raw trade ingestion | 実装済み | 利用可。rawとの件数・数量照合が必要 |
| burst detection | 実装済み | 利用可。定義versionを固定する |
| 1s/30s/5min rollup | 実装済み | burst列で検証済み |
| P0 trade primitives | 1秒列と30秒/5分のcore flow保存則rollupを実装 | 利用可。raw照合が必要 |
| book seed | 旧path依存 | 現行raw契約では利用不可 |
| book state | schemaは存在 | `book_seeded=false` の間は利用不可 |
| multi-level depth | seed済み全levelを1秒strict as-of集計 | fixture・Bitfinex実データで利用可。長時間独立照合が必要 |
| OFI / book event flow | 1秒OFI / add-cancel / replenishment / pullingを実装 | 1秒列は利用可。event-level独立照合とrollupは未完了 |
| trade-book interaction | strict pre-trade join、touch/through、slippage、sweep、top-depth比を1秒実装 | 実データ長時間独立照合とrollupは未完了 |
| liquidation / derivatives | 未実装 | 利用不可 |
| cross-market | 未実装 | 利用不可 |

## 3. 現行 `burst_features_v1`

現行 `burst_features_v1` path は互換運用のため維持し、row の
`_quality.feature_schema_version=orderflow_features_1s_v2` でP0/P1 payloadを識別する。v2ではraw tradeから `trade_open_1s` / `trade_high_1s` / `trade_low_1s` / `trade_close_1s` を追加計算し、上位足のOHLCVはこのcanonical列からrollupする。
`ts`、`market`、`_quality` を除き、現在は79 logical fieldsを含む。

- burst 基本、方向、集中度
- same-price / multilevel burst
- burst / 30秒 traded notional
- book-aware 名目列
- book state B1-B9
- board metrics

ただし「列が存在すること」と「信頼できる値が入ること」は別である。
特に book seed が成立していない row の book 系列は利用しない。seed済みrowでは、replayした全levelからmid±$100 / mid±$1000のquote-notional depthを計算し、1秒ごとのstrict pre-second状態を使う。
未実装列の 0 固定も実測値として扱わない。

## 4. 実装した Phase 0

raw trade のみから次を実装した。

- trade count: total / buy / sell
- traded qty・notional: total / buy / sell
- signed qty・notional
- qty・notional imbalance
- mean / median / max trade notional
- large trade count / notional share
- mean / median / p95 interarrival
- side flip count
- realized volatility 10秒 / 60秒

旧文書にあった次の6列を含むraw-trade P0列は、Phase 0として実装した。
ただし実データでの全market長時間検証は別途継続する。

```text
trade_count_1s
traded_notional_1s
signed_volume_1s
trade_imbalance_qty_1s
realized_vol_10s
realized_vol_60s
```

## 5. Phase 0 の完了判定

- raw件数と `trade_count_1s` の総和が一致
- raw `Σqty`、`Σprice×qty` と出力が許容誤差内で一致
- buy/sell と signed/imbalance の恒等式が成立
- 30秒・5分 rollup で count/qty/notional の保存則が成立
- no trade は count/flow が0、mean/maxはnull
- rolling warmup はnullであり、未来イベントを参照しない
- NaN/Infinity がない
- 同じraw・config・schemaから同じhashが生成される
- binance_spot 10分の実データで side、timestamp、finite、保存則、lookaheadを検証済み
- 全15 market・24時間の実データ検証を完了（2026-07-18T00:00Z〜2026-07-19T00:00Z、1秒行173,370、5,779ブロック、errors=0、quarantine=0、P0非有限値=0、schema欠落=0）

## 6. Phase 1 以降の前提

Book 系は次を満たすまで有効化しない。

1. raw `book_updates` の snapshot row または明示REST seedを取得
2. connector ごとの update semantics と sequence/checksum を実装
3. gap後は再seedまで無効化
4. best bid/ask、複数level depth、checksumを照合（複数level depthの計算とfixture検証は完了）
5. `_quality.book_seeded=true` かつ stale/gapなし

seed できない市場を 0 埋めして capability を偽装しない。P1 event-flowはseed済みrowだけで計算し、未seed・gap後はnullにする。
OFIの30秒・5分集約は実装済み。trade-book interactionは1秒実装済みで、rollupと長時間独立照合が残る。

## 7. 移行方針

- `burst_features_v1` path は既存consumer互換のため当面保持する
- rowの `feature_schema_version` で新しいOrderFlow payloadを識別する
- 専用pathへの切替は `scripts/tfp.mjs --orderflow` で実行できる。既存consumerとの段階移行期間はデフォルトの `burst_features_v1` を維持し、専用namespace側を検証してから運用既定値を切り替える（P0 payload自体の長時間検証は完了）
- schema registry で型、単位、window、null、aggregation operatorを固定する
- 旧ファイルを新schemaとして暗黙に読む処理は作らない
- 将来returnは `labels_forward` に分離する
- raw削除は downstream worker ではなく retention/archive job が行う
- 変換済みrawの削除は `scripts/run-cleanup-raw.sh` が担当する。削除前にTFP manifestの`status=committed`、30行の連続`features_1s`、各rowの`_quality.finalized=true`、safety marginを検証する。`book_updates`はさらにseed済みrowがあり、snapshotを含まないupdate-only fileだけを削除し、snapshot fileは保持する。通常運用は`--dry-run`確認後に実削除する。

実装状況を変更したときは、この表と合格証跡を同じ変更で更新する。

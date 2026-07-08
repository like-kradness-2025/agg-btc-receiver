# Size Dominance Two-Axis Analysis
対象: `data/raw_hot/2026-06-29/trade/*.jsonl`（隣接重複 tradeId/ts/price/qty/side を除外）
目的: 小口/中口/大口を「名前」ではなく、**数的支配** と **金額的支配** の2軸で読む。

## Bucket contract

現行 v1 は **per-trade USD notional**（約定価格 `price * qty`）で bucket を決める。

| Bucket | 境界 | 解釈 |
|---|---:|---|
| Small | `< $1K` | tiny / retail flow |
| Medium | `$1K <= notional < $10K` | active small / bridge flow |
| Large | `>= $10K` | market-impact flow。**Whaleではない** |

v2候補では `$100K+` を `Whale` として別bucketに分離する。ただし v1 の `Large` を `$100K+` に置き換えない。

## 指標定義
- `N_b`: bucket b の trade count
- `Q_b`: bucket b の BTC qty 合計
- `V_b`: bucket b の USD notional 合計 (`Σ price*qty`)
- `n_b = N_b / ΣN`: 数的 share
- `q_b = Q_b / ΣQ`: qty share（補助）
- `m_b = V_b / ΣV`: 金額 share（主軸）
- `D_num = log2(n_b / (1/K))`: 数的支配
- `D_mon = log2(m_b / (1/K))`: 金額的支配
- `Δ = D_mon - D_num = log2(m_b / n_b)`: 1約定あたり金額の相対濃度
`0` は均等配分。正なら支配、負なら非支配。

**注意:** `D_num` / `D_mon` は bucket数 `K` に依存するため、v1 3bucket と v2 4bucket の値を直接比較しない。同じ `K` 内で比較する。
## 四象限
| 象限 | 意味 | 解釈 |
|---|---|---|
| `(+,+)` | 多数・多額 | 数でも金でも主導。広く厚い flow |
| `(+,-)` | 多数・少額 | 小口回転/参加幅/ノイズ寄り |
| `(-,+)` | 少数・多額 | 大口主導/impact flow/吸収候補 |
| `(-,-)` | 少数・少額 | 周辺的。主役にしない |
## v1 3bucket: Spot
raw lines=9,440,161, dedup skipped=988,654, trades=8,441,239
| bucket | count share | notional share | qty share | active-sec share | D_num | D_mon | Δ | quadrant |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| small | 89.33% | 11.70% | 11.71% | 99.94% | +1.42 | -1.51 | -2.93 | `(+,-)` |
| medium | 9.88% | 53.20% | 53.20% | 70.51% | -1.75 | +0.67 | +2.43 | `(-,+)` |
| large | 0.79% | 35.10% | 35.09% | 19.76% | -5.40 | +0.07 | +5.47 | `(-,+)` |

## v1 3bucket: Perp
raw lines=15,431,906, dedup skipped=1,286,175, trades=14,145,731
| bucket | count share | notional share | qty share | active-sec share | D_num | D_mon | Δ | quadrant |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| small | 78.92% | 4.64% | 4.64% | 99.68% | +1.24 | -2.85 | -4.09 | `(+,-)` |
| medium | 15.89% | 22.90% | 22.90% | 95.25% | -1.07 | -0.54 | +0.53 | `(-,-)` |
| large | 5.18% | 72.47% | 72.46% | 74.02% | -2.69 | +1.12 | +3.81 | `(-,+)` |

## v2候補 4bucket view（Whale分離）
### Spot
| bucket | count share | notional share | qty share | active-sec share | D_num | D_mon | Δ | quadrant |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| small | 89.33% | 11.70% | 11.71% | 99.94% | +1.84 | -1.10 | -2.93 | `(+,-)` |
| medium | 9.88% | 53.20% | 53.20% | 70.51% | -1.34 | +1.09 | +2.43 | `(-,+)` |
| large | 0.77% | 29.63% | 29.62% | 19.68% | -5.01 | +0.24 | +5.26 | `(-,+)` |
| whale | 0.02% | 5.47% | 5.47% | 0.81% | -10.60 | -2.19 | +8.40 | `(-,-)` |

### Perp
| bucket | count share | notional share | qty share | active-sec share | D_num | D_mon | Δ | quadrant |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| small | 78.92% | 4.64% | 4.64% | 99.68% | +1.66 | -2.43 | -4.09 | `(+,-)` |
| medium | 15.89% | 22.90% | 22.90% | 95.25% | -0.65 | -0.13 | +0.53 | `(-,-)` |
| large | 4.95% | 53.01% | 53.00% | 73.40% | -2.34 | +1.08 | +3.42 | `(-,+)` |
| whale | 0.23% | 19.46% | 19.46% | 14.89% | -6.76 | -0.36 | +6.40 | `(-,-)` |

## 読み筋
- Small は spot/perp とも `(+,-)`。**数的支配** は強いが、金額的には主導しない。参加幅・ノイズ・追随の読み。
- Large は v1 では spot/perp とも `(-,+)`。**金額的支配** が強く、CVD方向の一次信号として読む。
- Medium は spot では `(-,+)` に寄り、perp では中間。Small と Large の橋渡し/遷移層。
- `$100K+` は v2では Whale として分離する。ただし今回の全日分布では count share も notional share も均等基準を下回るため象限は `(-,-)`。一方で `Δ` は非常に大きく、**1約定あたり金額濃度は極端に高い**。よって連続CVDの主役ではなく、イベント/警告層として扱う。

## CVD解釈ルール
1. `Large CVD` を一次信号、`Medium` を橋渡し、`Small` を参加幅として読む。
2. 価格上昇 + Large不在 = 脆い上昇候補。
3. 横ばい + Large買い越し = 吸収/蓄積候補。
4. Small単独優勢 = 方向断定せず追随/ノイズ疑い。
5. Spot/Perp は先に分離して読み、aggregateは最後に確認する。

## 判定時の補助ゲート

象限だけで結論を出さない。最低限、以下を併記する。

| 補助指標 | 用途 |
|---|---|
| `active-sec share` | そのbucketが連続CVD線として読める密度を持つか。低い場合はイベント層扱い。 |
| `Δ = D_mon - D_num` | 1約定あたりの金額濃度。Whaleのように象限が `(-,-)` でも Δ が大きければイベント価値あり。 |
| `qty share` | 現行CVDがBTC qtyベースなので、notional share の補助確認に使う。主軸にはしない。 |
| spot/perp split | aggregate前に spot と perp を分けて読む。perp主導とspot主導を混ぜない。 |

## サブエージェント統合結果

並列サブエージェント3本の結論を統合する。

| Agent | 採用する点 | 修正/降格する点 |
|---|---|---|
| 指標設計 | NDI/MDI、CVD寄与率、平均取引サイズ、Spot/Perp分離、Whale sparse警告 | `MDI^Q` 主分析案は降格。金額的支配は `MDI^N`（USD notional）を主軸にする |
| 可視化設計 | Dominance Scatter、参照線、メトリクス表、count vs money構成比バー | Y軸 `qty_share` 推奨は降格。Y軸は `notional_share`、点サイズ/補助表に `qty_share` を使う |
| 実装影響 | v1にbucket別 notional列を追加するのが最小差分。Whale追加は別フェーズ | `trade-accumulator.mjs` という表記は現リポジトリでは `trade-aggregator.mjs` を指すため注意 |

## 次フェーズの最小実装案

次フェーズは2段階に分ける。**Phase 1では producer/schema の追加だけを行い、Whaleもchart変更も混ぜない。**

### Phase 1: producer/schema（最小実装）

現行 v1 で真の `count-share × notional-share` をライブJSONLから直接出すには、bucket別 notional列が必要。

#### 追加列（v1 3bucketのまま）

| side | columns |
|---|---|
| buy | `buy_small_notional`, `buy_medium_notional`, `buy_large_notional` |
| sell | `sell_small_notional`, `sell_medium_notional`, `sell_large_notional` |

既存の `buy_notional` / `sell_notional` は全bucket合計なので、二軸分析には bucket別 notional が必要。

#### Phase 1 受入基準

1. `buy_small_notional + buy_medium_notional + buy_large_notional ≈ buy_notional`
2. `sell_small_notional + sell_medium_notional + sell_large_notional ≈ sell_notional`
3. 旧JSONLに新列が無い場合、aggregate/storage上は **NULL** として保持し、真の0と区別する
4. 分析側は bucket別 notional がNULLの期間を true notional dominance の対象外にする。必要なら `qty_share proxy` と明示して別扱いする
5. v2 Whale (`$100K+`) はこのフェーズに混ぜない

### Phase 2: consumer/chart/report（後続）

Phase 1 のライブデータが溜まってから実装する。

1. chart/reportでは `notional_share` を金額的支配の主軸、`qty_share` を補助として表示
2. NULL期間と実測期間を混ぜて share を計算しない
3. Dominance Scatter / 構成比バー / active-sec density を追加する


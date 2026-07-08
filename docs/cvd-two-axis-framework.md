# AUX DRAFT — BTC CVD サイズバケット分析 — 二軸フレームワーク設計書

> **Status:** 補助ドラフト。正本は `docs/size-dominance-two-axis-analysis.md`。
> **重要:** 本ドラフト内の `MDI^Q`（qty share）主分析推奨は、最終方針では **補助指標** に降格。金額的支配の主軸は `MDI^N`（USD notional share）。

> 作成: 2026-06-30
> 対象: agg-btc-receiver v1 → v2 設計
> 言語: 日本語
> 目的: トレードサイズ分布を (1) 数的支配度 (2) 金額的支配度 の二軸で評価するロバストな分析フレームワークの設計

---

## 1. 背景と動機

### 1.1 現行 v1 の単一分類の問題点

agg-btc-receiver v1 は `trade-size-buckets.mjs` で定義された3つのバケットに基づいて
トレードを分類し、CVD（Cumulative Volume Delta）を計算する：

| バケット | USD notional 閾値 | BTC換算 (@$100k) |
|----------|-------------------|------------------|
| Small    | < $1K             | < 0.01 BTC       |
| Medium   | $1K – $9,999      | 0.01 – 0.099 BTC |
| Large    | ≥ $10K            | ≥ 0.1 BTC        |

**問題点**：
1. **一次元分類の限界**：すべてのトレードが単一の USD notional 閾値でのみ分類されている。
   これにより「トレード回数（数的支配度）」と「取引金額（金額的支配度）」が混在し、
   解釈上の混乱が生じる。
2. **カウントとボリュームの逆転現象**：実データ（2026-06-29）では：
   - Perp: Small が 78.96% のカウントを占めるが、notional は 4.64% に過ぎない
   - Large ($10K-$100K): カウント 4.94% だが notional は 52.96%
   - カウントで見れば「Small 優勢」、notional で見れば「Large 優勢」と正反対の結論になる
3. **単一 CVD ラインの情報損失**：`cvd_l` は Large バケットの純買い数量だが、
   「大口が数少ない大きなトレードを行った」のか「大口が頻繁に中規模トレードを行った」のか
   区別できない。

### 1.2 二軸フレームワークの必要性

トレードサイズ分布の解釈には、少なくとも以下の2つの次元が独立して必要である：

| 軸 | 定義 | 問い |
|----|------|------|
| **数的支配度** (Numerical Dominance) | 各バケットのトレード**回数**の比率 | 「誰が最も頻繁に取引しているか」 |
| **金額的支配度** (Monetary Dominance) | 各バケットの取引**数量/金額**の比率 | 「誰が最も多くのBTC/USDを動かしているか」 |

---

## 2. フレームワーク定義

### 2.1 基本記法

1秒窓（`ts`）において、market `m`、side `s ∈ {buy, sell}`、bucket `b ∈ {small, medium, large}` に対し：

| 記号 | 定義 | 単位 |
|------|------|------|
| `C_{s,b}(t)` | side `s`, bucket `b` のトレード**回数**（count） | 回/秒 |
| `Q_{s,b}(t)` | side `s`, bucket `b` の約定**数量**（qty） | BTC/秒 |
| `N_{s,b}(t)` | side `s`, bucket `b` の約定**金額**（notional） | USD/秒 |
| `P(t)` | 参照価格（mid_close） | USD/BTC |

関係式：
- `N_{s,b}(t) ≈ Q_{s,b}(t) × P(t)`（厳密には個別トレードの price×qty の和）
- 現行 schema は `Q_{s,b}` と `C_{s,b}` を保存。`N_{s,b}` は未保存（v2 候補）。

### 2.2 二軸メトリクス

#### 2.2.1 数的支配度（Numerical Dominance Index: NDI）

**定義**：任意の時間窓 `W`（例: 1秒, 1分, 1時間）において、全トレード回数に占める
各バケットの割合。

```
NDI_b(W) = Σ_{t∈W} [C_{buy,b}(t) + C_{sell,b}(t)]
           ──────────────────────────────────────────
           Σ_{t∈W} Σ_{b'} [C_{buy,b'}(t) + C_{sell,b'}(t)]
```

side 別も同様に定義可能：

```
NDI_{buy,b}(W) = Σ_{t∈W} C_{buy,b}(t)
                 ────────────────────────────────
                 Σ_{t∈W} Σ_{b'} C_{buy,b'}(t)
```

**値域**: `[0, 1]`。全バケットで合計すると `1`。

**解釈規則**：
| NDI_b 値 | 解釈 |
|-----------|------|
| > 0.5 | このバケットが**数的に支配的**（例: retail の Small） |
| 0.2 – 0.5 | **有意な参加**（例: active retail の Medium） |
| < 0.2 | **数的には少数派**（例: whale の Large） |
| < 0.05 | **極めて稀少**。CVD ラインとしての信頼性に注意 |

#### 2.2.2 金額的支配度（Monetary Dominance Index: MDI）

**定義**：任意の時間窓 `W` において、全取引数量（または金額）に占める各バケットの割合。

```
MDI^Q_b(W) = Σ_{t∈W} [Q_{buy,b}(t) + Q_{sell,b}(t)]
             ──────────────────────────────────────────
             Σ_{t∈W} Σ_{b'} [Q_{buy,b'}(t) + Q_{sell,b'}(t)]

MDI^N_b(W) = Σ_{t∈W} [N_{buy,b}(t) + N_{sell,b}(t)]
             ──────────────────────────────────────────
             Σ_{t∈W} Σ_{b'} [N_{buy,b'}(t) + N_{sell,b'}(t)]
```

**Qベース（数量）と Nベース（notional）の違い**：
- `MDI^Q` は BTC 数量のシェア。BTC建てのマーケットインパクトを評価する際に有用。
- `MDI^N` は USD 金額のシェア。フィアット建ての資本フローを評価する際に有用。
- BTC価格が変動すると、同じ数量でも `N` ベースのシェアは変化する。
- **推奨**: 両方を併記。主分析は `MDI^Q`（現行 schema で計算可能）を使用。

**解釈規則**：
| MDI_b 値 | 解釈 |
|-----------|------|
| > 0.5 | このバケットが**金額的に支配的**（例: $10K-$100K の Large） |
| 0.2 – 0.5 | **有意な資本参加** |
| < 0.2 | **金額的には少数派**（例: retail の Small） |
| < 0.05 | **金額的影響はごくわずか** |

### 2.3 二軸マトリクス（支配度クロス分類）

NDI と MDI の組み合わせにより、各バケットの市場参加特性を4象限で分類する：

```
          MDI（金額的支配度）
          低                    高
     ┌─────────────────┬─────────────────┐
 高  │  Type A: 頻発小口  │  Type B: 頻発大口  │
     │  (Retail noise)  │  (Institutional   │
     │                  │   flow - rare)    │
N    │  Smallバケット    │  （理論的には     │
D    │  が典型           │   存在しにくい）   │
I    ├─────────────────┼─────────────────┤
     │  Type C: 稀発小口  │  Type D: 稀発大口  │
 低  │  (Negligible)    │  (Whale flow)    │
     │                  │  Large/Whale      │
     │                  │  バケットが典型    │
     └─────────────────┴─────────────────┘
```

**実データでの期待分布（2026-06-29 実測に基づく）**：

| バケット | NDI（カウント比） | MDI^Q（qty比） | 象限 | 解釈 |
|----------|-------------------|----------------|------|------|
| Small    | **高**（~78-89%） | **低**（~5-12%） | Type A | Retail noise。カウントでは支配的だが金額的影響は小さい |
| Medium   | **中**（~10-16%） | **中**（~23-53%） | Type A/B 境界 | Active retail / small participant。バランス型 |
| Large    | **低**（~1-5%） | **高**（~35-53%） | Type D | Whale/Institutional flow。カウントは少ないが金額的支配力が大きい |

### 2.4 CVD 寄与分解（Bucket Contribution Decomposition）

各バケットが全体 CVD にどれだけ寄与しているかを分解する。

**定義**：

```
CVD_b(t) = Q_{buy,b}(t) - Q_{sell,b}(t)           # バケット別 CVD（BTC/秒）
CVD_total(t) = Σ_b CVD_b(t)                        # 全体 CVD

CVD寄与率_b(W) = Σ_{t∈W} |CVD_b(t)|
                ────────────────────
                Σ_{t∈W} Σ_{b'} |CVD_{b'}(t)|
```

絶対値ベースの寄与率を用いることで、正味打ち消し合いの影響を排除する。

**解釈**：
| パターン | CVD_s | CVD_m | CVD_l | 含意 |
|----------|-------|-------|-------|------|
| 全バケット同符号 | + | + | + | Broad-based buying/selling。強い方向性 |
| Small 逆行 | − | + | + | Retail が fade、大口が方向性 |
| Large 逆行 | + | + | − | 大口が分配/集荷。天井/底シグナルの可能性 |

---

## 3. 数式要約（リファレンスカード）

### 3.1 秒次集計（既存 schema から算出可能）

```
# 1秒あたりのバケット別集計（既存カラムから）
C_{buy,b}(t) = buy_{b}_count   # buy_small_count, buy_medium_count, buy_large_count
C_{sell,b}(t) = sell_{b}_count
Q_{buy,b}(t) = buy_{b}_qty     # buy_small_qty, buy_medium_qty, buy_large_qty
Q_{sell,b}(t) = sell_{b}_qty
```

### 3.2 窓集計（任意の集計窓 W）

```
NDI_b(W) = Σ_W (C_{buy,b} + C_{sell,b}) / Σ_W Σ_{b'} (C_{buy,b'} + C_{sell,b'})

MDI^Q_b(W) = Σ_W (Q_{buy,b} + Q_{sell,b}) / Σ_W Σ_{b'} (Q_{buy,b'} + Q_{sell,b'})

CVD_b(W) = Σ_W (Q_{buy,b} - Q_{sell,b})          # 累積和

CVD寄与率_b(W) = Σ_W |Q_{buy,b} - Q_{sell,b}| / Σ_W Σ_{b'} |Q_{buy,b'} - Q_{sell,b'}|

平均取引サイズ_b(W) = Σ_W (Q_{buy,b} + Q_{sell,b}) / Σ_W (C_{buy,b} + C_{sell,b})
```

### 3.3 v2 拡張（notional カラム追加時）

```
MDI^N_b(W) = Σ_W (N_{buy,b} + N_{sell,b}) / Σ_W Σ_{b'} (N_{buy,b'} + N_{sell,b'})

実効レート_b(W) = Σ_W (N_{buy,b} + N_{sell,b}) / Σ_W (Q_{buy,b} + Q_{sell,b})
```

実効レートは各バケットの平均約定価格を示し、
バケット間のスリッページ傾向を評価できる。

---

## 4. 解釈ルール

### 4.1 ルール 1: カウント優勢 ≠ 金額優勢

> 「Small が最も多い」からといって「Small が市場を動かしている」とは限らない。

現行 CVD チャートで `cvd_s` の振幅が小さいのは、Small の CVD 方向が
バラバラ（買いと売りが拮抗）で、累積しても打ち消し合うため。
NDI が高くても CVD インパクトは小さいのが Small の特徴。

### 4.2 ルール 2: 金額支配度の高いバケットの CVD 方向を重視

> マーケットインパクトの観点では MDI の高いバケットの CVD 符号が重要。

Large ($10K+) の MDI が 50% を超える場合、Large の CVD 方向が
市場全体の方向性を決定する。Small/Medium の CVD が逆行していても、
Large の CVD が示す方向が支配的である。

### 4.3 ルール 3: 平均取引サイズの経時変化を監視

> 平均取引サイズの急増は、参加者構成の変化（大口参入/退出）を示唆する。

```
avg_trade_size_b(t_window) = Q_total,b / C_total,b
```

平均取引サイズが上昇トレンドにある場合：
- 同一バケット内でより大きなトレードが増加している
- または大口参加者が当該バケットに降りてきている（バケット境界の硬直的性質）

### 4.4 ルール 4: Spot と Perp で支配度構造が異なる

| 指標 | Spot | Perp | 解釈 |
|------|------|------|------|
| Small NDI | **非常に高い** (~89%) | 高い (~79%) | Spot は retail がさらに支配的 |
| Large MDI | 低い (~35%) | **高い** (~72%) | Perp は大口の資本集中度が高い |
| Large の密度 | 低い（0.83%/秒） | 高い（74%/秒） | Spot の Large CVD は sparse。ノイズに注意 |

**CVD 分析上の含意**：
- Spot CVD の Large ラインはデータ密度が低いため、短期 window ではノイズが多い。
  長期 window（1h+）でなら意味のある信号となる。
- Perp CVD は全バケットで十分な密度があり、1分～5分 window でも信頼できる。

### 4.5 ルール 5: Whale 追加時の閾値設計

v2 で `Whale (≥$100K)` を追加する場合：

```
Small:   < $1K         NDI高 / MDI低
Medium:  $1K–$9,999    NDI中 / MDI中
Large:   $10K–$99,999  NDI低-中 / MDI高
Whale:   ≥ $100K       NDI極低 / MDI中
```

Whale は NDI が極めて低い（0.02-0.23%）ため、CVD ラインとしては
大きな集計窓（15分～1時間）でなければ有意な形状を示さない。
短時間窓では「階段状」の CVD になる（イベント駆動型）。

---

## 5. ピットフォール（落とし穴）

### 5.1 ピットフォール 1: 閾値の硬直性（Threshold Rigidity）

**問題**：バケット閾値が USD 固定であるため、BTC 価格変動により
バケットの実質的な意味が変化する。

```
BTC @$100k 時: Large ≥ $10K = ≥ 0.1 BTC
BTC @$50k 時:  Large ≥ $10K = ≥ 0.2 BTC
BTC @$200k 時: Large ≥ $10K = ≥ 0.05 BTC
```

**影響**：
- BTC 価格が2倍になると、同じ USD 閾値でも BTC 数量は半分になる
- `MDI^Q`（BTC数量ベース）で見ると、バケット間の閾値が実質的にシフトする
- 長期バックテストでは、価格レジームによって同じ「Large」の意味が異なる

**対策（v2 候補）**：
- 閾値を BTC 数量ベースに変更する（0.1 BTC, 1 BTC, 10 BTC）
- または `MDI^Q` と `MDI^N` の両方を常に併記し、価格変動の影響を可視化する
- 閾値自体を動的（ATR 連動など）にする案もあるが、解釈の一貫性が損なわれる

### 5.2 ピットフォール 2: 集計窓依存性（Window-Size Dependency）

**問題**：NDI と MDI は集計窓 `W` の長さに依存する。

- **1秒窓**：多くの秒で Large はゼロ。NDI 計算不能（0/0）。
- **1分窓**：Large のカウントが 1–5 程度。NDI の推定誤差が大きい。
- **1時間窓**：全バケットで安定した推定値。
- **1日窓**：日内パターンが平均化され、セッション間の違いが消失。

**対策**：
- 分析目的に応じて複数の窓サイズを併用する
- 短期（1分–5分）：CVD 方向のリアルタイム監視用
- 中期（1時間）：支配度構造の安定推定用
- 長期（1日）：バックテスト・参加者構造の経時変化分析用
- 各窓でのサンプルサイズを報告し、信頼区間を併記する

### 5.3 ピットフォール 3: Side 非対称性の見落とし

**問題**：買い側と売り側で同じバケットでも支配度が異なる場合がある。

例：暴落時に Large sell が急増し、Large buy は変わらない。
→ NDI_large は上昇するが、その内訳は sell 偏重。
→ 全体 NDI だけ見ると「大口が活発化」に見えるが、
　実際は「大口の売り抜け」である。

**対策**：
- NDI と MDI は必ず side 別（buy/sell）でも計算する
- `NDI_buy` と `NDI_sell` の差（NDI skew）を監視指標とする

```
NDI_skew_b = NDI_buy,b - NDI_sell,b
```
- 正：買いが数的優勢（強気）
- 負：売りが数的優勢（弱気）
- ゼロ付近：均衡

### 5.4 ピットフォール 4: 平均取引サイズの分布歪み

**問題**：平均取引サイズ（`Q_total,b / C_total,b`）は外れ値に弱い。

1つの $500K トレードが Large バケットの平均を大きく歪める。
特に Whale バケットでは、サンプル数が少ないため1トレードの影響が極大。

**対策**：
- 平均だけでなく中央値（median）も併記する（要 raw trade データ）
- または truncated mean（上位/下位 1% を除外）を使用する
- 1秒集計レベルでは不可能なため、raw trade JSONL からの再集計が必要

### 5.5 ピットフォール 5: CVD 累積における Survivorship Bias

**問題**：累積 CVD は「打ち消し合い」を隠蔽する。

2つのシナリオ：
- A: 各秒で `CVD_l = +0.1 BTC`（一貫した買い越し）
- B: 各秒で `buy_l = +1 BTC, sell_l = -0.9 BTC`（活発な売買の結果 +0.1）

累積 CVD は両者とも同じラインを描くが、市場の質は全く異なる。
**A** は一方向の大口需要。**B** は両方向に活発な大口の応酬。

**対策**：
- 累積 CVD だけでなく、**CVD寄与率**（絶対値ベース）も併記する
- シナリオ A: CVD_l の寄与率は高い（方向が一貫）
- シナリオ B: CVD_l の寄与率は低い（買いと売りが打ち消し合う）
- または buy/sell を分離したグロスフロー分析を行う

### 5.6 ピットフォール 6: Spot/Perp 間の CVD 比較における市場構造の違い

**問題**：Spot と Perp では参加者構造が異なるため、同じバケットでも意味が違う。

- Spot Large: 比較的少ない（~1% カウント）。現物の大口移動。カストディアル。
- Perp Large: 活発（~5% カウント）。レバレッジ取引の大口。投機的。

Spot の Large CVD 上昇 = 現物の大口買い（強気の現物需要）
Perp の Large CVD 上昇 = 先物の大口買い（レバレッジロング）

同じ「Large CVD 上昇」でも、Spot と Perp では資金の質が異なる。
Spot は「永続的な資本移動」、Perp は「一時的な投機ポジション」の可能性が高い。

**対策**：
- Spot CVD と Perp CVD は常に分離して表示する（現行の agg チャートはこれに準拠）
- Spot Large の CVD 方向は、Perp Large よりも長期的な方向性の信頼度が高いと解釈する
- Spot/Perp の Large CVD の乖離（basis trade の代理変数）を監視する

---

## 6. 実装推奨

### 6.1 現行 v1 schema で今すぐ実装可能な指標

既存の `data/1s_features/*/*.jsonl` から、追加カラムなしで計算可能：

```python
# すべて既存カラムから算出可能
NDI_small  = (buy_small_count + sell_small_count) / total_count
MDI_Q_small = (buy_small_qty + sell_small_qty) / total_qty

# CVD 寄与率
cvd_small_contribution = abs(buy_small_qty - sell_small_qty) / (
    abs(buy_small_qty - sell_small_qty) +
    abs(buy_medium_qty - sell_medium_qty) +
    abs(buy_large_qty - sell_large_qty)
)
```

**欠けているもの**：
- バケット別 notional（`buy_small_notional` 等）→ 現在未保存。MDI^N は計算不可。
- side 別のバケット集計 → 既存カラムに存在（`buy_small_count` 等）。side 別 NDI/MDI は計算可能。

### 6.2 v2 schema で追加すべきカラム

現在の設計書（`cvd-size-thresholds-design.md`）の v2 候補に、以下を追加することを推奨：

```
# バケット別 notional（MDI^N 計算用）
buy_small_notional,  buy_medium_notional,  buy_large_notional
sell_small_notional, sell_medium_notional, sell_large_notional
```

これにより、NDI、MDI^Q、MDI^N の三指標がすべて計算可能になる。

### 6.3 推奨分析パイプライン

```
raw trades (trade/*.jsonl)
    │
    ▼
1s aggregation (feature-accumulator.mjs)
    ├── count by side × bucket → C_{s,b}
    ├── qty by side × bucket   → Q_{s,b}
    └── notional by side × bucket → N_{s,b}  [v2]
    │
    ▼
Window aggregator (新規 Python モジュール)
    ├── NDI(W) by bucket, by side
    ├── MDI^Q(W) by bucket, by side
    ├── MDI^N(W) by bucket, by side  [v2]
    ├── CVD contribution by bucket
    ├── avg trade size by bucket
    └── NDI skew by bucket
    │
    ▼
Output: dashboard metrics + alert thresholds
```

---

## 7. アラート閾値（初期推奨値）

| 指標 | 条件 | アラートレベル | 意味 |
|------|------|----------------|------|
| NDI_large | 1時間窓で >0.15 に急上昇 | ⚠️ Warning | 大口の取引頻度が異常増加（パニック/操作の可能性） |
| MDI^Q_large | >0.65 | ⚠️ Warning | 大口の金額支配が極度に高い（流動性の偏り） |
| NDI_skew_large | \|skew\| > 0.3 | 🔴 Critical | 大口の買い/売り偏りが顕著（方向性の強い大口フロー） |
| CVD寄与率_large | <0.3 かつ CVD_large 符号反転頻発 | ℹ️ Info | 大口が両方向に活発に取引（マーケットメイク/応酬） |
| 平均取引サイズ_large | 1時間窓で +50% 以上変動 | ⚠️ Warning | 大口の取引パターンが変化 |

---

## 8. 用語集

| 用語 | 英語 | 定義 |
|------|------|------|
| 数的支配度 | Numerical Dominance (NDI) | 総トレード回数に占める各バケットの割合 |
| 金額的支配度 | Monetary Dominance (MDI) | 総取引数量/金額に占める各バケットの割合 |
| CVD | Cumulative Volume Delta | 買い数量 − 売り数量 の累積和 |
| CVD 寄与率 | CVD Contribution Ratio | 絶対値 CVD のバケット別内訳比率 |
| 平均取引サイズ | Average Trade Size | バケット内の総数量 ÷ 総トレード回数 |
| NDI skew | NDI Skew | 買いNDI − 売りNDI。side偏りの指標 |
| 実効レート | Effective Rate | バケット内の総notional ÷ 総数量 |
| 象限分類 | Quadrant Classification | NDI×MDI の二軸マトリクスによる参加者タイプ分類 |

---

## 9. まとめ

1. **二軸フレームワークの核心**：トレードサイズ分析は「誰が頻繁に取引しているか」（NDI）と
   「誰が多くの資本を動かしているか」（MDI）の二軸で評価すべきである。
   これらは逆相関することが実データでも確認されている。

2. **v1 即時対応**：既存 schema のみで NDI、MDI^Q、CVD 寄与率は計算可能。
   これらの指標を `cvd_size_buckets.py` に追加することで、
   現行チャートの解釈精度を向上させられる。

3. **v2 で notional カラムを追加**：`buy_{bucket}_notional` / `sell_{bucket}_notional` を
   追加し、MDI^N と実効レートを計算可能にする。設計コストは小さい。

4. **Whale バケット追加時の注意**：Whale (≥$100K) は NDI が極めて低いため、
   短時間窓の CVD ラインとしてはノイズが支配的。1時間以上の窓で初めて
   意味のあるシグナルとなる。

5. **Spot/Perp 分離は必須**：参加者構造が根本的に異なるため、
   同一バケットでも Spot と Perp では解釈を変える必要がある。
   現行の分離表示はこの点で正しい設計である。

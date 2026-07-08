# AUX DRAFT — BTC サイズバケット分布 可視化設計書

> **Status:** 補助ドラフト。正本は `docs/size-dominance-two-axis-analysis.md`。
> **重要:** 本ドラフトは実装容易性から `qty_share` をY軸推奨しているが、最終方針では **金額的支配 = USD notional share** を主軸とする。`qty_share` は現行CVDがBTC qtyベースであるための補助表示に限定する。

## 1. 概要

既存 `scripts/cvd_size_buckets.py` は Small/Medium/Large のサイズ別 CVD（累積出来高差分）を時系列で描画する。
本設計では、**「件数支配度（Numerical Dominance）」** と **「金額支配度（Monetary Dominance）」** の二軸で、
各サイズバケットが市場構造の中でどのような位置を占めるかを静的に分析・可視化するチャートを提案する。

---

## 2. データソースと利用可能フィールド

### 2.1 入力データ（`data/1s_features/{date}/{market}.jsonl`）

既存 `load_data()` が読み込む1秒集計JSONL。以下のフィールドが今回の分析に関係する：

| フィールド | 型 | 意味 |
|---|---|---|
| `buy_small_count` | int | Small買い注文の取引回数 |
| `buy_medium_count` | int | Medium買い注文の取引回数 |
| `buy_large_count` | int | Large買い注文の取引回数 |
| `sell_small_count` | int | Small売り注文の取引回数 |
| `sell_medium_count` | int | Medium売り注文の取引回数 |
| `sell_large_count` | int | Large売り注文の取引回数 |
| `buy_small_qty` | int | Small買い数量（BTC整数×10^8） |
| `buy_medium_qty` | int | Medium買い数量 |
| `buy_large_qty` | int | Large買い数量 |
| `sell_small_qty` | int | Small売り数量 |
| `sell_medium_qty` | int | Medium売り数量 |
| `sell_large_qty` | int | Large売り数量 |
| `buy_notional` | int | 総買い想定元本（USDセント） |
| `sell_notional` | int | 総売り想定元本 |
| `mid_close` | float | 参照価格（ドル） |
| `market` | str | 市場名 |

### 2.2 サイズ区分定義（既存踏襲）

| バケット | 条件 | 色（既存踏襲） |
|---|---|---|
| Small | < $1,000 notional | `#4ade80` (緑) |
| Medium | $1,000 – $10,000 | `#fbbf24` (黄) |
| Large | >= $10,000 | `#f43f5e` (赤) |

---

## 3. 二軸の定義

### 3.1 件数支配度（Numerical Dominance）＝ X軸

```
count_share(bucket) = (buy_{bucket}_count + sell_{bucket}_count) / SUM_over_all_buckets(buy_*_count + sell_*_count)
```

**意味**：全取引回数のうち、そのサイズバケットが何%を占めるか。
この値が高いほど「取引の"数"では支配的」と言える（多くの場合 Small が高くなる）。

### 3.2 金額支配度（Monetary Dominance）＝ Y軸

設計上の選択肢が2つある：

**案A（数量ベース）**：
```
qty_share(bucket) = (buy_{bucket}_qty + sell_{bucket}_qty) / SUM_over_all_buckets(buy_*_qty + sell_*_qty)
```

**案B（想定元本ベース）**：
`notional_share` は数量×価格の近似が必要。厳密には `buy_notional` / `sell_notional` はバケット別に存在しないため、
近似値として `qty * mid_close` で代用、または数量シェアと同一視する。

**推奨：案A（数量シェア）を採用**。理由：
- データが直接利用可能
- BTC数量は価格変動の影響を受けないため、純粋なサイズ構造分析に適する
- 想定元本は数量シェアとほぼ比例するため、解釈上の差異は小さい

---

## 4. 推奨チャートタイプ：**散布図＋参照線（Dominance Scatter）**

### 4.1 全体構成

```
┌──────────────────────────────────────────────────────┐
│  BTC サイズバケット支配度マップ                        │
│  (Spot / Perp 集計, 過去N時間)                        │
├──────────────────────────┬───────────────────────────┤
│                          │                           │
│   [散布図]               │  [凡例・メトリクス表]      │
│   Y軸: 数量シェア(%)     │                           │
│                          │  Market  | Bucket | Count│
│   ● = Spot              │  ────────┼────────┼──────│
│   ▲ = Perp              │  spot    | Small  | 78%  │
│                          │  spot    | Medium | 15%  │
│   ●Large                 │  spot    | Large  |  7%  │
│        ↗(少数取引で      │  perp    | Small  | 65%  │
│          大量支配)       │  ...                     │
│     ●Medium              │                           │
│           ●Small         │  [KPI]                   │
│  (多数取引だが            │  Gini係数: 0.xx          │
│   金額支配は小)           │  Small偏重度: xx%        │
│                          │                           │
│  X軸: 取引回数シェア(%)  │                           │
│                          │                           │
│  ┈┈ y=x (比例線)         │                           │
│  ┊ 33.3% (均等線)        │                           │
│                          │                           │
├──────────────────────────┴───────────────────────────┤
│  [Spot 内訳バー]          [Perp 内訳バー]             │
│  積み上げ: Count / Qty   積み上げ: Count / Qty        │
└──────────────────────────────────────────────────────┘
```

### 4.2 メイン散布図の詳細仕様

| 項目 | 仕様 |
|---|---|
| Figure size | 20×12 inch（左右パネル構成のため広め） |
| 背景色 | `#0b1628`（既存踏襲） |
| フォントサイズ | タイトル 22pt, 軸ラベル 16pt, 目盛り 12pt, 凡例 14pt |
| X軸 | 取引回数シェア (%), 範囲 0–100, 線形 |
| Y軸 | 取引数量シェア (%), 範囲 0–100, 線形 |
| データ点 | バケット×集計区分 で最大6点（Small/Medium/Large × Spot/Perp） |

#### 点のスタイル

| 属性 | Spot | Perp |
|---|---|---|
| マーカー形状 | `o`（円） | `^`（上三角） |
| サイズ | 数量の絶対値に比例（s=100〜800） | 同左 |
| エッジ色 | 白, linewidth=1.5 | 白, linewidth=1.5 |
| 面の色 | Small=#4ade80, Medium=#fbbf24, Large=#f43f5e | 同色 |
| 透明度 | alpha=0.85 | alpha=0.85 |
| ラベル | 点の近傍にバケット名をannotation | 同左 |

#### 参照線

| 線 | スタイル | 意味 |
|---|---|---|
| y = x (45度線) | 白破線, alpha=0.3, linewidth=1.0 | **比例支配線**：この線上にあるバケットは、取引回数と取引数量が比例している（「普通」の状態）。 |
| x = 33.3% / y = 33.3% | 灰色点線, alpha=0.2 | **均等線**：3バケットが等しく分担した場合の目安線。 |
| 垂直・水平のガイド線 | 各点から軸へのドロップライン（細い破線, alpha=0.15） | 読み取り補助 |

#### グラフ領域の意味的解釈（quadrant labels）

```
         Y (数量シェア)
         ↑
  100%   │  少数大量型          │  大型支配型
         │  (Few whale trades   │  (Whale dominance)
         │   dominate volume)   │
         │                      │
         │───────比例線(y=x)─────│
         │                      │
         │  小口細分化型        │  多数大口型
         │  (Retail            │  (Institutional
         │   fragmentation)    │   churn)
    0%   │                      │
         └──────────────────────→ X (回数シェア)
         0%                   100%
```

### 4.3 補助パネル：サイドバー情報

散布図の右側に情報パネルを配置（GridSpecで `width_ratios=[3, 1]`）：

- **凡例**：マーカー形状（Spot/Perp）と色（Small/Medium/Large）の対応表
- **メトリクス表**：集計区分ごとの各バケットの count_share / qty_share 数値表
- **KPI表示**：
  - 加重平均乖離度（各点の y=x からの距離の数量加重平均）
  - Small 集中度（回数シェア − 数量シェア）

### 4.4 下部補助パネル（オプション）：積み上げバーチャート

散布図の下にコンパクトな積み上げバーを2本配置し、Spot/Perp それぞれの
「回数構成比」と「数量構成比」を並べて視覚的に比較できるようにする。

```
Spot:  [████ Small 78%][██ Med 15%][█ Lg 7%]  ← Count
       [███ Small 45%][████ Med 35%][██ Lg 20%] ← Qty

Perp:  [█████ Small 65%][███ Med 25%][██ Lg 10%] ← Count
       [███ Small 35%][████ Med 40%][████ Lg 25%] ← Qty
```

---

## 5. 統合方法（既存 `cvd_size_buckets.py` との共存）

### 5.1 推奨アプローチ：**既存スクリプトに `--dominance` フラグを追加**

最もシンプルで破壊的変更が少ない方法。

#### 追加する要素

**1. 新規関数 `compute_dominance(df)`**

```python
def compute_dominance(df):
    """
    Compute count-share and qty-share per (market_type, size_bucket).

    Returns
    -------
    DataFrame with columns:
        type, bucket, count_share, qty_share, total_count, total_qty
    """
```

既存の `df`（`load_data()` の戻り値）には以下のカラムが既に存在する：
- `b_s`, `b_m`, `b_l`（buy qty by size）
- `s_s`, `s_m`, `s_l`（sell qty by size）
- `buy_small_count` などは `load_data` 内で rename されていないので、元カラム名でアクセス

修正：`load_data()` 内で count 系カラムも rename して `df` に残す（現在は drop されていないので問題ない）。

**2. 新規関数 `chart_dominance(df, out_path)`**

散布図＋参照線＋補助パネルを描画する。

**3. CLI引数追加**

```python
p.add_argument('--dominance', action='store_true',
               help='Dominance scatter chart (count-share vs qty-share)')
p.add_argument('--dom-out', type=str, default=None,
               help='Output path for dominance chart')
```

**4. main() への分岐追加**

```python
if args.dominance:
    out = args.dom_out or os.path.join(BASE_DIR, 'dominance_scatter.png')
    chart_dominance(df, out)
```

### 5.2 トレードオフ分析

| 案 | 長所 | 短所 |
|---|---|---|
| **A: 既存スクリプトにフラグ追加（推奨）** | データ読み込みを共有できる。ユーザーが1つのスクリプトで完結。メンテナンス負荷が低い。 | `cvd_size_buckets.py` が肥大化する可能性。ただし追加関数は2つのみで許容範囲。 |
| B: 新規スクリプト `size_dominance.py` に分離 | 関心の分離が明確。ファイルが小さい。 | `load_data()` を重複 or import する必要がある。import できるように `cvd_size_buckets.py` をモジュール化する手間が発生。 |
| C: 共通ライブラリ `lib/charts.py` に `load_data` を切り出し | 最も綺麗な設計。 | 既存コードの大幅なリファクタリングが必要。タスクの範囲を超える。 |

**→ 案Aを強く推奨**。追加コードは約150行程度で、既存の346行に対して管理可能な増加量。

---

## 6. 実装イメージ（擬似コード）

### 6.1 支配度計算

```python
def compute_dominance(df):
    """Compute count/qty share per (type, bucket)."""
    # Aggregate totals per market_type
    grp = df.groupby('type').agg(
        cnt_s = ('buy_small_count', 'sum'),   # + sell_small_count も
        cnt_m = ('buy_medium_count', 'sum'),
        cnt_l = ('buy_large_count', 'sum'),
        qty_s = ('b_s', 'sum'),  # already net of buy - sell? No — need buy+sell
        qty_m = ('b_m', 'sum'),
        qty_l = ('b_l', 'sum'),
        # ... also need sell side counts and qtys
    )
    # ...
```

**注意**：`load_data()` 内で `b_s` 等は既に `buy_small_qty` → `b_s` に rename されている。
しかし売り側の count は rename されていないため、別途集計が必要。
`load_data()` の返す df には元カラムが（`cols` リストに含まれていれば）保持される。

現在の `cols` リスト：
```python
cols = ['ts', 'market', 'buy_small_qty', 'buy_medium_qty', 'buy_large_qty',
        'sell_small_qty', 'sell_medium_qty', 'sell_large_qty', 'mid_close']
```

**→ count系のカラムを `cols` に追加するパッチが必要**（1行変更）。

```python
cols = ['ts', 'market',
        'buy_small_qty', 'buy_medium_qty', 'buy_large_qty',
        'sell_small_qty', 'sell_medium_qty', 'sell_large_qty',
        'buy_small_count', 'buy_medium_count', 'buy_large_count',
        'sell_small_count', 'sell_medium_count', 'sell_large_count',
        'mid_close']
```

### 6.2 散布図描画

```python
def chart_dominance(df, out_path, hours):
    """Draw dominance scatter chart."""
    dom = compute_dominance(df)  # DataFrame: type, bucket, count_share, qty_share, ...

    fig = plt.figure(figsize=(20, 12))
    gs = GridSpec(1, 2, width_ratios=[3, 1])

    # ── Left: Scatter ──
    ax = fig.add_subplot(gs[0])
    ax.set_facecolor('#0b1628')

    # Reference lines
    ax.plot([0, 100], [0, 100], '--', color='white', alpha=0.25, lw=1.0,
            label='比例線 (y=x)')
    ax.axvline(33.3, color='gray', alpha=0.15, ls=':', lw=0.8)
    ax.axhline(33.3, color='gray', alpha=0.15, ls=':', lw=0.8)

    # Plot points
    for _, row in dom.iterrows():
        marker = 'o' if row['type'] == 'spot' else '^'
        color = SIZE_COLORS[['Small', 'Medium', 'Large'].index(row['bucket'])]
        size = row['total_qty'] * SCALE  # bubble size prop to absolute qty
        ax.scatter(row['count_share'], row['qty_share'],
                   s=size, c=color, marker=marker,
                   edgecolors='white', linewidth=1.5, alpha=0.85, zorder=5)
        # Annotation
        ax.annotate(f"{row['type']}\n{row['bucket']}",
                    (row['count_share'], row['qty_share']),
                    textcoords="offset points", xytext=(10, 10),
                    fontsize=10, color=TEXT_COLOR)

    # Axis labels
    ax.set_xlabel('取引回数シェア (%) — Numerical Dominance', ...)
    ax.set_ylabel('取引数量シェア (%) — Monetary Dominance', ...)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_title(f'BTC サイズバケット支配度マップ ({hours}h)',
                 color=TEXT_COLOR, fontsize=FS_TITLE, fontweight='bold')

    # Quadrant labels (軽いテキスト)
    ax.text(20, 85, '少数大量型', color='white', alpha=0.15, fontsize=14, ha='center')
    ax.text(80, 85, '大型支配型', color='white', alpha=0.15, fontsize=14, ha='center')
    ax.text(20, 15, '小口細分化型', color='white', alpha=0.15, fontsize=14, ha='center')
    ax.text(80, 15, '多数大口型', color='white', alpha=0.15, fontsize=14, ha='center')

    # ── Right: Metrics panel ──
    ax2 = fig.add_subplot(gs[1])
    ax2.axis('off')
    # Render text table of metrics
    ...

    fig.savefig(out_path, dpi=100, facecolor='#0b1628')
```

---

## 7. チャート解釈ガイド

### 7.1 典型的な期待パターン

| パターン | 散布図上の位置 | 市場解釈 |
|---|---|---|
| **典型的リテール市場** | Small が右下（回数多・数量小）、Large が左上（回数少・数量大） | 多数の小口取引と少数の大口取引が共存する健全な構造 |
| **クジラ支配市場** | Large が右上に張り付く | 大口が回数でも数量でも支配的。流動性が偏っている可能性 |
| **アルゴリズム市場** | Medium が比例線付近に位置 | 中規模のアルゴ取引がバランス良く執行されている |
| **極端な断片化** | Small が(x=90%, y=10%)付近 | 取引のほとんどが小口だが数量インパクトは小さい（リテール中心） |

### 7.2 Gini係数的解釈

y=x 線からの各点の距離の加重平均を「支配度非対称性指数」として表示することで、
市場のサイズ偏重度を単一指標で把握できる。

---

## 8. 将来拡張案

1. **時系列アニメーション**：1時間ごとの支配度散布図をGIF化し、時間変化を観察
2. **buy/sell 分離表示**：買い側・売り側で別々にプロット（非対称性の発見）
3. **市場間比較ヒートマップ**：全市場×全バケットの支配度をヒートマップで俯瞰
4. **取引所別詳細**：`--market` 指定で単一市場の支配度を表示（`--markets` で全市場）

---

## 9. 変更影響範囲サマリ

| 変更対象 | 内容 | 行数 |
|---|---|---|
| `scripts/cvd_size_buckets.py` L88 | `cols` リストに count 系6カラム追加 | 1行 |
| `scripts/cvd_size_buckets.py` 新規追加 | `compute_dominance(df)` 関数 | ~40行 |
| `scripts/cvd_size_buckets.py` 新規追加 | `chart_dominance(df, price_df, out_path, hours)` 関数 | ~100行 |
| `scripts/cvd_size_buckets.py` `main()` | `--dominance` 引数＋分岐 | ~10行 |

**合計：約150行の追加。既存の動作は一切変更なし。**

---

## 10. 使用例（想定CLI）

```bash
# Spot / Perp 集計の支配度散布図（デフォルト6時間）
python3 scripts/cvd_size_buckets.py --dominance

# 24時間で分析
python3 scripts/cvd_size_buckets.py --dominance --hours 24

# 出力先指定
python3 scripts/cvd_size_buckets.py --dominance --dom-out /tmp/dominance.png

# 既存CVDチャートと同時出力
python3 scripts/cvd_size_buckets.py --agg --dominance --hours 6
```

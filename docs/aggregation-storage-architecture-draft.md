# Aggregation Storage Architecture Draft

**Status:** draft  
**Purpose:** 集約データを `1s` / `30s` の固定発想で縛らず、特徴量の計算層と保存層を分離する。  
**Motivation:** 現状は `1s_features` と `30s_book` を前提に実装・dashboard・run report が組まれているが、特
徴量整理の本丸は時間幅よりも **意味の違う派生物をどう分けるか** にある。

---

## 1. 結論

保存設計は

- **1s / 30s のような時間幅中心**

ではなく、

- **dataset family（何を保存するか）**
- **view/window（どの時間粒度で切るか）**
- **schema version（どの意味で保存するか）**

の 3 軸で考える。

---

## 2. なぜ 1s / 30s 固定発想が危ないか

### 2.1 意味の違うものが同じ「集約」で混ざる
例:
- OHLCV は trade-bucket local
- burst core は overlap-based
- run/gap は bucket-local structure
- book snapshot shape は boundary-state / window projection

同じ 1s row に載っていても、計算の意味は全然違う。

### 2.2 30s_book は「30秒だから 30s」ではない
本質は
- macro liquidity shape
- bounded depth snapshot summary
- market coverage tier を持つ book projection

であって、30秒という長さ自体は一つの view にすぎない。

### 2.3 将来の再利用がしにくい
固定的に
- `1s_features`
- `30s_book`

で作ると、将来
- 5s trade_flow
- 10s burst_summary
- 60s book_shape
- event-driven snapshots

を増やすたびに設計が破綻する。

---

## 3. 保存設計の新しい考え方

### 3.1 dataset family
まず「何系の派生物か」で分ける。

推奨 family:

1. **trade_features**
   - OHLCV
   - qty/notional
   - size bucket
   - side structure
   - burst 派生もここに含めてよい

2. **book_state**
   - boundary state
   - spread / mid / microprice
   - ring depth
   - best queue state

3. **book_shape**
   - macro な価格帯ごとの板量
   - coverage_tier 付き
   - 現在の `30s_book` はここ

4. **quality_metrics**
   - stale
   - missing
   - invalid counts
   - source lag

5. **run_reports**
   - 処理件数
   - 入力 manifest
   - invalid row 数
   - generated dataset 数

### 3.2 view / window
family の次に、どう切るかを持つ。

例:
- `1s`
- `5s`
- `10s`
- `30s`
- `60s`
- `event`
- `session`

重要なのは、**window は family の属性であって family そのものではない**こと。

### 3.3 schema version
同じ family + window でも意味変更は version で分ける。

例:
- `trade_features.core.v1`
- `trade_features.core.v2`
- `book_shape.usd1bins.v1`
- `burst_summary.slice1.v1`

---

## 4. 推奨ディレクトリ設計

現行:

- `derived_v1/1s_features/...`
- `derived_v1/30s_book/...`
- `derived_v1/runs/...`

草案:

- `derived_v2/trade_features/1s/<date>/<market>.jsonl`
- `derived_v2/book_state/1s/<date>/<market>.jsonl`
- `derived_v2/book_shape/30s/<date>/<market>.jsonl`
- `derived_v2/quality_metrics/1s/<date>/<market>.jsonl`
- `derived_v2/run_reports/<run_id>.json`

または schema version をさらに明示するなら:

- `derived_v2/trade_features/core_v1/1s/...`
- `derived_v2/book_shape/usd1bins_v1/30s/...`

---

## 5. 現在の出力をこの考え方へ写すとどうなるか

### 5.1 `1s_features`
これは 1 個の dataset ではなく、実際には混成体。

中身は少なくとも:
- trade_features
- book_state
- quality_metrics
- burst_summary
- local_structure

が混ざっている。

**結論:**
短期的には `1s_features` のまま出してよいが、概念上は

- `1s_core_features`
- `1s_burst_features`
- `1s_book_quality`

の論理分割を持って設計する。

### 5.2 `30s_book`
これは名前を変えるとより正確。

候補:
- `book_shape/30s`
- `book_snapshot_bins/30s`
- `macro_book_shape/30s`

この中で一番意味が明快なのは **`book_shape/30s`**。

---

## 6. 実装方針

### Phase A: 論理設計を変える（今）
- 既存の物理 path は急いで壊さない
- docs 上で family / window / version の 3 軸へ切り替える
- `1s_features` / `30s_book` は **legacy physical names** と明記する

### Phase B: run report を柔軟化
現状 run report は
- `feature_rows`
- `book_rows`

の 2 種前提。

これを将来的に:
- `datasets.trade_features_1s.rows`
- `datasets.book_shape_30s.rows`
- `datasets.quality_metrics_1s.rows`

のような map 型にする。

### Phase C: dashboard を dataset-aware にする
現状の Aggregation Pipeline パネルは `feature_rows` / `book_rows` 固定。

今後は
- dataset family
- latest completed window
- rows written
- schema version

を動的に並べる方がよい。

---

## 7. いま固定してよい最小運用ルール

### Rule 1
**時間幅は dataset identity ではない。**

### Rule 2
**まず family を決めてから、その family の window を決める。**

### Rule 3
**同じ row に混在していても、意味の違う feature 群は docs 上で別ブロックとして管理する。**

### Rule 4
**`1s_features` と `30s_book` は当面の physical output 名であり、論理モデル名ではない。**

### Rule 5
**新しい dataset を足すときは、まず family / window / version を宣言してから列を議論する。**

---

## 8. この方針での次アクション

次にやるべきは次の 2 本。

1. **`docs/1s-feature-core-contract.md`**
   - ただしタイトルは "1s 固定" ではなく、trade_features / book_state / quality を意識して書く

2. **`docs/book-shape-contract.md`**
   - 現 `30s_book` の論理名を定義する
   - 30秒は first view として扱う

その後、実装・dashboard・run report の命名を順に寄せる。

---

## 9. 暫定結論

- 保存は 1s / 30s 前提ではなく **family × window × version** で考える
- `1s_features` は当面残してよいが、論理的には分解して扱う
- `30s_book` は実質 `book_shape/30s` であり、時間幅より意味を前に出すべき
- これで将来の 5s / 10s / 60s / event-driven dataset を自然に追加できる

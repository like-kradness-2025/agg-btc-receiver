# Feature Foundation Contract Draft

**Status:** draft  
**Purpose:** 1s / burst / 30s 系の特徴量でブレないために、まず「特徴量の意味の根幹」を固定する。  
**Source priority:** 本文書 < 個別 contract。衝突時は個別 contract が優先。

---

## 1. まず固定するべきこと

特徴量を列名の一覧としてではなく、**計算層**で分けて固定する。

本系統の特徴量は少なくとも次の 5 層に分かれる。

1. **Trade-bucket local**
   - その 1 秒の trade だけで閉じる
   - 例: `open/high/low/close`, `trade_count`, `buy_notional`, size bucket

2. **Book-boundary state**
   - 秒の開始/終了時点の book state を forward-fill して読む
   - 例: `mid_open`, `mid_close`, `spread_bps_open`, `spread_bps_close`, `best_bid_open/close`, `best_ask_open/close`

3. **Book-event flow**
   - その 1 秒の depth update event から加算する
   - 例: `bid_add_qty_near`, `best_replenish_count`, `depth_update_count`

4. **Burst-overlap summary**
   - full ordered trade stream で先に burst を作り、その後 1 秒に overlap で落とす
   - 例: `burst_count_1s`, `max_burst_notional_1s`, `same_price_burst_*`, `multilevel_burst_*`, `buy_burst_notional_1s`

5. **Bucket-local structure / validation**
   - 秒内 print だけで閉じる、または秒内の burst-associated print + book classification を使う
   - 例: `max_same_side_run_prints_1s`, `side_flip_count_1s`, `same_side_gap_ms_*`, `burst_at_touch_ratio_1s`

この 5 層を混ぜないことが最優先。

---

## 2. 時間基準

### 2.1 1s row
- 1 行 = `market × 1 second`
- second bucket = `[ts, ts + 1000)`
- `ts` は epoch ms の秒 floor

### 2.2 30s row
- 1 行 = `market × 30 second window`
- 30s bucket = `[window_start_ms, window_start_ms + 30000)`

### 2.3 burst
- burst は **秒ごとに作らない**
- full ordered trade stream 上で先に形成する
- 1s への反映は overlap rule を使う

---

## 3. NULL と 0 の原則

### 3.1 trade-only / deterministic decomposition
trade だけで決まる count/sum/max は、該当なしなら **0**。

対象:
- `trade_count`
- `buy_notional` / `sell_notional`
- burst core
- same-price burst
- multilevel burst
- directional burst summary

### 3.2 empty sample statistics
サンプル集合が空の統計量は **NULL**。

対象:
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`
- 将来の percentile / avg 系で empty sample のもの

### 3.3 book-derived state unavailable
book state が stale / missing / unsynchronized で観測不能なら **NULL**。

対象:
- `mid_open` / `mid_close`
- `spread_bps_*`
- book depth state
- burst book validation ratio/count のうち観測不能なもの

### 3.4 observed zero events
book state は利用可能だが qualifying event が 0 件なら **0**。

対象:
- `depth_update_count`
- `best_replenish_count`
- `burst_depletion_count_1s`（book usable のとき）
- `burst_replenish_after_touch_count_1s`（book usable のとき）

---

## 4. 1s 特徴量の大分類

### A. Trade-bucket local（確定度: 高）
既存 `docs/1s-features-schema.md` と `docs/1s-features-schema-v2.md` で方向性が一致している層。

- `open`, `high`, `low`, `close`
- `vwap`
- `trade_count`
- `buy_qty` / `sell_qty`（または現実装の `buy_volume` / `sell_volume` を rename）
- `buy_notional`, `sell_notional`, `delta_notional`
- size bucket 系

**未固定点:**
- 現実装は total bucket (`small_volume` など)
- schema は buy/sell 別 bucket (`buy_small_qty`, `sell_small_qty` など)
- ここは rename ではなく **意味が違う** ので要決定

### B. Book-boundary state（確定度: 高）
`1s-features-schema.md` と `1s-features-schema-v2.md` でほぼ一致。

- `mid_open`, `mid_close`
- `spread_bps_open`, `spread_bps_close`
- `best_bid_open`, `best_ask_open`
- `best_bid_close`, `best_ask_close`

### C. Book depth state / flow（確定度: 中）
過去 schema と既存 `feature-accumulator` に両方根拠がある。

- depth ring / bps bucket
- add/cancel qty near/deep
- `best_replenish_count`
- `depth_update_count`
- `stale_ms`
- `missing_flag`

**未固定点:**
- v1 schema の cumulative depth と v2 proposal の ring depth のどちらを採るか
- best queue dynamics 13 列を P0 に入れるか、後段フェーズに分けるか

### D. Burst-overlap summary（確定度: 高）
burst contract 群でかなり固定済み。

- core burst 4列
- same-price burst 3列
- multilevel burst 3列
- directional burst 3列
- concentration 1列

### E. Bucket-local structure / validation（確定度: 高）
`burst-summary-contract.md` と `burst-book-validation-contract.md` で計算層が固定済み。

- `max_same_side_run_prints_1s`
- `side_flip_count_1s`
- `same_side_gap_ms_min_1s`
- `same_side_gap_ms_p25_1s`
- `burst_at_touch_ratio_1s`
- `burst_through_ratio_1s`
- `burst_depletion_count_1s`
- `burst_replenish_after_touch_count_1s`

---

## 5. いまブレやすい論点

### 5.1 名前と意味がズレているもの
- 実装: `buy_volume` / `sell_volume`
- schema: `buy_qty` / `sell_qty`

- 実装: `small_volume`, `medium_volume`, `large_volume`
- schema: `buy_small_qty`, `sell_small_qty`, ...

この差は cosmetic ではなく、**片側別か total か**の意味差。

### 5.2 v1 schema と v2 proposal が混ざりやすい
- `1s-features-schema.md` は比較的 lean
- `1s-features-schema-v2.md` は best queue dynamics / imbalance / microprice / cross-venue まで含む拡張案

したがって、今「特徴量をきっちり整理する」では、
**lean core** と **extended optional** を分離して決める必要がある。

### 5.3 burst 系は 1s local ではない
burst 系を OHLCV と同列に「その秒の trade を数える」感覚で扱うとズレる。

- burst core / same-price / multilevel / directional / concentration は **overlap-based**
- run/gap / at-touch validation は **bucket-local**

この二層は絶対に混同しない。

---

## 6. 固定したい最小コアセット

まずブレなく固定すべき最小コアは次の 4 ブロック。

### Core-1: trade local
- OHLCV
- trade_count
- buy/sell qty
- buy/sell notional
- delta_notional
- buy/sell size buckets

### Core-2: book boundary + quality
- mid_open / mid_close
- spread_bps_open / close
- best_bid/ask_open / close
- depth_update_count
- stale_ms
- missing_flag

### Core-3: burst overlap
- burst core 4列
- same-price 3列
- multilevel 3列
- directional 3列
- concentration 1列

### Core-4: bucket-local structure
- max_same_side_run_prints_1s
- side_flip_count_1s
- same_side_gap_ms_min_1s
- same_side_gap_ms_p25_1s

book validation 4列は core に入れてもよいが、book synchronization contract と一緒に凍結する方が安全。

---

## 7. 次の文書化単位

この draft の次は、列を増やすのではなく、次の 2 本に分けて固定するのがよい。

1. **`docs/1s-feature-core-contract.md`**
   - trade local
   - book boundary
   - quality
   - bucket-local structure
   - null/zero
   - naming

2. **`docs/1s-feature-burst-contract.md`**
   - 既存 burst contract 群を 1s row schema に落とした adapter 文書
   - overlap-based と bucket-local validation の境界を固定

---

## 8. 現時点の暫定結論

- 特徴量は「列名一覧」ではなく **計算層** で固定すべき
- 既にかなり固まっているのは burst contract 群
- いちばんブレやすいのは trade local の naming と size bucket semantics
- 次に固めるべきは **1s core 特徴量 contract** であり、30s 設計より先

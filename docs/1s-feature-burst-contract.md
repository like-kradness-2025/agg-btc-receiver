# 1s Feature Burst Contract

**Status:** draft v1  
**Scope:** burst 系 1 秒特徴量の adapter 文書 — 既存 burst contract 群を 1s row schema に投影し、計算層・NULL/0 ルール・lean core / extended optional 境界を固定する。  
**Out of scope:** burst 形成そのもの（→ `docs/burst-formation-contract.md`）、same-price 分解（→ `docs/same-price-burst-contract.md`）、multilevel 分類（→ `docs/multilevel-burst-contract.md`）、directional / concentration / run / gap 要約（→ `docs/burst-summary-contract.md`）、book-aware 検証（→ `docs/burst-book-validation-contract.md`）。  
**Governing foundation:** `docs/feature-foundation-contract-draft.md`  
**Storage relation:** 本 contract の列群は論理的には `burst / 1s / core.v1`。physical output では `1s_features` row に混在してよいが、論理 identity は分離管理する。  
**Scope clarification vs 1s-feature-core-contract:** `docs/1s-feature-core-contract.md` は非 burst 系のみを扱う。v1 では `max_same_side_run_prints_1s` / `side_flip_count_1s` / `same_side_gap_ms_*` を本 contract 側へ移管する。理由は、burst-associated structure / validation と一緒に review した方が意味境界が割れにくいため。

---

## 1. 本 contract の位置づけ

### 1.1 Adapter layer

本 contract は **既存 burst contract 群に対する adapter 文書** である。

既存 burst contract 群が authoritative であり、本 contract がそれらと矛盾した場合は既存 contract が勝つ。
本 contract の役割は、既存 contract 群が定義した意味を **1s row の列としてどう投影するか** のみを固定することにある。

### 1.2 Authoritative source hierarchy

優先順位（上位が勝つ）:

1. `docs/burst-formation-contract.md` — burst 形成そのもの
2. `docs/same-price-burst-contract.md` — same-price 分解
3. `docs/multilevel-burst-contract.md` — multilevel 分類
4. `docs/burst-summary-contract.md` — directional / concentration / run / gap 要約
5. `docs/burst-book-validation-contract.md` — book-aware 検証
6. **本 contract** — 上記の 1s row 投影

### 1.3 本 contract が定義しないもの

- burst 形成の split rule、threshold scope、値
- same-price / multilevel の検出ロジックそのもの
- directional / concentration の計算式そのもの
- book-aware ratio / count の分類ロジックそのもの

これらはすべて個別 contract に委譲する。

---

## 2. スコープ境界

### 2.1 Core burst features（lean core）

trade stream のみから deterministic に導出され、book state を一切必要としない列群。
v1 実装の必須対象。

内訳:
- core burst 4 列（overlap-based）
- same-price burst 3 列（overlap-based）
- multilevel burst 3 列（overlap-based）
- directional burst 3 列（overlap-based）
- concentration 1 列（overlap-based）
- bucket-local print structure 4 列（bucket-local, trade-only）

### 2.2 Book-aware validation features（extended optional, Phase 5）

book state（best bid / best ask）を必要とし、at-touch / through 分類または depletion / replenish 検出に依存する列群。
v1 では extended optional。実装優先度は core burst features より低い。

内訳:
- burst_at_touch_ratio_1s
- burst_through_ratio_1s
- burst_depletion_count_1s
- burst_replenish_after_touch_count_1s

### 2.3 境界ルール

- book-aware validation 列は burst 形成に一切関与しない（post-formation validation）
- book-aware validation 列が core burst 列の値を書き換えることはない
- book state が unavailable でも core burst 列は常に出力可能

---

## 3. 計算層分類

### 3.0 タイムスタンプ基準

burst feature の 1s row タイムスタンプ基準は `docs/1s-feature-core-contract.md` §1 から継承する:
- `ts` は exchange event time 基準、exchange 時刻がない venue のみ recv_time フォールバック
- このルールは burst 形成時の burst_start_ts / burst_end_ts にも適用される
- burst overlap 計算は burst_start_ts と burst_end_ts をこの基準で正規化された ts から計算する

### 3.1 Overlap-based（burst-overlap summary）

**定義:** full ordered trade stream 上で先に burst を形成し、その後 1s bucket に overlap rule で落とす。

**overlap rule（burst-formation-contract §10.2 より）:**
- 1s bucket = `[bucket_start_ts, bucket_start_ts + 1000)`
- burst interval = `[burst_start_ts, burst_end_ts]`
- burst が bucket に overlap する条件: `burst_start_ts < bucket_end_ts AND burst_end_ts >= bucket_start_ts`

**結果:**
- bucket 境界をまたぐ burst は複数 1s row に寄与しうる
- burst は bucket 内で再形成されない

### 3.2 Bucket-local

**定義:** その 1s bucket 内の print / event のみから計算する。bucket 境界をまたがない。

**2 つのサブ分類:**

| サブ分類 | 依存データ | 列 |
|---|---|---|
| trade-only | trade prints のみ | max_same_side_run_prints_1s, side_flip_count_1s, same_side_gap_ms_min_1s, same_side_gap_ms_p25_1s |
| book-dependent | trade prints + book state | burst_at_touch_ratio_1s, burst_through_ratio_1s, burst_depletion_count_1s, burst_replenish_after_touch_count_1s |

---

## 4. Column definitions

### 4.1 Group B1: Core burst（overlap-based）

Source: `docs/burst-formation-contract.md`

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `burst_count_1s` | BIGINT | 1s bucket に overlap する Phase 1 burst の数 | 0（no bursts overlap） |
| `max_burst_notional_1s` | DOUBLE | overlap する Phase 1 burst の `burst_notional` 最大値 | 0（no bursts overlap） |
| `max_burst_prints_1s` | BIGINT | overlap する Phase 1 burst の `burst_print_count` 最大値 | 0（no bursts overlap） |
| `max_burst_duration_ms_1s` | DOUBLE | overlap する Phase 1 burst の `burst_duration_ms` 最大値 | 0（no bursts overlap） |

注釈:
- `burst_count_1s` は「その秒内で開始かつ終了した burst の数」ではない。overlap する burst すべてを数える。
- `max_burst_notional_1s` は trade raw notional ではなく burst-level `burst_notional` の最大値。

### 4.2 Group B2: Same-price burst（overlap-based）

Source: `docs/same-price-burst-contract.md`

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `same_price_burst_count_1s` | BIGINT | 1s bucket に overlap する same-price sub-run の数 | 0 |
| `same_price_burst_max_len_1s` | BIGINT | overlap する same-price sub-run の `same_price_print_count` 最大値 | 0 |
| `same_price_burst_notional_1s` | DOUBLE | overlap する全 same-price sub-run の `same_price_notional` 合計 | 0 |

注釈:
- same-price sub-run は Phase 1 burst 内部の sub-structure。単独で burst 形成されるわけではない。
- `same_price_burst_count_1s > burst_count_1s` は正常。1 つの Phase 1 burst 内に複数の same-price sub-run が存在しうるため。
- price equality は canonical normalized numeric price key で評価する（same-price-burst-contract §4）。

### 4.3 Group B3: Multilevel burst（overlap-based）

Source: `docs/multilevel-burst-contract.md`

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `multilevel_burst_count_1s` | BIGINT | 1s bucket に overlap する multilevel Phase 1 burst の数 | 0 |
| `multilevel_burst_max_span_ticks_1s` | BIGINT | overlap する multilevel burst の `span_ticks` 最大値 | 0 |
| `multilevel_burst_notional_1s` | DOUBLE | overlap する全 multilevel burst の `burst_notional` 合計 | 0 |

注釈:
- multilevel 分類は Phase 1 burst の属性。`max_price != min_price` の burst のみ multilevel。
- `span_ticks = round((burst_max_price - burst_min_price) / tick_size)`。`tick_size` は market/venue 設定から取得し、burst tape から推論しない。
- same-price 列と multilevel 列は同時に非ゼロになりうる（1 つの Phase 1 burst が multilevel かつ same-price sub-run を含むのは正常）。

### 4.4 Group B4: Directional burst（overlap-based）

Source: `docs/burst-summary-contract.md` §3

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `buy_burst_notional_1s` | DOUBLE | overlap する buy-side burst の `burst_notional` 合計 | 0 |
| `sell_burst_notional_1s` | DOUBLE | overlap する sell-side burst の `burst_notional` 合計 | 0 |
| `burst_delta_notional_1s` | DOUBLE | `buy_burst_notional_1s - sell_burst_notional_1s` | 0 |

注釈:
- `burst_delta_notional_1s` の正値は buy-side burst dominance、負値は sell-side burst dominance を示す。
- これらの列は burst notional のみを対象とし、全 trade notional ではない。

### 4.5 Group B5: Concentration（overlap-based）

Source: `docs/burst-summary-contract.md` §4

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `largest_burst_share_notional_1s` | DOUBLE | `max_burst_notional_1s_overlap / total_burst_notional_1s` | 0（no bursts overlap） |

注釈:
- 分母は overlap する全 burst の notional 合計（全 trade notional ではない）
- burst が 1 つだけ overlap する場合、値は `1.0`
- 範囲: bursts overlap 時は `(0, 1]`、no bursts 時は `0`

### 4.6 Group B6: Bucket-local print structure（bucket-local, trade-only）

Source: `docs/burst-summary-contract.md` §6-9

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `max_same_side_run_prints_1s` | BIGINT | 1s bucket 内の最長 same-side contiguous print run 長 | 0（0 prints in bucket）；1（1 print） |
| `side_flip_count_1s` | BIGINT | 1s bucket 内の隣接 print pair で side が異なる回数 | 0（0 or 1 print） |
| `same_side_gap_ms_min_1s` | DOUBLE | 1s bucket 内の same-side 隣接 pair gap 最小値 | **NULL**（G is empty） |
| `same_side_gap_ms_p25_1s` | DOUBLE | 1s bucket 内の same-side 隣接 pair gap 25th percentile（線形補間） | **NULL**（G is empty） |

注釈:
- これらの列は **overlap-based ではない**。bucket 境界をまたがない。
- `max_same_side_run_prints_1s` は `max_burst_prints_1s` とは異なる。前者は gap/duration split rule を無視し、純粋に side persistence のみを見る。
- `side_flip_count_1s` は bucket 境界をまたぐ flip をカウントしない。
- same-side gap sample `G` の定義: 1s bucket 内の隣接 print pair `(p_i, p_{i+1})` のうち `p_i.side == p_{i+1}.side` である pair の `gap_ms = p_{i+1}.ts - p_i.ts` 集合。same-side だが隣接でない pair（間に opposite-side print が入る）は含まない。
- gap 列の `NULL` vs `0` の区別: `NULL` は「same-side 隣接 pair サンプルが存在しない」、`0` は「実測値として 0ms gap が観測された」を意味する。

### 4.7 Group B7: Book-aware validation（bucket-local, book-dependent, Phase 5）

Source: `docs/burst-book-validation-contract.md`

| Column | Type | Semantics | Null/Zero |
|---|---|---|---|
| `burst_at_touch_ratio_1s` | DOUBLE | 1s bucket 内の分類可能 burst-associated notional に占める at-touch notional の割合 | **NULL**（`N_total_classified = 0`） |
| `burst_through_ratio_1s` | DOUBLE | 1s bucket 内の分類可能 burst-associated notional に占める through notional の割合 | **NULL**（`N_total_classified = 0`） |
| `burst_depletion_count_1s` | BIGINT | 1s bucket 内で at-touch burst activity と co-occur した best-level depletion イベント数 | **NULL**（book unavailable）；0（book available, no qualifying event） |
| `burst_replenish_after_touch_count_1s` | BIGINT | 1s bucket 内で at-touch burst activity と co-occur した best-level replenishment イベント数 | **NULL**（book unavailable）；0（book available, no qualifying event） |

注釈:
- burst-associated trade とは「Phase 1 burst に属し、かつ `ts` が 1s bucket 内にある print」。
- ratio の分母 `N_total_classified` は「分類可能な burst-associated notional」のみ。分類不能な notional は分母から除外。
- ratio の範囲: `[0, 1]`。`0` は「分類可能な burst-associated trade が存在し、numerator が 0」を意味する。`NULL` は「分類可能な burst-associated trade が 1 件も存在しない」を意味する。
- `burst_at_touch_ratio_1s + burst_through_ratio_1s <= 1`（at-touch と through は相互排他）。
- depletion / replenish count の `NULL` vs `0`: `NULL` は book state が利用不可でイベント検出不能、`0` は book state 利用可能かつ検出イベント 0 件。
- これらの列は **因果証明ではない**。co-occurrence の集計にすぎない。

---

## 5. Lean core vs extended optional

### 5.1 Lean core（v1 必須）

| Group | 列数 | 計算層 | 依存データ |
|---|---|---|---|
| B1: Core burst | 4 | overlap-based | trade only |
| B2: Same-price burst | 3 | overlap-based | trade only |
| B3: Multilevel burst | 3 | overlap-based | trade only |
| B4: Directional burst | 3 | overlap-based | trade only |
| B5: Concentration | 1 | overlap-based | trade only |
| B6: Bucket-local print structure | 4 | bucket-local | trade only |
| **Lean core 合計** | **18** | | |

### 5.2 Extended optional（Phase 5）

| Group | 列数 | 計算層 | 依存データ |
|---|---|---|---|
| B7: Book-aware validation | 4 | bucket-local | trade + book state |
| **Extended optional 合計** | **4** | | |

### 5.3 境界ルール

- lean core は book state に一切依存しない。book connector が不在でも 18 列すべてが出力可能。
- extended optional は book state の可用性に依存する。book unavailable 時は ratio 列が NULL、count 列が NULL になる。
- lean core / extended optional の分割は実装優先度の指定であり、論理的な列の所属ではない。22 列すべてが `burst / 1s` に属する。

---

## 6. Bucket-local structure group の帰属明確化

### 6.1 移管の経緯

`docs/feature-foundation-contract-draft.md` では `max_same_side_run_prints_1s` / `side_flip_count_1s` / `same_side_gap_ms_*` を「bucket-local structure」として core 候補に含めていた。

v1 では、これら 4 列を `docs/1s-feature-core-contract.md` から本 contract へ移管する。
理由は、burst-associated structure（run, flip）および burst-adjacent texture（gap）は burst contract 群と一緒に review した方が意味境界が割れにくいため。

### 6.2 現在の帰属

| 列 | 帰属 | 理由 |
|---|---|---|
| `max_same_side_run_prints_1s` | burst contract（本 doc） | Phase 1 burst の `max_burst_prints_1s` との対比で意味が決まる |
| `side_flip_count_1s` | burst contract（本 doc） | burst side alternation pattern の補完指標 |
| `same_side_gap_ms_min_1s` | burst contract（本 doc） | burst gap threshold との対比で解釈される timing texture |
| `same_side_gap_ms_p25_1s` | burst contract（本 doc） | 同上 |

### 6.3 計算層の一貫性

これら 4 列は bucket-local だが、burst contract の文脈で定義される。
overlap-based 列と bucket-local 列が同じ contract に混在することは許容する。
重要なのは「計算層を混同しないこと」であり、「同じ contract に置かないこと」ではない。

---

## 7. NULL / 0 ルール

`docs/1s-feature-core-contract.md` §9 および `docs/feature-foundation-contract-draft.md` §3 と一貫性を保つ。

### 7.1 Trade-derived count / sum / max（0）

該当する burst / trade event が存在しない場合、値は **0**。

対象:
- Group B1 全列（core burst）
- Group B2 全列（same-price burst）
- Group B3 全列（multilevel burst）
- Group B4 全列（directional burst）
- Group B5（concentration）
- Group B6 の count 系: `max_same_side_run_prints_1s`, `side_flip_count_1s`

### 7.2 Empty-sample statistics（NULL）

サンプル集合が空の統計量は **NULL**。

対象:
- Group B6 の gap 系: `same_side_gap_ms_min_1s`, `same_side_gap_ms_p25_1s`

### 7.3 Book-dependent ratio with empty classified denominator（NULL）

分類可能な burst-associated notional が 0 の場合、ratio は **NULL**。

対象:
- `burst_at_touch_ratio_1s`, `burst_through_ratio_1s`

### 7.4 Book-dependent count with unavailable book state（NULL）

book state が利用不可でイベント検出不能の場合、count は **NULL**。

対象:
- `burst_depletion_count_1s`, `burst_replenish_after_touch_count_1s`

### 7.5 Observed zero events with available book state（0）

book state は利用可能だが qualifying event が 0 件の場合、count は **0**。

対象:
- `burst_depletion_count_1s`, `burst_replenish_after_touch_count_1s`

### 7.6 ルール一覧表

| 条件 | 値 | 対象列グループ |
|---|---|---|
| 該当 burst/trade なし | `0` | B1, B2, B3, B4, B5, B6 count |
| empty sample | `NULL` | B6 gap |
| classified denominator 空 | `NULL` | B7 ratio |
| book unavailable | `NULL` | B7 count |
| book available, 0 events | `0` | B7 count |

---

## 8. Output path and dataset identity

### 8.1 Logical dataset identity

```
burst / 1s / core.v1
```

- `family` = `burst`
- `window` = `1s`
- `version` = `core.v1`

### 8.2 Physical output path

`docs/aggregation-storage-contract.md` §6.2 の canonical recommended layout に従う（version `core.v1` → path segment `core_v1`）:

```
derived_v2/burst/core_v1/1s/<date>/<market>.jsonl
```

short-term では legacy `derived_v1/1s_features/...` に混在出力してよいが、long-term では上記 path へ分離する。

### 8.3 Version semantics

`version` は意味変更の契約。以下の変更が生じた場合、version を `core.v2` に引き上げる:

- 既存列の意味変更
- null/zero semantics の変更
- 同名列の計算層変更（overlap-based ↔ bucket-local）
- required column set の破壊的変更

単なる列追加が backward-compatible である場合は同 version に残してよいが、疑義があるなら version を上げる（aggregation-storage-contract §5.2）。

### 8.4 Run report 表現

`docs/aggregation-storage-contract.md` §8 の dataset-aware run report に従い、burst dataset は次のように表現する:

```json
{
  "family": "burst",
  "window": "1s",
  "version": "core.v1",
  "rows": <count>
}
```

---

## 9. Non-burst 1s core との関係

| 観点 | `1s-feature-core-contract.md` | 本 contract |
|---|---|---|
| 対象列群 | trade local, book boundary, book depth, book flow, quality | burst 全列（overlap + bucket-local structure + book-aware validation） |
| 計算層 | bucket-local, boundary state, event flow | overlap-based, bucket-local |
| book 依存 | 一部あり（boundary/depth/flow） | extended optional のみ |
| bucket-local structure | **含まない**（本 contract へ移管済み） | **含む**（B6） |
| dataset identity | `trade_features / 1s / core.v1`, `book_state / 1s / core.v1`, `quality_metrics / 1s / core.v1` | `burst / 1s / core.v1` |

両 contract の和集合が 1s feature の全体像を構成する。列の重複はない。

---

## 10. Implementation constraints

本 contract に従う実装は以下を満たさなければならない:

- burst 形成は full ordered trade stream 上で先に実行し、1s bucket 内で再形成しない
- overlap-based 列は形成済み burst から overlap rule で計算する
- bucket-local 列は bucket 境界をまたがない
- same-price と multilevel は同一の canonical price key を使う
- `multilevel_burst_max_span_ticks_1s` の `tick_size` は market/venue 設定から取得し、burst tape から推論しない
- gap 列の empty sample は `NULL`、実測 0ms は `0` として区別する
- book-aware ratio 列の分母は分類可能 notional のみとし、分類不能分を除外する
- book-aware count 列の `NULL`（book unavailable）と `0`（book available, 0 events）を区別する

本 contract に従う実装は以下を行ってはならない:

- burst 形成に price continuity を split rule として使う
- burst 形成に book state を使う
- same-price / multilevel 分類で burst 境界をまたぐ merge を行う
- `largest_burst_share_notional_1s` の分母に全 trade notional を使う
- bucket-local 列に cross-second adjacency を流入させる
- book-aware 列の NULL を暗黙に 0 へ置換する
- burst feature を parent-order identity として再解釈する命名やドキュメントを生成する

---

## 11. Exit check
本 contract は以下のすべてを満たすとき有効:

本 contract は以下のすべてを満たすとき有効:

- [x] burst contract 群が authoritative source であり、本 contract は adapter であることが明示されている
- [x] core burst features（lean core）と book-aware validation（extended optional）の境界が明示されている
- [x] 全 22 列について、group / 列名 / type / 意味 / 計算層 / null-zero rule / source contract が定義されている
- [x] bucket-local structure 4 列の本 contract への帰属が明示され、移管理由が記載されている
- [x] output path が canonical layout `derived_v2/<family>/<version_path>/<window>/...` に従い定義されている（version `core.v1` → path segment `core_v1`）
- [x] dataset identity が `burst / 1s / core.v1` で定義されている
- [x] non-burst core との関係と境界が明示されている
- [x] いかなる箇所でも burst = parent-order identity を暗示していない
- [x] タイムスタンプ基準が `docs/1s-feature-core-contract.md` §1 から継承されている

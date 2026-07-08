# Aggregation Storage Contract

**Status:** draft v1  
**Purpose:** 集約データの保存設計を、固定的な `1s` / `30s` 名称から切り離し、**family × window × version** で扱うための契約。  
**Authoritative scope:** 保存レイヤの論理モデル、dataset identity、path 命名原則、run report の表現原則。

---

## 1. Design goal

この契約の目的は、集約データを「何秒集約か」だけで分類しないことにある。

集約データはまず:
- **何を表すデータか** (`family`)
- **どの粒度で切られた view か** (`window`)
- **どの意味定義の schema か** (`version`)

で識別する。window は dataset identity の一部である。ただし、family を置き換える dataset 名ではない。

---

## 2. Dataset identity

### 2.1 Canonical identity
各 dataset は次の 3 要素で識別する。

- `family`
- `window`
- `version`

### 2.2 Examples
- `trade_features / 1s / core.v1`
- `book_state / 1s / core.v1`
- `book_shape / 30s / usd1bins.v1`
- `quality_metrics / 1s / core.v1`
- `burst / 1s / core.v1`
- `run_reports / run / v1`

### 2.3 Rule
同じ `window` を使っていても `family` が違えば別 dataset。

---

## 3. Family definitions

### 3.1 `trade_features`
trade event から直接または deterministic に導かれる特徴量群（burst 系を除く）。

含みうるもの:
- OHLCV
- trade count
- buy/sell qty/notional
- delta notional
- size buckets
- local print structure

### 3.2 `book_state`
book の境界状態または depth event flow から導かれる特徴量群。

### 3.3 `book_shape`
window 単位で保存する macro liquidity shape / bounded depth snapshot summary。

### 3.4 `quality_metrics`
観測品質・欠損・stale・invalid 数を表す補助 dataset。

### 3.5 `burst`
burst 系特徴量群。burst 形成・same-price sub-run・multilevel 分類・directional concentration・bucket-local print structure を含む。
現行 contract では `1s` view を定義しているが、family が window に束縛されるわけではない。

### 3.6 `run_reports`
集約ジョブ自体の処理結果・入力 manifest・行数・invalid 数の記録。

---

## 4. Window semantics

### 4.1 Meaning
`window` は dataset family の属性であり、family の代わりではない。

### 4.2 Allowed examples
- `1s`, `5s`, `10s`, `30s`, `60s`, `event`, `run`

### 4.3 Rule
新しい保存物を追加するときは、まず family を定義し、その後で適切な window を選ぶ。

---

## 5. Version semantics

### 5.1 Meaning
`version` は path 上の文字列ではなく、**意味変更の契約**である。

### 5.2 Rule
次の変更は version を上げるべき対象: 既存列の意味変更、null/zero semantics の変更、同名列の計算層変更、required column set の破壊的変更。
単なる列追加が backward-compatible である場合は同 version に残してよいが、疑義があるなら version を上げる。

---

## 6. Physical path guidance

### 6.1 Short-term compatibility rule
既存の physical path はすぐには壊さない。現在の `derived_v1/1s_features/...`, `derived_v1/30s_book/...`, `derived_v1/runs/...` は **legacy physical names** として当面維持してよい。

### 6.2 Recommended future layout
Canonical recommended な path pattern:

```
derived_v2/<family>/<version_path>/<window>/<date>/<market>.jsonl
```

ただし `run_reports` は window=`run` で date/market が固定されないため、例外的に:

```
derived_v2/run_reports/v1/<run_id>.json
```

**Path 内の version 表現:** logical version string に `.` が含まれる場合、path segment では `_` に置換する。
- `core.v1` → `core_v1`
- `usd1bins.v1` → `usd1bins_v1`

Examples:
- `derived_v2/trade_features/core_v1/1s/<date>/<market>.jsonl`
- `derived_v2/book_state/core_v1/1s/<date>/<market>.jsonl`
- `derived_v2/book_shape/usd1bins_v1/30s/<date>/<market>.jsonl`
- `derived_v2/quality_metrics/core_v1/1s/<date>/<market>.jsonl`
- `derived_v2/burst/core_v1/1s/<date>/<market>.jsonl`
- `derived_v2/run_reports/v1/<run_id>.json`

---

## 7. Interpretation of current outputs

### 7.1 `1s_features`
`1s_features` は単一 family ではなく、実際には複合 row である。少なくとも trade local、book boundary state、book event flow、quality、burst summaries、bucket-local structure を内包する。short-term では `1s_features` の physical output を許容するが、long-term では logical families として分解して管理する。

### 7.2 `30s_book`
`30s_book` の論理名は `book_shape / 30s` とみなす。

---

## 8. Run report contract

### 8.1 Problem
現在の run report が `feature_rows` / `book_rows` の 2 系統前提だと、dataset が増えると表現力不足になる。

### 8.2 Required direction
run report は dataset-aware にする。

推奨 shape:

```json
{
  "run_id": "...",
  "completed_at": "...",
  "datasets": [
    {"family": "trade_features", "window": "1s", "version": "core.v1", "rows": 1234},
    {"family": "book_shape", "window": "30s", "version": "usd1bins.v1", "rows": 52}
  ]
}
```

### 8.3 Rule
新しい dataset を足すたびに `datasets` 配列へ identity 付き object を追加する。

---

## 9. Dashboard contract

Dashboard は将来的に dataset-aware になる。現状の `feature_rows` / `book_rows` 固定表示は legacy 互換として許容する。

---

## 10. Rules for future dataset discussions

1. 時間幅だけで dataset 名を決めない
2. 最初に family を宣言する
3. 次に window を宣言する
4. 意味変更があるなら version を宣言する
5. 列議論はその後に行う

---

## 11. Immediate consequences for current work

- `1s_features` をそのまま議論するのではなく、まずその中の logical blocks を分けて考える
- `30s_book` は `book_shape / 30s` として契約化する
- burst 系は独立した `burst` family として扱う
- quality 系は論理的には独立 block として管理する

---

## 12. Exit check

この保存契約は次の条件をすべて満たすとき有効。

- [ ] dataset identity が family × window × version で説明されている
- [ ] `burst` family が §3 に正規の family として定義されている
- [ ] current physical names と future logical names が区別されている
- [ ] run report が dataset-aware に拡張可能である
- [ ] `30s_book` が時間幅名ではなく意味名で再解釈されている
- [ ] canonical path が `derived_v2/<family>/<version_path>/<window>/<date>/<market>.jsonl` で統一されている
- [ ] logical version と path segment の変換ルール（`.` → `_`）が定義されている
- [ ] `run_reports` の例外的 layout が明記されている

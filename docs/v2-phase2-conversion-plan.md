# agg-btc-receiver v2 phase2 conversion plan

## Goal
receiver の外でデータ変換責務を整理し、`raw_hot` を唯一の受信一次保存ソースにする。

## Confirmed phase1 state
- receiver は raw trade / raw depth / raw snapshot / liquidation を保存する
- receiver 内では 1s_features, premium/basis, market data REST 補助収集を行わない

## Existing assets to adapt

### 1. `scripts/convert-to-parquet.mjs`
- 入力: `data/raw_hot/{date}/{stream}/{market}.jsonl`
- 出力: `data/parquet/{date}/{stream}/{market}.parquet`
- 役割: **raw archive / verification**
- 維持方針: そのまま使う

### 2. `scripts/aggregate-1s.mjs`
- 入力: `data/1s_features/{date}/{market}.jsonl`
- 出力: `data/agg/{market}.parquet`
- 役割: **1s_features の merge / compaction**
- 維持方針: そのまま使う

## Missing bridge
現状足りないのは以下だけ:

- `raw_hot/trade`
- `raw_hot/depth`
- `raw_hot/snapshot`

から

- `data/1s_features/{date}/{market}.jsonl`

を作る変換段。

## Phase2 design decision
新しい receiver 本体や新しい live 入口は作らない。

### Chosen pipeline
1. receiver → `data/raw_hot/...`
2. raw archive (optional / batch) → `scripts/convert-to-parquet.mjs`
3. raw feature build (new phase2 bridge) → `data/1s_features/...`
4. feature merge → `scripts/aggregate-1s.mjs`

## Constraints
- 既存構造をアダプトする
- receiver 本体は触らない
- 変換は別プロセス / 別実行単位で扱う
- 1s_features schema は当面維持する
- 新規エントリポイント乱立を避ける

## Recommended implementation approach
### Option A (recommended)
既存 `lib/feature-accumulator.mjs` を **オフライン再生** に使う薄い変換スクリプトを追加する。

入力:
- raw trade JSONL
- raw depth JSONL
- raw snapshot JSONL

処理:
- ts 順に replay
- trade → `feedTrade`
- depth → `feedDepth`
- second boundary / snapshot state で `feedSecond`
- 出力は従来どおり `data/1s_features/...`

理由:
- `aggregate-1s.mjs` を変更しなくてよい
- 既存 schema / downstream を壊さない
- feature 計算責務だけを receiver 外へ移せる

## Non-goals
- 1s_features schema v2 への移行
- agg parquet schema 変更
- market data REST 系の復帰
- receiver への逆戻り

## Next implementation scope
- raw→1s_features bridge の設計
- event ordering / replay policy の仕様化
- 最小サンプルで fixture replay test を作る

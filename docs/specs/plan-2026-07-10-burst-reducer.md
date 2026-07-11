# DS4-Flash 実行計画: バースト特徴量レデューサー v1

**文書 ID:** plan-2026-07-10-burst-reducer
**日付:** 2026-07-10
**対象リポジトリ:** `agg-btc-receiver`（`/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver`）
**アーキテクチャ設計:** `docs/specs/design-2026-07-10-burst-reducer.md`
**機能仕様:** `docs/specs/specify-2026-07-09-burst-features.md`（節番号で参照。本計画書で仕様を重複定義しない）
**実行環境:** Node.js 22+, ESM (`"type": "module"`), `node:test`, `node:assert/strict`
**想定実装者:** ゼロコンテキストの Flash エージェント。安全でない推測をする傾向があるため、すべての指示を明示的に記述する。

---

## 目次

1. [Flash エージェント向け絶対ルール](#1-flash-エージェント向け絶対ルール)
2. [アンチパターン集](#2-アンチパターン集)
3. [スコープ外リスト](#3-スコープ外リスト)
4. [フェーズ概要](#4-フェーズ概要)
5. [タスクカード](#5-タスクカード)
   - [Task 0: リポジトリ検証と構造確認](#task-0-リポジトリ検証と構造確認)
   - [Task 1: Golden fixtures 作成](#task-1-golden-fixtures-作成)
   - [Task 2: スキーマ・契約定義](#task-2-スキーマ契約定義)
   - [Task 3: InputValidator 実装](#task-3-inputvalidator-実装)
   - [Task 4: BlockScanner 実装](#task-4-blockscanner-実装)
   - [Task 5: BurstDetector 実装](#task-5-burstdetector-実装)
   - [Task 6: FeatureComputer (1s) trade-only コア実装](#task-6-featurecomputer-1s-trade-only-コア実装)
   - [Task 7: OutputCommitter 実装](#task-7-outputcommitter-実装)
   - [Task 8: ReducerPipeline 統合](#task-8-reducerpipeline-統合)
   - [Task 9: CLI エントリポイント](#task-9-cli-エントリポイント)
   - [Task 10: 契約テスト統合](#task-10-契約テスト統合)
   - [Task 11: 実データ検証](#task-11-実データ検証)
6. [テスト実行コマンド一覧](#6-テスト実行コマンド一覧)

---

## 1. Flash エージェント向け絶対ルール

以下のルールに違反した場合、実装は即座に却下される:

1. **本番コードの変更禁止。** `lib/`, `scripts/`, `orderflow_monitor.mjs`, `fairprice_monitor.mjs`, `dashboard.mjs`, `test/` 内の**既存ファイル**を一切変更しない。既存ファイルへの追記も禁止。本計画書で明示的に指定された新規ファイルのみ作成する（例: `scripts/reduce-burst-v1.mjs`, `lib/burst-reducer/*.mjs`）。新規テストファイルは `test/burst-reducer/` 以下のみ許可。`package.json` への変更は Task 9 記載の script 追加のみ許可。
2. **新規ファイルは本計画書で指定されたパスのみ。** 計画書にないファイルを作成しない。
3. **`data/burst_agg/` と `data/1s_features/` に一切書き込まない。** 出力先は常に `data/derived/burst_features_v1/` 以下。
4. **BurstBuilder を再実装しない。** 常に `import { BurstBuilder } from '../burst-builder.mjs'`（`lib/burst-reducer/*.mjs` から）または `import { BurstBuilder } from '../../lib/burst-builder.mjs'`（`test/burst-reducer/*.test.mjs` から）を使用。パラメータは `gap_threshold_ms=50, max_burst_duration_ms=5000` に固定。
5. **NULL を 0 に置換しない。** P1 契約に従う: #13 `burst_notional_vs_top_depth` = `null` + `_quality.book_seeded: false`。#14 `burst_mid_move_bps_1s` = `0`（`null` ではない、P1 契約上の観測値なし）。#15-#22 = `0`（`null` ではない）。全22列が物理出力に存在する。
6. **生ブロックファイルを削除しない。** `data/live_v3/` 内のファイルは読み取りのみ。
7. **部分出力禁止。** 入力検証失敗時はブロック全体を破棄し、部分的な行を出力しない。
8. **ts のみの冪等性判定禁止。** composite key `{schema_version}:{market}:{block_start_ms}:{input_sha256}` を使用。block_start 単独使用禁止。
9. **30s 集計で 1s overlap 値の単純合計禁止。** `_overlap_sum_30s` と `_sum_30s`（直接集計）を区別。
10. **チェックポイントとデータコミットを分離しない。** 5-step atomic commit（intent→data→checkpoint→committed）で論理ユニットとして扱う。
11. **`scripts/burst-agg.mjs` の出力スキーマを継承しない。** 本計画書で定義する独自スキーマを使用。
12. **暗黙の warmup 推測禁止。** checkpoint なしで始める最初の 1 ブロック（30 行）のみ `_quality.warmup: true`。再起動で checkpoint 復元できれば `warmup: false`。30 blocks ではない。
13. **マーケット別パラメータ上書き禁止。** v1 は全マーケット同一パラメータ。ただし `tick_size` は `market_tick_size` ハードコードマップから取得。
14. **MVP14 正式 spec 準拠。** `burst_notional_vs_30s_traded_notional`（#12）を P1a scope に含め、agg_trades input を読む。これが完了するまで「MVP14 complete」を名乗らない。book-dependent #13/#14 は P4 で、P1 出力は契約上 #13=null, #14=0 + quality flags。
15. **未定義の npm パッケージ追加禁止。** `package.json` に既に存在する依存以外を追加してはならない（Node.js 標準ライブラリのみで実装）。
16. **`console.log` による非構造化出力禁止（production code）。** `lib/burst-reducer/**` および `scripts/reduce-burst-v1.mjs` の production コードでは、構造化 JSON Lines ログを `process.stderr` に出力する。`console.log` / stdout への出力はこれら production ファイルでは禁止。ただし、シェルコマンド（`echo`, Node.js `--input-type=module -e` の one-liner）やテスト/検証スクリプトは stdout を使用してもよいが、ログ出力には `console.error` を推奨する。検証 one-liner 内の `console.log` は検証専用例外（verification-only exception）とし、production コードには絶対に含めない。
17. **同一block内の ts decrease は E004 quarantine/fail（正規化禁止）。** 同一 ts は許可し、順序は `(ts, hasTradeId ? 0 : 1, normalizedTradeIdOrEmpty, source_file_line_index)` で一意に決める。このコンパレータに違反した実装は即座に却下。
18. **P1 は raw read-only。** `data/live_v3/` のファイルを一切削除/移動しない。クリーンアップは P6 のみ。
19. **`??` 演算子: JS nullish-coalescing only。** 本計画書内の `??` はすべて JavaScript の nullish-coalescing 演算子であり、未解決の TODO マーカーや「要確認」標識ではない。実際の `?? → 要確認` のようなマーカーが残存していた場合は即座に削除すること。P1 コード生成時に `??` を認識できない場合は標準の `!= null ? ... : ...` に置き換えてもよい。

---

## 2. アンチパターン集

以下は Flash エージェントが犯しがちな誤りとその防止策:

| # | アンチパターン | なぜ間違いか | 正しい実装 |
|---|---|---|---|
| AP1 | `import assert from 'assert'` | `node:assert/strict` を使用すること | `import assert from 'node:assert/strict'` |
| AP2 | `require()` を使用 | ESM プロジェクト（`"type": "module"`） | `import ... from '...'` のみ |
| AP3 | ファイル拡張子 `.js` | プロジェクトの慣習は `.mjs` | 全新規ファイルは `.mjs` 拡張子 |
| AP4 | `describe/it` を `node:test` 以外から import | `node:test` が標準 | `import { describe, it } from 'node:test'` |
| AP5 | テストファイルを `test/` 直下に置く | 既存テストと混在 | `test/burst-reducer/*.test.mjs` |
| AP6 | 相対パスを間違える（`../../lib/` の深さ） | 新規コードの配置場所により変わる | `lib/burst-reducer/*.mjs` → `../burst-builder.mjs`。`test/burst-reducer/*.test.mjs` → `../../lib/burst-builder.mjs` |
| AP7 | `JSON.parse` の try-catch 省略 | 不正な JSON 行でクラッシュ | 全行を try-catch でパース |
| AP8 | `Math.floor(ts / 1000) * 1000` の代わりに `Date` オブジェクト使用 | タイムゾーンの影響を受ける | `ts - (ts % 1000)` を使用 |
| AP9 | 文字列としてのソート（`'10' < '2'`） | 数値順と異なる結果 | `a - b` で数値ソート |
| AP10 | `burst.start_ts >= ws && burst.start_ts < we` を 1s overlap に使用 | 1s overlap の条件が異なる（仕様 §2.3） | `burst_start_ts < bucketEnd && burst_end_ts >= bucketStart` |
| AP11 | `fs.writeFileSync` のみでコミット | クラッシュ時に中途半端なファイルが残る | `.tmp` 書き込み → `fs.renameSync` |
| AP12 | バーストなし秒の行をスキップ | ゼロ埋め行が必要（分析の連続性） | 全秒の行を出力 |
| AP13 | `import { readFileSync } from 'fs'` | `node:fs` を使用 | `import { readFileSync } from 'node:fs'` |
| AP14 | `import { after } from 'node:test'` | ESM + `node:test` では正しい。cleanup に必須 | `import { describe, it, after } from 'node:test'`。Task 4, 7, 8, 10 のテストでは `after` が必須 |
| AP15 | `require()` または `console.log` を production code で使用 | ESM プロジェクトで `require` 不可。production コード（`lib/burst-reducer/**`, `scripts/reduce-burst-v1.mjs`）では `console.log` 禁止（rule #16）。検証 one-liner 内の `console.log` は verification-only exception。 | 検証 one-liner は `node --input-type=module -e '...'` で実行し、production code では一貫して `process.stderr` + JSON Lines を使用。テストファイル内の `console.error` は許可。 |

---

## 3. スコープ外リスト

以下の項目は本計画の範囲外。実装時に遭遇しても着手しないこと:

- 生ブロックファイル（`data/live_v3/`）の削除（P6 のクリーンアップフェーズ）
- 板リプレイ統合（P4 の book state replay）
- 30s 集約層（P3）
- 5min クロスマーケット層（P5）
- 研究 7 特徴量（P6）
- `outlier_trade_flag_1s`（P6）
- マーケット別パラメータ設定
- `agg_trades/` ディレクトリの読み取り（**P1a で `burst_notional_vs_30s_traded_notional` (#12) に必須。範囲内**）
- `book_updates/` ディレクトリの読み取り（P4 で必要）
- CLI `--delete-processed` フラグ
- 並列処理・マルチスレッド
- 既存の Receiver コードへの変更
- npm パッケージ追加
- DuckDB / Parquet 出力

---

## 4. フェーズ概要

```
P0  (Task 0-2):    準備 ─ リポジトリ確認 + fixtures + スキーマ
P1a (Task 3-8):    trade-only MVP コア + 1-block lag commit + #12 実装
P1b (Task 7b-9):   checkpoint persistence + restart determinism + atomic recovery
P3  (将来):        30s 集約
P4-P6 (将来):     板統合 + 5min + 研究特徴量
```

本計画書は **P0 + P1a + P1b** の全タスクをカバーする。

**重要: checkpoint/restart は「P2 将来機能」ではない。P1b 必須。**

v1 Phase 1 = P1a + P1b = 最小実行可能単位。checkpoint 永続化と再起動決定性を含む。

---

## 5. タスクカード

各タスクカードの形式:
- **時間目安:** 2-5 分（小さく分割）
- **作成/変更ファイル:** 絶対パス
- **振る舞い:** 期待動作
- **疑似コード/アサーション:** 実装の骨格
- **テスト:** 最低 1 つのテストケース
- **コマンド:** 検証用の実行コマンド
- **期待結果:** 成功時の出力
- **停止条件:** 失敗時の判断基準

---

### Task 0: リポジトリ検証と構造確認

**時間目安:** 2 分
**作成/変更ファイル:** なし（読み取りのみ）

**振る舞い:**
リポジトリの構造と既存ファイルが設計書の前提と一致していることを確認する。

**疑似コード/アサーション:**
```
ASSERT: package.json の "type" === "module"
ASSERT: lib/burst-builder.mjs が存在し、export class BurstBuilder を含む
ASSERT: lib/replay-book-state.mjs が存在し、export function replayBestBookState を含む
ASSERT: test/ ディレクトリに *.test.mjs ファイルが存在
ASSERT: data/live_v3/trades/ 以下にマーケットディレクトリが存在
ASSERT: docs/specs/specify-2026-07-09-burst-features.md が存在
ASSERT: docs/specs/design-2026-07-10-burst-reducer.md が存在
```

**テスト:** なし（手動確認）

**コマンド:**
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node -e "import('./lib/burst-builder.mjs').then(m => { if (!m.BurstBuilder) throw new Error('BurstBuilder missing'); console.error('OK: BurstBuilder found'); })"
node -e "import('./lib/replay-book-state.mjs').then(m => { if (!m.replayBestBookState) throw new Error('replayBestBookState missing'); console.error('OK: replayBestBookState found'); })"
node --test test/burst-builder.test.mjs 2>&1 | tail -3
ls data/live_v3/trades/
node --input-type=module -e "import { readFileSync } from 'node:fs'; const j=JSON.parse(readFileSync('package.json','utf8')); console.error('type:', j.type);"
```

**期待結果:**
- `OK: BurstBuilder found`
- `OK: replayBestBookState found`
- burst-builder テスト pass
- マーケットディレクトリ一覧表示
- `type: module`

**停止条件:** いずれかの ASSERT が失敗 → タスク停止。上位に環境不備を報告。

---

### Task 1: Golden fixtures 作成

**時間目安:** 5 分
**作成/変更ファイル:**
- `test/fixtures/burst-v1/trades-basic.jsonl`（作成）
- `test/fixtures/burst-v1/expected-features-1s.jsonl`（作成）
- `test/fixtures/burst-v1/trades-cross-boundary.jsonl`（作成）
- `test/fixtures/burst-v1/expected-cross-boundary-1s.jsonl`（作成）
- `test/fixtures/burst-v1/trades-empty-block.jsonl`（作成 — 空ファイル）
- `test/fixtures/burst-v1/expected-empty-block-1s.jsonl`（作成 — 全30行ゼロ）
- `test/fixtures/burst-v1/trades-single-print-burst.jsonl`（作成 — 単一print=バーストの明示）
- `test/fixtures/burst-v1/expected-single-print-burst-1s.jsonl`（作成）
- `test/fixtures/burst-v1/trades-cross-block-tail.jsonl`（作成 — block N tail）
- `test/fixtures/burst-v1/trades-cross-block-head.jsonl`（作成 — block N+1 head）
- `test/fixtures/burst-v1/expected-cross-block-restart-1s.jsonl`（作成 — restart後期待値）
- `test/fixtures/burst-v1/agg-trades-basic.jsonl`（作成 — #12 用 agg_trades。総 traded_notional=10000 を含むブロック）
- `test/fixtures/burst-v1/agg-trades-zerovolume.jsonl`（作成 — #12 zero-denominator：全 volume=0 で denominator=0。入力存在・検証済みで値=0を確認）
- `test/fixtures/burst-v1/expected-features-1s-12-nonzero.jsonl`（作成 — #12 非ゼロ denominator の golden expected。`burst_notional_vs_30s_traded_notional` を明示的に比較）
- `test/fixtures/burst-v1/expected-features-1s-12-zero.jsonl`（作成 — #12 zero denominator の golden expected：denom=0 → 値=0）

**#12 `burst_notional_vs_30s_traded_notional` の fixture 3 ケース:**
1. **(previous+current agg blocks, nonzero denominator):** `agg-trades-basic.jsonl` に block N の範囲 `[N_start-30000, N_end)` をカバーする agg rows。総 traded notional=10000。`trades-basic.jsonl` の trade と組み合わせると、ts=1000 の 1s bucket では `burst_notional_vs_30s_traded_notional = 704/10000 = 0.0704`。**この数値 0.0704 は ts=1000 の 1 秒にのみ適用され、他の秒では分母と分子が異なる。** per-second bucket ごとに分母が変わる点に注意。golden expected には全 30 秒の期待値を含める。
2. **(all required blocks present, zero volume):** `agg-trades-zerovolume.jsonl` — 必要な agg ブロックが存在し全行 `volume=0, vwap=100` → notional=0。denom=0 → `burst_notional_vs_30s_traded_notional=0`。**E007 は throw されない**（volume=0 は有効な状態、欠落とは異なる）
3. **(one required block absent → E007):** agg_trades ルックバック範囲 `[N_start-30000, N_end)` の一部ブロックが欠落 → `validateAggLookback` が `valid=false` → pipeline が E007 quarantine/fail。N uncommitted。**TDD アサーション: detector snapshot、checkpoint byte、manifest byte が pre-failure と完全同一**

**振る舞い:**
手計算可能な最小 trade シーケンスと、対応する期待出力 JSONL を作成する。
各フィクスチャは 30 秒ブロック 1 つ分の trade データ。

**single-print burst の明示（重要）:**
single-print（同一sideの単独trade）も `BurstBuilder` の仕様上 burst として形成される。
`trades-single-print-burst.jsonl` でこの動作を明示的にテストする。`trades-empty-block.jsonl`（旧称 `trades-no-burst.jsonl`）の誤った前提（「side違い=non-burst」はバーストなしだが、single-printがバーストでないという前提は誤り）は完全に削除する。

**疑似コード/アサーション（trades-basic.jsonl）:**
```jsonl
{"market":"test","price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t1"}
{"market":"test","price":100,"qty":2,"side":"buy","ts":1010,"tradeId":"t2"}
{"market":"test","price":101,"qty":1,"side":"sell","ts":1100,"tradeId":"t3"}
{"market":"test","price":101,"qty":3,"side":"sell","ts":1110,"tradeId":"t4"}
```
期待:
- 2 bursts: buy(t1,t2, gap=10ms ≤ 50 → 1 burst), sell(t3,t4, gap=10ms ≤ 50 → 1 burst)
- ts=1000-1999 が 1s bucket。1000ms の bucket に両方 overlap。
- burst_count_1s = 2
- same_price_burst_count_1s = 2（buy burst: price 100×2 → same_price。sell burst: price 101×2 → same_price）
- total_burst_notional = 100×1+100×2 + 101×1+101×3 = 100+200+101+303 = 704

**expected-features-1s.jsonl（該当秒のみ抜粋、P1 契約値）:**
```jsonl
{"ts":1000,"market":"test","burst_count_1s":2,"total_burst_notional_1s":704,"max_burst_notional_1s":404,"max_burst_prints_1s":2,"max_burst_duration_ms_1s":10,"buy_burst_notional_1s":300,"sell_burst_notional_1s":404,"burst_imbalance_ratio_1s":-0.1477,"largest_burst_share_notional_1s":0.5739,"same_price_burst_count_1s":2,"multilevel_burst_count_1s":0,"burst_notional_vs_30s_traded_notional":0,"burst_notional_vs_top_depth":null,"burst_mid_move_bps_1s":0,"same_price_burst_max_len_1s":0,"same_price_burst_notional_1s":0,"multilevel_burst_max_span_ticks_1s":0,"multilevel_burst_max_span_bps_1s":0,"multilevel_burst_notional_1s":0,"same_price_absorption_ratio_1s":0,"burst_delta_notional_1s":0,"outlier_trade_flag_1s":0,"_quality":{"book_seeded":false,"trade_count_this_second":4,"warmup":true,"input_block_ids":["0"]}}
```

**trades-cross-boundary.jsonl:**
```jsonl
{"market":"test","price":100,"qty":1,"side":"buy","ts":29950,"tradeId":"t1"}
{"market":"test","price":100,"qty":1,"side":"buy","ts":29970,"tradeId":"t2"}
```
30 秒ブロック [0, 30000) 内の trades。バーストは 29950-29970（gap=20ms ≤ 50 → 1 burst）。この burst は ts=29000 の 1s bucket [29000, 30000) に overlap する。

**trades-empty-block.jsonl:**
空ファイル（0 trades）。30ブロックすべての秒で全バースト項目 = 0 の期待出力。
expected-empty-block-1s.jsonl は全30行が `burst_count_1s: 0`, `total_burst_notional_1s: 0` など。

**trades-single-print-burst.jsonl:**
```jsonl
{"market":"test","price":100,"qty":1,"side":"buy","ts":500,"tradeId":"t1"}
```
single-print = 1つのburst（`BurstBuilder` の仕様上、同一sideの単独tradeもburstとして形成される）。
期待: burst_count_1s = 1（ts=0の1s bucketにoverlap）、total_burst_notional_1s = 100。
single-printがburstでないという誤った前提を完全に排除する。

**trades-cross-block-tail.jsonl + trades-cross-block-head.jsonl（P1b red test）:**
block N (0-30000) の末尾 trades:
```jsonl
{"market":"test","price":100,"qty":1,"side":"buy","ts":29950,"tradeId":"t1"}
{"market":"test","price":100,"qty":1,"side":"buy","ts":29970,"tradeId":"t2"}
```
block N+1 (30000-60000) の先頭 trades:
```jsonl
{"market":"test","price":100,"qty":1,"side":"buy","ts":30010,"tradeId":"t3"}
```
N 末尾の open burst（t1,t2）が N+1 先頭 t3 で継続するか確定するケース。restart あり/なし両方で byte-identical な final shards+manifests になることを P1b の red test とする。

**テスト:** Task 10 で golden テストを作成する。

**コマンド:**
```bash
mkdir -p test/fixtures/burst-v1
# ファイル作成後、JSON として正しいか検証
node --input-type=module -e "import { readFileSync } from 'node:fs'; const lines=readFileSync('test/fixtures/burst-v1/trades-basic.jsonl','utf8').trim().split('\n'); lines.forEach((l,i)=>{if(!l)return;try{JSON.parse(l)}catch(e){console.error('Line',i+1,'invalid');process.exit(1)}}); console.error('OK:',lines.length,'valid lines');"
```

**期待結果:** `OK: 4 valid lines`（trades-basic.jsonl の場合）

**停止条件:** JSONL がパース不可 → 修正。フィクスチャの期待値が設計書の定義と矛盾 → 設計書優先でフィクスチャを修正。

---

### Task 2: スキーマ・契約定義

**時間目安:** 3 分
**作成/変更ファイル:**
- `lib/burst-reducer/schema.mjs`（作成）

**振る舞い:**
出力行の型定義と定数を 1 ファイルに集約する。
定数はハードコード（CLI 引数不可）。

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/schema.mjs

export const SCHEMA_VERSION = 'burst_features_v1';

// 固定バーストパラメータ（v1 全マーケット共通）
export const GAP_THRESHOLD_MS = 50;
export const MAX_BURST_DURATION_MS = 5000;

// マーケット別 tick size（ハードコードマップ）
// multilevel_burst_max_span_ticks_1s の計算に必須。未定義マーケットでは当該列=null。
export const MARKET_TICK_SIZE = new Map([
  ['binance_spot', 0.01],
  ['binance_spot_usdc', 0.01],
  ['binance_perp', 0.01],
  ['binance_perp_btcusdc', 0.01],
  ['bybit_perp', 0.10],
  ['bybit_spot', 0.01],
  ['okx_perp', 0.10],
  ['okx_spot', 0.10],
  ['coinbase_spot', 0.01],
  ['kraken_spot', 0.10],
  ['hyperliquid_perp', 0.10],
  ['bitmex_perp', 0.50],
  ['bitstamp_spot', 0.01],
  ['crypto_com_spot', 0.01],
  ['bitfinex_spot', 0.01],
]);

/** マーケットの tick size を取得。未定義なら null */
export function getTickSize(market) {
  return MARKET_TICK_SIZE.get(market) ?? null;
}

// 出力パス
export const DERIVED_DIR = 'data/derived/burst_features_v1';
export const FEATURES_1S_DIR = 'features_1s';
export const FEATURES_30S_DIR = 'features_30s';
export const FEATURES_5MIN_DIR = 'features_5min';
export const MANIFESTS_DIR = 'manifests';
export const CHECKPOINTS_DIR = 'manifests/checkpoints';

// 物理行エンベロープキー（feature ではないが物理 JSON に含まれる）
export const ROW_ENVELOPE_FIELDS = ['ts', 'market', '_quality'];

// 1s 出力特徴量フィールド名のみ（#1-#22、順序固定、22列）
export const FEATURE_1S_FIELDS = [
  'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
  'max_burst_prints_1s', 'max_burst_duration_ms_1s',
  'buy_burst_notional_1s', 'sell_burst_notional_1s',
  'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
  'same_price_burst_count_1s', 'multilevel_burst_count_1s',
  'burst_notional_vs_30s_traded_notional',
  'burst_notional_vs_top_depth', 'burst_mid_move_bps_1s',
  'same_price_burst_max_len_1s', 'same_price_burst_notional_1s',
  'multilevel_burst_max_span_ticks_1s', 'multilevel_burst_max_span_bps_1s',
  'multilevel_burst_notional_1s', 'same_price_absorption_ratio_1s',
  'burst_delta_notional_1s', 'outlier_trade_flag_1s',
];

// Phase 1 で実装する trade-only フィールド（#1-#12）
export const PHASE1_FIELDS = new Set([
  'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
  'max_burst_prints_1s', 'max_burst_duration_ms_1s',
  'buy_burst_notional_1s', 'sell_burst_notional_1s',
  'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
  'same_price_burst_count_1s', 'multilevel_burst_count_1s',
  'burst_notional_vs_30s_traded_notional',
]);

// 板依存フィールド（#13, #14）
export const BOOK_DEPENDENT_FIELDS = new Set([
  'burst_notional_vs_top_depth', 'burst_mid_move_bps_1s',
]);

// 研究フィールド（#15-#21。P1 では常に 0）
export const RESEARCH_FIELDS = new Set([
  'same_price_burst_max_len_1s', 'same_price_burst_notional_1s',
  'multilevel_burst_max_span_ticks_1s', 'multilevel_burst_max_span_bps_1s',
  'multilevel_burst_notional_1s', 'same_price_absorption_ratio_1s',
  'burst_delta_notional_1s',
]);

// 監視フィールド（#22。P1 では常に 0）
export const MONITORING_FIELDS = new Set([
  'outlier_trade_flag_1s',
]);

/**
 * ゼロ埋めベース行を生成する（P1 契約に準拠）。
 * #1-#12: 0、#13: null、#14: 0、#15-#22: 0。
 * P1 での 0 は「観測値なし」を意味し、データ欠損ではない。
 */
export function createBaseRow(ts, market, quality) {
  return {
    ts,
    market,
    burst_count_1s: 0,
    total_burst_notional_1s: 0,
    max_burst_notional_1s: 0,
    max_burst_prints_1s: 0,
    max_burst_duration_ms_1s: 0,
    buy_burst_notional_1s: 0,
    sell_burst_notional_1s: 0,
    burst_imbalance_ratio_1s: 0,
    largest_burst_share_notional_1s: 0,
    same_price_burst_count_1s: 0,
    multilevel_burst_count_1s: 0,
    burst_notional_vs_30s_traded_notional: 0,
    burst_notional_vs_top_depth: null,     // #13: P1 null（book なし明示）
    burst_mid_move_bps_1s: 0,              // #14: P1 0（null ではない）
    same_price_burst_max_len_1s: 0,        // #15: P1 0
    same_price_burst_notional_1s: 0,        // #16: P1 0
    multilevel_burst_max_span_ticks_1s: 0,  // #17: P1 0
    multilevel_burst_max_span_bps_1s: 0,    // #18: P1 0
    multilevel_burst_notional_1s: 0,        // #19: P1 0
    same_price_absorption_ratio_1s: 0,      // #20: P1 0
    burst_delta_notional_1s: 0,             // #21: P1 0
    outlier_trade_flag_1s: 0,               // #22: P1 0
    _quality: quality,
  };
}
// _quality 契約根拠: 仕様 §9.2a。input_block_ids は raw trade ブロック ID のみ。agg hashes は manifest auxiliary_input_hashes に管理。
// NOTE: テストフィクスチャ内の `input_block_ids: ["0"]`、`["trades-basic"]` 等の shorthand 値はテスト専用。
// 本番コードでは `input_block_ids` に正準 raw trade ブロック識別子（UTC block_start_ms 文字列）を使用する。
// 例: `input_block_ids: ["1751821200000"]`（block start ms の文字列）

/**
 * 秒境界にフロアする（ts - (ts % 1000)）
 */
export function floorToSecond(ts) { return ts - (ts % 1000); }

/**
 * 1s バケットリストを生成 [block_start, block_start+30000) をカバーする全 1s
 */
export function* generateSeconds(blockStartMs) {
  for (let s = blockStartMs; s < blockStartMs + 30000; s += 1000) {
    yield s;
  }
}
```

**テスト疑似コード（`test/burst-reducer/schema.test.mjs` 想定）:**
```javascript
// test/burst-reducer/schema.test.mjs
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  FEATURE_1S_FIELDS,
  ROW_ENVELOPE_FIELDS,
  createBaseRow,
} from '../../lib/burst-reducer/schema.mjs';

describe('Schema / contract', () => {
  it('FEATURE_1S_FIELDS has exactly 22 logical feature columns', () => {
    // 22 = logical feature columns (#1-#22)
    assert.equal(FEATURE_1S_FIELDS.length, 22);
  });

  it('ROW_ENVELOPE_FIELDS are ts, market, _quality', () => {
    assert.deepEqual(ROW_ENVELOPE_FIELDS, ['ts', 'market', '_quality']);
  });

  it('createBaseRow produces 25 physical JSON top-level keys', () => {
    // 25 = ts + market + 22 feature columns + _quality
    const row = createBaseRow(1000, 'test', {
      book_seeded: false,
      trade_count_this_second: 0,
      warmup: true,
      input_block_ids: [],
    });
    assert.equal(Object.keys(row).length, 25);
  });
});
```

**コマンド:**
```bash
node --input-type=module -e "import('./lib/burst-reducer/schema.mjs').then(m => { console.error('SCHEMA_VERSION:', m.SCHEMA_VERSION); console.error('PHASE1_FIELDS size:', m.PHASE1_FIELDS.size); console.error('FEATURE_1S_FIELDS.length:', m.FEATURE_1S_FIELDS.length); console.error('ROW_ENVELOPE_FIELDS:', m.ROW_ENVELOPE_FIELDS); const row = m.createBaseRow(1000, 'test', {book_seeded:false,trade_count_this_second:0,warmup:true,input_block_ids:[]}); console.error('base row burst_count:', row.burst_count_1s); console.error('base row top_depth:', row.burst_notional_vs_top_depth); console.error('base row total keys:', Object.keys(row).length); });"
```

**期待結果:**
```
SCHEMA_VERSION: burst_features_v1
PHASE1_FIELDS size: 12
FEATURE_1S_FIELDS.length: 22
ROW_ENVELOPE_FIELDS: [ 'ts', 'market', '_quality' ]
base row burst_count: 0
base row top_depth: null
base row total keys: 25
```

**停止条件:** インポートエラー → ファイルパス確認。`PHASE1_FIELDS.size !== 12` → フィールドリスト修正。

---

### Task 3: InputValidator 実装

**時間目安:** 4 分
**作成/変更ファイル:**
- `lib/burst-reducer/input-validator.mjs`（作成）
- `test/burst-reducer/input-validator.test.mjs`（作成）

**振る舞い:**
1 つの 30 秒ブロックの trades JSONL 文字列を受け取り、各行を検証する。
1 行でも無効なら例外をスロー（ブロック全体検疫）。

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/input-validator.mjs

/**
 * @param {string} blockContent - JSONL の生文字列
 * @param {number} blockStartMs - ブロック開始時刻（30秒境界）
 * @returns {{trades: TradePrint[], inputSha256: string}}
 * @throws {Error} 無効な行が 1 つでもあれば即座に throw
 */
export function validateAndParseTrades(blockContent, blockStartMs) {
  const blockEndMs = blockStartMs + 30000;
  const trades = [];
  const lines = blockContent.trim().split('\n');
  let prevTs = -Infinity;

  for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
    const line = lines[lineIdx];
    if (!line) continue;

    let obj;
    try {
      obj = JSON.parse(line);
    } catch (e) {
      throw new Error(`E001: JSON parse error at line ${lineIdx + 1}: ${e.message}`);
    }

    // E002: 必須フィールド
    if (obj.ts === undefined || obj.side === undefined || obj.price === undefined || obj.qty === undefined) {
      throw new Error(`E002: missing required field at line ${lineIdx + 1}. Got: ${Object.keys(obj).join(',')}`);
    }

    const ts = Number(obj.ts);
    const price = Number(obj.price);
    const qty = Number(obj.qty);
    const side = String(obj.side);

    // E003: 値範囲
    if (price <= 0 || !isFinite(price)) throw new Error(`E003: invalid price ${obj.price} at line ${lineIdx + 1}`);
    if (qty <= 0 || !isFinite(qty)) throw new Error(`E003: invalid qty ${obj.qty} at line ${lineIdx + 1}`);
    if (side !== 'buy' && side !== 'sell') throw new Error(`E003: invalid side "${side}" at line ${lineIdx + 1}`);

    // E004: 単調性（同一tsは許可、減少は quarantine/fail）
    if (ts < prevTs) {
      throw new Error(`E004: ts decreased from ${prevTs} to ${ts} at line ${lineIdx + 1}`);
    }
    prevTs = ts;

    // E005: 範囲
    if (ts < blockStartMs || ts >= blockEndMs) {
      throw new Error(`E005: ts ${ts} outside block [${blockStartMs}, ${blockEndMs}) at line ${lineIdx + 1}`);
    }

    trades.push({
      ts,
      price,
      qty,
      side,
      _idx: lineIdx,
      tradeId: obj.tradeId || undefined,
      market: obj.market || 'unknown',
    });
  }

  // 安定ソート: ts ASC → tradeId ASC → _idx ASC
  trades.sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts;
    if (a.tradeId && b.tradeId) {
      const cmp = String(a.tradeId).localeCompare(String(b.tradeId), undefined, {numeric: true});
      if (cmp !== 0) return cmp;
    } else if (a.tradeId) return -1;
    else if (b.tradeId) return 1;
    return a._idx - b._idx;
  });

  // SHA256
  const inputSha256 = computeSha256(blockContent);

  return { trades, inputSha256 };
}

import { createHash } from 'node:crypto';
function computeSha256(str) {
  return createHash('sha256').update(str).digest('hex');
}
```

**テスト（`test/burst-reducer/input-validator.test.mjs`）:**
```javascript
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { validateAndParseTrades } from '../../lib/burst-reducer/input-validator.mjs';

describe('InputValidator', () => {
  it('valid trades pass validation', () => {
    const content = '{"market":"test","price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t1"}\n';
    const { trades, inputSha256 } = validateAndParseTrades(content, 0);
    assert.equal(trades.length, 1);
    assert.equal(trades[0].price, 100);
    assert.equal(typeof inputSha256, 'string');
    assert.equal(inputSha256.length, 64);
  });

  it('throws E001 on invalid JSON', () => {
    assert.throws(() => validateAndParseTrades('not json\n', 0), /E001/);
  });

  it('throws E002 on missing ts', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy"}\n', 0), /E002/);
  });

  it('throws E003 on negative price', () => {
    assert.throws(() => validateAndParseTrades('{"price":-1,"qty":1,"side":"buy","ts":1000}\n', 0), /E003/);
  });

  it('throws E005 on ts outside block', () => {
    assert.throws(() => validateAndParseTrades('{"price":1,"qty":1,"side":"buy","ts":99999}\n', 0), /E005/);
  });

  it('stable sort: same ts, tradeId ascending', () => {
    const content = [
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t3"}',
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t1"}',
      '{"price":100,"qty":1,"side":"buy","ts":1000,"tradeId":"t2"}',
    ].join('\n');
    const { trades } = validateAndParseTrades(content, 0);
    assert.equal(trades[0].tradeId, 't1');
    assert.equal(trades[1].tradeId, 't2');
    assert.equal(trades[2].tradeId, 't3');
  });
});
```

**コマンド:**
```bash
mkdir -p test/burst-reducer
node --test test/burst-reducer/input-validator.test.mjs
```

**期待結果:** 全 6 テスト pass。

**停止条件:** テスト失敗 → `lib/burst-reducer/input-validator.mjs` のロジック修正。

---

### Task 4: BlockScanner 実装

**時間目安:** 4 分
**作成/変更ファイル:**
- `lib/burst-reducer/block-scanner.mjs`（作成）
- `test/burst-reducer/block-scanner.test.mjs`（作成）

**振る舞い:**
`data/live_v3/trades/<market>/` を走査し、指定時刻範囲内の 30 秒ブロックファイルを列挙する。
ファイル名が 00/30 秒境界に揃っていることを検査する。

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/block-scanner.mjs

import { readdirSync, existsSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * @typedef {{ ms: number, fullPath: string, market: string, date: string }} BlockInfo
 */

/**
 * @param {string} dataDir - 'data/live_v3'
 * @param {string} market
 * @param {number} fromMs - epoch ms（30秒境界）
 * @param {number} toMs - epoch ms（30秒境界）
 * @returns {BlockInfo[]} ブロック開始時刻昇順
 */
export function scanTradeBlocks(dataDir, market, fromMs, toMs) {
  const tradesDir = join(dataDir, 'trades', market);
  if (!existsSync(tradesDir)) return [];

  const blocks = [];
  const dateDirs = readdirSync(tradesDir).filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d));

  for (const dateDir of dateDirs) {
    const datePath = join(tradesDir, dateDir);
    const timeFiles = readdirSync(datePath).filter(f => f.endsWith('.jsonl'));

    for (const tf of timeFiles) {
      const base = tf.replace('.jsonl', '');
      const [h, m, s] = base.split('-').map(Number);
      if (isNaN(h) || isNaN(m) || isNaN(s)) continue;

      // E006: 00/30 境界チェック（quarantine/fail、skip+warn 禁止）
      if (s !== 0 && s !== 30) {
        throw new Error(`E006: filename not on 00/30 boundary: ${tf} (seconds=${s})`);
      }

      const fileMs = Date.UTC(
        parseInt(dateDir.slice(0,4)), parseInt(dateDir.slice(5,7))-1, parseInt(dateDir.slice(8,10)),
        h, m, s
      );

      if (fileMs < toMs && fileMs + 30000 > fromMs) {
        blocks.push({ ms: fileMs, fullPath: join(datePath, tf), market, date: dateDir });
      }
    }
  }

  blocks.sort((a, b) => a.ms - b.ms);
  return blocks;
}
```

**テスト（`test/burst-reducer/block-scanner.test.mjs`）:**
```javascript
import assert from 'node:assert/strict';
import { describe, it, after } from 'node:test';
import { existsSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { scanTradeBlocks } from '../../lib/burst-reducer/block-scanner.mjs';

describe('BlockScanner', () => {
  const tmpDir = join('test', 'fixtures', 'burst-v1', 'tmp-scan');

  after(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
  });

  it('returns empty for non-existent market', () => {
    const result = scanTradeBlocks('data/live_v3', 'nonexistent_market_xyz', 0, 9999999999999);
    assert.deepEqual(result, []);
  });

  it('detects 30s files from temp fixture dir', () => {
    const dataDir = tmpDir;
    const market = 'test_scan';
    rmSync(join(dataDir, 'trades', market), { recursive: true, force: true });

    // create temp fixture: 2 valid blocks + 1 invalid
    const dateDir = join(dataDir, 'trades', market, '2026-07-10');
    mkdirSync(dateDir, { recursive: true });
    writeFileSync(join(dateDir, '00-00-00.jsonl'), '{"ts":0}\n');
    writeFileSync(join(dateDir, '00-00-30.jsonl'), '{"ts":30000}\n');
    writeFileSync(join(dateDir, '00-01-00.jsonl'), '{"ts":60000}\n');

    // Test range that includes all blocks
    const result = scanTradeBlocks(dataDir, market, 0, 120000);
    assert.equal(result.length, 3);
    for (const b of result) {
      assert.ok(b.ms % 30000 === 0, `block ${b.ms} not on 30s boundary`);
    }
    assert.equal(result[0].ms, 0);
    assert.equal(result[1].ms, 30000);
    assert.equal(result[2].ms, 60000);
  });

  it('throws E006 on non-00/30 boundary filename (quarantine/fail)', () => {
    const dataDir = tmpDir;
    const market = 'test_e006';
    rmSync(join(dataDir, 'trades', market), { recursive: true, force: true });

    const badDir = join(dataDir, 'trades', market, '2026-07-10');
    mkdirSync(badDir, { recursive: true });
    writeFileSync(join(badDir, '00-00-15.jsonl'), '{"ts":15000}\n');

    assert.throws(() => scanTradeBlocks(dataDir, market, 0, 120000), /E006/);
  });
});
```

**コマンド:**
```bash
node --test test/burst-reducer/block-scanner.test.mjs
```

**期待結果:** 実データが存在すればブロックが列挙され、全ブロックが 30 秒境界にある。

**停止条件:** `Date.UTC` が誤ったタイムスタンプを生成する場合、タイムゾーン処理を確認。

---

### Task 5: BurstDetector 実装

**時間目安:** 3 分
**作成/変更ファイル:**
- `lib/burst-reducer/burst-detector.mjs`（作成）
- `test/burst-reducer/burst-detector.test.mjs`（作成）

**振る舞い:**
`BurstBuilder` の薄いラッパー。マーケットごとに 1 インスタンス。
ブロック間の open burst 引き継ぎを管理する。

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/burst-detector.mjs

import { BurstBuilder } from '../burst-builder.mjs';
import { GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS, getTickSize } from './schema.mjs';
import { serializeBurstBuilderState, restoreBurstBuilderState, getClosedBurstsSnapshot } from './burst-state-codec.mjs';

export class BurstDetector {
  /**
   * @param {string} market
   * @param {Object|null} [savedState=null] - serializeBurstBuilderState の出力形式。
   *   初回起動時は null。再起動時は checkpoint の open_burst を渡す。
   */
  constructor(market, savedState = null) {
    this._market = market;
    const tickSize = getTickSize(market);
    this._builder = new BurstBuilder({
      market,
      gap_threshold_ms: GAP_THRESHOLD_MS,
      max_burst_duration_ms: MAX_BURST_DURATION_MS,
      tick_size: tickSize,  // null for unknown markets; ticks columns will be null, bps still computable
    });
    this._isFirstBlock = (savedState === null);
    this._pendingBlockInfo = null;  // 1-block lag: pending block 情報

    // チェックポイントから BurstBuilder 内部状態を復元するには、
    // burst-state-codec.mjs を使用する。このファイルのみが BurstBuilder の
    // privates（_open, _closedBursts, _nextId）にアクセスできる。
    // 他のすべてのファイルは codec API 経由でのみ BurstBuilder 内部状態を読み書きする。
    // _open/_closedBursts/_nextId への文字列リテラルによる直接アクセスは禁止。
    // NOTE: constructor は BurstBuilder に固定オプションで作成し、
    //       savedState 非 null の場合 restoreBurstBuilderState を呼んで復元する。
    //       BurstBuilder のコンストラクタオプションに checkpoint を渡してはならない。
    if (savedState) {
      restoreBurstBuilderState(this._builder, savedState);
    }
  }

  get isFirstBlock() { return this._isFirstBlock; }
  get market() { return this._market; }

  feedTrades(trades) {
    for (const t of trades) {
      this._builder.feedTrade(t);
    }
  }

  getClosedBurstsOverlapping(secondTs) {
    return this._builder.getClosedBurstsOverlapping(secondTs);
  }

  /**
   * 全閉鎖済みバーストのディープコピーを返す。
   * codec API 経由で BurstBuilder の private 状態にアクセスする。
   * 直接 _closedBursts を返さない（カプセル化）。
   */
  getAllClosedBursts() {
    return getClosedBurstsSnapshot(this._builder);
  }

  getOpenBurstState() {
    return serializeBurstBuilderState(this._builder);
  }

  /**
   * ブロック終了時に呼ぶ。open burst は閉じず、チェックポイント用に取得する。
   * flushAll は呼ばない（open burst は次ブロックに引き継ぐため）。
   */
  finalizeBlock() {
    // 何もしない。open burst は保持。
    // flushAll は全ブロック処理完了後、または中間的な集計が不要な場合のみ呼ぶ。
  }

  flushAll() {
    this._builder.flushAll();
  }
}
```

**テスト（`test/burst-reducer/burst-detector.test.mjs`）:**
```javascript
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { GAP_THRESHOLD_MS, MAX_BURST_DURATION_MS } from '../../lib/burst-reducer/schema.mjs';

describe('BurstDetector', () => {
  it('uses correct fixed parameters', () => {
    assert.equal(GAP_THRESHOLD_MS, 50);
    assert.equal(MAX_BURST_DURATION_MS, 5000);
  });

  it('forms burst from trades within gap threshold', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 1000, side: 'buy', price: 100, qty: 1 },
      { ts: 1040, side: 'buy', price: 100, qty: 1 }, // gap=40 <= 50
    ]);
    // flushAll to close the burst for querying
    bd.flushAll();
    const bursts = bd.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bursts[0].burst_print_count, 2);
  });

  it('splits on gap > 50ms', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 1000, side: 'buy', price: 100, qty: 1 },
      { ts: 1060, side: 'buy', price: 100, qty: 1 }, // gap=60 > 50
    ]);
    bd.flushAll();
    const bursts = bd.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 2);
  });

  it('returns open burst state for checkpoint', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    const state = bd.getOpenBurstState();
    assert.ok(state);
    assert.equal(state.schemaVersion, 1);
    assert.ok(Array.isArray(state.closedBursts));
    assert.equal(state.closedBursts.length, 0);
    assert.ok(typeof state.nextId === 'number' && state.nextId >= 1);
    assert.ok(state.open !== null);
    assert.equal(state.open.side, 'buy');
    assert.equal(state.open.prints.length, 1);
  });

  it('isFirstBlock is true when no checkpoint', () => {
    const bd = new BurstDetector('test');
    assert.equal(bd.isFirstBlock, true);
  });

  it('restores open burst from checkpoint', () => {
    const bd1 = new BurstDetector('test');
    bd1.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    const cp = bd1.getOpenBurstState();

    const bd2 = new BurstDetector('test', cp);
    assert.equal(bd2.isFirstBlock, false);
    // feed another trade continuing the burst
    bd2.feedTrades([{ ts: 1020, side: 'buy', price: 100, qty: 1 }]);
    bd2.flushAll();
    const bursts = bd2.getClosedBurstsOverlapping(1000);
    assert.equal(bursts.length, 1);
    assert.equal(bursts[0].burst_print_count, 2);
  });
});
```

**コマンド:**
```bash
node --test test/burst-reducer/burst-detector.test.mjs
```

**期待結果:** 全 6 テスト pass。

**停止条件:** BurstBuilder の内部 API（`_open`, `_closedBursts`, `_nextId`）に依存しているため、`lib/burst-builder.mjs` の更新で破損した場合はアサーションが失敗。BurstBuilder の内部状態アクセスは `lib/burst-reducer/burst-state-codec.mjs` に一元化されている。それ以外のファイルからの `_open`/`_closedBursts`/`_nextId` への文字列リテラルによる直接アクセスは禁止。codec が破損した場合は codec のみ修正する。

---

### Task 5b: BurstStateCodec 実装（BurstBuilder 内部状態シリアライズ/リストア）

**時間目安:** 3 分
**作成/変更ファイル:**
- `lib/burst-reducer/burst-state-codec.mjs`（作成）
- `test/burst-reducer/burst-state-codec.test.mjs`（作成）

**振る舞い:**
BurstBuilder の内部状態（`_open`, `_closedBursts`, `_nextId`）をシリアライズ/リストアする
専用 codec。**このファイルのみが BurstBuilder の privates にアクセスできる。**
他のすべてのファイルは codec API 経由でのみ BurstBuilder 内部状態を読み書きする。

**絶対ルール:**
- `lib/burst-reducer/burst-state-codec.mjs` 以外の planned 新規ファイル（`burst-detector.mjs`, `pipeline.mjs`, `output-committer.mjs`, `pending-block-manager.mjs`）は、`_open`、`_closedBursts`、`_nextId` の文字列リテラルによる直接アクセスを一切行わない。
- 既存の `lib/burst-builder.mjs` は変更しない（codec は BurstBuilder の外部から内部を読み取る）。

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/burst-state-codec.mjs

// BurstBuilder の actual internal field shapes（lib/burst-builder.mjs より）:
//
//   this._open = {
//     side:          'buy' | 'sell',
//     start_ts:      number,
//     end_ts:        number,
//     prints:        [{ ts, price, qty, side, ...trade }],  // spread original trade
//     min_price:     number,
//     max_price:     number,
//     sum_notional:  number,  // price * qty の累積
//     sum_qty:       number,
//   }
//
//   this._closedBursts = [{
//     burst_id:             string,       // e.g. "binance_spot-1"
//     market:               string,
//     side:                 'buy' | 'sell',
//     burst_notional:       number,
//     burst_print_count:    number,
//     burst_duration_ms:    number,
//     burst_start_ts:       number,
//     burst_end_ts:         number,
//     min_price:            number,
//     max_price:            number,
//     distinct_price_count: number,
//     span_ticks:           number,
//     same_price_runs:      SamePriceRun[],
//     prints:               TradePrint[],
//   }]
//
//   this._nextId: number  // 次の burst_id 発行用カウンタ

const SCHEMA_VERSION = 1;

/**
 * serializeBurstBuilderState(builder)
 * BurstBuilder の再起動に必要な全状態を JSON-safe な deep clone として返す。
 * _open が null の場合は { open: null, closedBursts: [], nextId } を返す。
 *
 * @param {BurstBuilder} builder
 * @returns {{ schemaVersion: number, open: Object|null, closedBursts: Object[], nextId: number }}
 */
export function serializeBurstBuilderState(builder) {
  return {
    schemaVersion: SCHEMA_VERSION,
    open: builder._open ? deepCloneOpen(builder._open) : null,
    closedBursts: builder._closedBursts.map(b => deepCloneClosedBurst(b)),
    nextId: builder._nextId,
  };
}

/**
 * restoreBurstBuilderState(builder, state)
 * state を BurstBuilder インスタンスにリストアする。
 * state は serializeBurstBuilderState の出力形式であること。
 * スキーマバージョン不一致または形状不正時は E020 を throw する。
 *
 * @param {BurstBuilder} builder
 * @param {{ schemaVersion: number, open: Object|null, closedBursts: Object[], nextId: number }} state
 * @throws {Error} E020 on schema mismatch or malformed state
 */
export function restoreBurstBuilderState(builder, state) {
  if (!state || typeof state !== 'object') {
    throw new Error('E020: burst state codec: state is not an object');
  }
  if (state.schemaVersion !== SCHEMA_VERSION) {
    throw new Error(`E020: burst state codec: schema version mismatch: expected ${SCHEMA_VERSION}, got ${state.schemaVersion}`);
  }
  if (!Array.isArray(state.closedBursts)) {
    throw new Error('E020: burst state codec: closedBursts is not an array');
  }
  if (typeof state.nextId !== 'number' || !isFinite(state.nextId) || state.nextId < 1) {
    throw new Error(`E020: burst state codec: invalid nextId: ${state.nextId}`);
  }

  // _open のリストア（null 許容）
  if (state.open !== null && state.open !== undefined) {
    if (!state.open.side || !state.open.prints) {
      throw new Error('E020: burst state codec: open state missing required fields');
    }
    builder._open = deepCloneOpen(state.open);
  } else {
    builder._open = null;
  }

  // _closedBursts のリストア
  builder._closedBursts = state.closedBursts.map(b => deepCloneClosedBurst(b));

  // _nextId のリストア
  builder._nextId = state.nextId;
}

// ── private deep-clone helpers ──

function deepCloneOpen(o) {
  return {
    side: o.side,
    start_ts: o.start_ts,
    end_ts: o.end_ts,
    prints: o.prints.map(p => ({ ...p })),
    min_price: o.min_price,
    max_price: o.max_price,
    sum_notional: o.sum_notional,
    sum_qty: o.sum_qty,
  };
}

function deepCloneClosedBurst(b) {
  return {
    burst_id: b.burst_id,
    market: b.market,
    side: b.side,
    burst_notional: b.burst_notional,
    burst_print_count: b.burst_print_count,
    burst_duration_ms: b.burst_duration_ms,
    burst_start_ts: b.burst_start_ts,
    burst_end_ts: b.burst_end_ts,
    min_price: b.min_price,
    max_price: b.max_price,
    distinct_price_count: b.distinct_price_count,
    span_ticks: b.span_ticks,
    same_price_runs: b.same_price_runs.map(r => ({ ...r })),
    prints: b.prints.map(p => ({ ...p })),
  };
}

/**
 * getClosedBurstsSnapshot(builder)
 * BurstBuilder._closedBursts のディープコピーを返す。
 * 全 planned ファイルはこの API 経由でのみ closed bursts にアクセスする。
 * 直接 _closedBursts を読み取ってはならない。
 *
 * @param {BurstBuilder} builder
 * @returns {Object[]} deep-cloned closed bursts array
 */
export function getClosedBurstsSnapshot(builder) {
  return builder._closedBursts.map(b => deepCloneClosedBurst(b));
}

/**
 * validateClosedBurst(burst)
 * deep-clone 前に閉鎖済みバーストの形状が安全かを検証する。
 * undefined な same_price_runs / prints による TypeError を防止する。
 * 必須フィールドが欠落または不正な場合は E020 を throw する。
 *
 * @param {Object} b
 * @throws {Error} E020 on malformed burst
 */
export function validateClosedBurst(b) {
  if (!b || typeof b !== 'object') {
    throw new Error('E020: burst state codec: closed burst is not an object');
  }
  const requiredNumbers = ['burst_notional', 'burst_print_count', 'burst_duration_ms',
    'burst_start_ts', 'burst_end_ts', 'min_price', 'max_price',
    'distinct_price_count', 'span_ticks'];
  for (const key of requiredNumbers) {
    if (typeof b[key] !== 'number' || !isFinite(b[key])) {
      throw new Error(`E020: burst state codec: invalid or missing field "${key}" in closed burst`);
    }
  }
  if (typeof b.burst_id !== 'string' || !b.burst_id) {
    throw new Error('E020: burst state codec: missing burst_id');
  }
  if (!Array.isArray(b.same_price_runs)) {
    throw new Error('E020: burst state codec: same_price_runs is not an array');
  }
  if (!Array.isArray(b.prints)) {
    throw new Error('E020: burst state codec: prints is not an array');
  }
}
```

**テスト（`test/burst-reducer/burst-state-codec.test.mjs`）:**
```javascript
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstBuilder } from '../../lib/burst-builder.mjs';
import { serializeBurstBuilderState, restoreBurstBuilderState, getClosedBurstsSnapshot } from '../../lib/burst-reducer/burst-state-codec.mjs';

describe('BurstStateCodec', () => {
  it('round-trip: serialize → restore → serialize produces identical state', () => {
    const b1 = new BurstBuilder({ market: 'test' });
    b1.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    b1.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 2 });
    b1.flushAll();

    const state1 = serializeBurstBuilderState(b1);
    assert.equal(state1.schemaVersion, 1);
    assert.equal(state1.closedBursts.length, 1);
    assert.equal(state1.open, null);
    assert.ok(state1.nextId > 1);

    const b2 = new BurstBuilder({ market: 'test' });
    restoreBurstBuilderState(b2, state1);

    const state2 = serializeBurstBuilderState(b2);
    assert.deepEqual(state2, state1);
  });

  it('restore then feed same next trade → byte-identical closed bursts', () => {
    const b1 = new BurstBuilder({ market: 'test' });
    b1.feedTrade({ ts: 100, side: 'buy', price: 100, qty: 1 });
    const cp = serializeBurstBuilderState(b1); // open burst

    const b2 = new BurstBuilder({ market: 'test' });
    restoreBurstBuilderState(b2, cp);
    b2.feedTrade({ ts: 120, side: 'buy', price: 100, qty: 2 });
    b2.flushAll();

    // byte-identical closed bursts via codec API (no direct _closedBursts access)
    const bursts2 = getClosedBurstsSnapshot(b2);
    assert.equal(bursts2.length, 1);
    assert.equal(bursts2[0].burst_print_count, 2);
    assert.equal(bursts2[0].burst_notional, 300);
    assert.equal(bursts2[0].burst_id, 'test-1'); // _nextId restored correctly
  });

  it('malformed state throws E020', () => {
    const b = new BurstBuilder({ market: 'test' });
    assert.throws(() => restoreBurstBuilderState(b, null), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 99 }), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 1, closedBursts: 'not_array', nextId: 1 }), /E020/);
    assert.throws(() => restoreBurstBuilderState(b, { schemaVersion: 1, closedBursts: [], nextId: -1 }), /E020/);
  });

  it('no direct _open/_closedBursts/_nextId string access outside codec (executable grep guard)', () => {
    // EXECUTABLE GREP GUARD: reads planned production files and asserts
    // forbidden private identifier string literals are absent.
    // Scanned files: planned production .mjs only (not test/, not codec itself).
    // Exclusion: test files MAY contain the strings as the guard's own needle;
    // the assertion only scans production planned files.
    import { readFileSync, existsSync } from 'node:fs';
    const PLANNED_PRODUCTION_FILES = [
      'lib/burst-reducer/burst-detector.mjs',
      'lib/burst-reducer/pipeline.mjs',
      'lib/burst-reducer/output-committer.mjs',
      'lib/burst-reducer/pending-block-manager.mjs',
    ];
    const FORBIDDEN = ['._open', '._closedBursts', '._nextId'];
    // NOTE: the codec file (burst-state-codec.mjs) is EXCLUDED — it needs these accessors.
    // NOTE: test files are excluded — they may reference the strings as needles.
    let violations = 0;
    for (const f of PLANNED_PRODUCTION_FILES) {
      if (!existsSync(f)) {
        // File not yet created (pre-build step); skip.
        continue;
      }
      const content = readFileSync(f, 'utf8');
      for (const needle of FORBIDDEN) {
        // But exclude comment lines that mention the field names in prose.
        // Simple check: any code-level occurrence of needle (not inside // or /*).
        const lines = content.split('\n');
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i].trim();
          if (line.startsWith('//') || line.startsWith('*') || line.startsWith('/*')) continue;
          if (line.includes(needle)) {
            console.error(`VIOLATION: ${f}:${i+1}: direct access ${needle}`);
            violations++;
          }
        }
      }
    }
    assert.equal(violations, 0, `${violations} encapsulation violations found in planned production files`);
  });
});
```

**コマンド:**
```bash
node --check lib/burst-reducer/burst-state-codec.mjs
node --test test/burst-reducer/burst-state-codec.test.mjs
```

**期待結果:** 全 4 テスト pass。round-trip で byte-identical な状態が復元されること。

**停止条件:** BurstBuilder の内部 field shape が変更された場合、codec の deep-clone 定義を更新する。スキーマバージョンをインクリメントする。

---

### Task 6: FeatureComputer (1s) trade-only コア実装

**時間目安:** 5 分
**作成/変更ファイル:**
- `lib/burst-reducer/feature-computer-1s.mjs`（作成）
- `lib/burst-reducer/agg-trades-reader.mjs`（作成 — #12 用 agg_trades 読み取り・検証・lookup 構築）
- `test/burst-reducer/feature-computer-1s.test.mjs`（作成）

**agg-trades-reader 責務:**
- `data/live_v3/agg_trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl` を読み取り
- **Authoritative agg JSON input columns:** `ts`, `volume`, `vwap` のみ
- 各行の検証: `isFinite(volume) && volume >= 0`、`isFinite(vwap) && vwap > 0`
- ブロックファイル名と期待範囲の一致を検証（`HH-MM-SS` が 00/30 境界）
- `buildTradedNotionalLookup(aggRows, blockStartMs)` をエクスポート
- E007: 必要な key が欠落する場合 throw（不完全な検証済みソースカバレッジ）

**振る舞い:**
1 つの 30 秒ブロックに対し、全 30 秒（1s × 30）の特徴量行を計算する。
overlap 判定、ゼロ埋め、warmup フラグ、品質情報を正しく設定する。
Phase 1a では trade-only 11 フィールド + `burst_notional_vs_30s_traded_notional`（#12）の計 12 フィールドを計算。
`burst_notional_vs_30s_traded_notional` は agg_trades input から `[second_ts-30000, second_ts)` の総約定 notional を分母として per-second で計算する。分母が厳密に 0 の場合、値は `0`。agg_trades ブロック欠落時は N を quarantine/fail。
板依存 #13=`null`、#14=`0`、研究 #15-#22=`0`（P1 契約値。`null` ではない）

**疑似コード/アサーション:**
```javascript
// lib/burst-reducer/feature-computer-1s.mjs

import { createBaseRow, floorToSecond, generateSeconds } from './schema.mjs';

const EPS = 1e-10;

/**
 * @param {Object} params
 * @param {BurstDetector} params.detector
 * @param {number} params.blockStartMs
 * @param {number[]} params.tradeTsList - 全 trade の ts（trade_count_this_second 算出用）
 * @param {boolean} params.warmup
 * @param {string[]} params.inputBlockIds
 * @param {Map<number,number>} [params.lookupTradedNotional30s] - second_ts → traded_notional_30s for #12. Map with second boundary keys.
 * @returns {Object[]} 30 行（1s × 30）
 */
export function computeFeatures1s({ detector, blockStartMs, tradeTsList, warmup, inputBlockIds, lookupTradedNotional30s }) {
  const rows = [];
  for (const secondTs of generateSeconds(blockStartMs)) {
    const overlapping = detector.getClosedBurstsOverlapping(secondTs);
    const tradeCount = tradeTsList.filter(ts => ts >= secondTs && ts < secondTs + 1000).length;

    const quality = {
      book_seeded: false,
      trade_count_this_second: tradeCount,
      warmup,
      input_block_ids: inputBlockIds,
    };

    // ── #12 fail-closed: complete 30-key lookup is a block-level validity prerequisite ──
    // All 30 output seconds MUST have agg_trades coverage validated by the caller.
    // This applies to no-burst rows too — a zero-burst row still must not mask
    // missing agg coverage. computeFeatures1s MUST throw E007 if lookup missing
    // or no key for ANY secondTs, even when overlapping.length === 0.
    if (!lookupTradedNotional30s || !lookupTradedNotional30s.has(secondTs)) {
      throw new Error(`E007: lookupTradedNotional30s is missing for secondTs=${secondTs}. Caller must provide complete agg_trades coverage.`);
    }

    if (overlapping.length === 0) {
      rows.push(createBaseRow(secondTs, detector.market, quality));
      continue;
    }

    // ── trade-only 11 features ──
    const burstCount = overlapping.length;
    let totalNotional = 0;
    let maxNotional = 0;
    let maxPrints = 0;
    let maxDuration = 0;
    let buyNotional = 0;
    let sellNotional = 0;
    let samePriceCount = 0;
    let multilevelCount = 0;

    for (const b of overlapping) {
      totalNotional += b.burst_notional;
      if (b.burst_notional > maxNotional) maxNotional = b.burst_notional;
      if (b.burst_print_count > maxPrints) maxPrints = b.burst_print_count;
      if (b.burst_duration_ms > maxDuration) maxDuration = b.burst_duration_ms;

      if (b.side === 'buy') buyNotional += b.burst_notional;
      else sellNotional += b.burst_notional;

      if (b.distinct_price_count === 1) samePriceCount++;
      else multilevelCount++;
    }

    const imbalanceRatio = (buyNotional - sellNotional) / (buyNotional + sellNotional + EPS);
    const largestShare = totalNotional > 0 ? maxNotional / totalNotional : 0;

    // ── #12: burst_notional_vs_30s_traded_notional ──
    // FAIL CLOSED guarantee: E007 check above already ensured lookup has key.
    const denom = lookupTradedNotional30s.get(secondTs);
    let vs30s = 0;
    vs30s = (denom > 0) ? totalNotional / denom : 0;  // denom exactly 0 → output 0 (valid aux, zero volume)

    const row = createBaseRow(secondTs, detector.market, quality);
    row.burst_count_1s = burstCount;
    row.total_burst_notional_1s = totalNotional;
    row.max_burst_notional_1s = maxNotional;
    row.max_burst_prints_1s = maxPrints;
    row.max_burst_duration_ms_1s = maxDuration;
    row.buy_burst_notional_1s = buyNotional;
    row.sell_burst_notional_1s = sellNotional;
    row.burst_imbalance_ratio_1s = imbalanceRatio;
    row.largest_burst_share_notional_1s = largestShare;
    row.same_price_burst_count_1s = samePriceCount;
    row.multilevel_burst_count_1s = multilevelCount;
    row.burst_notional_vs_30s_traded_notional = vs30s;
    // 板依存は createBaseRow で null/0 済み、研究・監視は 0 済み（P1 契約値）

    rows.push(row);
  }
  return rows;
}
```

**テスト（`test/burst-reducer/feature-computer-1s.test.mjs`）:**
```javascript
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { computeFeatures1s } from '../../lib/burst-reducer/feature-computer-1s.mjs';

// Helper: complete 30-key zero-denominator lookup for valid-path tests
const zeroLookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 0]));

describe('FeatureComputer 1s', () => {
  it('returns 30 rows for a block', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 1000, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [1000],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    assert.equal(rows.length, 30); // 30 秒 = 30 rows
  });

  it('zero-fills seconds with no bursts', () => {
    const bd = new BurstDetector('test');
    // バーストなし
    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      assert.equal(row.burst_count_1s, 0);
      assert.equal(row.total_burst_notional_1s, 0);
    }
  });

  it('computes correct features for one burst', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 520, side: 'buy', price: 100, qty: 2 },
    ]);
    bd.flushAll();

    const rows = computeFeatures1s({
      detector: bd,
      blockStartMs: 0,
      tradeTsList: [500, 520],
      warmup: true,
      inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });

    const row0 = rows[0]; // secondTs = 0
    assert.equal(row0.burst_count_1s, 1); // burst [500,520] overlaps [0,1000)
    assert.equal(row0.total_burst_notional_1s, 300);
    assert.equal(row0.max_burst_notional_1s, 300);
    assert.equal(row0.buy_burst_notional_1s, 300);
    assert.equal(row0.sell_burst_notional_1s, 0);
    assert.equal(row0.burst_imbalance_ratio_1s, 1.0); // all buy
    assert.equal(row0.same_price_burst_count_1s, 1);
    assert.equal(row0.multilevel_burst_count_1s, 0);

    const row1 = rows[1]; // secondTs = 1000
    assert.equal(row1.burst_count_1s, 0); // burst ended at 520, no overlap with [1000,2000)
  });

  it('warmup flag is set', () => {
    const bd = new BurstDetector('test');
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [], warmup: true, inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      assert.equal(row._quality.warmup, true);
    }
  });

  it('invariants hold', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([
      { ts: 500, side: 'buy', price: 100, qty: 1 },
      { ts: 540, side: 'sell', price: 101, qty: 1 },
    ]);
    bd.flushAll();
    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500, 540], warmup: false, inputBlockIds: ['0'],
      lookupTradedNotional30s: zeroLookup,
    });
    for (const row of rows) {
      // burst_count = same_price + multilevel
      assert.equal(row.burst_count_1s, row.same_price_burst_count_1s + row.multilevel_burst_count_1s);
      // total = buy + sell
      assert.ok(Math.abs(row.total_burst_notional_1s - (row.buy_burst_notional_1s + row.sell_burst_notional_1s)) < 0.01);
      // imbalance in [-1, 1]
      assert.ok(row.burst_imbalance_ratio_1s >= -1.0 && row.burst_imbalance_ratio_1s <= 1.0);
      // share in [0, 1]
      assert.ok(row.largest_burst_share_notional_1s >= 0 && row.largest_burst_share_notional_1s <= 1.0);
      // book-dependent: #13=null, #14=0 per P1 contract
      assert.equal(row.burst_notional_vs_top_depth, null);
      assert.equal(row.burst_mid_move_bps_1s, 0);
      // research: #15-#22 = 0 per P1 contract
      assert.equal(row.same_price_burst_max_len_1s, 0);
      assert.equal(row.outlier_trade_flag_1s, 0);
    }
  });
});
```

**コマンド:**
```bash
node --test test/burst-reducer/feature-computer-1s.test.mjs
```

**期待結果:** 全 5 テスト pass。不変条件がすべての行で成立。

**停止条件:** 不変条件違反 → 計算ロジック修正。`NaN` が出る場合は EPS の適用箇所確認。

---

### Task 7: OutputCommitter 実装（分解: 7a-7g）

**時間目安:** 合計 20 分（7 サブタスク × 2-3 分）
**作成/変更ファイル:**
- `lib/burst-reducer/manifest-manager.mjs`（作成 — 7a）
- `lib/burst-reducer/output-committer.mjs`（作成 — 7b-7e）
- `test/burst-reducer/manifest-manager.test.mjs`（作成 — 7a）
- `test/burst-reducer/output-committer.test.mjs`（作成 — 7f）
- `test/burst-reducer/recovery.test.mjs`（作成 — 7g）

**振る舞い（atomic block shard commit with commitFinalizedBlock）:**

| サブタスク | 内容 | TDD アサーション |
|---|---|---|
| **7a: ManifestManager** | manifest 読み書き、intent write + atomic rename、committed 更新、**composite key** `{schema_version}:{market}:{block_start_ms}:{input_sha256}` 判定、`auxiliary_input_hashes` の保存 | intent/committed status 遷移、composite key 形式、auxiliary_input_hashes が manifest に存在 |
| **7b: Stage shard bytes + SHA256** | 行を final 同 filesystem の `.staging/<run_id>/` に書き込み（`features_1s/<market>/<YYYY-MM-DD>/.staging/<run_id>/<HH-MM-SS>.jsonl`）、row content SHA256 計算 | row hash が正しいことのテスト、stage 後も本番パスにファイルが存在しないこと |
| **7c: Write intent manifest** | `status: "intent"` の manifest record を fsync + atomic rename（`.tmp` → `.json`）。`auxiliary_input_hashes` と staged_path を含める | intent status 検証、auxiliary_input_hashes が record に保存 |
| **7d: Atomic final block shard rename** | staged file を本番パスに `renameSync(stagedFile, outputPath)`（`features_1s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`）。重複書き込み禁止。 | ファイル存在・SHA256 一致 |
| **7e: Atomic checkpoint + committed manifest rename** | checkpoint（`last_committed=N`, `pending_block=N+1_info`, `open_burst=nextDetectorState`）を atomic rename、manifest を `status: "committed"` に更新して atomic rename | チェックポイントに N+1 pending が保存、pending_block が null でない、generation が単調増加 |
| **7f: Startup recovery table** | intent+data absent、intent+data present/mismatch、committed+data mismatch、crash between data rename/checkpoint の全ケース対応 | 全 recovery status の red→green |
| **7g: Pipeline assembly (1-block lag)** | 1-block lag のコミット順序を実装。N+1 valid → N 初回計算 → N commit（N+1 は次 pending） | cross-block restart fixture、empty-next-block watermark |

**出力 shard パス（canonical）:**
```
data/derived/burst_features_v1/features_1s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl   # HH-MM-SS は 00 または 30 秒のみ（1s block shard: 30行）
data/derived/burst_features_v1/features_30s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl  # HH-MM-SS は 00 または 30 秒のみ（30s window: 1行）
data/derived/burst_features_v1/features_5min/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl # seconds=00, UTC minute は 5 の倍数。00/30 秒制約は適用されない（5min window: 1行）
data/derived/burst_features_v1/quarantine/<market>/<block_start_ms>.json             # エラー report（raw は移動しない）
```

**疑似コード/アサーション（output-committer.mjs skeleton — commitFinalizedBlock シグネチャ）:**
```javascript
// lib/burst-reducer/output-committer.mjs

import { mkdirSync, writeFileSync, renameSync, existsSync, readFileSync, unlinkSync, openSync, fsyncSync, closeSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { SCHEMA_VERSION, DERIVED_DIR, FEATURES_1S_DIR, MANIFESTS_DIR, CHECKPOINTS_DIR } from './schema.mjs';

function sha256(content) {
  return createHash('sha256').update(content).digest('hex');
}

/**
 * writeFileDurable — write file content followed by fsync on the file descriptor,
 * then close the descriptor. Ensures data reaches storage before any subsequent
 * atomic rename makes it visible.
 * @param {string} path
 * @param {string} content
 */
function writeFileDurable(path, content) {
  writeFileSync(path, content, 'utf8');
  const fd = openSync(path, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

/**
 * fsyncDirectory — fsync the directory entry after a rename to ensure the
 * directory metadata (the rename) is durable on disk before proceeding.
 * @param {string} dir - absolute or relative path to a directory
 */
function fsyncDirectory(dir) {
  const fd = openSync(dir, 'r');
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

function formatBlockTime(blockStartMs) {
  const d = new Date(blockStartMs);
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${hh}-${mm}-${ss}`;
}

function formatDate(blockStartMs) {
  const d = new Date(blockStartMs);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
}

/** @returns {string} composite key: {schema_version}:{market}:{block_start_ms}:{input_sha256} */
function compositeKey(market, blockStartMs, inputSha256) {
  return `${SCHEMA_VERSION}:${market}:${blockStartMs}:${inputSha256}`;
}

export class OutputCommitter {
  constructor(market, runId) {
    this._market = market;
    this._runId = runId;
    // Staging shares the same filesystem as final target for atomic rename.
    // Staged file paths: features_1s/<market>/<YYYY-MM-DD>/.staging/<run_id>/<HH-MM-SS>.jsonl
    this._manifestPath = join(DERIVED_DIR, MANIFESTS_DIR, `${market}.json`);
    this._checkpointPath = join(DERIVED_DIR, CHECKPOINTS_DIR, `${market}.json`);
  }

  /**
   * 5-step atomic commit for finalized block N.
   * N の行は N+1 の全 trades 投入後に初めて計算され、ここで一度だけコミットされる。
   *
   * @typedef {Object} PendingBlock
   * @property {number} block_start_ms
   * @property {string} trade_input_sha256
   * @property {Record<string,string>} auxiliary_input_hashes
   * @property {{market:string, block_start_ms:number, input_path:string}} replay_identity
   * @property {Object|null} open_burst_before_N1
   *
   * @param {Object} finalizedBlock - { block_start_ms, input_sha256, date, time }
   * @param {PendingBlock|null} nextPendingBlock — N+1 info for next pending; null at EOF
   *   Type contract: nextPendingBlock: PendingBlock | null
   * @param {Object|null} nextDetectorState - N+1 処理後の open burst 状態
   * @param {Object[]} rows - N の 30 行（初回計算・確定済み）
   * @param {Object} manifestInputs - { auxiliary_input_hashes: Record<string,string> }
   * @param {number} checkpointGeneration
   * @param {string} commitId - UUID v4
   * @param {boolean} isEofFinalization - true if this is the final block at EOF (no next pending)
   *
   * INVARIANTS (enforced before intent):
   *   - !isEofFinalization => nextPendingBlock MUST be non-null AND checkpoint pending non-null
   *   - isEofFinalization  => nextPendingBlock MUST be null AND checkpoint pending null
   *   - violation of either → throw E031 before intent
   */
  commitFinalizedBlock(finalizedBlock, nextPendingBlock, nextDetectorState, rows, manifestInputs, checkpointGeneration, commitId, isEofFinalization) {
    const { block_start_ms, input_sha256 } = finalizedBlock;

    // ═══ E031: EOF nullable contract validation BEFORE intent ═══
    if (!isEofFinalization && nextPendingBlock === null) {
      throw new Error('E031: non-EOF commit requires non-null nextPendingBlock');
    }
    if (isEofFinalization && nextPendingBlock !== null) {
      throw new Error('E031: EOF commit requires null nextPendingBlock');
    }

    // 1. Validate rows
    this._validateRows(rows, block_start_ms);

    const date = formatDate(block_start_ms);
    const time = formatBlockTime(block_start_ms);
    const content = rows.map(r => JSON.stringify(r)).join('\n') + '\n';
    const stagedHash = sha256(content);
    const key = compositeKey(this._market, block_start_ms, input_sha256);

    // 2. Stage rows with fsync durability in same-filesystem staging dir (atomic rename target)
    const outputDir = join(DERIVED_DIR, FEATURES_1S_DIR, this._market, date);
    const stagedFile = join(outputDir, '.staging', this._runId, `${time}.jsonl`);
    mkdirSync(dirname(stagedFile), { recursive: true });
    writeFileDurable(stagedFile, content);

    // 3. Intent manifest (writeFileDurable + atomic rename + fsyncDirectory)
    //    _writeIntentManifest writes the record to a .tmp, writeFileDurable, renameSync, fsyncDirectory
    //    staged_path = stagedFile, output_path = final canonical path
    this._writeIntentManifest(key, input_sha256, stagedHash, block_start_ms, date, time, checkpointGeneration, commitId, manifestInputs, stagedFile);

    // 4. Final data shard: atomic rename of the SAME staged file to canonical block shard path
    //    ステージング済みファイルをそのままリネーム。複製・重複書き込みは行わない。
    //    Rename is followed by fsyncDirectory on the containing directory for durability.
    const outputPath = join(outputDir, `${time}.jsonl`);
    mkdirSync(dirname(outputPath), { recursive: true });
    renameSync(stagedFile, outputPath);
    fsyncDirectory(outputDir);
    const finalHash = sha256(content);

    // 5. Checkpoint（writeFileDurable + atomic rename + fsyncDirectory）
    //    + committed manifest（writeFileDurable + atomic rename + fsyncDirectory）
    //    OutputCommitter writes nextGeneration = checkpointGeneration + 1 internally
    const nextGeneration = checkpointGeneration + 1;
    this._writeCheckpointWithPending({
      last_committed_block_start: block_start_ms,
      pending_block: isEofFinalization ? null : nextPendingBlock,
      open_burst: isEofFinalization ? null : nextDetectorState,
      generation: nextGeneration,
    });
    this._writeCommittedManifest(key, finalHash, nextGeneration, commitId);

    // No staged cleanup needed — file was renamed, not copied.

    return { key, stagedHash, finalHash, nextGeneration, staged_path: stagedFile, output_path: outputPath };
  }

  _validateRows(rows, blockStartMs) {
    if (rows.length !== 30) throw new Error(`E030: expected 30 rows, got ${rows.length}`);
    for (let i = 0; i < 30; i++) {
      if (rows[i].ts !== blockStartMs + i * 1000) throw new Error(`E030: row ${i} ts mismatch`);
      if (rows[i].market !== this._market) throw new Error(`E030: row ${i} market mismatch`);
    }
  }

  _writeIntentManifest(key, inputSha256, stagedHash, blockStartMs, date, time, gen, commitId, manifestInputs) {
    // Write manifest record with status:"intent" to .tmp → writeFileDurable → renameSync → fsyncDirectory(manifestsDir)
    // includes auxiliary_input_hashes from manifestInputs
    // E032 on fsync/rename failure
  }
  _writeCheckpointWithPending({ last_committed_block_start, pending_block, open_burst, generation }) {
    // Write checkpoint to .tmp → writeFileDurable → renameSync → fsyncDirectory(checkpointDir)
    // E032 on fsync/rename failure
  }
  _writeCommittedManifest(key, finalHash, gen, commitId) {
    // Update manifest record to status:"committed" → write to .tmp → writeFileDurable → renameSync → fsyncDirectory(manifestsDir)
    // E032 on fsync/rename failure
  }
}
```

**quarantine パス（エラー時のみ）:**
```
data/derived/burst_features_v1/quarantine/<market>/<block_start_ms>.json
```
- raw 入力は一切移動/削除しない
- intent, output shard, checkpoint は進めない
- N+1 無効時は N を uncommitted のまま維持。エラー report のみを quarantine に書き込む

**禁止パターン（必ず排除）:**
- 旧 `commit1sBlock` シグネチャ — 代わりに `commitFinalizedBlock` を使用
- 旧 `_writeCommittedCheckpoint` メソッド — 代わりに `_writeCheckpointWithPending(...)` を使用（N+1 pending state を含む）
- コミット後の checkpoint で pending block 情報をクリアしない — 常に次の pending block（N+1）情報を保存
- ブロック開始時刻のみをキーとする実装（composite key `{schema_version}:{market}:{block_start_ms}:{input_sha256}` を使用）
- 日付のみで識別する出力名（block shard 方式を使用）

**コマンド:**
```bash
node --test test/burst-reducer/output-committer.test.mjs
```

**期待結果:** 全テスト pass。
- **通常コミット（non-EOF）:** `pending_block` が null でなく N+1 の情報を含む checkpoint が作成される。canonical block shard (`features_1s/test_committer/2026-07-10/00-00-00.jsonl`, 30行)、committed manifest (`manifests/test_committer.json`, `auxiliary_input_hashes` を含む) も生成。
- **EOF コミット:** `pending_block` = null、`open_burst` = null の checkpoint が作成される。E031 が throw されない。
テスト後のクリーンアップ成功。

**停止条件:** ディレクトリ作成権限エラー → `data/derived/` の存在確認。

---

### Task 8: ReducerPipeline 統合（1-block lag + commitFinalizedBlock）

**時間目安:** 8 分
**作成/変更ファイル:**
- `lib/burst-reducer/pipeline.mjs`（作成）
- `lib/burst-reducer/pending-block-manager.mjs`（作成）
- `test/burst-reducer/pipeline.test.mjs`（作成）
- `test/burst-reducer/cross-block-restart.test.mjs`（作成 — red test）

**振る舞い:**
全コンポーネントを統合し、**1-block lag commit（commitFinalizedBlock 方式）** でブロックを処理する。pending finalized N と候補 N+1 の処理順序は厳密に以下の通り:

1. **read+validate candidate N+1 raw** — format/schema/ordering ONLY（E001-E005）。N+1 の agg lookback 検証は行わない（N+1 自身の finalize 時に行う）
2. **validate finalized N required agg aux blocks** — `[N_start - 30000, N_end)` と交差する全 agg_trades ブロックの存在・妥当性を検証
3. **if step 1 or 2 fails** → 対象ブロック（候補 N+1 or finalized N）の quarantine report を書き込み；**detector/open_burst/checkpoint/manifest は pre-feed と byte-identical のまま；即座に異常終了**
4. **capture `openBurstBeforeCandidate`** — `detector.getOpenBurstState()` を呼び出し、N+1 投入前の状態を checkpoint 用にキャプチャ
5. **feed ALL sorted N+1 trades** — 完全ソート済みシーケンスを `detector.feedTrades()` に投入
6. **compute N first time with complete #12 lookup** — `buildTradedNotionalLookup()` で agg_trades 分母を生成し、`computeFeatures1s()` で N の 30 行を初回計算（N+1 投入後の確定済み burst 状態を使用。`max_burst_duration=5000 < 30000` 保証により N-origin burst が確定。N+1 空なら watermark=N+1_end が証明）
7. **commit N once with N+1 next pending** — `commitFinalizedBlock(finalizedBlock=N, nextPendingBlock=N+1_info, nextDetectorState, rows, manifestInputs, gen, commitId)` で N を単一コミット
8. N+1 が次 pending に → ループ継続（N+2 へ）
9. **EOF:** `flushAll()` → 最終 pending を finalize → `commitFinalizedBlock(..., nextPendingBlock=null, isEofFinalization=true)`

**TDD アサーション一覧（Task 8 の全テストで検証）:**

| # | テストケース | アサーション |
|---|---|---|
| **TDD-8a** | N tail open burst → N+1 first 5s continuation | N+1 の先頭 5 秒以内に継続 trade あり。全 trades 投入後、N の burst が N+1 先頭まで継続する行が N の最終秒に正しく重複計上される。N の final 行が byte-identical |
| **TDD-8b** | N tail open burst → N+1 empty watermark | N+1 が空ファイル（0 trades）。N+1 の watermark で N の open burst が閉じたと確定。N の行を計算 → commit。N+1 が次 pending。N の burst が N+1 に継続しない |
| **TDD-8c** | Restart after N committed before N+1 processed | checkpoint に last_committed=N, pending_block=N+1_info あり。再起動後、N+2 を読み込み → N+1 の行を計算 → N+1 を commit → 出力が byte-identical |
|| **TDD-8d** | Invalid N+1 keeps N uncommitted — detector snapshot, checkpoint, manifest unchanged | N+1 の trades が E004（ts decrease）を含む。quarantine にエラーレポート。N は uncommitted のまま。**TDD 厳密アサーション: failure 前後で detector.getOpenBurstState() の snapshot、checkpoint ファイルの byte 内容、manifest ファイルの byte 内容が完全に同一であること。** 再試行で N+1 が修正されれば N が正常にコミットされる |
| **TDD-8e** | Crash between data rename and checkpoint（N final data あり、checkpoint 未書き込み） | recovery logic が checksums で N の完全性を検証。有効なら checkpoint（N+1 pending を含む）を書き込み → committed manifest。出力 byte-identical |
|| **TDD-8f** | #12 agg lookback — finalized N's aux validated | N の finalize 時に `[N_start-30000, N_end)` と交差する agg_trades ブロックの存在を検証。`auxiliary_input_hashes` が manifest に記録。`burst_notional_vs_30s_traded_notional` が per-second denominator で計算される |
| **TDD-8g** | #12 agg lookback — missing N's aux | finalized N に必要な agg_trades ブロックが欠落。N を quarantine/fail（N は uncommitted）。エラーレポートに欠落ブロックのパスを明記。分母が厳密に 0（aux は存在）の場合は異常ではなく値=0 |
| **TDD-8h** | Empty block as first block | block 0 が空。空ブロックとして pending に設定。block 1 の処理で block 0 の行（全ゼロ）が計算・コミットされる |

**疑似コード/アサーション（pipeline.mjs skeleton — commitFinalizedBlock 方式）:**
```javascript
// lib/burst-reducer/pipeline.mjs

import { readFileSync, existsSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { scanTradeBlocks } from './block-scanner.mjs';
import { validateAndParseTrades } from './input-validator.mjs';
import { BurstDetector } from './burst-detector.mjs';
import { computeFeatures1s } from './feature-computer-1s.mjs';
import { OutputCommitter } from './output-committer.mjs';
import { loadCheckpoint } from './pending-block-manager.mjs';
import { DERIVED_DIR } from './schema.mjs';

function sha256File(path) {
  if (!existsSync(path)) return null;
  return createHash('sha256').update(readFileSync(path, 'utf8')).digest('hex');
}

/**
 * Validate agg_trades lookback blocks that intersect [targetBlockStart-30000, targetBlockEnd).
 * Called when finalizing a block N: targetBlockStart = N.block_start_ms.
 * Returns { auxiliary_input_hashes, aggRows, coverageComplete, valid }.
 * - auxiliary_input_hashes: Record<string,string> hash map for manifest recording
 * - aggRows: loaded and parsed agg_trades rows from all intersecting blocks
 * - coverageComplete: true iff all required blocks were found and validated
 * - valid: false if any required block is missing (legacy; use coverageComplete)
 */
function validateAggLookback(dataDir, market, targetBlockStartMs) {
  const blockEndMs = targetBlockStartMs + 30000;
  const lookbackStart = targetBlockStartMs - 30000;
  // Required coverage: all 30s blocks intersecting [lookbackStart, blockEndMs)
  // which means the two adjacent 30s blocks: [lookbackStart, targetBlockStartMs)
  // and [targetBlockStartMs, blockEndMs). This is typically 2 blocks.
  const requiredStarts = [
    lookbackStart,              // previous 30s block (e.g. N_start - 30000)
    targetBlockStartMs,         // current 30s block (N itself)
  ];

  // Derive date and HH-MM-SS for each required block start
  // Scan agg_trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl for each required start
  // Validate each found file:
  //   - file exists
  //   - filename matches 00/30 boundary (same as trade blocks)
  //   - compute SHA256 for auxiliary_input_hashes
  //   - parse JSONL rows: each row must have ts, volume, vwap
  //     validate: isFinite(volume) && volume >= 0, isFinite(vwap) && vwap > 0
  //     (rows with volume=0 are valid; not equivalent to missing blocks)

  const auxiliary_input_hashes = {};
  const aggRows = [];
  const missing = [];

  for (const bs of requiredStarts) {
    const d = new Date(bs);
    const dateStr = `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
    const hh = String(d.getUTCHours()).padStart(2,'0');
    const mm = String(d.getUTCMinutes()).padStart(2,'0');
    const ss = String(d.getUTCSeconds()).padStart(2,'0');
    const relPath = `agg_trades/${market}/${dateStr}/${hh}-${mm}-${ss}.jsonl`;
    const fullPath = join(dataDir, relPath);

    if (!existsSync(fullPath)) {
      missing.push(relPath);
      continue;
    }
    // Validate file: parse rows
    // (In production this throws E007 on parse errors; pseudo shows intent)
    const content = readFileSync(fullPath, 'utf8');
    const hash = createHash('sha256').update(content).digest('hex');
    auxiliary_input_hashes[`${bs}`] = hash;

    const lines = content.trim().split('\n').filter(l => l);
    for (const line of lines) {
      const row = JSON.parse(line);
      if (!isFinite(row.volume) || row.volume < 0) {
        throw new Error(`E007: invalid agg volume ${row.volume} in ${relPath}`);
      }
      if (!isFinite(row.vwap) || row.vwap <= 0) {
        throw new Error(`E007: invalid agg vwap ${row.vwap} in ${relPath}`);
      }
      aggRows.push(row);
    }
  }

  const coverageComplete = (missing.length === 0);
  return {
    auxiliary_input_hashes,
    aggRows,
    coverageComplete,
    missing,
    valid: coverageComplete,  // legacy; use coverageComplete
  };
}

/**
 * buildTradedNotionalLookup — generates per-second notional denominator for #12.
 *
 * Authoritative agg JSON input columns: `ts`, `volume`, `vwap`.
 * Validates: finite nonnegative `volume`, finite positive `vwap`.
 * Notional = `volume * vwap`.
 *
 * @param {Object} aggResult — output of validateAggLookback, includes loaded agg rows
 * @param {number} blockStartMs — N's block start ms (30s block)
 * @returns {Map<number,number>} secondTs → sum(notional) for [secondTs-30000, secondTs)
 * @throws {Error} E007 if any required 30s window key absent due to incomplete validated source coverage
 */
function buildTradedNotionalLookup(aggResult, blockStartMs) {
  // FAIL CLOSED: verify coverageComplete before making map.
  // This ensures missing source cannot silently create a zero map.
  if (!aggResult.coverageComplete) {
    throw new Error('E007: agg_trades lookback coverage is incomplete — cannot build notional lookup');
  }

  const lookup = new Map();

  // Load ALL agg rows from validated blocks intersecting [blockStartMs-30000, blockStartMs+30000)
  const aggRows = aggResult.aggRows;

  // Validate each agg row: finite nonnegative volume, finite positive vwap
  for (const row of aggRows) {
    if (!isFinite(row.volume) || row.volume < 0) throw new Error(`E007: invalid agg volume ${row.volume}`);
    if (!isFinite(row.vwap) || row.vwap <= 0) throw new Error(`E007: invalid agg vwap ${row.vwap}`);
    row._notional = row.volume * row.vwap;
  }

  // Generate ALL 30 `secondTs` keys for block N
  for (let s = blockStartMs; s < blockStartMs + 30000; s += 1000) {
    let sumNotional = 0;
    // Sum rows where secondTs-30000 <= row.ts < secondTs
    for (const row of aggRows) {
      if (s - 30000 <= row.ts && row.ts < s) {
        sumNotional += row._notional;
      }
    }
    // MUST throw E007 if any key absent due to incomplete source coverage
    // (return 0 only if rows present/validated and sum is exactly zero)
    lookup.set(s, sumNotional);
  }

  // Verify source coverage: if any secondTs key could not be computed due to
  // missing source blocks, throw E007. This check is done by validateAggLookback
  // before calling buildTradedNotionalLookup; if we reach here, coverage is valid.
  return lookup;
}

/**
 * 1-block lag pipeline with commitFinalizedBlock.
 *
 * Strict ordering for pending finalized N and candidate N+1:
 *  1. read+validate candidate N+1 raw format/schema/ordering ONLY
 *  2. validate finalized N required agg aux blocks for union [N_start-30000, N_end)
 *  3. if either fails: quarantine report; detector/checkpoint/manifest byte-identical; exit
 *  4. capture openBurstBeforeCandidate = detector.getOpenBurstState()
 *  5. feed ALL sorted N+1 trades
 *  6. compute N first time with complete #12 lookup
 *  7. commit N once with N+1 next pending
 */
export async function runPipeline({ dataDir, market, fromMs, toMs, runId, outputRoot }) {
  const derivedDir = outputRoot || 'data/derived/burst_features_v1';
  const blocks = scanTradeBlocks(dataDir, market, fromMs, toMs);
  if (blocks.length === 0) {
    log('INFO', market, 'no blocks to process');
    return { processed: 0, errors: 0, manifestUpdates: [] };
  }

  const cp = loadCheckpoint(market);
  const detector = new BurstDetector(market, cp?.open_burst ?? null);
  const committer = new OutputCommitter(market, runId);

  let processed = 0, errors = 0;
  let checkpointGeneration = cp?.generation ?? 0;

  // pendingBlock is the NEXT block to be finalized (N+1 in the lag scheme)
  // It contains only identity info, NOT computed rows
  let pendingBlock = cp?.pending_block ?? null;
  const manifestUpdates = [];

  // Restore warmup from checkpoint
  let warmup = cp ? false : true;

  // If restart with pending_block, load and validate the pending block's immutability
  if (pendingBlock !== null) {
    const replayPath = pendingBlock.replay_identity.input_path;
    const currentSha = sha256File(replayPath);
    if (currentSha !== pendingBlock.trade_input_sha256) {
      throw new Error(`E021: pending block ${pendingBlock.block_start_ms} input changed (expected ${pendingBlock.trade_input_sha256}, got ${currentSha})`);
    }
    // ASSERT: pending_block has NO staged rows — only identity info
  }

  for (let i = 0; i < blocks.length; i++) {
    const candidateBlock = blocks[i];  // This is "current N+1" from pending's perspective

    // ═══ Step 1: read+validate candidate N+1 raw format/schema/ordering ONLY ═══
    const content = readFileSync(candidateBlock.fullPath, 'utf8');
    const { trades, inputSha256 } = validateAndParseTrades(content, candidateBlock.ms);
    // E001-E005 validation done; N+1's agg lookback is NOT validated here

    // ═══ Step 2: validate finalized N required agg aux blocks ═══
    let aggResult = null;
    if (pendingBlock !== null) {
      aggResult = validateAggLookback(dataDir, market, pendingBlock.block_start_ms);
      if (!aggResult.valid) {
        // ═══ Step 3: aux failure → quarantine; detector/checkpoint/manifest unchanged; exit ═══
        // TDD assertion: detector snapshot, checkpoint file bytes, manifest file bytes
        // MUST be identical to pre-failure state before calling writeQuarantineReport
        const detectorSnapshot = detector.getOpenBurstState();
        writeQuarantineReport(market, pendingBlock.block_start_ms,
          'E007: missing agg_trades lookback blocks for finalized N', aggResult.missing);
        throw new Error(`E007: missing auxiliary inputs for finalized block ${pendingBlock.block_start_ms}`);
      }
    }

    // ═══ Step 4: capture open burst state BEFORE feeding N+1 trades ═══
    const openBurstBeforeCandidate = detector.getOpenBurstState();

    // ═══ Step 5: feed ALL sorted N+1 trades ═══
    detector.feedTrades(trades);

    // ═══ Step 6: compute N first time with complete #12 lookup ═══
    if (pendingBlock !== null) {
      // Build per-second denominator lookup for #12 — throws E007 if coverage incomplete
      const tradedNotional30s = buildTradedNotionalLookup(aggResult, pendingBlock.block_start_ms);

      // Compute N's rows FOR THE FIRST TIME (N+1 state now in detector)
      const nTradesContent = readFileSync(pendingBlock.replay_identity.input_path, 'utf8');
      const { trades: nTrades } = validateAndParseTrades(nTradesContent, pendingBlock.block_start_ms);

      const nRows = computeFeatures1s({
        detector,  // has N+N+1 state, N's closed bursts are now in _closedBursts
        blockStartMs: pendingBlock.block_start_ms,
        tradeTsList: nTrades.map(t => t.ts),
        warmup: warmup,
        inputBlockIds: [String(pendingBlock.block_start_ms)],
        lookupTradedNotional30s: tradedNotional30s,  // #12 denominator per second — MUST be present
      });

      // Commit N via commitFinalizedBlock — N+1 becomes next pending
      const gen = checkpointGeneration;
      const commitId = randomUUID();

      // ═══ Step 7: commit N once with N+1 next pending ═══
      const nextPendingInfo = {
        block_start_ms: candidateBlock.ms,
        trade_input_sha256: inputSha256,
        auxiliary_input_hashes: {},  // N+1's agg hashes filled when N+1 is finalized
        replay_identity: {
          market,
          block_start_ms: candidateBlock.ms,
          input_path: candidateBlock.fullPath,
        },
        open_burst_before_N1: openBurstBeforeCandidate,  // captured BEFORE feedTrades
      };

      const result = committer.commitFinalizedBlock(
        { block_start_ms: pendingBlock.block_start_ms, input_sha256: pendingBlock.trade_input_sha256, date: formatDate(pendingBlock.block_start_ms), time: formatBlockTime(pendingBlock.block_start_ms) },
        nextPendingInfo,
        detector.getOpenBurstState(),  // post-feed detector state
        nRows,
        { auxiliary_input_hashes: aggResult.auxiliary_input_hashes },
        gen,
        commitId,
        false  // isEofFinalization: false — normal mid-stream commit
      );

      checkpointGeneration = result.nextGeneration;
      manifestUpdates.push({ blockMs: pendingBlock.block_start_ms, ...result });
      processed++;
      warmup = false;
    }

    // Set candidate (N+1) as new pending — NO rows computed yet
    pendingBlock = {
      block_start_ms: candidateBlock.ms,
      trade_input_sha256: inputSha256,
      auxiliary_input_hashes: {},
      replay_identity: {
        market,
        block_start_ms: candidateBlock.ms,
        input_path: candidateBlock.fullPath,
      },
      open_burst_before_N1: openBurstBeforeCandidate,
    };

    log('INFO', market, `block ${candidateBlock.ms}: ${trades.length} trades, pending (1-block lag)`);
  }

  // ═══ EOF: flushAll → finalize final pending block ═══
  if (pendingBlock !== null) {
    detector.flushAll();
    const finalTradesContent = readFileSync(pendingBlock.replay_identity.input_path, 'utf8');
    const { trades: finalTrades } = validateAndParseTrades(finalTradesContent, pendingBlock.block_start_ms);

    // Validate final pending's aux inputs
    const aggResult = validateAggLookback(dataDir, market, pendingBlock.block_start_ms);
    if (!aggResult.valid) {
      writeQuarantineReport(market, pendingBlock.block_start_ms,
        'E007: missing agg_trades lookback blocks for final pending', aggResult.missing);
      throw new Error(`E007: missing auxiliary inputs for final block ${pendingBlock.block_start_ms}`);
    }

    const tradedNotional30s = buildTradedNotionalLookup(aggResult, pendingBlock.block_start_ms);

    const finalRows = computeFeatures1s({
      detector,
      blockStartMs: pendingBlock.block_start_ms,
      tradeTsList: finalTrades.map(t => t.ts),
      warmup,
      inputBlockIds: [String(pendingBlock.block_start_ms)],
      lookupTradedNotional30s: tradedNotional30s,
    });

    const gen = checkpointGeneration;
    const commitId = randomUUID();
    const result = committer.commitFinalizedBlock(
      { block_start_ms: pendingBlock.block_start_ms, input_sha256: pendingBlock.trade_input_sha256, date: formatDate(pendingBlock.block_start_ms), time: formatBlockTime(pendingBlock.block_start_ms) },
      null,  // nextPendingBlock: null — no next pending at EOF
      detector.getOpenBurstState(),
      finalRows,
      { auxiliary_input_hashes: aggResult.auxiliary_input_hashes },
      gen,
      commitId,
      true  // isEofFinalization: true
    );
    checkpointGeneration = result.nextGeneration;
    manifestUpdates.push({ blockMs: pendingBlock.block_start_ms, ...result });
    processed++;
  }

  return { processed, errors, manifestUpdates };
}

function log(level, market, msg) {
  process.stderr.write(JSON.stringify({ ts: new Date().toISOString(), level, market, msg }) + '\n');
}
```

**cross-block restart TDD test（test/burst-reducer/cross-block-restart.test.mjs）:**
- TDD-8a: N tail open burst → N+1 first 5s continuation。N の最終秒の burst が N+1 の先頭 trades まで延長。全行 byte-identical
- TDD-8b: N tail open burst → N+1 empty watermark。N の open burst が N_end で閉じる。N コミット、N+1 が次 pending
- TDD-8c: restart after N committed before N+1。checkpoint に N+1 pending 情報あり。再起動後 byte-identical
- TDD-8d: invalid N+1（E004 ts decrease）→ quarantine。N uncommitted。**TDD 厳密アサーション: failure 前後で `detector.getOpenBurstState()` snapshot、checkpoint ファイル byte 内容、manifest ファイル byte 内容が完全同一**
- TDD-8e: crash between data rename and checkpoint。recovery で checksum 検証後 checkpoint 書き込み
- TDD-8f: #12 agg lookback — finalized N's aux validated。N の aux が `[N_start-30000, N_end)` 範囲で検証。合格 → auxiliary_input_hashes が manifest に存在
- TDD-8g: #12 agg lookback — missing N's aux → quarantine/fail。N uncommitted。**TDD 厳密アサーション: failure 前後で detector snapshot、checkpoint byte、manifest byte が完全同一**
- TDD-8h: empty first block → all-zero rows computed in next iteration
- **TDD-8i: `open_burst_before_N1` snapshot test** — checkpoint 永続化後の `pending_block.open_burst_before_N1` が `detector.getOpenBurstState()` の pre-feed キャプチャと deep-equal。バーストを変更する fixture で post-feed state と異なることを確認。再起動後も checkpoint から reload して byte-identical 結果を確認
- **TDD-8j: non-EOF commit** — `!isEofFinalization` 時に nextPendingBlock 非 null + checkpoint pending 非 null を検証
- **TDD-8k: EOF commit** — `isEofFinalization` 時に nextPendingBlock=null + checkpoint pending=null + E031 throw しないことを検証

**コマンド:**
```bash
node --test test/burst-reducer/pipeline.test.mjs
node --test test/burst-reducer/cross-block-restart.test.mjs
```

**期待結果:** 全テスト pass。全 TDD アサーションが green。

---

### Task 9: CLI エントリポイント

**時間目安:** 4 分
**作成/変更ファイル:**
- `scripts/reduce-burst-v1.mjs`（作成）

**振る舞い:**
CLI からパイプラインを起動するエントリポイント。
必須引数: `--from`, `--to`。オプション: `--markets`, `--data`, `--output-root`。

`--output-root` は出力ディレクトリのルートをオーバーライドする（テスト分離用）。
デフォルトは `data/derived/burst_features_v1`。テスト分離（`data/derived/burst_features_v1_validation/<run_id>`）のみ許可し、
本番パス (`data/derived/burst_features_v1`) 以外を本番で使用してはならない。

**疑似コード/アサーション:**
```javascript
#!/usr/bin/env node
// scripts/reduce-burst-v1.mjs

import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { runPipeline } from '../lib/burst-reducer/pipeline.mjs';

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { data: 'data/live_v3', markets: null, from: null, to: null, outputRoot: null };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--data': opts.data = args[++i]; break;
      case '--markets': opts.markets = args[++i]; break;
      case '--from': opts.from = args[++i]; break;
      case '--to': opts.to = args[++i]; break;
      case '--output-root': opts.outputRoot = args[++i]; break;
      case '--help':
        console.error(`Usage: node scripts/reduce-burst-v1.mjs --from <ISO|epoch_ms> --to <ISO|epoch_ms> [--markets <csv>] [--data <dir>] [--output-root <dir>]`);
        console.error(`  --output-root  Override output root dir (default: data/derived/burst_features_v1). Use only for test isolation.`);
        process.exit(0);
    }
  }

  if (!opts.from || !opts.to) {
    console.error('ERROR: --from and --to are required');
    process.exit(1);
  }

  return opts;
}

function isoToMs(iso) {
  const ms = new Date(iso).getTime();
  if (isNaN(ms)) throw new Error(`Invalid ISO timestamp: ${iso}`);
  return ms;
}

function detectMarkets(dataDir) {
  const tradesDir = join(dataDir, 'trades');
  if (!existsSync(tradesDir)) return [];
  return readdirSync(tradesDir, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name);
}

async function main() {
  const opts = parseArgs();
  const fromMs = isoToMs(opts.from);
  const toMs = isoToMs(opts.to);
  const runId = `run-${new Date().toISOString().replace(/[:.]/g, '-')}`;
  const outputRoot = opts.outputRoot || 'data/derived/burst_features_v1';

  const markets = opts.markets
    ? opts.markets.split(',').map(s => s.trim())
    : detectMarkets(opts.data);

  if (markets.length === 0) {
    console.error('ERROR: no markets found');
    process.exit(1);
  }

  process.stderr.write(JSON.stringify({ level: 'INFO', msg: `Starting reducer v1`, runId, markets, fromMs, toMs }) + '\n');

  let totalProcessed = 0;
  let totalErrors = 0;

  for (const market of markets) {
    try {
      const result = await runPipeline({
        dataDir: opts.data,
        market,
        fromMs,
        toMs,
        runId,
        outputRoot,
      });
      totalProcessed += result.processed;
      totalErrors += result.errors;
    } catch (e) {
      process.stderr.write(JSON.stringify({ level: 'FATAL', market, error: e.message }) + '\n');
      process.exit(1);
    }
  }

  process.stderr.write(JSON.stringify({ level: 'INFO', msg: 'Reducer complete', processed: totalProcessed, errors: totalErrors }) + '\n');
  process.exit(totalErrors > 0 ? 1 : 0);
}

main().catch(e => {
  process.stderr.write(JSON.stringify({ level: 'FATAL', error: e.message }) + '\n');
  process.exit(1);
});
```

**テスト:** 実データ検証でカバー（Task 11）。

**コマンド:**
```bash
# シンタックスチェック
node --check scripts/reduce-burst-v1.mjs

# help
node scripts/reduce-burst-v1.mjs --help

# 引数不足でエラー
node scripts/reduce-burst-v1.mjs 2>&1; echo "exit: $?"
```

**期待結果:**
- シンタックスチェック pass
- `--help` で使用方法表示、exit 0
- 引数不足で `ERROR: --from and --to are required`、exit 1

**停止条件:** `--from`, `--to` の ISO 8601 パースに失敗 → エラーメッセージ改善。

---

### Task 10: 契約テスト統合

**時間目安:** 5 分
**作成/変更ファイル:**
- `test/burst-reducer/golden.test.mjs`（作成）

**振る舞い:**
Task 1 で作成した golden fixtures を使用し、End-to-end の契約テストを実行する。
trade JSONL → validate → burst detect → feature compute → 期待 JSONL と比較。

**疑似コード/アサーション:**
```javascript
// test/burst-reducer/golden.test.mjs

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { validateAndParseTrades } from '../../lib/burst-reducer/input-validator.mjs';
import { BurstDetector } from '../../lib/burst-reducer/burst-detector.mjs';
import { computeFeatures1s } from '../../lib/burst-reducer/feature-computer-1s.mjs';

const FIXTURES_DIR = join('test', 'fixtures', 'burst-v1');

function loadJsonl(path) {
  const content = readFileSync(path, 'utf8');
  return content.trim().split('\n').filter(l => l).map(l => JSON.parse(l));
}

describe('Golden fixtures', () => {
  // Helper: complete 30-key zero-denominator lookup for valid-path golden tests
  const zeroLookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 0]));

  it('trades-basic: matches expected features', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-basic.jsonl');
    const expectedPath = join(FIXTURES_DIR, 'expected-features-1s.jsonl');

    const tradeContent = readFileSync(tradePath, 'utf8');
    const { trades } = validateAndParseTrades(tradeContent, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: true,
      inputBlockIds: ['trades-basic'],
      lookupTradedNotional30s: zeroLookup,
    });

    const expected = loadJsonl(expectedPath);

    // 期待行が定義されている秒のみ比較
    for (const exp of expected) {
      const row = rows.find(r => r.ts === exp.ts);
      assert.ok(row, `missing row for ts ${exp.ts}`);

      // trade-only 12 fields（#1-#12）
      const fields = [
        'burst_count_1s', 'total_burst_notional_1s', 'max_burst_notional_1s',
        'max_burst_prints_1s', 'max_burst_duration_ms_1s',
        'buy_burst_notional_1s', 'sell_burst_notional_1s',
        'burst_imbalance_ratio_1s', 'largest_burst_share_notional_1s',
        'same_price_burst_count_1s', 'multilevel_burst_count_1s',
        'burst_notional_vs_30s_traded_notional',
      ];
      for (const f of fields) {
        if (Number.isFinite(exp[f]) && Number.isFinite(row[f])) {
          assert.ok(Math.abs(row[f] - exp[f]) < 0.01, `${f}: expected ${exp[f]}, got ${row[f]}`);
        } else {
          assert.equal(row[f], exp[f], `${f}: expected ${exp[f]}, got ${row[f]}`);
        }
      }
    }
  });

  it('trades-cross-boundary: overlap works', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-cross-boundary.jsonl');
    const expectedPath = join(FIXTURES_DIR, 'expected-cross-boundary-1s.jsonl');

    const tradeContent = readFileSync(tradePath, 'utf8');
    const { trades } = validateAndParseTrades(tradeContent, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: true,
      inputBlockIds: ['cross-boundary'],
      lookupTradedNotional30s: zeroLookup,
    });

    const expected = loadJsonl(expectedPath);
    for (const exp of expected) {
      const row = rows.find(r => r.ts === exp.ts);
      assert.ok(row, `missing row for ts ${exp.ts}`);
      assert.equal(row.burst_count_1s, exp.burst_count_1s);
    }
  });

  it('trades-empty-block: all zero (empty block)', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-empty-block.jsonl');
    const tradeContent = readFileSync(tradePath, 'utf8');
    const { trades } = validateAndParseTrades(tradeContent, 0);

    const detector = new BurstDetector('test');
    // empty: no trades to feed
    if (trades.length > 0) detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: false,
      inputBlockIds: ['empty-block'],
      lookupTradedNotional30s: zeroLookup,
    });

    for (const row of rows) {
      assert.equal(row.burst_count_1s, 0, `ts ${row.ts}: expected 0 bursts`);
    }
  });

  it('trades-single-print-burst: single trade = 1 burst', () => {
    const tradePath = join(FIXTURES_DIR, 'trades-single-print-burst.jsonl');
    const tradeContent = readFileSync(tradePath, 'utf8');
    const { trades } = validateAndParseTrades(tradeContent, 0);

    const detector = new BurstDetector('test');
    detector.feedTrades(trades);
    detector.flushAll();

    const rows = computeFeatures1s({
      detector,
      blockStartMs: 0,
      tradeTsList: trades.map(t => t.ts),
      warmup: false,
      inputBlockIds: ['single-print'],
      lookupTradedNotional30s: zeroLookup,
    });

    // ts=0 bucket should have 1 burst (single print at ts=500)
    const row0 = rows.find(r => r.ts === 0);
    assert.ok(row0, 'missing row for ts 0');
    assert.equal(row0.burst_count_1s, 1, 'single print must be 1 burst');
    assert.equal(row0.total_burst_notional_1s, 100);
  });

  it('#12 burst_notional_vs_30s — nonzero denominator', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const lookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 10000]));
    // All 30 keys present: denom=10000 traded notional in each 30s window

    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500],
      warmup: false, inputBlockIds: ['test'],
      lookupTradedNotional30s: lookup,
    });
    const row0 = rows.find(r => r.ts === 0);
    assert.ok(row0);
    assert.equal(row0.total_burst_notional_1s, 100);
    assert.equal(row0.burst_notional_vs_30s_traded_notional, 100 / 10000); // 0.01
  });

  it('#12 burst_notional_vs_30s — zero denominator, valid aux input', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    const lookup = new Map(Array.from({length: 30}, (_, i) => [i * 1000, 0]));
    // All 30 keys present: denom exactly 0 in each window, valid aux input

    const rows = computeFeatures1s({
      detector: bd, blockStartMs: 0, tradeTsList: [500],
      warmup: false, inputBlockIds: ['test'],
      lookupTradedNotional30s: lookup,
    });
    const row0 = rows.find(r => r.ts === 0);
    assert.ok(row0);
    assert.equal(row0.burst_notional_vs_30s_traded_notional, 0); // denom=0 → output 0
  });

  it('#12 burst_notional_vs_30s — missing required aux throws E007', () => {
    const bd = new BurstDetector('test');
    bd.feedTrades([{ ts: 500, side: 'buy', price: 100, qty: 1 }]);
    bd.flushAll();

    // No lookupTradedNotional30s — simulates missing aux (quarantine path)
    // computeFeatures1s MUST throw E007, NEVER silently return 0
    assert.throws(() => {
      computeFeatures1s({
        detector: bd, blockStartMs: 0, tradeTsList: [500],
        warmup: false, inputBlockIds: ['test'],
        // lookupTradedNotional30s intentionally omitted
      });
    }, /E007/);
  });

  it('#12 burst_notional_vs_30s — incomplete map throws E007 even with zero bursts', () => {
    const bd = new BurstDetector('test');
    // No trades fed — all 30 rows will have zero bursts
    bd.flushAll();

    // Simulate incomplete lookup: only 29 keys, missing the last second
    const incompleteLookup = new Map(Array.from({length: 29}, (_, i) => [i * 1000, 0]));

    assert.throws(() => {
      computeFeatures1s({
        detector: bd, blockStartMs: 0, tradeTsList: [],
        warmup: false, inputBlockIds: ['test'],
        lookupTradedNotional30s: incompleteLookup,
      });
    }, /E007/);
  });
});
```

**コマンド:**
```bash
node --test test/burst-reducer/golden.test.mjs
```

**期待結果:** 全 8 テスト pass（basic, cross-boundary, empty-block, single-print-burst, #12 nonzero denom, #12 zero denom, #12 missing aux throws E007, #12 incomplete lookup throws E007 with zero-burst rows）。実際の計算値と手計算の期待値が一致。

**停止条件:** 期待値との不一致 → フィクスチャの期待値が正しいか再検証。計算ロジックのバグかフィクスチャの誤りかを切り分け。

---

### Task 11: 実データ検証（2 連続ブロック）

**時間目安:** 5 分
**作成/変更ファイル:** なし（検証のみ）

**振る舞い:**
実際の `data/live_v3/trades/` のデータに対し、CLI を実行して出力を目視および自動検証する。

**1-block lag には少なくとも 2 つの連続ブロックが必要。** 候補 N+1 が N をコミットするためには、
1 ブロックだけではパイプラインが EOF パスに入り正常コミットのパスを通らない。
Task 8 の controlled pipeline test が正常コミットをカバーする。実データ検証では 2 ブロックを選択する。

**前提条件:**
- 検証は隔離されたテスト出力ルート `TEST_OUTPUT_ROOT` を使用する。`TEST_OUTPUT_ROOT` は `data/derived/burst_features_v1_validation/<run_id>/` に設定する。
- Task 9 の CLI は `--output-root` オプションをサポートし、本番デフォルト（`data/derived/burst_features_v1`）をオーバーライドできる。テスト分離用の値のみ許可する。
- `TEST_OUTPUT_ROOT` 配下の既存の manifest/checkpoint が存在しない状態から始める。これにより checkpoint 不在 → 初回ブロック `warmup=true` が決定論的に検証される。
- 実データ（`data/live_v3/`）は読み取り専用。一切削除/移動しない。

**コマンド:**
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver

# 1. 全テスト実行
node --test test/burst-reducer/*.test.mjs

# 2. 選択マーケットの最初の 2 つの連続する貿易ブロックファイルを動的に発見
#    YYYY-MM-DD のハードコード禁止。pathlib glob で動的に発見。
MARKET=binance_spot

# 最初のファイルを動的に発見（日付ディレクトリを再帰的に走査）
FIRST_FILE=$(node --input-type=module -e "
  import { readdirSync, existsSync } from 'node:fs';
  import { join } from 'node:path';
  const base = join('data/live_v3/trades', '${MARKET}');
  if (!existsSync(base)) { process.exit(1); }
  const all = [];
  const dateDirs = readdirSync(base, { withFileTypes: true })
    .filter(d => d.isDirectory() && /^\d{4}-\d{2}-\d{2}$/.test(d.name))
    .sort((a, b) => a.name.localeCompare(b.name));
  for (const dd of dateDirs) {
    const datePath = join(base, dd.name);
    const files = readdirSync(datePath)
      .filter(f => f.endsWith('.jsonl'))
      .sort();
    for (const f of files) {
      all.push(join(datePath, f));
    }
  }
  if (all.length === 0) { process.exit(1); }
  // print first file path to stdout (verification subprocess exception for shell capture)
  console.log(all[0]);
" 2>/dev/null)
echo "First file: $FIRST_FILE"
if [ -z "$FIRST_FILE" ]; then echo "ERROR: No data found for ${MARKET}"; exit 1; fi

# 3. 2 番目の連続ファイルを発見（FIRST_FILE と同じ日付 dir 内の次の filesystem entry）
DATE_DIR=$(basename "$(dirname "$FIRST_FILE")")
TIME_FILE_1=$(basename "$FIRST_FILE" .jsonl)
# HH-MM-SS から epoch ms を計算
START_EPOCH_1=$(python3 -c "
import sys, os
f = '${FIRST_FILE}'
base = os.path.basename(f).replace('.jsonl','')
h,m,s = map(int, base.split('-'))
date = os.path.basename(os.path.dirname(f))
y,mth,d = map(int, date.split('-'))
import calendar
ms = calendar.timegm((y,mth,d,h,m,s)) * 1000
print(ms)
")
# 2 番目のブロックは +30000ms。ファイルが存在し連続することを確認
START_EPOCH_2=$((START_EPOCH_1 + 30000))
SECOND_FILE=$(python3 -c "
import os
f = '${FIRST_FILE}'
dirn = os.path.dirname(f)
# 次の 30s ブロックの HH-MM-SS を計算
from datetime import datetime, timedelta, timezone
d = datetime.fromtimestamp(${START_EPOCH_2}/1000, tz=timezone.utc)
time2 = d.strftime('%H-%M-%S')
f2 = os.path.join(dirn, time2 + '.jsonl')
if os.path.exists(f2):
    print(f2)
else:
    print('')
")
echo "First  block: $FIRST_FILE (epoch: $START_EPOCH_1)"
echo "Second block: $SECOND_FILE (epoch: $START_EPOCH_2)"
if [ -z "$SECOND_FILE" ]; then
  echo "ERROR: Second consecutive block not found. Need at least 2 consecutive blocks for 1-block lag commit."
  exit 1
fi

# 4. --from/--to に epoch ms を使用。END = SECOND_EPOCH + 30000（2 ブロック分カバー）
END_EPOCH=$((START_EPOCH_2 + 30000))
echo "Range: from=$START_EPOCH_1 to=$END_EPOCH (epoch ms)"

# 5. ファイル名が 00/30 秒境界に準拠しているか検証
python3 -c "
f1 = '${FIRST_FILE}'
f2 = '${SECOND_FILE}'
for f in [f1, f2]:
    base = f.rsplit('/',1)[1].replace('.jsonl','')
    sec = int(base.split('-')[2])
    if sec not in (0, 30):
        raise SystemExit(f'E006: file {f} seconds={sec} not on 00/30 boundary')
print('Boundary check OK')
"

# 6. テスト隔離出力ルート
RUN_ID=validation-$(date +%Y%m%d-%H%M%S)
TEST_OUTPUT_ROOT="data/derived/burst_features_v1_validation/${RUN_ID}"
echo "TEST_OUTPUT_ROOT: $TEST_OUTPUT_ROOT"

# 7. パイプラインを --output-root 付きで実行
node scripts/reduce-burst-v1.mjs \
  --markets "${MARKET}" \
  --from "${START_EPOCH_1}" \
  --to "${END_EPOCH}" \
  --output-root "${TEST_OUTPUT_ROOT}" 2>&1

# 8. マニフェスト検証（直接 node で readFileSync）
echo "=== Manifest check ==="
MANIFEST_PATH="${TEST_OUTPUT_ROOT}/manifests/${MARKET}.json"
node --input-type=module -e "
  import { readFileSync, existsSync } from 'node:fs';
  const path = '${MANIFEST_PATH}';
  if (!existsSync(path)) { console.error('MANIFEST MISSING'); process.exit(1); }
  const m = JSON.parse(readFileSync(path,'utf8'));
  console.error('schema:', m.schema_version);
  const keys = Object.keys(m.processed_blocks);
  console.error('blocks committed:', keys.length);
  if (keys.length > 0) console.error('first key:', keys[0]);
"

# 9. 出力 shard 検証（動的パス、直接 readFileSync）
echo "=== Output shard verification ==="
SHARD_PATH=$(node --input-type=module -e "
  import { readFileSync, existsSync } from 'node:fs';
  const mpath = '${MANIFEST_PATH}';
  if (!existsSync(mpath)) { console.error('NO_MANIFEST'); process.exit(1); }
  const m = JSON.parse(readFileSync(mpath,'utf8'));
  const keys = Object.keys(m.processed_blocks);
  if (keys.length === 0) { console.error('NO_BLOCKS'); process.exit(1); }
  const first = m.processed_blocks[keys[0]];
  console.error(first.output_paths.features_1s);
" 2>&1)

echo "Discovered shard path: ${SHARD_PATH}"
if [ -z "${SHARD_PATH}" ] || [ "${SHARD_PATH}" = "NO_BLOCKS" ] || [ "${SHARD_PATH}" = "NO_MANIFEST" ]; then
  echo "ERROR: No output shard found in manifest"
  exit 1
fi

FULL_SHARD_PATH="${TEST_OUTPUT_ROOT}/${SHARD_PATH}"
echo "=== First 3 output rows (direct read, no pipe) ==="
node --input-type=module -e "
  import { readFileSync, existsSync } from 'node:fs';
  const path = '${FULL_SHARD_PATH}';
  if (!existsSync(path)) { console.error('SHARD MISSING:', path); process.exit(1); }
  const content = readFileSync(path, 'utf8');
  const lines = content.trim().split('\n').slice(0, 3);
  for (const l of lines) {
    const r = JSON.parse(l);
    console.error('ts:', r.ts, 'burst_count:', r.burst_count_1s, 'total_notional:', r.total_burst_notional_1s.toFixed(2), 'warmup:', r._quality.warmup);
  }
"

# 10. 不変条件検証（直接 readFileSync、pipe 禁止）
echo "=== Invariant check ==="
node --input-type=module -e "
  import { readFileSync, existsSync } from 'node:fs';
  const path = '${FULL_SHARD_PATH}';
  if (!existsSync(path)) { console.error('SHARD MISSING'); process.exit(1); }
  const content = readFileSync(path, 'utf8');
  const lines = content.trim().split('\n');
  let ok = true;
  lines.forEach((l, i) => {
    try {
      const r = JSON.parse(l);
      if (r.ts % 1000 !== 0) { console.error('Line', i+1, 'ts not on second boundary:', r.ts); ok = false; }
      if (r.burst_count_1s !== r.same_price_burst_count_1s + r.multilevel_burst_count_1s) {
        console.error('Line', i+1, 'burst count invariant failed');
        ok = false;
      }
      if (Math.abs(r.total_burst_notional_1s - (r.buy_burst_notional_1s + r.sell_burst_notional_1s)) > 0.02) {
        console.error('Line', i+1, 'notional invariant failed');
        ok = false;
      }
      if (r.burst_imbalance_ratio_1s < -1.0 || r.burst_imbalance_ratio_1s > 1.0) {
        console.error('Line', i+1, 'imbalance out of range:', r.burst_imbalance_ratio_1s);
        ok = false;
      }
      if (r.burst_notional_vs_top_depth !== null) { console.error('Line', i+1, '#13 should be null'); ok = false; }
      if (r.burst_mid_move_bps_1s !== 0) { console.error('Line', i+1, '#14 should be 0'); ok = false; }
      if (r.outlier_trade_flag_1s !== 0) { console.error('Line', i+1, '#22 should be 0'); ok = false; }
    } catch(e) { console.error('Line', i+1, 'JSON parse error:', e.message); ok = false; }
  });
  console.error(lines.length + ' rows checked, ' + (ok ? 'ALL OK' : 'FAILURES FOUND'));
  process.exit(ok ? 0 : 1);
"

echo "=== Validation complete ==="
```

**期待結果:**
- 全ユニットテスト pass
- 実データでパイプラインが exit 0 で完了
- 2 つの連続ブロックが処理され、1-block lag の正常コミットパスを通過
- 出力 block shard JSONL が `TEST_OUTPUT_ROOT` 配下に存在し、各行が有効
- 不変条件全 pass
- `_quality.warmup` が checkpoint 不在の最初の 1 ブロック（30行）のみで true（決定論的に検証される）
- ハードコードされた日付パス（`data/live_v3/trades/${MARKET}/2026-07-10`）が存在しないこと
- ISO開始時刻の中間変数を作らず、START_EPOCH を一度だけ計算すること
- `cat ... | node` パイプラインが存在しないこと（全ファイル読み取りは direct `readFileSync`）
- 検証 one-liner 内の出力は `console.error` に統一（stdout が必要な shell capture は verification-only annotation 付き）

**停止条件:** 実データが存在しない場合、Task 0 の確認に戻る。`data/live_v3/trades/` がなければ Receiver が稼働しているか確認。
`--output-root` が Task 9 で未実装の場合は、Task 9 に `--output-root` を追加してから再試行。

---

## 6. テスト実行コマンド一覧

```bash
# 全ユニットテスト
node --test test/burst-reducer/*.test.mjs

# 個別テスト
node --test test/burst-reducer/input-validator.test.mjs
node --test test/burst-reducer/block-scanner.test.mjs
node --test test/burst-reducer/burst-detector.test.mjs
node --test test/burst-reducer/feature-computer-1s.test.mjs
node --test test/burst-reducer/output-committer.test.mjs
node --test test/burst-reducer/golden.test.mjs
node --test test/burst-reducer/pipeline.test.mjs

# 既存テストが壊れていないことの確認
node --test test/*.test.mjs

# シンタックスチェック（全新規ファイル）
node --check lib/burst-reducer/schema.mjs
node --check lib/burst-reducer/input-validator.mjs
node --check lib/burst-reducer/block-scanner.mjs
node --check lib/burst-reducer/burst-state-codec.mjs
node --check lib/burst-reducer/burst-detector.mjs
node --check lib/burst-reducer/feature-computer-1s.mjs
node --check lib/burst-reducer/output-committer.mjs
node --check lib/burst-reducer/pipeline.mjs
node --check scripts/reduce-burst-v1.mjs
```

---

## 付録 B: ファイル作成順序

依存関係に基づく作成順序。先行タスクのファイルが存在しないと後続タスクのテストが import エラーになる:

```
1. lib/burst-reducer/schema.mjs           (Task 2)  ← 全ファイルの基底
2. lib/burst-reducer/input-validator.mjs  (Task 3)  ← schema に依存
3. lib/burst-reducer/block-scanner.mjs   (Task 4)  ← 独立
4. lib/burst-reducer/burst-state-codec.mjs (Task 5b) ← burst-builder に依存
5. lib/burst-reducer/burst-detector.mjs  (Task 5)  ← schema + burst-builder + burst-state-codec に依存
6. lib/burst-reducer/feature-computer-1s.mjs (Task 6) ← schema + detector に依存
7. lib/burst-reducer/output-committer.mjs (Task 7)  ← schema に依存
8. lib/burst-reducer/pipeline.mjs        (Task 8)  ← 上記全ファイルに依存
9. scripts/reduce-burst-v1.mjs          (Task 9)  ← pipeline に依存
```

テストファイルも同順で作成する（対応する実装ファイルの直後）。

---

## 付録 C: Flash エージェントチェックリスト

各タスク完了後、以下のチェック項目を自己検証すること:

- [ ] 本番コード（`lib/`, `scripts/`, `test/` の既存ファイル）を変更していないか
- [ ] 出力先は `data/derived/burst_features_v1/` 以下か（`data/burst_agg/` や `data/1s_features/` に書き込んでいないか）
- [ ] `import` 文のパスは正しいか（`node:fs`, `node:path`, `node:test` を使用しているか。相対パスの `../../` の深さは正しいか）
- [ ] `.mjs` 拡張子を使用しているか
- [ ] `gap_threshold_ms=50`, `max_burst_duration_ms=5000` はハードコードされているか
- [ ] NULL が必要な箇所で `0` や `undefined` を使っていないか
- [ ] 全テストが `node --test test/burst-reducer/*.test.mjs` で pass するか
- [ ] 既存テストが `node --test test/*.test.mjs` で壊れていないか
- [ ] 標準出力（`console.log`）を production code で使用していないか（ルール #16: `lib/burst-reducer/**` + `scripts/reduce-burst-v1.mjs` では `process.stderr` に JSON Lines。検証 one-liner は verification-only exception。）
- [ ] `npm install` を実行していないか（新規依存なし）
- [ ] ファイルが Unix 改行（LF）であるか
- [ ] `_open`, `_closedBursts`, `_nextId` への文字列リテラル直接アクセスが `lib/burst-reducer/burst-state-codec.mjs` 以外の全 planned ファイルに存在しないか（grep で検証）

# Feature Compression Pipeline 設計書

**文書 ID:** design-2026-07-10-feature-compression-pipeline
**日付:** 2026-07-10
**対象リポジトリ:** `agg-btc-receiver`（`/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver`）
**文書種別:** WHAT（docs-only。コード変更前の設計固定）
**関連文書:**
- `docs/specs/design-2026-07-10-burst-reducer.md`
- `docs/specs/plan-2026-07-10-burst-reducer.md`
- `docs/specs/plan-2026-07-10-burst-reducer-p0-safety.md`
- `docs/specs/plan-2026-07-10-burst-reducer-remediation.md`
- `docs/specs/specify-2026-07-09-burst-features.md`
- `docs/decisions/adr-009-burst-feature-spec-v1.md`

---

## 1. 目的

本書は、Receiver 後段で raw 市場データを一定間隔の特徴量データセットへ圧縮する pipeline の WHAT を定義する。
対象は次の 4 層である。

1. raw input
2. `features_1s` canonical dataset
3. `features_30s` compressed dataset
4. `features_5min` summary dataset

本書は「何をどの境界で、どの責務として、どのセマンティクスで出すか」を固定する。
実装順、テスト順、変更手順は別紙 `plan-2026-07-10-feature-compression-pipeline.md` に分離する。

---

## 2. 背景と前提

現状の観測:
- Receiver は raw-only。責務は受信と保存まで。
- 入力は `trades` / `book_updates` / `liquidations` の 30s shard。
- 既存 burst reducer は raw trades から `features_1s` を出力しているが、これは 30s shard ごとに 30 行を作る構成であり、`features_30s` / `features_5min` は未実装。
- 既存 `features_1s` は 22 特徴量契約を持つ。今回の圧縮 pipeline 追加でこの契約を壊してはならない。
- ADR-009 は `1s → 30s → 5min` の方向を Accepted としている。

ユーザー原則:
- Receiver は raw 受信保存のみ。
- 後段は market 別に古い block から単純逐次。
- single-writer。
- 95 点レビューゲート。
- 実装前に決定事項を固定する。

### 2.1 P0 checkpoint 現状実測値（2026-07-10）

現行本番 checkpoint は FULL serialize（`serializeMinimalBurstState` 実装済みだが未使用）により、closedBursts 累積で肥大している。

| market | on-disk bytes | closedBursts 数 | 状態 |
|---|---|---|---|
| `binance_perp` | 533,623,819 | 98,730 | FULL, 肥大 |
| `binance_spot` | 536,296,776 | 59,633 | FULL, 肥大 |
| `bybit_perp` | 536,672,479 | 46,965 | FULL, 肥大 |

根因: closedBursts 配列が全履歴を checkpoint に含み、block 数に比例して線形増加する。`serializeMinimalBurstState` は実装済みだが、現行 checkpoint 生成 path では使われていない。

**on-disk JSON checkpoint 制限（推奨デフォルト/受入条件）:**
- `warn`: 256 KiB（超えたら structured log 警告）
- `hard fail`: 1 MiB（超えたら処理停止。P0 完了条件）
- block 数に比例しないことをテストで証明する（synthetic huge-closed-bursts fixture: 100,000 closedBursts でも checkpoint <= 1MiB）

**legacy 500MB checkpoint 移行手順:**
1. 本番停止（全 market）
2. 現行 checkpoint を backup ディレクトリへ隔離（削除禁止）
3. manifest / final shard 整合を検証（block hash chain 完全性確認）
4. 整合確認後、deterministic rebuild で新 checkpoint（`serializeMinimalBurstState` 使用）を生成
5. **in-place 変換禁止。** 古い checkpoint を直接編集してはならない
6. 整合不明の場合は quarantine + 本番停止。手動検証なしの再開禁止
7. 再起動後、first commit が byte-identical であることを検証（再 build 前の出力と完全一致）

---

## 3. システム境界

### 3.1 Receiver との境界

Receiver と compression pipeline の責務境界は厳密に分離する。

Receiver の責務:
- 市場データ受信
- raw event の正規化最小限
- 30s block shard への保存
- live 系ディレクトリの維持

Compression pipeline の責務:
- raw shard の逐次読取
- event-time に基づく特徴量集約
- 1s canonical / 30s compressed / 5min summary の生成
- watermark 判定
- empty と missing の区別
- restart / idempotency / single-writer 制御
- quality flag と observability 出力

禁止事項:
- Receiver に feature 圧縮責務を戻さない
- Receiver に checkpoint や downstream state を持たせない
- raw input を compression pipeline 側の都合で変更・削除しない

### 3.2 入出力 path 案

入力（read-only）:

```text
data/live_v3/
  trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

出力（proposal）:

```text
data/derived/burst_features_v1/
  features_1s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  features_30s/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  features_5min/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  manifests/<market>.json
  manifests/checkpoints/<market>.json
  quarantine/<market>/<block_start_ms>.json
  metrics/<YYYY-MM-DD>/<market>.jsonl
```

補足:
- `features_1s` は canonical dataset とする。
- `features_30s` は 30s block ごと 1 行の compressed dataset とする。
- `features_5min` は `features_30s` と別 dataset とし、5min summary 専用の inventory を持つ。
- 初期 hot format は JSONL block shard とする。
- 別 format（Parquet 等）は out-of-scope。

---

## 4. レイヤ構成と責務

### 4.1 raw layer

役割:
- source-of-truth
- 再処理可能な最小単位
- live/backfill 双方の入力源

責務:
- 受信済み事実の保持
- 30s shard の境界確定
- downstream 再計算のための監査証跡

非責務:
- 1s / 30s / 5min 集約結果の意味付け
- downstream checkpoint
- cleanup 自動化（初期）

### 4.2 `features_1s` canonical layer

役割:
- feature 圧縮の canonical 基盤
- 既存 22 特徴量契約の唯一の正準ソース
- 30s / 5min の上位層の入力

責務:
- 1 秒境界の feature row を連続生成する
- 30s block から 30 row を確定出力する
- 既存 22 特徴量契約を維持する
- null/zero/quality の契約を固定する
- 後段が direct 再集計に必要な最低限メタデータを持つ

設計原則:
- `features_1s` は canonical。上位層が raw を毎回総なめする前提にはしない。
- ただし「どの列が 1s rollup から安全に再集計できるか」は列ごとに明示する。
- v1 は trades-only で開始し、book/liquidation は段階導入する。

### 4.3 `features_30s` compressed layer

役割:
- `features_1s` から 30 秒単位の問い合わせコストを落とす
- 30s window の direct 集計値と rollup 集計値を分離して保持する

責務:
- 30s window ごとに 1 行を出力する
- 列ごと演算子 matrix に従って集計する
- overlap-based 1s sum と direct market total を混同しない
- recompute 必須列を raw / canonical / auxiliary から再計算する

重要原則:
- `features_30s` は「サイズ圧縮」だけでなく「意味圧縮」でもある。
- 同じ 30s 行の中に、rollup 由来値と direct/recompute 由来値が共存する。
- どの列がどの演算子かは実装前に固定必須。

### 4.4 `features_5min` summary layer

役割:
- 5min 粒度の summary dataset（正準名: `features_5min`）
- dashboard / analytics / backtest の軽量入力

責務:
- 5min window ごとに summary row を出力する
- 30s の圧縮結果を使い、必要列のみ direct/recompute を許容する
- `features_30s` とは別 dataset として inventory を管理する

設計原則:
- 5min は summary 別 dataset とする
- 5min は今回 scope に含むが、初回実装優先度は 30s より後ろ
- cross-market 結合や高次分析の入口ではあるが、その設計までは広げない
- 詳細指標（z-score, percentile, spot-perp divergence, regime 等）は P3 entry まで延期
- `summary_5min` 表記は使用しない。本 pipeline における 5min 正準 dataset 名は `features_5min` のみ

---

## 5. 逐次処理モデル

### 5.1 単位

- 最小入力単位: market ごとの 30s raw shard
- 最小出力単位:
  - `features_1s`: 30 行 / 30s shard
  - `features_30s`: 1 行 / 30s shard
  - `features_5min`: 1 行 / 5min shard

### 5.2 順序

- market 別に独立
- 各 market 内では古い block から単純逐次
- out-of-order 処理禁止
- 同一 market 並列 writer 禁止

### 5.3 inventory モード

2 モードを区別する。

1. live mode
   - 権威は `finalized-through`
   - 未確定 horizon より先を finalize しない

2. backfill mode
   - 権威は frozen inventory
   - frozen inventory で宣言された block だけを確定対象とする

推奨デフォルト:
- live = finalized-through inventory
- backfill = frozen inventory

---

## 6. direct 集計 vs 1s rollup

### 6.1 基本方針

30s / 5min 集約は、すべてを単純 sum しない。
列ごとに次のいずれかの演算子を取る。

- `sum`
- `max`
- `weighted`
- `direct`
- `recompute`
- `last`
- `quality`

> **既存 spec `specify-2026-07-09-burst-features.md` §6.2（lines 488-521）の 30s draft table は reference draft である。**
> 本書の §6.6 P2 承認済み matrix が **P2 正本** であり、両者が矛盾する場合は本書が優先する。

### 6.2 重要ルール

最重要ルール:
- overlap-based 1s sum を direct market total に使わない。

理由:
- burst の 1s row は overlap semantics を持つため、単一 burst が複数 1s bucket に現れる。
- そのため `total_burst_notional_1s` 等の単純 30 個 sum は「window 内 overlap exposure」としては有効でも、「30s 実体総量」とは一致しない。
- 特に market total, traded total, burst direct total は direct または recompute が必要。

### 6.3 direct 集計を使うケース

direct が必要な代表例:
- 30s 実体の total burst notional
- 30s 実体の burst count（overlap count ではない定義を採る場合）
- market total traded notional
- liquidation total notional
- book depth snapshot 依存値

### 6.4 rollup 集計を使うケース

1s rollup が安全な代表例:
- max of 1s maxima
- last finalized quality state
- 30s 内での欠損フラグ集約
- 1s canonical の coverage / null-rate / zero-rate 統計

### 6.5 recompute を使うケース

次は recompute 候補:
- weighted average 系
- ratio の再分母が 30s/5min で変わる列
- direct total を分母に持つ正規化列
- book/liquidation を join する列

### 6.6 P2 `features_30s` trades-only 列ごと演算子 matrix（承認済み正本）

本 matrix の **A〜C は P2 `features_30s` で物理出力する trades-only の全列**を定義する。D は matrix の出力列ではなく、P4 まで出力しない列を明示する除外表である。

レイヤ契約は次のとおり分離する。
- `features_1s`: 既存契約どおり #1〜#22 の22列を物理出力し続ける。P1では #13=`null`、#14〜#22=`0` の既存契約を維持する。
- `features_30s` P2: 下記A〜Cのtrades-only列だけを物理出力する。#13〜#22を元にした30秒集約列は出力しない。
- `features_30s` P4: book・研究・監視列の意味と演算子を別途承認した後に、Dの対象列を追加できる。

overlap exposure 列（1s rollup 由来、同一 burst の複数秒重複を含む）と direct entity 列（30s window 内の実体 burst から直接計算、重複なし）は**別名列で共存**させる。

**凡例:**
- **Operator**: `rollup`（`features_1s` の 30 行から集約）、`direct`（30s window の実体 burst から直接計算）、`recompute`（raw/canonical/auxiliary から分子分母を再計算）
- **Source**: 集約元データの所在
- **Quality**: 出力の信頼性・phase 情報

#### A. rollup 列（`features_1s` 30行から集約）

| output column | meaning | operator | source | quality |
|---|---|---|---|---|
| `burst_count_mean_30s` | 1秒あたり平均 overlap burst 数 | rollup: mean | `features_1s.burst_count_1s` × 30 | P1 canonical |
| `burst_count_max_30s` | 1秒あたり最大 overlap burst 数 | rollup: max | `features_1s.burst_count_1s` × 30 | P1 canonical |
| `burst_notional_overlap_sum_30s` | 1s overlap notional の 30s 合計（重複あり） | rollup: sum | `features_1s.total_burst_notional_1s` × 30 | P1 canonical。⚠ 実勢 notional ではない |
| `burst_notional_overlap_max_30s` | 1s overlap notional の 30s 内最大 | rollup: max | `features_1s.total_burst_notional_1s` × 30 | P1 canonical |
| `burst_notional_overlap_p95_30s` | 1s overlap notional の 30s 内 P95 | rollup: p95 | `features_1s.total_burst_notional_1s` × 30 | P1 canonical |
| `max_burst_notional_max_30s` | 30s 内の最大単一 burst notional（1s max の max） | rollup: max | `features_1s.max_burst_notional_1s` × 30 | P1 canonical |
| `max_burst_notional_mean_30s` | 1s max burst notional の 30s 平均 | rollup: mean | `features_1s.max_burst_notional_1s` × 30 | P1 canonical |
| `max_burst_prints_max_30s` | 30s 内の最大 print 数（1s max の max） | rollup: max | `features_1s.max_burst_prints_1s` × 30 | P1 canonical |
| `max_burst_duration_max_30s` | 30s 内の最大 duration（1s max の max） | rollup: max | `features_1s.max_burst_duration_ms_1s` × 30 | P1 canonical |

#### B. direct 列（30s window の実体 burst から直接計算）

| output column | meaning | operator | source | quality |
|---|---|---|---|---|
| `burst_unique_count_30s` | 30s window 内の一意な burst 数（重複排除） | direct: unique count | BurstBuilder closedBursts + open burst（30s window 内） | P2 direct |
| `burst_notional_sum_30s` | 30s window 内の全 burst notional 合計（重複なし実勢値） | direct: sum | BurstBuilder closedBursts（30s window 内） | P2 direct。`burst_notional_overlap_sum_30s` とは別物 |
| `burst_notional_max_30s` | 30s window 内の最大 burst notional | direct: max | BurstBuilder closedBursts（30s window 内） | P2 direct |
| `buy_burst_notional_sum_30s` | 30s window 内 buy-side burst notional 合計 | direct: sum | BurstBuilder closedBursts（side=buy, 30s window 内） | P2 direct |
| `sell_burst_notional_sum_30s` | 30s window 内 sell-side burst notional 合計 | direct: sum | BurstBuilder closedBursts（side=sell, 30s window 内） | P2 direct |
| `same_price_burst_unique_count_30s` | 30s window 内の一意な same-price burst 数 | direct: unique count | BurstBuilder closedBursts（distinct_price_count==1, 30s window 内） | P2 direct |
| `multilevel_burst_unique_count_30s` | 30s window 内の一意な multilevel burst 数 | direct: unique count | BurstBuilder closedBursts（distinct_price_count>=2, 30s window 内） | P2 direct |

#### C. recompute 列（raw/canonical から再計算）

| output column | meaning | operator | source | quality |
|---|---|---|---|---|
| `burst_notional_vs_traded_notional_30s` | burst 実勢 notional / 30s 総約定 notional（#12 30s 版） | recompute | `burst_notional_sum_30s` / `traded_notional_30s`（raw trades `[s-30000,s)` から再計算） | P2 recompute。分母 zero → `0` |
| `burst_imbalance_ratio_30s` | `(buy_direct - sell_direct) / (buy_direct + sell_direct + eps)` | recompute | `buy_burst_notional_sum_30s`, `sell_burst_notional_sum_30s` | P2 recompute。範囲 `[-1.0, 1.0]` |
| `largest_burst_share_30s` | `burst_notional_max_30s / burst_notional_sum_30s`（direct largest / direct total） | recompute | `burst_notional_max_30s`, `burst_notional_sum_30s` | P2 recompute。範囲 `(0, 1]` |

#### D. not emitted until P4

以下の列は **P4 まで物理出力しない**（P1 placeholder 由来の偽統計防止）。quality 列に `phase` を記録する。

| # | column | reason | emit phase | quality phase label |
|---|---|---|---|---|
| #13 | `burst_notional_vs_top_depth` | book state 依存 | P4 | `P1_book_null`（P1-P3 では null） |
| #14 | `burst_mid_move_bps_1s`（30s 集約列） | book state 依存 | P4 | `P1_book_zero`（P1-P3 では 0） |
| #15 | `same_price_burst_max_len_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #16 | `same_price_burst_notional_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #17 | `multilevel_burst_max_span_ticks_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #18 | `multilevel_burst_max_span_bps_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #19 | `multilevel_burst_notional_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #20 | `same_price_absorption_ratio_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #21 | `burst_delta_notional_1s`（30s 集約列） | 研究列。P1 placeholder `0` | P4 | `P1_placeholder` |
| #22 | `outlier_trade_flag_1s`（30s 集約列） | 監視列。P1 placeholder `0` | P4 | `P1_placeholder` |

---

## 7. watermark / empty / missing

### 7.1 用語

- `arrived-valid`: expected block が存在し、parse/validate 済み
- `arrived-empty-valid`: block が存在し、空だが有効
- `not-yet-arrived`: finalized horizon より先、または inventory 未宣言
- `verified-missing`: finalized horizon / inventory 上は存在すべきだが、欠落・破損・不整合
- `eof-finalizable`: 権威ある horizon により最終 finalize してよい状態

### 7.2 empty と missing の区別

- empty は valid data
- missing は error condition
- empty を missing 扱いしてはならない
- missing を empty 扱いしてはならない

### 7.3 レイヤ別扱い

`features_1s`:
- trade なし秒は 0 row を出す
- block 自体が空でも 30 行の row を出す
- quality に empty block の事実を残す

`features_30s` / `features_5min`:
- 入力 coverage が 100% なら empty でも row を出す
- 必要入力の一部 missing なら quarantine / stop

### 7.4 watermark ルール

- live mode では `finalized-through` が権威
- range exhaustion や単なる directory enumeration は EOF 証拠にならない
- 5min finalize も同じ原則に従う

---

## 8. restart / idempotency / single-writer

### 8.1 restart

要求:
- crash / kill / restart 後に byte-identical output を再構成できること
- pending block を失わないこと
- live/backfill いずれでも cursor が逆行しないこと

P0 として最重要:
- checkpoint state bounded 化

bounded 化の意味:
- 再開に不要な巨大履歴を持たない
- open state と pending identity のみ保持する
- checkpoint が block 数に比例して肥大しない

### 8.2 idempotency

- 出力は composite identity で一意化する
- block_start だけでは不十分
- input hash / schema version / market を含む
- 同じ入力から同じ出力を再生成しても二重 commit しない

### 8.3 single-writer lock identity

lock identity は以下で構成する:

```
absolute outputRoot + schema_version + market
```

namespace 統合:
- live / backfill / manual / cron の全モードが同一 namespace で lock を取得する
- 取得失敗（既存 lock あり）は blocked / no-write（処理継続不可・待機のみ）
- stale 判定は OS `flock` に委譲する（lock file の mtime や PID による自前判定禁止）
- **read-only inspection（既存出力の整合確認、manifest 読み取り）のみ lock 不要**

### 8.4 P0 checkpoint 制限（再掲）

| 閾値 | 値 | 動作 |
|---|---|---|
| `warn` | 256 KiB on-disk JSON | structured log 警告出力 |
| `hard fail` | 1 MiB on-disk JSON | 処理停止。P0 完了条件 |

テスト要件: synthetic 100,000 closedBursts fixture で checkpoint <= 1 MiB を証明すること。

### 8.5 legacy checkpoint 移行（再掲要約）

- 本番停止 → backup 隔離 → manifest/final shard 整合検証 → deterministic rebuild
- **in-place 変換禁止**
- 整合不明 → quarantine + 停止
- 再起動後 first commit byte-identical 検証

---

## 9. quality / null / zero

### 9.1 既存契約を壊さない

今回の pipeline は既存 22 特徴量契約を壊してはならない。
特に `features_1s` では現行契約を維持する。

### 9.2 null と zero の意味

- `null`: 値が概念上存在するが、この layer / phase では未計算または適用不能
- `0`: 観測結果としてゼロ、または契約上のゼロ埋め値

### 9.3 quality の責務

quality は値本体とは別に次を運ぶ。

- coverage
- empty block 判定
- warmup / partial / recovered の状態
- upstream dataset version
- auxiliary dependency の有無
- finalized 根拠

### 9.4 上位層での quality 伝播

上位層は quality を落とさない。
少なくとも次を持つ。

- `source_layer`
- `source_window_count`
- `has_empty_input`
- `has_missing_input`
- `recomputed_columns`
- `finalized`

---

## 10. retention / cleanup

### 10.1 初期方針

初期は削除禁止。

対象:
- raw input
- `features_1s`
- `features_30s`
- `features_5min`
- manifests / checkpoints / quarantine

### 10.2 cleanup の扱い

- cleanup は初期手動
- retention 日数は後決め
- 自動削除 job は今回 scope 外
- まずは正しさと bounded state を先に固定する

### 10.3 将来の方向

- retention は layer 別に持つ可能性がある
- ただし raw 削除は feature parity・監査性・再計算性が揃うまで解禁しない

---

## 11. observability

### 11.1 最低限必要な観測項目

- processed block count
- committed shard count
- blocked_reason
- quarantine count
- restart / recovery count
- checkpoint byte size
- pending state byte size
- layer 別 row count
- coverage / empty / missing 件数
- direct/recompute 列の件数

### 11.2 ログ粒度

- market ごと
- block ごと
- layer ごと
- structured JSON Lines を前提

### 11.3 レビュー観点

95 点レビューゲートでは少なくとも次を確認できること。

- なぜ block が止まったか
- どの layer まで commit 済みか
- empty と missing を区別できているか
- checkpoint が bounded か
- duplicate commit が起きていないか

---

## 12. 段階導入

推奨導入順:

1. P0 checkpoint 修復・bounded 化
2. P1 `features_1s` canonical 安定化（trades-only）
3. P2 `features_30s` compression（trades-only）
4. P3 `features_5min` summary
5. P4 book / liquidation 拡張
6. P5 retention / cleanup

この順序の意図:
- まず state explosion と restart correctness を塞ぐ
- 次に canonical layer を固定する
- その上で 30s 圧縮に進む
- 5min は 30s の意味が固まってから導入する

### 12.1 P1 `features_1s` 取扱明確化

P1 trades-only scope における source 固定:

- **#12 `burst_notional_vs_30s_traded_notional` の分母 source: raw trades のみ**
  - window: `[second_ts - 30000, second_ts)`（左閉右開）
  - reader: `lib/burst-reducer/raw-trades-notional-reader.mjs`（本番唯一の source）
  - `agg-trades-reader.mjs` は **deprecated。P1 から除去済み**。import/reference 禁止
- **agg fixture（agg-trades に依存する fixture）は P1 から除去**
  - 代わりに raw trades `[s-30000, s)` の nonzero / zero / missing / hash mismatch fixture を使用
- P1 で `#15-#22`（研究列）は `0` 出力（P1 placeholder 由来）
  - **P4 まで real value を emit しない**（偽統計の出力防止）
  - quality 列に `phase: "P1_placeholder"` を記録
- P1 で `#13 burst_notional_vs_top_depth`、`#14 burst_mid_move_bps_1s`（book 依存列）は P4 まで not emitted（#13=`null`、#14=`0` の P1 契約を維持）
  - quality 列に `phase: "P1_book_null"` / `phase: "P1_book_zero"` を記録

---

## 13. out-of-scope

本書の明確な out-of-scope:

- Receiver への feature 圧縮責務の再導入
- raw input schema の再設計
- Parquet / DuckDB / DB 格納方式の採用
- multi-writer / distributed writer
- market 横断 join の一般化
- online query API 設計
- 自動 cleanup 実装
- retention 日数の最終決定
- 圧縮 format の最終決定
- analytics / dashboard UX の設計
- 22 特徴量契約の破壊的変更

---

## 14. 本書で固定する推奨デフォルト

実装前提としての推奨デフォルト:

- `features_1s` を canonical dataset とする
- v1 は trades-only で始める
- 今回 scope は `features_30s` までを主対象とする
- `features_5min` は summary 別 dataset とする（正準名: `features_5min`。`summary_5min` 表記不使用）
- live は `finalized-through` inventory を使う
- backfill は frozen inventory を使う
- cleanup は初期手動
- hot format は JSONL block shard
- 圧縮方式 / retention 日数は後決め
- 既存 22 特徴量契約を壊さない
- on-disk JSON checkpoint: warn=256 KiB, hard fail=1 MiB（P0 受入条件）
- lock identity = `absolute outputRoot + schema_version + market`。全モード同一 namespace。stale 判定は OS `flock` に委譲
- `agg-trades-reader.mjs` は deprecated。P1 から除去済み
- #12 source は raw trades `[s-30000,s)` only
- `指定-2026-07-09-burst-features.md` §6.2 は reference draft。本書 §6.6 が P2 正本

## 15. 決定事項一覧

### 15.1 今決める事項（本書で固定済み。要ユーザー承認）

以下の事項は本書で固定した。実装着手前にユーザー承認を必須とする。

| # | 事項 | 決定 | 根拠/影響 |
|---|---|---|---|
| 1 | P0 checkpoint bounded 制限 | warn=256 KiB, hard fail=1 MiB on-disk JSON | 測定可能な P0 完了条件。block 数比例の抑制 |
| 2 | legacy 500MB checkpoint 移行手順 | 本番停止→backup隔離→整合検証→deterministic rebuild。in-place 変換禁止 | データ整合性保証。restart byte-identical 検証前提 |
| 3 | lock identity 形式 | `absolute outputRoot + schema_version + market` | single-writer 強制。live/backfill/manual/cron 同一 namespace |
| 4 | lock 取得失敗・stale 判定 | 取得失敗=blocked/no-write。stale 判定=OS `flock` 委譲。read-only inspection は lock 不要 | 自前 stale 判定の誤り防止 |
| 5 | `features_5min` 正準名 | 本 pipeline では `features_5min` に統一。`summary_5min` 表記不使用 | naming 揺れ防止。§4.4 参照 |
| 6 | 5min 詳細指標延期 | z-score, percentile, spot-perp divergence, regime 等は P3 entry まで延期 | 30s 意味確定前に 5min 詳細を前倒ししない |
| 7 | P2 `features_30s` operator matrix | 本書 §6.6 が承認済み正本。既存 spec §6.2 は reference draft | 列ごと operator の実装根拠。overlap/direct 別名列共存 |
| 8 | #12 30s 版演算子 | `recompute`。`burst_notional_sum_30s / traded_notional_30s`（raw trades から分母再計算） | §6.6 C 参照 |
| 9 | #12 P1 source 固定 | raw trades `[s-30000,s)` only。`agg-trades-reader.mjs` deprecated | 単一 source に正準化。P1 fixture から agg fixture 除去 |
| 10 | #15-#22 研究列 emit phase | P4 まで物理出力しない。P1-P3 は quality 列に `phase: "P1_placeholder"` を記録 | 偽統計の出力防止 |
| 11 | #13-#14 book 依存列 emit phase | P4 まで物理出力しない。P1-P3 は quality 列に `phase: "P1_book_null"/"P1_book_zero"` を記録 | book state 未実装時の null/zero 契約維持 |
| 12 | canonical dataset | `features_1s`。v1 は trades-only | ADR-009 の方向と整合 |

### 15.2 後決め事項（P3 以降で決定。現時点では実装着手不要）

以下の事項は後続 phase で決定する。P2 実装着手の前提ではない。

| # | 事項 | 現状 | 決定予定 phase |
|---|---|---|---|
| 1 | `features_5min` summary 指標セット最小構成 | 未定。詳細指標は P3 entry で設計 | P3 |
| 2 | book / liquidation 有効化 phase | P4 予定 | P4（P3 完了後） |
| 3 | quality schema 追加項目（5min 用） | 未定 | P3 |
| 4 | live `finalized-through` の供給元 | `finalized-through` を推奨 | P0（実装時） |
| 5 | backfill inventory 最小 schema | 未定 | P0（実装時） |
| 6 | retention 日数 | 後決め。初期削除禁止 | P5 |
| 7 | 将来の圧縮 format（Parquet 等） | JSONL block shard 固定。format 変更は後決め | P5 |
| 8 | hot format 最終決定 | JSONL block shard を推奨デフォルト | 後決め（長期運用後） |
| 9 | 圧縮方式（gzip/zstd/none） | 後決め | P5 |

---

## 16. 受入れ観点

本設計が満たすべき受入れ観点:

- Receiver との責務境界が明確
- 1s / 30s / 5min の層責務が分離されている
- direct 集計と 1s rollup の混同が防止されている
- watermark / empty / missing が定義済み
- restart / idempotency / single-writer が前提化されている
- quality / null / zero の意味が固定されている
- retention 初期削除禁止が明記されている
- observability の最低要件がある
- P0 として checkpoint bounded 化が最優先になっている
- 明確な out-of-scope がある

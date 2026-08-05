# Feature Compression Pipeline 実装計画書

**文書 ID:** plan-2026-07-10-feature-compression-pipeline
**日付:** 2026-07-10
**対象リポジトリ:** `agg-btc-receiver`（`/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver`）
**文書種別:** docs-only planning artifact
**対応設計:** `docs/specs/design-2026-07-10-feature-compression-pipeline.md`
**関連文書:**
- `docs/specs/design-2026-07-10-burst-reducer.md`
- `docs/specs/plan-2026-07-10-burst-reducer.md`
- `docs/specs/plan-2026-07-10-burst-reducer-p0-safety.md`
- `docs/specs/plan-2026-07-10-burst-reducer-remediation.md`
- `docs/specs/specify-2026-07-09-burst-features.md`
- `docs/decisions/adr-009-burst-feature-spec-v1.md`

> コード変更禁止。本文書は実装順・決定事項・検証観点を固定するための計画書である。

---

## 1. 目的

Receiver 後段の feature compression pipeline を、既存契約を壊さず段階導入する。
最初に直すべきものは throughput ではなく correctness であり、特に P0 の checkpoint state bounded 化を先行させる。

---

## 2. 絶対ルール

1. コード変更禁止。今回の成果物は docs のみ。
2. 既存 22 特徴量契約を壊さない。
3. Receiver の責務は raw-only のまま維持する。
4. same market は single-writer を守る。
5. overlap-based 1s sum を direct market total に使わない。
6. empty と missing を混同しない。
7. 初期 cleanup は手動。削除自動化は後回し。
8. 95 点レビューゲート未達の phase は次に進めない。

---

## 3. 推奨デフォルト（本計画の前提）

- `features_1s` を canonical とする
- v1 は trades-only
- 今回の主 scope は `features_30s`
- `features_5min` は summary 別 dataset（正準名: `features_5min`。`summary_5min` 不使用）
- live = `finalized-through` inventory
- backfill = frozen inventory
- cleanup 初期手動
- hot format = JSONL block shard
- 圧縮方式 / retention 日数は後決め
- 既存 22 特徴量契約を壊さない
- on-disk JSON checkpoint: warn=256 KiB, hard fail=1 MiB
- lock identity = `absolute outputRoot + schema_version + market`。全モード同一 namespace。stale 判定=OS `flock` 委譲
- `agg-trades-reader.mjs` deprecated。#12 source = raw trades `[s-30000,s)` only
- 既存 spec §6.2 は reference draft。設計書 §6.6 が P2 正本

---

## 4. 実装順

実装順は固定とする。

1. P0 checkpoint 修復
2. P1 `features_1s` canonical 安定化
3. P2 `features_30s` compression
4. P3 `features_5min` summary
5. P4 book / liquidation
6. P5 retention

理由:
- まず restart correctness と state boundedness を直す
- 次に canonical layer を正準化する
- その後に圧縮層と summary 層へ進む
- 付加データや削除は最後

---

## 5. フェーズ別計画

### P0. checkpoint 修復

目的:
- checkpoint state を bounded 化する
- restart / recovery / idempotency の破綻を先に塞ぐ
- same market single-writer を強制する

**現状実測値（2026-07-10）:**
| market | on-disk bytes | closedBursts | 状態 |
|---|---|---|---|
| `binance_perp` | 533,623,819 | 98,730 | FULL, 肥大 |
| `binance_spot` | 536,296,776 | 59,633 | FULL, 肥大 |
| `bybit_perp` | 536,672,479 | 46,965 | FULL, 肥大 |

根因: `serializeMinimalBurstState` 実装済みだが未使用。closedBursts 全履歴が checkpoint に含まれる。

**on-disk JSON 制限（推奨デフォルト/受入条件）:**
- `warn`: 256 KiB
- `hard fail`: 1 MiB（P0 完了条件）
- テスト: synthetic 100,000 closedBursts fixture で checkpoint <= 1 MiB（block 数に比例しない証明）

**legacy 500MB checkpoint 移行手順:**
1. 本番停止（全 market）
2. 現行 checkpoint を backup 隔離
3. manifest / final shard 整合検証
4. deterministic rebuild で新 checkpoint 生成（`serializeMinimalBurstState` 使用）
5. in-place 変換禁止
6. 整合不明 → quarantine + 停止
7. 再起動後 first commit byte-identical 検証

変更候補ファイル:
- `lib/burst-reducer/burst-state-codec.mjs`
- `lib/burst-reducer/pipeline.mjs`
- `lib/burst-reducer/pending-block-manager.mjs`
- `lib/burst-reducer/output-committer.mjs`
- `lib/burst-reducer/manifest-manager.mjs`
- `lib/burst-reducer/block-scanner.mjs`
- `scripts/reduce-burst-v1.mjs`
- 必要なら `scripts/` 配下の lock helper
- `test/burst-reducer/burst-state-codec.test.mjs`
- `test/burst-reducer/cursor-restart.test.mjs`
- `test/burst-reducer/horizon.test.mjs`
- `test/burst-reducer/output-committer.test.mjs`
- `test/burst-reducer/manifest-manager.test.mjs`
- `test/burst-reducer/pipeline.test.mjs`

TDD fixture:
- cross-block burst restart fixture
- intent residue fixture
- absent next block / explicit empty next block fixture
- huge closed-bursts synthetic fixture
- duplicate writer contention fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/burst-reducer/burst-state-codec.test.mjs
node --test test/burst-reducer/cursor-restart.test.mjs
node --test test/burst-reducer/horizon.test.mjs
node --test test/burst-reducer/output-committer.test.mjs
node --test test/burst-reducer/manifest-manager.test.mjs
node --test test/burst-reducer/pipeline.test.mjs
```

停止条件:
- checkpoint サイズが block 数に比例して増える
- checkpoint が 1 MiB hard fail を超過する
- restart 後に byte-identical output を保てない
- pending block を lost する
- intent recovery が duplicate commit を生む
- live horizon なしで EOF flush する

95 点レビューゲート:
- checkpoint <= 1 MiB（hard fail）で bounded。100,000 closedBursts fixture で証明
- same-market concurrent writer が防止される（lock identity 検証）
- not-yet-arrived と verified-missing が分離される
- restart / kill / resume で composite key 重複がない
- manifest / final shard / checkpoint の整合が検証できる
- legacy 500MB checkpoint からの移行手順が文書化・検証済み
- 再起動後 first commit が byte-identical

### P1. `features_1s` canonical 安定化

目的:
- `features_1s` を canonical dataset として固定する
- 既存 22 特徴量契約を保ったまま semantics を明文化する
- 30s compression の入力品質を保証する

変更候補ファイル:
- `lib/burst-reducer/schema.mjs`
- `lib/burst-reducer/feature-computer-1s.mjs`
- `lib/burst-reducer/burst-detector.mjs`
- `lib/burst-reducer/input-validator.mjs`
- `lib/burst-reducer/raw-trades-notional-reader.mjs`
- `test/burst-reducer/schema.test.mjs`
- `test/burst-reducer/feature-computer-1s.test.mjs`
- `test/burst-reducer/burst-detector.test.mjs`
- `test/burst-reducer/input-validator.test.mjs`
- `test/burst-reducer/raw-trades-notional-reader.test.mjs`
- `test/burst-reducer/golden.test.mjs`

> **P1 から除去済み: `lib/burst-reducer/agg-trades-reader.mjs`（deprecated）。** import/reference 禁止。代わりに `raw-trades-notional-reader.mjs` が #12 の唯一の source。

TDD fixture:
- trades-only canonical fixture
- all-zero empty block fixture
- null-vs-zero contract fixture
- overlap boundary fixture
- raw trades #12 denominator nonzero / zero / missing / hash mismatch fixture（agg fixture は除去）
- existing 22-column compatibility fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/burst-reducer/schema.test.mjs
node --test test/burst-reducer/feature-computer-1s.test.mjs
node --test test/burst-reducer/burst-detector.test.mjs
node --test test/burst-reducer/input-validator.test.mjs
node --test test/burst-reducer/raw-trades-notional-reader.test.mjs
node --test test/burst-reducer/golden.test.mjs
```

停止条件:
- 22 列契約が崩れる
- null と zero の意味が壊れる
- overlap semantics が fixture と不一致
- empty block で 30 row を出せない
- #12 分母が raw trades `[s-30000,s)` 以外の source を参照している
- #12 分母 zero / missing / hash mismatch の扱いが未固定

95 点レビューゲート:
- `features_1s` が canonical として説明可能
- 既存 consumers が破壊されない
- empty / null / zero / quality の契約がテストで固定
- overlap exposure と direct total の違いが docs とテストで一貫
- #12 が単一 source（raw trades）に正準化されている
- `agg-trades-reader.mjs` への参照が除去されている
- #15-#22 研究列は P1 placeholder `0`、#13-#14 book 列は P1 契約値のままである

### P2. `features_30s` compression

目的:
- `features_1s` から 30s compressed dataset を追加する
- 列ごと演算子 matrix を固定し、direct と rollup を分離する
- 今回スコープの中心として 30s を成立させる

変更候補ファイル:
- `lib/burst-reducer/schema.mjs`
- `lib/burst-reducer/feature-computer-30s.mjs`（新規候補）
- `lib/burst-reducer/pipeline.mjs`
- `lib/burst-reducer/output-committer.mjs`
- `lib/burst-reducer/manifest-manager.mjs`
- `scripts/reduce-burst-v1.mjs`
- `test/burst-reducer/feature-computer-30s.test.mjs`（新規候補）
- `test/burst-reducer/pipeline.test.mjs`
- `test/burst-reducer/golden.test.mjs`

TDD fixture:
- 30s 全秒 coverage fixture
- overlap-heavy burst fixture
- direct-vs-rollup divergence fixture
- weighted denominator shift fixture
- recompute-required ratio fixture
- quality propagation fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/burst-reducer/feature-computer-30s.test.mjs
node --test test/burst-reducer/pipeline.test.mjs
node --test test/burst-reducer/golden.test.mjs
```

停止条件:
- 列ごとの演算子 matrix 未決定
- overlap-based 1s sum を direct market total に流用している
- direct と recompute の区別が消える
- 30s 出力の quality provenance が欠落する

95 点レビューゲート:
- 30s の全列が operator 付きで説明できる
- direct / recompute / rollup の根拠がある
- 30s 行の意味が analytics で誤読されない
- overlap exposure 指標と direct total 指標が別列で共存する

### P3. `features_5min` summary

目的:
- `features_5min` を summary 別 dataset として追加する
- 30s の意味を保ったまま 5min 集約へ上げる
- 詳細指標（z-score, percentile, spot-perp divergence, regime 等）は P3 entry で設計・実装する。P2 段階では前倒ししない

変更候補ファイル:
- `lib/burst-reducer/schema.mjs`
- `lib/burst-reducer/feature-computer-5min.mjs`（新規候補）
- `lib/burst-reducer/pipeline.mjs`
- `lib/burst-reducer/output-committer.mjs`
- `lib/burst-reducer/manifest-manager.mjs`
- `test/burst-reducer/feature-computer-5min.test.mjs`（新規候補）
- `test/burst-reducer/pipeline.test.mjs`

TDD fixture:
- 10 x 30s into 1 x 5min fixture
- partial coverage blocked fixture
- summary quality fixture
- 5min direct-vs-rollup divergence fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/burst-reducer/feature-computer-5min.test.mjs
node --test test/burst-reducer/pipeline.test.mjs
```

停止条件:
- 5min summary 指標セットが未固定
- 30s と 5min の責務境界が曖昧
- 5min finalize 条件が horizon 契約と矛盾

95 点レビューゲート:
- 5min が別 dataset として独立説明できる
- source coverage と quality が追跡できる
- 5min が 30s の雑な再ラベルではない
- 正準 dataset 名が `features_5min` で統一されている（`summary_5min` 不使用）

### P4. book / liquidation

目的:
- trades-only v1 に book / liquidation を段階導入する
- null placeholder を意味ある列へ進化させる
- **P4 で #13 `burst_notional_vs_top_depth`、#14 `burst_mid_move_bps_1s` を実数出力に切り替える**
- **P4 で #15-#22 研究列の実数出力を開始する**（P1-P3 の quality ラベル `P1_placeholder`/`P1_book_null`/`P1_book_zero` を解除）

変更候補ファイル:
- `lib/replay-book-state.mjs`
- `lib/burst-reducer/schema.mjs`
- `lib/burst-reducer/feature-computer-1s.mjs`
- `lib/burst-reducer/feature-computer-30s.mjs`（導入後）
- `lib/burst-reducer/feature-computer-5min.mjs`（導入後）
- `lib/burst-reducer/pipeline.mjs`
- `test/replay-book-state.test.mjs`
- `test/burst-reducer/feature-computer-1s.test.mjs`
- `test/burst-reducer/golden.test.mjs`

TDD fixture:
- seeded book fixture
- unseeded book fixture
- liquidation present / absent fixture
- top-depth null/known fixture
- book quality propagation fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/replay-book-state.test.mjs
node --test test/burst-reducer/feature-computer-1s.test.mjs
node --test test/burst-reducer/golden.test.mjs
```

停止条件:
- trades-only 契約を壊す
- null placeholder 解除の意味が未固定
- book seed 不十分なのに値を出す

95 点レビューゲート:
- null/known の移行条件が明文化
- book / liquidation が optional dependency として扱える
- trades-only fallback が壊れない

### P5. retention

目的:
- cleanup / retention を安全に導入する
- 正しさ確立後に保存コストを下げる

変更候補ファイル:
- `scripts/cleanup-raw.mjs`
- `scripts/cleanup-jsonl.mjs`
- `scripts/cleanup.mjs`
- retention docs / runbook
- 必要なら manifest inventory 周辺
- 対応テストまたは dry-run 検証スクリプト

TDD fixture:
- dry-run cleanup fixture
- keep-last-N-days fixture
- dependency-aware cleanup fixture
- quarantine-preserve fixture

検証コマンド候補:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node scripts/cleanup.mjs --help
node scripts/cleanup-jsonl.mjs --help
node scripts/cleanup-raw.mjs --help
```

停止条件:
- raw 再計算性を壊す
- derived と manifest の整合が失われる
- dry-run なしで削除を始める

95 点レビューゲート:
- cleanup が手動 / dry-run / rollback 可能
- raw / derived / manifest 依存順が壊れない
- retention 日数が運用前提と一致する

---

## 6. 30s 列ごとの演算子 matrix（実装前の必須決定）

> **P2 正本は `design-2026-07-10-feature-compression-pipeline.md` §6.6 に定義済み。**
> 以下は参照用の抜粋であり、完全な matrix は設計書を参照すること。

P2 に着手する前に、30s の各列が次のどれかを持つ matrix を固定すること。設計書 §6.6 で固定済みの operator 分類:

- `rollup`: `features_1s` の 30 行から集約（mean, max, sum, p95）
- `direct`: 30s window の実体 burst から直接計算（unique count, sum, max）
- `recompute`: raw/canonical/auxiliary から分子分母を再計算

overlap exposure 列と direct entity 列は**別名列で共存**する（設計書 §6.6 A/B 参照）。

最低限の決定対象（設計書 §6.6 で固定済み）:
- burst count 系: `burst_count_mean_30s`/`burst_count_max_30s`（rollup）、`burst_unique_count_30s`（direct）
- total burst notional 系: `burst_notional_overlap_sum_30s`/`max`/`p95`（rollup）、`burst_notional_sum_30s`/`max`（direct）
- buy/sell split 系: `buy_burst_notional_sum_30s`/`sell_burst_notional_sum_30s`（direct）
- ratio 系: `burst_imbalance_ratio_30s`（recompute: `(buy-sell)/(buy+sell)`）、`largest_burst_share_30s`（recompute: `max/total`）
- same-price / multilevel 系: `same_price_burst_unique_count_30s`/`multilevel_burst_unique_count_30s`（direct）
- #12 30s 版: `burst_notional_vs_traded_notional_30s`（recompute: `burst_notional_sum_30s / traded_notional_30s`）
- #13-#14 book 依存列: P4 まで not emitted（設計書 §6.6 D 参照）
- #15-#22 研究列: P4 まで not emitted。P1-P3 では quality 列に `phase: "P1_placeholder"` を記録

明示ルール:
- overlap-based 1s sum を direct market total に使わない
- `total_burst_notional_1s` の 30s 単純和は overlap exposure 指標としてのみ使用可能（`burst_notional_overlap_sum_30s`）
- direct market total / direct burst total / traded total は direct または recompute で別列にする

---

## 7. ユーザーが決める事項

> **本節の決定事項は設計書 `design-2026-07-10-feature-compression-pipeline.md` §15 と同期している。**
> 設計書が正本。矛盾時は設計書を優先。

### 7.1 今決める事項（設計書 §15.1 で固定済み。要ユーザー承認）

| # | 事項 | 決定 | 根拠 |
|---|---|---|---|
| 1 | P0 checkpoint bounded 制限 | warn=256 KiB, hard fail=1 MiB on-disk JSON | 測定可能な P0 完了条件 |
| 2 | legacy 500MB checkpoint 移行 | 本番停止→backup隔離→整合検証→deterministic rebuild。in-place 変換禁止 | restart byte-identical 前提 |
| 3 | lock identity | `absolute outputRoot + schema_version + market`。全モード同一 namespace | single-writer 強制 |
| 4 | lock 取得失敗 | blocked/no-write。stale 判定=OS `flock` 委譲。read-only inspection は lock 不要 | 自前 stale 判定の誤り防止 |
| 5 | `features_5min` 正準名 | `features_5min` に統一。`summary_5min` 不使用 | naming 揺れ防止 |
| 6 | 5min 詳細指標 | P3 entry まで延期 | 30s 意味確定前の前倒し禁止 |
| 7 | P2 operator matrix | 設計書 §6.6 が正本。既存 spec §6.2 は reference draft | 列ごと operator の実装根拠 |
| 8 | #12 30s 版 | `recompute`。`burst_notional_sum_30s / traded_notional_30s` | raw trades から分母再計算 |
| 9 | #12 P1 source | raw trades `[s-30000,s)` only。`agg-trades-reader.mjs` deprecated | 単一 source 正準化 |
| 10 | #15-#22 研究列 | P4 まで not emitted。quality に `phase: "P1_placeholder"` | 偽統計防止 |
| 11 | #13-#14 book 列 | P4 まで not emitted。quality に `phase: "P1_book_null"/"P1_book_zero"` | book state 未実装時の契約維持 |
| 12 | canonical dataset | `features_1s`。v1 は trades-only | ADR-009 と整合 |

### 7.2 後決め事項（P3 以降で決定。P2 実装着手の前提ではない）

| # | 事項 | 現状 | 決定予定 phase |
|---|---|---|---|
| 1 | `features_5min` summary 指標セット | 未定。P3 entry で設計 | P3 |
| 2 | book / liquidation 有効化 | P4 予定 | P4 |
| 3 | quality schema 追加項目（5min 用） | 未定 | P3 |
| 4 | live `finalized-through` 供給元 | `finalized-through` 推奨 | P0（実装時） |
| 5 | backfill inventory 最小 schema | 未定 | P0（実装時） |
| 6 | retention 日数 | 後決め。初期削除禁止 | P5 |
| 7 | 圧縮 format（Parquet 等） | JSONL block shard 固定 | P5 |
| 8 | hot format 最終決定 | JSONL block shard 推奨 | 後決め |
| 9 | 圧縮方式（gzip/zstd/none） | 後決め | P5 |

---

## 8. フェーズ横断の検証観点

全 phase 共通:
- byte-identical restart
- same input => same output
- no duplicate composite key
- empty と missing の誤判定なし
- quality provenance 追跡可能
- 既存 22 特徴量契約非破壊

推奨横断コマンド:
```bash
cd /home/weed420/dev/github/like-kradness-2025/agg-btc-receiver
node --test test/burst-reducer/*.test.mjs
```

注記:
- 実際に wildcard が shell 展開しない環境では個別 test 指定に分解する。
- 30s / 5min 導入後は phase 専用 test を追加して gate を更新する。

---

## 9. 明確な停止線

次のいずれかが残るなら P2 以降へ進まない。

1. checkpoint が bounded でない（> 1 MiB on disk）
2. restart で byte-identical が取れない
3. `features_1s` canonical 契約が曖昧
4. 30s operator matrix 未決定（設計書 §6.6 未承認）
5. overlap-based 1s sum を direct total に使っている
6. empty と missing が未分離
7. quality 伝播が未定義
8. 95 点レビューゲート未達
9. `agg-trades-reader.mjs` への参照が残っている
10. #12 が raw trades 以外の source を参照している

---

## 10. 95 点レビューゲートの採点観点

各 phase の 95 点レビューでは、少なくとも次を採点対象にする。

- correctness
- replay / restart determinism
- idempotency
- single-writer safety
- null / zero / quality contract
- direct vs rollup semantics
- observability
- scope discipline

減点対象の具体例:
- 仕様未固定のまま実装で意味を埋める
- overlap exposure を direct total と誤称する
- cleanup を早く入れすぎる
- userspace assumption で horizon を推定する
- 22 特徴量契約を暗黙に変える

---

## 11. 実装着手前チェックリスト

- [ ] P0 bounded checkpoint 契約をユーザー承認（warn=256 KiB, hard fail=1 MiB）
- [ ] P0 legacy 500MB checkpoint 移行手順をユーザー承認
- [ ] P0 lock identity（`absolute outputRoot + schema_version + market`）をユーザー承認
- [ ] P0 stale 判定=OS `flock` 委譲、read-only inspection lock 不要をユーザー承認
- [ ] `features_1s` canonical 方針をユーザー承認
- [ ] P2 30s operator matrix（設計書 §6.6）をユーザー承認
- [ ] 既存 spec §6.2 は reference draft、設計書 §6.6 が P2 正本であることをユーザー承認
- [ ] `features_5min` 正準名統一（`summary_5min` 不使用）をユーザー承認
- [ ] 5min 詳細指標を P3 entry まで延期する方針をユーザー承認
- [ ] #12 P1 source = raw trades `[s-30000,s)` only、`agg-trades-reader.mjs` deprecated をユーザー承認
- [ ] #15-#22 研究列 P4 まで not emitted をユーザー承認
- [ ] #13-#14 book 列 P4 まで not emitted をユーザー承認
- [ ] live `finalized-through` の供給元を固定
- [ ] frozen inventory の最小 schema を固定
- [ ] cleanup 初期手動を承認
- [ ] 既存 22 特徴量契約非破壊を承認

---

## 12. この計画書の結論

最初にやるべきことは P0 checkpoint 修復であり、30s 圧縮そのものではない。
その次に `features_1s` を canonical として安定化し、列ごとの operator matrix を固定してから `features_30s` へ進む。
5min・book/liquidation・retention は、その後に段階導入するのが最も安全である。

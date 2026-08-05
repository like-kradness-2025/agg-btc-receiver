# TFP 再計画 独立設計レビュー — Phase A/B/C 再分割案

- **文書 ID:** `review-2026-07-12-tfp-replan-phase-abc`
- **レビュー種別:** 独立設計レビュー（sounding-board / architecture reviewer）
- **対象 repo:** `/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver`（branch `v2`）
- **レビュー日:** 2026-07-12
- **コード変更:** 禁止（本レビューは読取のみ）
- **参照正本:**
  - `docs/specs/specify-2026-07-11-tradeflow-pipeline.md`
  - `docs/specs/plan-2026-07-11-tradeflow-pipeline-phase-a.md`
  - `docs/specs/specify-2026-07-11-tfp-book-contract-p0.md`
  - `docs/specs/plan-2026-07-10-receiver-raw-only.md`
  - `docs/specs/plan-2026-07-11-tfp-p0-1-horizon-proof.md`

---

## 0. 凡例

- **[事実]**: 実ファイル・実コード・実 git log から確認した記述
- **[判断]**: レビューアの分析・推論・設計意見
- **[未確認]**: コード・テストを読んだが実行確認していない／親 agent の検証が必要な項目

---

## 1. 現状の事実整理

### 1.1 Git 状態（2026-07-12）

**[事実]** `git status` on branch `v2`:

- **Modified（unstaged）**: `lib/burst-reducer/feature-computer-1s.mjs`
- **Untracked**: `lib/burst-reducer/rollup.mjs`
- **Staged changes**: なし
- **最新 5 commits**:
  ```
  70ad546 fix: P0-3 intent gen inequality (>= → >) + Fixture 21 test
  e660761 feat: P0-3 crash recovery test coverage (Fixtures 16-20)
  996a9c2 feat: P0-2 checkpoint boundedness + fix package.json test glob
  053e2ae feat: P0-1 horizon proof / frozen inventory validation
  3194c77 feat: PDD Phase A (TFP safety) + Receiver raw-only + P0-0 book contract + P0-1 horizon proof design
  ```

### 1.2 Receiver raw-only 状態

**[事実]** `lib/orderflow-worker.mjs` は既に raw-only 契約を満たしている:

- `rawTradeRotationWriters`（trades）、`bookUpdateRotationWriters`（book_updates）、`liquidationRotationWriters`（liquidations）の 3 writer のみ
- `TradeAggregator`、`agg_trades`、`snapshots`、`book_snapshots` の import/instance は存在しない
- `doFlushCycle()` / flush timer は存在しない
- shutdown は raw 3 writer の `finalize()` のみ
- startup recovery は connect 前に実行（`prepareMarket` → `startupRecovery` → `connectMarket`）
- `orderflow_monitor.mjs` に `DerivativesHelper`/`MarketDataCollector` は存在しない（aux collector 分離済み）
- `test/orderflow-worker-raw-only.test.mjs` が 11 tests 存在

**[判断]** Receiver raw-only 化は **完了** している。TFP 再計画で Receiver に追加作業は不要。

### 1.3 Market 単位 single-writer 状態

**[事実]**

- `test/burst-reducer/lock.test.mjs` が 3 tests 存在
- `lib/burst-reducer/pipeline.mjs` の `runPipeline()` は `reconcileMarketState()` を呼び出し、checkpoint/manifest の整合性を検証してから処理開始
- `lib/burst-reducer/recovery.mjs` は `reconcileMarketState()` を export し、intent/committed レコードの状態整合を検証
- `specify-2026-07-11-tradeflow-pipeline.md` §4.1: `flock -x -n <output_root>/locks/<market>.lock` 契約が明記
- `plan-2026-07-11-tradeflow-pipeline-phase-a.md` Task 5: lock の回帰試験

**[未確認]** 以下の実装有無は未確認（コード検索結果に `flock` を含むファイルがヒットしなかった）:

- `flock -x -n` による lock 獲得スクリプトが実ファイルとして存在するか
- lock 獲得失敗時の structured skip 出力が実装されているか

**[判断]** `reconcileMarketState` による checkpoint/manifest 整合性検査は single-writer の「データ整合性 guard」として機能しているが、プロセス間の同時実行排除（`flock`）の実装状態は **親 agent の検証が必要**。`flock` が未実装なら Phase A の未達項目となる。

### 1.4 Checkpoint / Recovery 状態

**[事実]**

- `lib/burst-reducer/recovery.mjs`（275 行）: `reconcileMarketState()` が intent レコードの staged→final rename、hash 検証、generation 整合性チェックを実装
- `lib/burst-reducer/manifest-manager.mjs`（224 行）: `loadManifest`/`loadCheckpoint` が空ファイル・不正 JSON 時に `MANIFEST_CORRUPT`/`CHECKPOINT_CORRUPT` sentinel を返し、元ファイルを `.bak.<timestamp>` に退避
- `lib/burst-reducer/schema.mjs`: `CHECKPOINT_SIZE_WARN=256KiB`, `CHECKPOINT_SIZE_HARD_LIMIT=1MiB`
- `lib/burst-reducer/output-committer.mjs`: `commitFinalizedBlock()` が `nextGeneration` を単調増加、checkpoint に `getMinimalBurstState()`（closedBursts 非保存）を使用
- `test/burst-reducer/recovery.test.mjs`: 21 tests
- `test/burst-reducer/checkpoint-size.test.mjs`: 5 tests
- `test/burst-reducer/manifest-manager.test.mjs`: 13 tests
- `test/burst-reducer/cursor-restart.test.mjs`: 11 tests

**[判断]** Checkpoint/recovery の基本 safety（fail-closed, corrupt sentinel, minimal state, bounded size, monotonic generation, intent reconcile）は **充足している**。残課題は:

1. `flock` によるプロセス間排他（上述）
2. book_updates kind の checkpoint/recovery 対応（現在 `kind === 'trades'` のみ）

### 1.5 Book 機能 状態

**[事実]**

- `docs/specs/specify-2026-07-11-tfp-book-contract-p0.md`（403 行）: 極めて詳細な P0-0 契約。envelope v1, replay state machine, sequence gap, anchor `strict <`, board MVP 候補定義、状態遷移表、frozen inventory kind 分離、ASSUMED_EMPTY_GAP の trade-only 限定を規定
- `test/tfp-book-contract-fixture.test.mjs`（1314 行、24 tests）: **独立 verifier**。production コードを import せず、fixture から BookStateMachine を自前実装し、全状態遷移・commit/cursor/quarantine 判定を検証
- `lib/replay-book-state.mjs`（137 行）: `replayBestBookState()` が sorted book events から `bookAtTime(ts)` lookup 関数を生成。strict `< anchor` を実装済み
- `test/replay-book-state.test.mjs`: 6 tests

**Working tree 変更（`feature-computer-1s.mjs` diff）**:

- `computeFeatures1s()` のシグネチャに `bookSnapshotAt`、`prevMid` パラメータ追加
- 返り値が `Object[]` → `{ rows: Object[], nextPrevMid: number|null }` に変更
- `#13 burst_notional_vs_top_depth` と `#14 burst_mid_move_bps_1s` の計算ロジック追加
- `createBaseRow`（P1 contract: #13=null, #14=0）の後でこれらのフィールドを上書き

**[判断]**

1. **P0-0 契約の質**: Book contract 仕様は密度が高く、独立 verifier テストも充実している。この契約を production pipeline に組み込む前に、仕様→実装のギャップが 3 点ある。

2. **feature-computer-1s.mjs の diff は scope 混入**: この diff は以下の理由で問題がある:
   - P0-0 §9 は「#13 は現 P1 row では `null`、#14 は `0`。既存位置を board MVP 実値で上書きしない」と明記している
   - diff は既存 `#13 burst_notional_vs_top_depth` / `#14 burst_mid_move_bps_1s` カラムに直接 book 値を書き込んでおり、契約違反
   - 正しい実装は P0-0 §10 に従い、別名の候補列（例: `board_top_depth_ratio`, `board_mid_move_bps`）として出力すべき
   - 返り値型の変更（`Object[]` → `{rows, nextPrevMid}`）は既存呼び出し元（pipeline.mjs 内の `computeFeatures1s()` 呼び出し全 3 箇所）を破壊する。pipeline.mjs 側の対応 diff は存在しない

3. **pipeline.mjs は book 非対応**: 現行 `pipeline.mjs` の全 `computeFeatures1s()` 呼び出し（L427, L538, L668, および gap handler 内）は `bookSnapshotAt` を渡していない。つまり **book 特徴量は production pipeline では計算されない**。

4. **rollup.mjs も book 前提**: 後述の rollup.mjs は `FEATURE_1S_AGG` で `burst_notional_vs_top_depth`（ratio_mean）と `burst_mid_move_bps_1s`（mean）を集約対象に含めている。つまり rollup 実装も book 特徴量の存在を前提にしている。

### 1.6 Rollup 機能 状態

**[事実]**

- `lib/burst-reducer/rollup.mjs`（103 行）: **untracked 新規ファイル**
  - `aggregateWindow(rows, targetTs, windowSize)`: 指定 window サイズで 1s rows を集約
  - `aggregate30s(features1s)`: 30 行→1 行（window=30）
  - `aggregate5min(features1s)`: 300 行→1 行（window=300）
  - `FEATURE_1S_AGG`: 22 特徴量の集約 operator 定義。#13 を ratio_mean、#14 を mean で集約
  - `AGG_OPS`: sum, mean, max, min, last, ratio_mean
- **pipeline.mjs 内に rollup 呼び出しは存在しない**（未 wired）
- `specify-2026-07-11-tradeflow-pipeline.md` §2 Rollup stage 定義: 「初期導入では output を要求しない」
- `plan-2026-07-11-tradeflow-pipeline-phase-a.md` §6: 「Phase B: TFP rollup（今回の対象外）」

**[判断]**

- rollup.mjs は untracked で独立しており、production pipeline に影響しない。設計としては `specify` の Rollup stage 定義（§2）と整合している
- ただし `FEATURE_1S_AGG` が #13, #14 を集約対象に含めているのは、book 特徴量の存在を前提にしており、book が未実装の現状では部分的に dead code
- `schema.mjs` には `FEATURES_30S_DIR`、`FEATURES_5MIN_DIR` が定義済み。output-committer はこれらを使っていない

### 1.7 テスト構成

**[事実]** 全 573 tests（`it()` declarations）。burst-reducer 関連内訳:

| テストファイル | test 数 | カバレッジ領域 |
|---|---|---|
| `recovery.test.mjs` | 21 | intent reconcile, committed verify, 全 crash point |
| `horizon.test.mjs` | 21 | finalized-through, frozen inventory, kind 別 horizon |
| `raw-trades-notional-reader.test.mjs` | 29 | raw trade lookup, gap, boundary |
| `tfp-book-contract-fixture.test.mjs` | 24 | **独立 verifier** — book state machine, commit/cursor/quarantine |
| `input-validator.test.mjs` | 18 | JSON parse, timestamp validation |
| `manifest-manager.test.mjs` | 13 | corrupt sentinel, atomic write |
| `pipeline.test.mjs` | 12 | 統合パイプライン |
| `burst-detector.test.mjs` | 12 | burst 検出 |
| `cursor-restart.test.mjs` | 11 | checkpoint cursor, E022 |
| `feature-computer-1s.test.mjs` | 9 | 特徴量計算 |
| `golden.test.mjs` | 8 | byte-identical 回帰 |
| `burst-state-codec.test.mjs` | 7 | serialize/restore |
| `checkpoint-size.test.mjs` | 5 | boundedness |
| `schema.test.mjs` | 12 | 定数、createBaseRow |
| `block-scanner.test.mjs` | 4 | scan |
| `lock.test.mjs` | 3 | 同時実行 |
| `output-committer.test.mjs` | 5 | commit atomicity |

**[未確認]** 全 test の PASS/FAIL 状態は未実行のため不明。

---

## 2. スコープ混入分析

### 2.1 混入マップ

```
                        trade-only     book        rollup
                        (Phase A 済)   (未 planning) (Phase B)
                        ─────────────  ────────────  ─────────
pipeline.mjs            ● complete     ○ kind param   ○ 未 wired
                                        only (no      (rollup.mjs
                                        computation)  exists but
                                                      untracked)
                                        
feature-computer-1s.mjs ● complete     ▲ MODIFIED     －
  (committed)                          (book columns
                                        added to #13/#14,
                                        return type
                                        changed)
                                        
feature-computer-1s.mjs                ▲ MODIFIED     －
  (working tree diff)                  (contract
                                        violation:
                                        overwrites P1
                                        placeholders)

rollup.mjs (untracked)   －            ▲ 参照          ● exists
                                       (#13/#14       (not wired)
                                        in AGG defs)
```

記号: ●=充足, ▲=混入/問題あり, －=非該当

### 2.2 feature-computer-1s.mjs diff の具体的問題

**[事実]** diff の変更内容:

1. **返り値型変更**: `Object[]` → `{ rows: Object[], nextPrevMid: number|null }`
2. **新パラメータ追加**: `bookSnapshotAt`, `prevMid`
3. **#13 計算追加**: `burst_notional_vs_top_depth = totalNotional / topDepth` （book 前提）
4. **#14 計算追加**: `burst_mid_move_bps_1s = ((midPrice - latestMid) / latestMid) * 10000` （book 前提）
5. **ベース行への上書き**: `createBaseRow()` が設定した P1 placeholder（#13=null, #14=0）を後から上書き

**[判断]**

1. **P0-0 契約違反**: §9 は「#13 は現 P1 row では `null`、board MVP 実値は既存位置を上書きしない」と明示。diff はこれに違反
2. **破壊的変更**: 返り値型変更により、pipeline.mjs の全呼び出し箇所がコンパイルエラーになる（`nRows` を配列として扱っている L456, L538, L670, L683 すべてが破綻）
3. **責務混入**: `feature-computer-1s.mjs` は trade-only #1-#12 の計算に責務を限定すべき（`specify` §5「現フェーズは trade-only」）。book 値の注入は別 module または pipeline の orchestration 層で行うべき

---

## 3. Phase A/B/C 再分割案

### 3.1 現行 Phase 定義（specify §6）の評価

| 現行 Phase | 仕様記載 | 実装状態 |
|---|---|---|
| Phase A: TFP safety completion | P1-2 bounded retention, crash recovery, manifest 保全, deep clone 削除, lock/recovery/retention/cursor 回帰テスト | 実装概ね完了。`flock` 実装状態と rollup diff 混入が未整理 |
| Phase B: TFP rollup | 30s/5min 集約、同一 worker 内部 stage | untracked rollup.mjs のみ。未 wired |

**[判断]** 現行の Phase A/B 二分は粗すぎる。以下の理由で Phase A/B/C へ三分割を提案する:

1. Phase A のスコープに「TFP safety completion」と「book contract P0-0」と「P0-1 horizon proof」が混在しており、完了条件が曖昧
2. Phase B に rollup と book が暗黙に混入している（feature-computer diff と rollup.mjs の #13/#14 参照）
3. book 機能は単独で仕様密度が高く（403 行の契約 + 1314 行の独立 verifier）、独立 Phase に値する

### 3.2 提案: Phase A / Phase B / Phase C

#### Phase A: TFP Trade-Only Safety Completion（現行の実装修正・完成）

**スコープ**: trade-only pipeline の safety を完成させる。book も rollup も含まない。

**含める項目**:

| # | 項目 | 現状 | 残作業 |
|---|---|---|---|
| A1 | Receiver raw-only | 完了 | なし |
| A2 | Market 単位 single-writer（flock） | lock.test.mjs のみ | `flock -x -n` 実装と cron/backfill への統合 |
| A3 | Checkpoint boundedness | 完了（minimal state, 1MiB hard limit） | なし |
| A4 | Crash recovery（intent reconcile） | recovery.mjs + test 21 件 | なし |
| A5 | Manifest 破損保全 | manifest-manager.mjs corrupt sentinel | なし |
| A6 | Authoritative cursor（E022 skip 拒否） | pipeline.mjs L199-211 | なし |
| A7 | P1-2 bounded retention（closed burst prune） | pipeline.mjs L473, L590 | なし |
| A8 | Horizon proof / frozen inventory（trade-only） | checkFinalizedHorizon + horizon.test.mjs 21 件 | なし |
| A9 | ASSUMED_EMPTY_GAP / ASSUMED_REORDERED_INPUT | pipeline.mjs gap handler | なし |

**Phase A から明示的に除外する項目**:

| 除外項目 | 理由 |
|---|---|
| `feature-computer-1s.mjs` の working tree diff（#13/#14 追加） | P0-0 契約違反。revert する |
| `rollup.mjs` | Phase C へ移動 |
| book adapter / book replay の production wiring | Phase B へ移動 |
| `kind='book_updates'` の feature 計算 | Phase B へ移動 |

**Phase A 完了条件**:

1. 全 burst-reducer テスト PASS（`node --test test/burst-reducer/*.test.mjs`）
2. `npm test` 全 573 tests PASS
3. `flock` lock の実装確認（実ファイル存在 + lock.test.mjs の追加）
4. `feature-computer-1s.mjs` の working tree diff を revert（または book 対応を削除した clean state に reset）
5. `rollup.mjs` を削除または Phase C branch へ隔離
6. checkpoint <= 64KiB、state <= 256KiB の長系列実測
7. checkpoint が trades のみの `kind` で動作することを確認
8. 95 点以上の独立レビュー

#### Phase B: TFP Book Contract P0-0 → Production Wiring

**前提**: Phase A 完了。`feature-computer-1s.mjs` が clean trade-only 状態。

**スコープ**: P0-0 book contract を production pipeline に正しく統合する。rollup は含まない。

**含める項目**:

| # | 項目 | 内容 |
|---|---|---|
| B1 | Adapter: connector depth event → `book_updates_v1` envelope | `event_ts_ms`, `seq`, `prev_seq`, `source`, `schema_version` を canonicalize。現行 connector 出力（`type/bids/asks/ts/seq`）を envelope へ写像 |
| B2 | BookStateMachine production 実装 | P0-0 §5 の replay state を pipeline に組み込み。`replay-book-state.mjs` は既存だが、sequence gap / malformed / crossed book / quarantine は未実装 |
| B3 | Pipeline への book wiring（同一 block 内 trade+book join） | `processBlocks()` で同一 `block_start_ms` の `trades` + `book_updates` を join。book state machine を駆動し、`bookSnapshotAt(secondTs)` を生成 |
| B4 | Board MVP 候補列の追加（別名、上書き禁止） | P0-0 §10 に従い `board_top_depth_ratio`, `board_mid_move_bps`, `board_vs_30s`, `board_vs_depth` を `_quality` または別列として出力。#13=null, #14=0 は維持 |
| B5 | Book 特有の quarantine（sequence gap, crossed book, verified-missing） | P0-0 §13.4 状態遷移表の全状態を実装。ASSUMED_EMPTY_GAP を book に拡張しない |
| B6 | `kind='book_updates'` の checkpoint/recovery 対応 | manifest record に kind 追加。recovery.mjs の kind 対応 |
| B7 | Frozen inventory kind 分離検証 | P0-0 §13.6 の kind 分離を production で検証。trade inventory と book inventory の相互非干渉 |

**Phase B から明示的に除外する項目**:

| 除外項目 | 理由 |
|---|---|
| Rollup（30s/5min 集約） | Phase C へ |
| #13/#14 の実値化（既存列上書き） | P0-0 契約違反。board candidate は別名列で出す |
| 既存 #15-#22 の実値化 | P1 placeholder 維持 |

**Phase B 完了条件**:

1. `test/tfp-book-contract-fixture.test.mjs` 全 24 tests PASS（既存）
2. Book adapter の unit test 追加（depth event → envelope 変換の全パターン）
3. BookStateMachine + pipeline 統合テストで全 quarantine path をカバー
4. 同一 block（trades + book_updates）の join 処理で、trade-only #1-#12 が book の quarantine に影響されないことの検証
5. Board MVP 候補列が既存 #13/#14 を上書きしないことの静的検証
6. `kind='book_updates'` の checkpoint が bounded（<=64KiB）であること
7. 独立 verifier と production 実装の出力一致確認（golden test）
8. 95 点以上の独立レビュー

#### Phase C: TFP Rollup（30s / 5min Aggregation）

**前提**: Phase A と Phase B の完了。1s canonical rows に trade-only #1-#12 + board candidate 列が揃っている。

**スコープ**: 既存 `rollup.mjs` を正規実装に昇格させ、pipeline に wiring する。

**含める項目**:

| # | 項目 | 内容 |
|---|---|---|
| C1 | Rollup の正規 module 化 | `rollup.mjs` を commit。`FEATURE_1S_AGG` の定義を Phase B の出力列に合わせて修正（board candidate 列を aggregation 対象に追加） |
| C2 | Pipeline wiring | `processBlocks()` の commit 後または別 pass で rollup を実行。30s 出力 path を `FEATURES_30S_DIR` に出力 |
| C3 | Output schema の確定 | `specify` 別仕様（§5「Rollup の永続出力、schema、consumer は別仕様が承認されるまで導入しない」） |
| C4 | 5min aggregation | `aggregate5min()` を wiring |
| C5 | Rollup の checkpoint / manifest 対応 | rollup 出力の commit record を manifest に追加。checkpoint に rollup cursor を追加 |

**Phase C 完了条件**:

1. 30s/5min 出力の schema 仕様が承認済み
2. 全 aggregation operator の数学的正しさの検証（手計算 oracle との比較）
3. 「1s 欠損 → 30s 欠損」の伝播契約の定義とテスト
4. Rollup 出力の checkpoint/recovery が bounded
5. 既存 1s output への影響ゼロ

---

## 4. 依存関係図

```text
Phase A (Trade-Only Safety)
  ├─ A1: Receiver raw-only [DONE]
  ├─ A2: flock single-writer [要確認]
  ├─ A3-A9: checkpoint/recovery/horizon [DONE]
  │
  └──→ gate: 全 test PASS + flock 実装 + diff revert
        │
        ▼
Phase B (Book Contract → Production)
  前提: Phase A complete + feature-computer-1s.mjs clean
  ├─ B1: Adapter (connector → envelope)
  ├─ B2: BookStateMachine production
  ├─ B3: Pipeline trade+book join
  ├─ B4: Board MVP 候補列 (新規列, #13/#14 上書き禁止)
  ├─ B5: Book quarantine paths
  ├─ B6: kind='book_updates' checkpoint/recovery
  └─ B7: Frozen inventory kind 分離
        │
        └──→ gate: book fixture PASS + adapter test + join test
              │
              ▼
Phase C (Rollup: 30s/5min Aggregation)
  前提: Phase B complete (1s rows に board 候補列が存在)
  ├─ C1: rollup.mjs 正規化
  ├─ C2: Pipeline wiring
  ├─ C3: Output schema 確定
  ├─ C4: 5min aggregation
  └─ C5: Rollup checkpoint/manifest
```

---

## 5. 即時アクション推奨事項

### 5.1 緊急（Phase A 完了のため）

| # | アクション | 優先度 |
|---|---|---|
| 1 | `feature-computer-1s.mjs` の working tree diff を **revert** する。book 対応は Phase B で正しく実装 | **P0** |
| 2 | `rollup.mjs` を削除するか、Phase C 用の別 branch へ隔離する | **P0** |
| 3 | `flock` lock スクリプトの実装有無を確認する。未実装なら `scripts/acquire-market-lock.sh` を作成 | **P0** |
| 4 | `npm test` 全実行で PASS を確認する | **P1** |
| 5 | `git stash` または `git checkout -- lib/burst-reducer/feature-computer-1s.mjs` で clean state に戻す | **P0** |

### 5.2 構造的（Phase B/C 準備）

| # | アクション |
|---|---|
| 6 | `feature-computer-1s.mjs` の責務を「trade-only #1-#12」に固定する契約をコードコメントまたは spec に明示 |
| 7 | Book 値注入は pipeline orchestration 層（`processBlocks()`）で行い、`feature-computer-1s.mjs` を変更しない設計にする |
| 8 | `specify-2026-07-11-tradeflow-pipeline.md` §6 の Phase 定義を本レビューの 3 分割に更新 |

---

## 6. 未確認項目（親 agent 検証用）

| # | 未確認内容 | 確認方法 |
|---|---|---|
| U1 | `flock -x -n` 実装の有無 | `rg -r 'flock' scripts/ lib/` で検索 |
| U2 | `npm test` 全 573 tests の PASS/FAIL | `npm test` 実行 |
| U3 | `feature-computer-1s.mjs` diff の revert 後の test PASS | revert 後に `node --test test/burst-reducer/feature-computer-1s.test.mjs` |
| U4 | checkpoint の実際の on-disk size（boundedness 達成確認） | `ls -la data/derived/burst_features_v1/manifests/checkpoints/` |
| U5 | `reconcileMarketState` が trades 以外の kind で正しく動作するか | 該当テストの有無確認 |
| U6 | pipeline.mjs `processBlocksNonTrade` の本番到達性（`kind !== 'trades'` path） | `scripts/tfp.mjs --kind book_updates` の実装確認 |

---

## 7. 総合評価

**[判断]**

- **Receiver raw-only**: 完了。再作業不要。
- **Market 単位 single-writer**: checkpoint 整合性 guard は完了。`flock` プロセス間排他の実装状態が未確認。
- **Checkpoint/recovery**: trade-only については safety 機構が充足している（fail-closed, corrupt sentinel, minimal state, bounded size, intent reconcile）。book_updates kind の対応は Phase B。
- **Book 機能**: P0-0 契約と独立 verifier は極めて高品質。ただし production wiring は未着手。`feature-computer-1s.mjs` の working tree diff は契約違反を含むため revert 必須。
- **Rollup**: untracked の独立 module であり、production に影響なし。Phase C で正規化すべき。集約定義に #13/#14 を含めているのは book 前提であり、Phase B 完了後に修正が必要。
- **スコープ混入**: `feature-computer-1s.mjs` diff が最大の問題点。P0-0 契約で明示的に禁止された「既存 #13/#14 の上書き」を行っており、かつ返り値型変更が pipeline を破壊する。

**本レビューの Phase A/B/C 再分割案は、現行実装の到達点と仕様のギャップを埋め、book と rollup を独立した検証可能な単位に分離することを目的とする。**

# TradeFlow Pipeline Phase A 実装計画

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** TFP を一つの market-scoped downstream worker として安全に動かすため、既存 burst reducer の retention、recovery、manifest 保全、不要 clone と回帰試験を完了する。

**Architecture:** Receiver と raw input は変更しない。`lib/burst-reducer/` の既存 one-block-lag worker を TFP worker と位置づけ、burst/feature/将来 rollup は同一 process の内部 stage に保つ。今回 rollup output は実装しない。

**Tech Stack:** Node.js ESM、`node:test`、JSONL、SHA-256、atomic rename、`flock`。

---

## 共通ルール

- `data/live_v3/` を変更・削除しない。
- cron を開始・変更しない。
- 本番 `data/derived/burst_features_v1/` を validation run で使わない。
- production code は必ず RED → GREEN の順にする。
- 各 task 完了後に該当テスト、最後に `node --test test/burst-reducer/*.test.mjs && npm test` を実行する。

### Task 1: closed burst の時間境界 prune

**Objective:** closed burst の in-memory 累積を止め、必要な1秒 overlap は残す。

**Files:**
- Modify: `lib/burst-builder.mjs`
- Modify: `lib/burst-reducer/burst-detector.mjs`
- Modify: `lib/burst-reducer/pipeline.mjs`
- Test: `test/burst-reducer/burst-detector.test.mjs`
- Test: `test/burst-reducer/golden.test.mjs`

**RED:** `BurstBuilder` に、cutoff より古く、将来 bucket と overlap しない closed burst だけを削除する public method の failing test を追加する。boundary 条件 `burst_end_ts === safeCutoff` は保持する。

**GREEN:** `BurstBuilder.pruneClosedBurstsBefore(currentBlockStartMs)` を追加する。保持窓は `MAX_BURST_DURATION_MS + GAP_THRESHOLD_MS + 1000ms` を定数から導出し、magic number にしない。`BurstDetector` は wrapper を公開し、pipeline は commit 後、次の pending block start を基準に1回だけ呼ぶ。

**Acceptance:** prune 前後で境界またぎ burst の feature 行が byte-identical。長系列 fixture で closed burst 数が input block 数に比例して増えない。

### Task 2: full-state deep clone の除去

**Objective:** P1-1 後に未使用となった全 closedBurst clone を停止する。

**Files:**
- Modify: `lib/burst-reducer/pipeline.mjs`
- Test: `test/burst-reducer/burst-state-codec.test.mjs`

**RED:** minimal state が `closedBursts` を持たず、pipeline が `getOpenBurstState()` を呼ばないことを検証する test / static assertion を追加する。

**GREEN:** 未使用の `openBurstBeforeCandidate` と関連 full clone 呼び出しを削除する。checkpoint は `getMinimalBurstState()` のみを使う。

**Acceptance:** `serializeMinimalBurstState()` の出力に `closedBursts`、`same_price_runs`、closed-burst `prints` が存在しない。既存 restart golden は維持する。

### Task 3: recovery を fail-closed にする

**Objective:** intent と committed record のどの crash 状態でも、hash / generation / cursor を検証してからだけ復旧する。

**Files:**
- Create: `lib/burst-reducer/recovery.mjs`
- Modify: `lib/burst-reducer/pipeline.mjs`
- Modify: `lib/burst-reducer/manifest-manager.mjs`
- Test: `test/burst-reducer/recovery.test.mjs`

**RED:** 次の各 fixture を作り、期待する quarantine / resume を test first で固定する。
1. intent + staged hash 一致 + final 不在: rename 後に committed。
2. intent + final hash 一致 + checkpoint generation/cursor 一致: committed。
3. intent + hash不一致、generation不一致、または cursor不一致: quarantine、cursor不変。
4. committed + final shard欠損: quarantine。
5. committed + final hash不一致: quarantine。
6. committed + checkpoint generation/cursor不一致: quarantine。

**GREEN:** `reconcileMarketState()` を `recovery.mjs` に抽出する。record status は `intent` / `committed` / `quarantined` を正確に永続化し、quarantined record を intent に戻さない。recovery は対象 market の checkpoint と manifest を一貫した snapshot として扱う。

**Acceptance:** 各 commit crash point で restart 後に committed composite key は1つ、final shard は1つ、checkpoint generation は単調。安全性を証明できない状態は quarantine のみで自動 commit しない。

### Task 4: manifest 破損の保全と停止

**Objective:** manifest の空・JSON破損を silent new-manifest 扱いにしない。

**Files:**
- Modify: `lib/burst-reducer/manifest-manager.mjs`
- Test: `test/burst-reducer/manifest-manager.test.mjs`

**RED:** 空 manifest と invalid JSON manifest を用意し、元ファイルが `.bak.<timestamp>` に退避され、recovery result が `corrupt-manifest` quarantine になる failing test を作る。

**GREEN:** `loadManifest` を結果型または明示的 error に変更し、呼び出し元が corrupt manifest で処理を開始できないようにする。元ファイルを rename できない場合も新規 manifest を作らない。

**Acceptance:** 破損 manifest がある market では raw block を feed/commit しない。証跡ファイルと structured error が残る。

### Task 5: cursor skip と lock の回帰試験

**Objective:** checkpoint がある market の未記録範囲を、通常 `--from` で黙って skip できないようにする。

**Files:**
- Modify: `lib/burst-reducer/pipeline.mjs`
- Test: `test/burst-reducer/cursor-restart.test.mjs`
- Create: `test/burst-reducer/lock.test.mjs`

**RED:** checkpoint cursor より未来の `fromMs` を渡す test を追加する。期待値は E022 または structured blocked/quarantine であり、cursor が前進しないこと。lock test は同一 market/output root の二 subprocess を起動し、1つだけ lock を取得できることを要求する。

**GREEN:** checkpoint がある通常 run では `fromMs > cursor` を reject する。意図的 skip 機能を実装しない。lock helper の仕様を変える必要がある場合でも `flock -x -n` と skip JSON 契約を維持する。

**Acceptance:** future `--from` で input gap が発生しても commit/manifest 更新なし。lock 競合時、片方だけ reducer 本体に到達する。

### Task 6: 統合検証とレビュー準備

**Files:**
- Modify: `docs/worklog/2026-07-10-burst-reducer-remediation.md`

**Steps:**
1. `git diff --check`
2. `node --test test/burst-reducer/*.test.mjs`
3. `npm test`
4. 隔離 root だけを用いる synthetic long-run testで checkpoint <=64KiB、state <=256KiB、retention bounded を確認。
5. crash-point test evidence と test count を worklog に記録。

**Do not run:** cron、systemd unit、有効本番 output root への5分実運転。

**Review gate:** この Task 6 で得た実測証拠を添えて独立レビュー95点以上になってから、別途承認された5分制御テストに進む。

### Task 7: Raw gap を data-none として継続する

**Objective:** raw block の不在を valid-empty として扱い、実在 raw block の全期間 backfill を gap で停止させない。

**Files:**
- Modify: `lib/burst-reducer/raw-trades-notional-reader.mjs`
- Modify: `lib/burst-reducer/pipeline.mjs`
- Modify: `lib/burst-reducer/output-committer.mjs`
- Test: `test/burst-reducer/raw-trades-notional-reader.test.mjs`
- Test: `test/burst-reducer/cursor-restart.test.mjs` または新規 integration test

**RED/GREEN contract:**
1. lookback raw が不在でも `coverageComplete` は真、notional は存在 block だけから計算され、`assumedEmptyBlockStarts` に不在時刻が載る。
2. N→N+k の gap は E006/E007 を出さず N を一度だけ commit し、次の実在 blockを pending にする。
3. manifest committed record は `assumed_empty_input_blocks` を保存し、structured `ASSUMED_EMPTY_GAP` が出る。
4. 存在する malformed raw は従来どおり E007 fail-closed。

**Verification:** synthetic gap fixture で両側の実在 block の shard、hash、checkpoint cursor、assumed-empty audit record を検証する。全 test 後に隔離 root で binance_perp 全期間 backfill を再実行する。

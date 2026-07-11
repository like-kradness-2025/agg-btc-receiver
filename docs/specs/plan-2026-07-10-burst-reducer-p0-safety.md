# PDD PLAN: Burst Reducer P0 最小安全修正案

**文書 ID:** plan-2026-07-10-burst-reducer-p0-safety
**日付:** 2026-07-10
**対象リポジトリ:** `agg-btc-receiver`（`/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver`）
**親文書:** `docs/specs/design-2026-07-10-burst-reducer.md`, `docs/specs/plan-2026-07-10-burst-reducer.md`
**ステータス:** PLAN（コード変更なし。本ドキュメントは修正案のみ。EXECUTE は REVIEW 合格後に実施）
**既存 worklog:** `docs/worklog/2026-07-10-burst-reducer-remediation.md`
**既存 remediation plan:** `docs/specs/plan-2026-07-10-burst-reducer-remediation.md`

---

## 1. 現状観測と根拠

### 1.1 負荷観測（実測値）

| 観測項目 | 値 | 観測元 |
|---|---|---|
| チェックポイントサイズ（binance_spot） | **536,296,776 bytes (512MiB)** | `data/derived/burst_features_v1/manifests/checkpoints/binance_spot.json` |
| チェックポイントサイズ（binance_perp） | **533,623,819 bytes (509MiB)** | `data/derived/burst_features_v1/manifests/checkpoints/binance_perp.json` |
| チェックポイントサイズ（bybit_perp） | **536,672,479 bytes (512MiB)** | `data/derived/burst_features_v1/manifests/checkpoints/bybit_perp.json` |
| チェックポイントサイズ（bybit_spot） | **150MiB** | `data/derived/burst_features_v1/manifests/checkpoints/bybit_spot.json` |
| マニフェストサイズ（binance_perp） | 592KB | `data/derived/burst_features_v1/manifests/binance_perp.json` |
| マニフェストサイズ（bybit_spot） | 652KB | `data/derived/burst_features_v1/manifests/bybit_spot.json` |
| binance_spot チェックポイント内 closedBursts 数 | **59,633** | Python 実測: `open_burst.closedBursts.length` |
| binance_spot チェックポイント内 nextId | 59,634 | Python 実測: `open_burst.nextId` |
| 各 closed burst 内 prints の平均サイズ | 約 4 trade prints + same_price_runs | コード解析: `burst-state-codec.mjs` `deepCloneClosedBurst()` |
| 出力済み block shard 数 | 2,384 files | `find data/derived/burst_features_v1/features_1s/ -name '*.jsonl' \| wc -l` |
| 前回実行時の reducer CPU | ~110% | ユーザー報告 / worklog 記録 |
| 前回実行時の reducer RSS | マルチ GB 級 | ユーザー報告 / worklog 記録 |
| cron ジョブ | `266c0a8804a6`（停止中） | worklog 記録 |
| 実行中 reducer プロセス | なし（明示的 PID kill 済み） | worklog 記録 |

### 1.2 根本原因の特定

#### 原因 A: checkpoint 無制限肥大（P0）

**該当ファイル:** `lib/burst-reducer/burst-state-codec.mjs`（L44-L51, L84-L87, L146-L162）

```javascript
// L44-L51: serializeBurstBuilderState() は全 _closedBursts を deep clone
export function serializeBurstBuilderState(builder) {
  return {
    schemaVersion: SCHEMA_VERSION,
    open: builder._open ? deepCloneOpen(builder._open) : null,
    closedBursts: builder._closedBursts.map(b => deepCloneClosedBurst(b)), // ← 全閉塞バースト
    nextId: builder._nextId,
  };
}

// L146-L162: deepCloneClosedBurst() は各 burst の prints と same_price_runs を全コピー
function deepCloneClosedBurst(b) {
  return {
    // ... スカラー値 ...
    same_price_runs: b.same_price_runs.map(r => ({ ...r })),  // ← 全 same_price_runs
    prints: b.prints.map(p => ({ ...p })),                     // ← 全 trade prints
  };
}
```

**メカニズム:**
1. 各 30s ブロックの commit 時、`OutputCommitter.commitFinalizedBlock()`（`output-committer.mjs` L103-L115）が `detector.getOpenBurstState()` の戻り値（= `serializeBurstBuilderState(builder)` の結果）を checkpoint の `open_burst` フィールドに保存する。
2. `serializeBurstBuilderState` は `BurstBuilder._closedBursts` の**全要素**（59,633 個の閉塞バースト）を deep-clone する。
3. 各閉塞バーストは個別 trade の `prints` 配列と `same_price_runs` 配列を含む。
4. この `open_burst` state が checkpoint JSON にそのまま書き込まれ、512MiB に達する。

**契約違反:** 設計書 §6 は checkpoint に「open burst state（未確定バースト状態）」のみを保存すると定義しているが、実装は全 `_closedBursts` を含めている。`pending_block` 内にも `open_burst_before_N1` として重複保存（`output-committer.mjs` L193）。

#### 原因 B: pending_block 内の重複 state（P0）

**該当ファイル:** `lib/burst-reducer/pipeline.mjs`（L178-L188, L208-L218）

```javascript
// L178-L188: nextPendingInfo に openBurstBeforeCandidate（= detector.getOpenBurstState() 全体）を保存
const nextPendingInfo = {
  block_start_ms: candidateBlock.ms,
  trade_input_sha256: inputSha256,
  // ...
  open_burst_before_N1: openBurstBeforeCandidate,  // ← serializeBurstBuilderState の全出力 = 512MiB
};

// L208-L218: pendingBlock にも同様に保存
pendingBlock = {
  block_start_ms: candidateBlock.ms,
  trade_input_sha256: inputSha256,
  // ...
  open_burst_before_N1: openBurstBeforeCandidate,  // ← 再度 512MiB
};
```

`open_burst_before_N1` は設計上、N+1 ブロック処理開始**直前**の open burst state のみを保存すればよいが、実装は `serializeBurstBuilderState()` の全出力（全 close burst + open burst）を保存している。

#### 原因 C: 単一ライターロック不在（P0）

**該当ファイル:** `scripts/reduce-burst-v1.mjs`（L70-L86）

```javascript
for (const market of markets) {
  try {
    const result = await runPipeline({ ... });
    // ...
  } catch (e) { ... }
}
```

マーケット間は直列だが、**同一マーケットに対する複数プロセス（cron + 手動 backfill）の同時実行を防止する機構がない**。manifest/checkpoint の `.tmp` → atomic rename は単一プロセス内では安全だが、複数プロセス間では競合する。

**影響:** cron と手動バックフィルが同時に走ると、同一マーケットの manifest/checkpoint が競合し、破損・二重 commit・cursor 逆進のリスクがある。

#### 原因 D: 権威的カーソル不在（P0）

**該当ファイル:** `lib/burst-reducer/pipeline.mjs`（L102-L107）

```javascript
export async function runPipeline({ dataDir, market, fromMs, toMs, runId, outputRoot }) {
  const blocks = scanTradeBlocks(dataDir, market, fromMs, toMs);  // ← CLI の from/to でスキャン
  // ...
  const cp = loadCheckpoint(market, derivedDir);  // ← checkpoint も読み込むが
  // ...
  // checkpoint の cursor と CLI range の優先順位が未定義
```

`scanTradeBlocks`（`block-scanner.mjs` L19-53）は CLI の `[fromMs, toMs)` で block を列挙する。checkpoint 復元後も、CLI により checkpoint の cursor より古い block が渡されうる。

**契約違反:** 設計書 §6 の再起動ルールは「checkpoint が存在すれば、その `last_committed_block_start` 以降から再開する」ことを要求しているが、実装は CLI range を優先している。

#### 原因 E: intent reconcile 未実装（P1）

**該当ファイル:** `lib/burst-reducer/pipeline.mjs`（L110-L112）

```javascript
const cp = loadCheckpoint(market, derivedDir);
const detector = new BurstDetector(market, cp?.open_burst ?? null);
const committer = new OutputCommitter(market, runId, derivedDir);
```

起動時に intent 状態の残留（`.staging/` 内の staged ファイル、`status: "intent"` の manifest エントリ）を検査・修復するロジックがない。

設計書 §4.4 の startup recovery テーブルが要求する 6 状態すべての処理が未実装。

#### 原因 F: マニフェスト全体再書き込み（P2）

**該当ファイル:** `lib/burst-reducer/output-committer.mjs`（L135-L148）

```javascript
_writeManifest(manifest) {
  const tmpPath = join(this._manifestDir, `${this._market}.json.tmp`);
  const finalPath = join(this._manifestDir, `${this._market}.json`);
  mkdirSync(this._manifestDir, { recursive: true });
  writeFileDurable(tmpPath, JSON.stringify(manifest, null, 2) + '\n');  // ← 全体書き換え
  renameSync(tmpPath, finalPath);
  fsyncDirectory(this._manifestDir);
}
```

各ブロック commit ごとに manifest 全体（全 processed_blocks エントリ）をパース・シリアライズ・fsync している。処理済みブロック数に比例してコストが線形増加する。ただし、現状のチェックポイント肥大に比べれば影響は小さい（マニフェストは ~600KB で安定）。

---

## 2. 修正方針（優先度順）

### 2.0 全体原則

- **コード変更は本 PLAN が REVIEW 合格（>=95）してから実施する。**
- **book/RVZ 実装（#13-#22）は対象外。trade-only #1-#12 のみ。**
- **raw 入力（`data/live_v3/`）は一切変更・削除しない。**
- **すべての修正は既存テストを破壊しない。新規テストを追加する。**

---

### 2.1 P0-1: Single Writer + Cron 停止

#### 対象ファイル
- **新規:** `scripts/acquire-market-lock.sh`
- **修正:** `scripts/cron-reduce-burst-v1.sh`（または cron 定義）
- **修正:** `scripts/backfill-all-markets-serial.sh`（存在する場合）

#### 実装内容

1. **ロック獲得スクリプト** `scripts/acquire-market-lock.sh`:
   - `flock(1)` または `mkdir` によるアトミックなロックディレクトリ作成を使用。
   - ロックスコープ = `<output_root>/locks/<market>.lock`
   - 獲得失敗時は exit 0（skip）、exit 1（競合）のいずれかで、後続処理を行わない。
   - タイムアウト付き（デフォルト 0 = 即時 fail、`--wait <seconds>` で待機可能）。

2. **cron ジョブ**:
   - 既存の cron ジョブ `266c0a8804a6` は停止中。修正後も controlled test 中は停止を維持する。
   - cron スクリプト内で `acquire-market-lock.sh` を呼び出し、ロック獲得成功時のみ `reduce-burst-v1.mjs` を起動。
   - 各マーケットを個別の cron 行または直列ループで処理し、市場ごとにロックを獲得。

3. **手動 backfill**:
   - `scripts/backfill-all-markets-serial.sh` でも同一ロック機構を使用。
   - `--markets` が単一マーケットの場合でもロック獲得必須。

#### 契約

| 項目 | 値 |
|---|---|
| ロックファイルパス | `<output_root>/locks/<market>.lock` |
| ロック方式 | `mkdir` または `flock -x -n` |
| 獲得失敗時の動作 | exit 0（skip）、stderr に INFO ログ |
| タイムアウト | デフォルト 0（非待機）、`--wait` で指定可能 |

#### テスト

| テスト | 確認内容 |
|---|---|
| `test/burst-reducer/lock.test.mjs` | 同一マーケットの2プロセス同時起動 → 1つのみ実行、他は skip |
| 手動検証 | `acquire-market-lock.sh binance_spot & acquire-market-lock.sh binance_spot` → 2つ目が失敗 |

#### 負荷観測
- ロックファイルは空ファイル（inode のみ）。ディスク使用量無視可能。
- `flock` はカーネルレベルでアトミック。CPU 負荷なし。

#### Rollback 条件
- ロック獲得に 1 秒以上かかる → ロックファイル削除 + 旧 cron に戻す。
- ロック解放漏れで後続ジョブが永久ブロック → ロックファイル手動削除 + タイムアウト付きロックに差し替え。

---

### 2.2 P0-2: 権威的カーソル（Resume Cursor）

#### 対象ファイル
- **修正:** `lib/burst-reducer/pipeline.mjs`（L102-L107）
- **修正:** `lib/burst-reducer/block-scanner.mjs`（新規関数追加）
- **修正:** `scripts/reduce-burst-v1.mjs`（L50-L53）

#### 実装内容

1. **`runPipeline()` の修正**（`pipeline.mjs` L102-L107）:

```javascript
export async function runPipeline({ dataDir, market, fromMs, toMs, runId, outputRoot }) {
  const cp = loadCheckpoint(market, derivedDir);

  // ▼ P0-2: checkpoint が権威。cursor を決定
  let effectiveFromMs = fromMs;
  if (cp && cp.last_committed_block_start != null) {
    // checkpoint の cursor 以降からのみ処理
    const cursorMs = cp.last_committed_block_start + 30000;  // 次ブロック開始
    if (cursorMs > effectiveFromMs) {
      effectiveFromMs = cursorMs;
      log('INFO', market, `resume cursor: advancing from ${fromMs} to ${effectiveFromMs} (checkpoint generation ${cp.generation})`);
    }
    // pending_block がある場合、その block_start_ms から再開
    if (cp.pending_block && cp.pending_block.block_start_ms > effectiveFromMs) {
      effectiveFromMs = cp.pending_block.block_start_ms;
    }
  }
  // effectiveFromMs が toMs 以上なら処理ブロックなし
  if (effectiveFromMs >= toMs) {
    log('INFO', market, 'all blocks already processed up to cursor');
    return { processed: 0, errors: 0, manifestUpdates: [] };
  }

  const blocks = scanTradeBlocks(dataDir, market, effectiveFromMs, toMs);
```

2. **ブロック連続性検証**（`block-scanner.mjs` に新規関数追加）:

```javascript
export function validateBlockContinuity(blocks, expectedStartMs) {
  // blocks が expectedStartMs から 30s 間隔で連続しているか検証
  // 欠落 → ギャップをリスト化してログ出力（quarantine ではなく INFO で報告）
  // 処理はギャップの手前で停止し、exit 0
}
```

3. **CLI の `--from` 意味変更**（`reduce-burst-v1.mjs`）:
   - `--from` は「初期ディスカバリーの下限」であり、checkpoint が存在する場合は無視される。
   - help テキストを更新し、この動作を明記する。

#### 契約

| 項目 | 値 |
|---|---|
| カーソル決定ルール | checkpoint 優先。`last_committed_block_start + 30000` から開始 |
| pending_block の扱い | pending は再処理（N+1 未 commit のため） |
| CLI `--from` の役割 | checkpoint 不在時の初期ディスカバリー下限。checkpoint 存在時は無視 |
| ギャップ検出時 | ギャップの手前まで処理し exit 0。ギャップ情報を stderr に出力 |

#### テスト

| テスト | 確認内容 | 対象ファイル |
|---|---|---|
| `test/burst-reducer/pipeline.test.mjs`（拡張） | checkpoint あり → `--from` が過去でも cursor 以降のみ処理 | `pipeline.mjs` |
| `test/burst-reducer/block-scanner.test.mjs`（拡張） | 連続性検証、ギャップ検出 | `block-scanner.mjs` |
| `test/burst-reducer/golden.test.mjs`（拡張） | restart → 中断前と byte-identical 出力 | `pipeline.mjs` |
| 手動検証 | N ブロック処理 → 中断 → 再開 → 重複なし | 全ファイル |

#### 負荷観測
- カーソル判定は O(1)。ブロック列挙は既存の `scanTradeBlocks` が行うため負荷増加なし。
- ギャップ検証はブロックリストの単一走査（O(n)）。ブロック数千件でも 1ms 以下。

#### Rollback 条件
- restart 後に checkpoint が示す cursor より古いブロックが処理された → 即時停止。cursor ロジックを revert。
- restart 後、expect されるブロック数が checkpoint なし時の半分以下 → カーソルが進みすぎ。調査。

---

### 2.3 P0-3: Intent Reconcile（起動時リカバリ）

#### 対象ファイル
- **修正:** `lib/burst-reducer/pipeline.mjs`（L110-L112 前に新規リカバリブロック追加）
- **新規:** `lib/burst-reducer/recovery.mjs`（intent reconcile 専用モジュール）
- **修正:** `lib/burst-reducer/output-committer.mjs`（manifest 読み取り API を公開）

#### 実装内容

**`lib/burst-reducer/recovery.mjs`:**

```javascript
export function reconcileIntents(market, derivedDir) {
  // 1. manifest を読み込み、status: "intent" のエントリを列挙
  // 2. 各 intent エントリについて:
  //    a. staged ファイルが存在するか確認
  //    b. final shard が存在するか確認
  //    c. checkpoint の generation と manifest の generation を比較
  // 3. 状態に応じたリカバリ:
  //    - intent + final なし + staged あり → staged 削除 → 再試行
  //    - intent + final あり + checkpoint 一致 → hash 検証 → finish commit
  //    - intent + final あり + checkpoint 不一致 → quarantine
  //    - committed + final あり + checkpoint 一致 → skip（既処理）
  //    - committed + final なし → quarantine
  //    - committed + checkpoint 不一致 → quarantine
  // 4. リカバリ結果を返す
}
```

**呼び出し位置（`pipeline.mjs` L110 前）:**

```javascript
const cp = loadCheckpoint(market, derivedDir);

// ▼ P0-3: intent reconcile を最初に実行
const recoveryResult = reconcileIntents(market, derivedDir);
if (recoveryResult.errors.length > 0) {
  for (const err of recoveryResult.errors) {
    log('ERROR', market, `recovery: ${err.block} -> quarantine (${err.reason})`);
  }
}
if (recoveryResult.resumed.length > 0) {
  for (const r of recoveryResult.resumed) {
    log('INFO', market, `recovery: resumed commit for ${r.block}`);
  }
}
```

#### 契約

設計書 §4.4 の startup recovery テーブルを完全実装:

| manifest status | final shard | checkpoint | Action |
|---|---|---|---|
| (no record) | absent | — | 通常処理 |
| intent | absent | — | `.staging/` 削除 → 再試行 |
| intent | present | mismatch | row hash 検証 → 一致なら finish commit → 不一致なら quarantine |
| intent | present | match | step 3-5 から再開（finish commit） |
| committed | present | match | hash 検証 → 一致なら skip |
| committed | present | mismatch | quarantine（要調査） |
| committed | absent | — | quarantine（要調査） |

#### テスト

| テスト | 確認内容 |
|---|---|
| `test/burst-reducer/recovery.test.mjs`（新規） | 各状態からのリカバリ（intent + staged あり/なし etc.） |
| `test/burst-reducer/output-committer.test.mjs`（拡張） | commit 途中 kill → restart → 1 committed record のみ |
| 手動検証 | 各 commit step 後 kill → restart → 重複レコードなし、generation 単調増加 |

#### 負荷観測
- manifest 読み取りは `JSON.parse` 1 回（既存取込と同じ）。intent エントリ数は高々 1-2。
- staged ファイルの stat/削除は O(1)。

#### Rollback 条件
- recovery 中に manifest 破損 → quarantine レポート作成。元の manifest を `.bak` に退避。
- recovery が誤って committed block を再処理 → duplicate key が manifest に出現。即時停止。

---

### 2.4 P1-1: チェックポイント状態最小化

#### 対象ファイル
- **修正:** `lib/burst-reducer/burst-state-codec.mjs`（コア修正）
- **修正:** `lib/burst-reducer/pipeline.mjs`（L178-L188, L208-L218 open_burst_before_N1 削減）
- **新規:** `test/burst-reducer/burst-state-codec.test.mjs`（拡張）

#### 実装内容

**A. `burst-state-codec.mjs` の修正:**

現在の `serializeBurstBuilderState()` は全 `_closedBursts` をシリアライズしている。これを以下に変更する:

```javascript
export function serializeBurstBuilderState(builder) {
  return {
    schemaVersion: SCHEMA_VERSION,
    open: builder._open ? deepCloneOpen(builder._open) : null,
    // ▼ P1-1: closedBursts を保存しない。再開に不要。
    // closedBursts は checkpoint 永続化の対象外。
    // メモリ上の _closedBursts は BurstBuilder の内部状態として維持されるが、
    // 永続化しない。再起動時は空の closedBursts から始める。
    nextId: builder._nextId,
  };
}
```

**重要:** `_closedBursts` を永続化しないことの正当性:
- `_closedBursts` は 1s 特徴量計算の `getClosedBurstsOverlapping(secondTs)` で使われるが、これは現在処理中のブロック内の閉塞バーストのみを必要とする。
- one-block lag 方式では、N の行は N+1 の全 trades 投入後に計算される。この時点で必要な閉塞バーストは N ブロック内で閉じたバーストのみであり、過去ブロックの閉塞バーストは不要。
- 再起動後、pending_block から N+1 が再処理される。N+1 の trades 投入により N の閉塞バーストが再生成されるため、過去の全閉塞バーストの永続化は不要。

**B. `restoreBurstBuilderState()` の修正:**

```javascript
export function restoreBurstBuilderState(builder, state) {
  // ... validation ...

  // Restore _open (nullable)
  if (state.open !== null && state.open !== undefined) {
    // ...
    builder._open = deepCloneOpen(state.open);
  } else {
    builder._open = null;
  }

  // ▼ P1-1: _closedBursts は常に空配列で初期化。
  // 過去の閉塞バーストは再現不要（再起動後のブロック処理で再生成される）。
  builder._closedBursts = [];

  // Restore _nextId
  builder._nextId = state.nextId;
}
```

**C. `open_burst_before_N1` の削減（`pipeline.mjs`）:**

```javascript
// L178-L188 修正: open_burst_before_N1 には open burst 状態のみ保存
const nextPendingInfo = {
  block_start_ms: candidateBlock.ms,
  trade_input_sha256: inputSha256,
  auxiliary_input_hashes: {},
  replay_identity: { ... },
  open_burst_before_N1: {
    // ▼ P1-1: open burst 状態のみ。closedBursts は含まない
    schemaVersion: openBurstBeforeCandidate.schemaVersion,
    open: openBurstBeforeCandidate.open,  // null または open burst
    nextId: openBurstBeforeCandidate.nextId,
  },
};
```

#### 契約

| 項目 | 値 |
|---|---|
| checkpoint に保存する state | `schemaVersion`, `open`（未確定バーストのみ）, `nextId` |
| checkpoint に保存しない state | `closedBursts`（全閉塞バースト）, `prints`, `same_price_runs` |
| スキーマバージョン | 現行 `SCHEMA_VERSION = 1` を維持（後方互換のため） |
| 旧形式 checkpoint の扱い | `closedBursts` フィールドが存在しても無視。`open` と `nextId` のみ復元 |

#### テスト

| テスト | 確認内容 |
|---|---|
| `test/burst-reducer/burst-state-codec.test.mjs`（拡張） | シリアライズ出力に `closedBursts` が含まれないこと |
| `test/burst-reducer/burst-state-codec.test.mjs`（拡張） | round-trip 後、closedBursts が空であること |
| `test/burst-reducer/burst-state-codec.test.mjs`（拡張） | 旧形式（closedBursts あり）の checkpoint を読み込んでも E020 を throw しないこと |
| `test/burst-reducer/pipeline.test.mjs`（拡張） | restart 後の出力が byte-identical |
| `test/burst-reducer/golden.test.mjs`（拡張） | 境界またぎバーストの restart 後期待値が一致 |

#### 負荷観測

| 項目 | 修正前 | 修正後（予測） |
|---|---|---|
| checkpoint サイズ（binance_spot） | 512 MiB | < 1 KiB（open burst 1 個 + pending block info のみ） |
| checkpoint 書き込み時間 | 数秒（512 MiB の JSON.stringify + fsync） | < 1ms |
| checkpoint 読み取り時間 | 数秒（512 MiB の JSON.parse） | < 1ms |
| RSS 線形成長 | あり（closedBursts がメモリ常駐） | 低減（closedBursts は block 単位で GC 可能に） |

#### Rollback 条件
- restart 後の出力が中断前と byte-identical でない → 即時停止。境界またぎバーストの golden test が FAIL していないか確認。
- checkpoint 復元後、バースト ID（`burst_id`）の採番が重複 → `_nextId` 復元ロジック確認。
- 旧形式 checkpoint の読み取りで E020 → 後方互換コード追加。

---

### 2.5 P1-2: 閉塞バーストの境界付き保持（Closed Burst Bounded Retention）

#### 対象ファイル
- **修正:** `lib/burst-builder.mjs`（`getClosedBurstsOverlapping` の効率化のための準備）
- **修正:** `lib/burst-reducer/feature-computer-1s.mjs`（特になし。既に bounded）
- **新規テスト:** bounded retention の検証

#### 実装内容

**背景:** P1-1 で checkpoint から全閉塞バーストを削除した後も、メモリ上の `_closedBursts` 配列は全 block 処理を通じて累積する。メモリ使用量の線形成長を防ぐため、閉塞バーストの保持期間を制限する。

**方針:**

```javascript
// lib/burst-builder.mjs に新規メソッド追加
/**
 * 指定された timestamp より古い閉塞バーストを削除する。
 * 削除基準: burst_end_ts + lookback_window_ms < cutoffTs
 * lookback_window_ms = max_burst_duration_ms + gap_threshold_ms + safety_margin
 *   = 5000 + 50 + 1000 = 6050 ms
 * これにより、現在処理中のブロックの全 1s バケットが必要とする
 * 閉塞バーストが誤って削除されないことを保証する。
 */
pruneClosedBurstsBefore(cutoffTs) {
  const LOOKBACK_WINDOW = this._maxDuration + this._gapThreshold + 1000;  // 6050 ms
  const safeCutoff = cutoffTs - LOOKBACK_WINDOW;
  this._closedBursts = this._closedBursts.filter(
    b => b.burst_end_ts >= safeCutoff
  );
}
```

**呼び出し位置（`pipeline.mjs` の各ブロック処理後）:**

```javascript
// ブロック N の commit 後、N+1 の block_start_ms を基準に prune
// N+1 の block_start_ms = pendingBlock.block_start_ms
if (pendingBlock && pendingBlock.block_start_ms) {
  detector.pruneClosedBurstsBefore(pendingBlock.block_start_ms);
}
```

#### 契約

| 項目 | 値 |
|---|---|
| 保持期間 | `burst_end_ts >= (current_block_start_ms - 6050 ms)` |
| lookback window 内訳 | `max_burst_duration_ms(5000) + gap_threshold_ms(50) + safety_margin(1000)` |
| 削除タイミング | 各ブロック commit 後 |
| 安全マージンの根拠 | 1s overlap 判定は `burst_end_ts >= bucket_start_ts`（現在ブロックの最初の秒まで見る）。最も古い可能性のある bucket_start は `block_start_ms - 1000`。そこからさらに max duration + gap を引いても安全 |

#### テスト

| テスト | 確認内容 |
|---|---|
| `test/burst-reducer/burst-detector.test.mjs`（拡張） | prune 後も必要な閉塞バーストが残っていること |
| `test/burst-reducer/pipeline.test.mjs`（拡張） | 長系列処理後、RSS が線形成長しないこと |
| `test/burst-reducer/golden.test.mjs`（拡張） | prune 後も 1s overlap 結果が不変 |

#### 負荷観測

| 項目 | 修正前（予測） | 修正後（予測） |
|---|---|---|
| 60分処理後のメモリ上閉塞バースト数 | ~120,000（全累積） | ~200（2ブロック分 + safety） |
| 60分処理後の RSS | マルチ GB | < 200 MB |
| `getClosedBurstsOverlapping` の計算量 | O(N) 全走査 | O(K)（K ≪ N） |

#### Rollback 条件
- prune 後の overlap 判定が prune 前と不一致 → LOOKBACK_WINDOW を拡大。
- prune による性能改善が観測されない → 本修正を revert（closedBursts のメモリ常駐に戻す）。
- prune により burst_id で参照されるバーストが消失 → 上位 consumer が burst_id を必要としないことを確認。必要な場合は retention 方式を再設計。

---

### 2.6 P2: マニフェスト運用の最小安全修正

#### 対象ファイル
- **修正:** `lib/burst-reducer/output-committer.mjs`（`_loadManifest` / `_writeManifest`）
- **修正:** `lib/burst-reducer/manifest-manager.mjs`（オプション）

#### 実装内容

**A. マニフェスト atomic write の堅牢化（最小修正）:**

現在の実装は `JSON.parse` → 変更 → `JSON.stringify` → atomic rename で、論理的には正しい。ただし、manifest ファイルが破損した場合のリカバリがない。

```javascript
// output-committer.mjs _loadManifest() の強化
_loadManifest() {
  const path = join(this._manifestDir, `${this._market}.json`);
  if (!existsSync(path)) return null;
  try {
    const raw = readFileSync(path, 'utf8');
    if (raw.trim().length === 0) {
      // 空ファイル → 破損とみなして null 返却（新規 manifest 作成）
      log('WARN', this._market, 'manifest file is empty, treating as missing');
      return null;
    }
    return JSON.parse(raw);
  } catch (e) {
    // JSON 破損 → バックアップ + 新規 manifest
    const bakPath = path + '.bak.' + Date.now();
    try { renameSync(path, bakPath); } catch (_) {}
    log('ERROR', this._market, `manifest corrupted, backed up to ${bakPath}, creating new`);
    return null;
  }
}
```

**B. manifest からの古い intent エントリのクリーンアップ:**

P0-3 の intent reconcile 完了後、committed 状態のエントリのみを保持し、残留 intent エントリを削除する。

```javascript
// pipeline.mjs の起動時、reconcile 後
function cleanupManifestIntents(market, derivedDir) {
  const manifest = loadManifest(market, derivedDir);
  if (!manifest || !manifest.processed_blocks) return;

  const cleaned = {};
  let removedCount = 0;
  for (const [key, entry] of Object.entries(manifest.processed_blocks)) {
    if (entry.status === 'committed') {
      cleaned[key] = entry;
    } else {
      removedCount++;
    }
  }
  if (removedCount > 0) {
    manifest.processed_blocks = cleaned;
    writeManifestDurable(manifest);  // atomic rename
    log('INFO', market, `cleaned ${removedCount} non-committed manifest entries`);
  }
}
```

#### 契約

| 項目 | 値 |
|---|---|
| manifest 破損検出 | `JSON.parse` 失敗時、空ファイル時 |
| 破損時動作 | `.bak.<timestamp>` に退避 → 新規 manifest 作成 |
| intent クリーンアップ | reconcile 後、committed 以外のエントリを削除 |
| atomic write | 既存の `.tmp` → `rename` を維持 |

#### テスト

| テスト | 確認内容 |
|---|---|
| `test/burst-reducer/manifest-manager.test.mjs`（拡張） | 破損 manifest 読み取り → null + bak 作成 |
| `test/burst-reducer/manifest-manager.test.mjs`（拡張） | 空 manifest ファイル → null + warn |
| `test/burst-reducer/manifest-manager.test.mjs`（拡張） | intent クリーンアップ → committed のみ残存 |

#### 負荷観測
- manifest ファイルサイズは ~600KB で安定（committed ブロック数に比例）。
- 破損検出は `JSON.parse` の try-catch のみ（追加コストなし）。

#### Rollback 条件
- manifest 破損検出が過剰（正常ファイルを誤検出） → 検出閾値調整。
- クリーンアップで committed エントリが消失 → 即時停止。atomic write の順序確認。

---

## 3. 修正ファイル一覧

| 優先度 | ファイル | 操作 | 内容 |
|---|---|---|---|
| **P0-1** | `scripts/acquire-market-lock.sh` | **新規** | flock/mkdir による市場別ロック |
| **P0-1** | `scripts/cron-reduce-burst-v1.sh` | **修正** | ロック獲得呼び出し追加 |
| **P0-2** | `lib/burst-reducer/pipeline.mjs` | **修正** | カーソル決定ロジック（L102-L107）、ギャップ検出 |
| **P0-2** | `lib/burst-reducer/block-scanner.mjs` | **修正** | ブロック連続性検証関数追加 |
| **P0-2** | `scripts/reduce-burst-v1.mjs` | **修正** | `--from` の意味変更、help 更新 |
| **P0-3** | `lib/burst-reducer/recovery.mjs` | **新規** | intent reconcile 専用モジュール |
| **P0-3** | `lib/burst-reducer/pipeline.mjs` | **修正** | 起動時 recovery 呼び出し追加 |
| **P1-1** | `lib/burst-reducer/burst-state-codec.mjs` | **修正** | `serializeBurstBuilderState` から `closedBursts` 除去、`restoreBurstBuilderState` で空初期化 |
| **P1-1** | `lib/burst-reducer/pipeline.mjs` | **修正** | `open_burst_before_N1` から `closedBursts` 除去（L178-L188, L208-L218） |
| **P1-2** | `lib/burst-builder.mjs` | **修正** | `pruneClosedBurstsBefore()` 追加 |
| **P1-2** | `lib/burst-reducer/pipeline.mjs` | **修正** | ブロック commit 後の prune 呼び出し追加 |
| **P2** | `lib/burst-reducer/output-committer.mjs` | **修正** | `_loadManifest()` の破損耐性、intent クリーンアップ |
| — | `test/burst-reducer/lock.test.mjs` | **新規** | ロックテスト |
| — | `test/burst-reducer/pipeline.test.mjs` | **拡張** | cursor/restart/recovery テスト |
| — | `test/burst-reducer/block-scanner.test.mjs` | **拡張** | 連続性検証テスト |
| — | `test/burst-reducer/recovery.test.mjs` | **新規** | intent reconcile テスト |
| — | `test/burst-reducer/burst-state-codec.test.mjs` | **拡張** | closedBursts 非保存テスト |
| — | `test/burst-reducer/burst-detector.test.mjs` | **拡張** | prune テスト |
| — | `test/burst-reducer/manifest-manager.test.mjs` | **拡張** | 破損耐性テスト |

---

## 4. 検証ゲート

### 4.1 静的契約チェック（EXECUTE 前に親が実施）

```bash
# 負の probe: 旧カーソルパスがないこと
grep -rn 'fromMs.*toMs' lib/burst-reducer/pipeline.mjs | grep -v 'effectiveFromMs'

# 負の probe: checkpoint に closedBursts が含まれないこと
grep -rn 'closedBursts' lib/burst-reducer/burst-state-codec.mjs | grep -v '//' | grep -v '_closedBursts'

# 正の probe: effectiveFromMs が checkpoint を参照していること
grep -n 'effectiveFromMs' lib/burst-reducer/pipeline.mjs

# 正の probe: reconcileIntents が呼ばれていること
grep -n 'reconcileIntents' lib/burst-reducer/pipeline.mjs

# 負の probe: require() がないこと
grep -rn 'require(' lib/burst-reducer/*.mjs scripts/acquire-market-lock.sh
```

### 4.2 単体テスト

```bash
node --test test/burst-reducer/*.test.mjs
```

### 4.3 5 分制御テスト（REVIEW 合格後のみ）

- **市場:** 1 market（`binance_spot_usdc` 推奨。データ量少）
- **出力 root:** `data/derived/burst_features_v1_test_p0/`（本番と分離）
- **cron:** 停止維持
- **ロック:** 有効
- **監視:** CPU, RSS, IO, backlog, error count

### 4.4 5 分テストの合格基準

| 基準 | 閾値 |
|---|---|
| プロセス生存 | 5 分間継続稼働 |
| カーソル進行 | `last_committed_block_start` が単調増加 |
| 重複ブロック | manifest に同一 composite key のエントリなし |
| エラー | E007/E020/E031 なし |
| RSS | 線形成長なし、ホスト安全閾値（< 500 MB）以下 |
| checkpoint サイズ | < 10 KiB で安定 |
| CPU | Receiver を starvation させない（system load < 2.0） |
| 残留物 | `.tmp` / staged orphan / intent エントリなし |
| 出力スキーマ | 全行 25 key、#13=null、#14=0 |
| 再開性 | stop → restart で重複出力なし |

### 4.5 即時停止条件

| 条件 | アクション |
|---|---|
| RSS が 500 MB 超で増加継続 | SIGTERM → 調査 |
| 重複 composite key 出現 | SIGTERM → manifest 検証 |
| カーソル逆進 | SIGTERM → checkpoint 検証 |
| E007/E020/E031 | プロセス終了（既存動作） |
| 出力破損（行数≠30, schema 不一致） | SIGTERM → 出力検証 |
| 5 分間進捗なし | SIGTERM → ブロック available 確認 |

---

## 5. Rollback 計画

### 5.1 全体 rollback

1. `git revert <remediation-commit>`
2. 修正前の cron ジョブを再有効化（必要な場合）
3. `data/derived/burst_features_v1_test_p0/` を削除
4. 本番 `data/derived/burst_features_v1/` の checkpoint/manifest は **削除しない**（証拠保全）
5. 問題の checkpoint を `data/derived/burst_features_v1/manifests/checkpoints/<market>.json.bak.512mb` に rename して保全

### 5.2 コンポーネント別 rollback

| コンポーネント | rollback 方法 |
|---|---|
| P0-1 lock | `scripts/acquire-market-lock.sh` 削除。cron から lock 呼び出し除去 |
| P0-2 cursor | `effectiveFromMs` ロジック除去。`fromMs` を直接 `scanTradeBlocks` に渡す |
| P0-3 reconcile | `recovery.mjs` import 除去。`reconcileIntents()` 呼び出し除去 |
| P1-1 checkpoint | `deepCloneClosedBurst` を `serializeBurstBuilderState` に戻す |
| P1-2 prune | `pruneClosedBurstsBefore()` 呼び出し除去 |
| P2 manifest | `_loadManifest` の破損耐性コード除去（元のシンプル版に戻す） |

---

## 6. スコープ外（明示的除外）

- book/RVZ 実装（#13-#22）
- raw 入力削除（`data/live_v3/`）
- 30s/5min 集約層
- マルチスレッド・並列化
- Receiver コード変更
- DuckDB / Parquet 出力
- npm パッケージ追加
- マニフェストの分割・シャーディング（P2 は破損耐性のみ）
- 全面再設計

---

## 7. 参考文書

| 文書 | パス |
|---|---|
| アーキテクチャ設計書 | `docs/specs/design-2026-07-10-burst-reducer.md` |
| 実装計画書 | `docs/specs/plan-2026-07-10-burst-reducer.md` |
| 機能仕様書 | `docs/specs/specify-2026-07-09-burst-features.md` |
| 既存 worklog | `docs/worklog/2026-07-10-burst-reducer-remediation.md` |
| 既存 remediation plan | `docs/specs/plan-2026-07-10-burst-reducer-remediation.md` |
| PDD ストリーミング設計ゲート | `~/.hermes/skills/software-development/pdd/references/streaming-reducer-design-gate.md` |
| PDD パイプライン検証 | `~/.hermes/skills/software-development/pdd/references/streaming-data-pipeline-validation.md` |
| burst-state-codec | `lib/burst-reducer/burst-state-codec.mjs` |
| pipeline | `lib/burst-reducer/pipeline.mjs` |
| output-committer | `lib/burst-reducer/output-committer.mjs` |
| manifest-manager | `lib/burst-reducer/manifest-manager.mjs` |
| block-scanner | `lib/burst-reducer/block-scanner.mjs` |
| burst-detector | `lib/burst-reducer/burst-detector.mjs` |
| burst-builder | `lib/burst-builder.mjs` |
| CLI | `scripts/reduce-burst-v1.mjs` |
| テスト: codec | `test/burst-reducer/burst-state-codec.test.mjs` |
| テスト: pipeline | `test/burst-reducer/pipeline.test.mjs` |
| テスト: golden | `test/burst-reducer/golden.test.mjs` |
| テスト: output-committer | `test/burst-reducer/output-committer.test.mjs` |
| テスト: manifest-manager | `test/burst-reducer/manifest-manager.test.mjs` |

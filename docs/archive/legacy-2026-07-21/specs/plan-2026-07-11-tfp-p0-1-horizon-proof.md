# P0-1「Horizon Proof / Frozen Inventory Validation」実装設計書

- 文書 ID: `plan-2026-07-11-tfp-p0-1-horizon-proof`
- 対象: `agg-btc-receiver`
- 状態: 設計固定（実装前）
- 前提: P0-0 契約 (`specify-2026-07-11-tfp-book-contract-p0.md`) を正本とする
- 関連: `tfp-book-contract-vector-v1.json`、`plan-2026-07-10-burst-reducer-p0-safety.md`
- 実装しない: 本ドキュメントは仕様設計であり、コード変更を行わない

## 0. 現状分析サマリ

| コンポーネント | 現在の状態 | P0-1 での変更要否 |
|---|---|---|
| `scripts/tfp.mjs` | `--finalized-through` / `--frozen-inventory` をパースする。frozen inventory のバリデーションは `Array.isArray()` のみ | **要変更**: inventory schema 検証追加 |
| `lib/burst-reducer/pipeline.mjs` | `checkFinalizedHorizon()` が `block_kind === 'trade'` のハードコード検索。`no-horizon-proof` 単一分岐のみ | **要変更**: kind 別 horizon 判定、3分岐状態モデル、kind-aware pending |
| `lib/burst-reducer/block-scanner.mjs` | `scanTradeBlocks()` のみ。`trades/` ディレクトリをハードコード | **要変更**: `scanBookUpdateBlocks()` 追加 または kind パラメータ化 |
| `lib/burst-reducer/manifest-manager.mjs` | 単一 kind（trade）前提。kind フィールド無し | **変更なし**（P0-1 では kind を manifest に追加しない。P0-2 で対応） |
| `lib/burst-reducer/recovery.mjs` | kind 非対応 | **変更なし**（P0-2 で対応） |
| `lib/burst-reducer/schema.mjs` | kind 概念なし | **軽微変更**: kind enum 定数追加 |
| テスト | `horizon.test.mjs` は trade-only 前提。frozen inventory のテスト無し | **テスト追加**: kind 別 horizon テストケース |

## (A) 実装設計書

### 1. `--finalized-through` オプションの意味確定

#### 1.1 CLI 引数

`scripts/tfp.mjs` の既存 `--finalized-through <ISO>` のセマンティクスを明文化:

```text
--finalized-through <ISO>
  30s-aligned exclusive boundary。UTC ISO 8601 形式。
  このタイムスタンプ（epoch ms）は次の意味を持つ:
    - live モード: Receiver が「ここまでの raw ブロックは確定済み」と宣言した境界
    - backfill モード: frozen inventory と併用時、inventory で宣言された最終ブロックの次境界
```

**exclusive 境界の定義**: `finalizedThroughMs` は「この時刻より前のブロックは確定している」の exclusive upper bound。
- ブロック B（`block_start_ms = 30000`, `block_end_ms = 60000`）は `finalizedThroughMs > 60000` のとき確定
- `finalizedThroughMs === 60000` は B の直後境界 → B が EOF 対象
- `finalizedThroughMs < 60000` は B が horizon 外 → `not-yet-arrived`

#### 1.2 live / backfill 両方での意味

| モード | `--finalized-through` | `--frozen-inventory` | 意味 |
|---|---|---|---|
| live | 指定あり | 指定なし | `finalizedThroughMs` が権威。この時刻までに存在するべきブロックを確定。範囲外は `not-yet-arrived` |
| live | 指定なし | 指定なし | horizon proof 無し。全 pending は `no-horizon-proof` で blocked |
| backfill | 指定あり | 指定あり | inventory が権威。`finalizedThroughMs` は scan の上限としてのみ機能 |
| backfill | 指定なし | 指定あり | inventory が権威。inventory の最終ブロック + 30000 を implicit finalizedThrough とする |

**設計判断**: `--frozen-inventory` と `--finalized-through` を同時指定した場合、inventory が優先される（P0-0 §6.2「frozen inventory が列挙した block だけを処理対象とする」に従う）。`finalizedThroughMs` は inventory 外のブロックが `not-yet-arrived` かどうかの判定に使われる。

#### 1.3 30s aligned の検証

既存 `is30sAligned()` と `E040` を維持。追加で frozen inventory 内の全 `block_start_ms` も 30s-aligned であることを検証する。

### 2. Frozen Inventory Loader/Validator

#### 2.1 JSON Schema

frozen inventory ファイルの正規形:

```json
{
  "schema_version": "frozen_inventory_v1",
  "mode": "backfill",
  "frozen": true,
  "blocks": [
    {
      "market": "binance_spot",
      "kind": "trades",
      "block_start_ms": 1752192000000,
      "path": "trades/binance_spot/2026-07-11/00-00-00.jsonl",
      "sha256": "abc123..."
    },
    {
      "market": "binance_spot",
      "kind": "book_updates",
      "block_start_ms": 1752192000000,
      "path": "book_updates/binance_spot/2026-07-11/00-00-00.jsonl",
      "sha256": "def456..."
    }
  ]
}
```

#### 2.2 検証ステップ（loadFrozenInventory 拡張）

`scripts/tfp.mjs` の `loadFrozenInventory()` を以下の検証を含むよう拡張:

1. **トップレベル型**: 配列または `{blocks: [...]}` オブジェクト。配列の場合は P0-0 互換として受け入れ（後方互換）。オブジェクトの場合は `blocks` 配列を取り出す。
2. **必須フィールド（各 block entry）**:
   - `market` (string): 空文字列禁止
   - `kind` (string): `"trades"` または `"book_updates"` のみ許可
   - `block_start_ms` (number): 30s-aligned であること（`% 30000 === 0`）
   - `path` (string): `trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl` または `book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl` の形式
   - `sha256` (string): 64 文字の hex 文字列、または空許可（hash 未計算を示す）
3. **整合性チェック**:
   - `path` から抽出される market が entry の `market` と一致する
   - `path` から抽出される `block_start_ms` が entry の `block_start_ms` と一致する
   - `path` の先頭が `entry.kind` と一致する（`trades/...` or `book_updates/...`）
4. **重複検出**: 同一 `(market, kind, block_start_ms)` の重複エントリは FATAL
5. **結果**: 検証済みの `Map<kind, Map<market, Map<block_start_ms, entry>>>` 構造を返す

#### 2.3 kind の列挙型定義

`lib/burst-reducer/schema.mjs` に追加:

```js
export const INPUT_KIND = {
  TRADES: 'trades',
  BOOK_UPDATES: 'book_updates',
};
export const VALID_INPUT_KINDS = new Set([INPUT_KIND.TRADES, INPUT_KIND.BOOK_UPDATES]);
```

### 3. Block Scanner の Book Updates 対応

#### 3.1 スキャン関数の一般化

現在の `scanTradeBlocks(dataDir, market, fromMs, toMs)` は `dataDir/trades/<market>` をハードコードしている。

**設計**: `scanTradeBlocks` の内部ロジックを抽出し、kind を受け取る汎用関数にリファクタ:

```js
// 新規: kind をパラメータ化した汎用スキャン
export function scanBlocks(dataDir, kind, market, fromMs, toMs) {
  const kindDir = join(dataDir, kind, market);  // kind = 'trades' | 'book_updates'
  // ... 以下既存 scanTradeBlocks と同一ロジック
}

// 後方互換ラッパー（既存呼び出し元を壊さない）
export function scanTradeBlocks(dataDir, market, fromMs, toMs) {
  return scanBlocks(dataDir, 'trades', market, fromMs, toMs);
}

// 新規
export function scanBookUpdateBlocks(dataDir, market, fromMs, toMs) {
  return scanBlocks(dataDir, 'book_updates', market, fromMs, toMs);
}
```

#### 3.2 返り値の型

`BlockInfo` は変更しない（`{ ms, fullPath, market, date }`）。呼び出し元が kind を知っているため、`BlockInfo` に kind フィールドは追加しない。

#### 3.3 制約

- `book_updates` 用のスキャンも trades と同一の 30s block range セマンティクス
- `fileMs < toMs && fileMs + 30000 > fromMs` の範囲判定は共通
- E006（00/30 境界チェック）は共通

### 4. Pipeline の Finalized Horizon 拡張

#### 4.1 `checkFinalizedHorizon()` の kind 別再設計

既存の `checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market)` を kind-aware に拡張:

**新シグネチャ**:
```js
function checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market, kind)
```

**3 分岐状態モデル**（P0-0 §13.4 契約に基づく）:

| pending 状態 | 条件 | commit | cursor | quarantine | 説明 |
|---|---|---|---|---|---|
| `no-horizon-proof` | finalizedThroughMs === null かつ frozenInventory === null | false | retain | false | いかなる horizon proof も存在しない |
| `verified-missing` | horizon/inventory 内で次のブロックが欠落（kind=book_updates）| false | retain | true | 権威のある horizon 内でファイル消失→quarantine |
| `not-yet-arrived` | 次のブロック境界が horizon 外 | false | retain | false | まだ到着していないだけ。エラーではない |

**kind 別の分岐ロジック**:

```
checkFinalizedHorizon(pendingBlock, finalizedThroughMs, frozenInventory, dataDir, market, kind):
  if pendingBlock === null: return { canFinalize: false, state: 'no-pending', ... }

  nextBoundary = pendingBlock.block_start_ms + BLOCK_DURATION_MS

  // ── frozen inventory がある場合 ──
  if frozenInventory !== null:
    pendingEntry = frozenInventory.find(market, kind, pendingBlock.block_start_ms)
    nextEntry    = frozenInventory.find(market, kind, nextBoundary)

    if pendingEntry === null:
      return { canFinalize: false, state: 'verified-missing', reason: 'pending-not-in-inventory' }
    // hash 検証（sha256 が宣言されている場合のみ）
    if pendingEntry.sha256 && pendingEntry.sha256 !== actualSha:
      return { canFinalize: false, state: 'hash-mismatch', quarantine: true }

    if nextEntry === null:
      // inventory が pending で終端 → EOF 可能
      return { canFinalize: true, state: 'frozen-inventory-boundary', ... }
    // nextEntry が存在 → まだ EOF 不可
    return { canFinalize: false, state: 'next-block-in-inventory', ... }

  // ── finalizedThroughMs がある場合 ──
  if finalizedThroughMs !== null:
    if nextBoundary < finalizedThroughMs:
      // 次の境界が horizon 内
      if fileExists(nextBoundary, kind):
        return { canFinalize: false, state: 'next-block-exists', ... }
      else:
        // 次の境界は horizon 内だがファイルが無い
        if kind === 'trades':
          return { canFinalize: true, state: 'data-none-gap', ... }    // ASSUMED_EMPTY_GAP
        else:  // kind === 'book_updates'
          return { canFinalize: false, state: 'verified-missing', quarantine: true }

    if nextBoundary === finalizedThroughMs:
      return { canFinalize: true, state: 'finalized-through-boundary', ... }

    if pendingBlock.block_start_ms >= finalizedThroughMs:
      return { canFinalize: true, state: 'pending-at-horizon', ... }

    return { canFinalize: false, state: 'not-yet-arrived', ... }

  // ── horizon proof なし ──
  return { canFinalize: false, state: 'no-horizon-proof', ... }
```

#### 4.2 `runPipeline()` への kind パラメータ追加

`runPipeline()` のシグネチャに `kind = 'trades'` パラメータを追加。kind が `'book_updates'` の場合:

1. `scanBlocks(dataDir, 'book_updates', market, fromMs, toMs)` を使用
2. `checkFinalizedHorizon()` に `kind='book_updates'` を渡す
3. ASSUMED_EMPTY_GAP の代わりに verified-missing quarantine
4. バリデーションは `validateAndParseTrades` ではなく book 用の validator（P0-0 で定義される BookStateMachine）を使う（ただし P0-1 では book バリデーションの実装は scope 外）

#### 4.3 BLOCKED 出力の拡張

既存の BLOCKED structured log に `kind` と `blocked_state` を追加:

```json
{
  "level": "BLOCKED",
  "processed": 0,
  "blocked_reason": "not-yet-arrived",
  "blocked_state": "not-yet-arrived",
  "kind": "trades",
  "market": "binance_spot",
  "cursor_ms": 1752192000000,
  "expected_block_start_ms": 1752192030000
}
```

3 状態は `blocked_state` フィールドで区別:
- `"no-horizon-proof"` — 終端到達 + 証明なし
- `"verified-missing"` — 権威内欠落 → quarantine
- `"not-yet-arrived"` — 未到着

#### 4.4 Manifest Commit 制御

`verified-missing` 状態のブロックは:
- **feature shard を生成しない**（commit しない）
- **manifest に quarantine レコードを書き込む**
- **checkpoint cursor を進めない**（pending を保持）
- **quarantine report を生成する**: `<derivedDir>/quarantine/<market>/<block_start_ms>.json`
- reason code: `MISSING_FINALIZED_INPUT`

この制御は `processBlocks()` の EOF セクション（L497-566）で、`horizon.state === 'verified-missing'` の場合の分岐を追加することで実現する。

### 5. P0-0 契約整合

#### 5.1 ASSUMED_EMPTY_GAP の trade-only 維持（明示的ガード）

現在のコード（pipeline.mjs L286-361）では gap 検出時に `ASSUMED_EMPTY_GAP` を発行している。P0-1 では:

- `kind === 'trades'` の場合: 既存の ASSUMED_EMPTY_GAP ロジックを維持。gap 内の不在ブロックは zero contribution として commit 可能。
- `kind === 'book_updates'` の場合: gap 検出時は ASSUMED_EMPTY_GAP を発行せず、`verified-missing` として quarantine。commit しない。

**実装ガード**（pipeline.mjs `processBlocks()` の gap branch）:

```js
if (candidateBlock.ms > expectedNext) {
  if (kind === 'book_updates') {
    // book_updates gap → verified-missing quarantine, cursor retain
    writeQuarantineReport(derivedDir, market, pendingBlock.block_start_ms,
      'MISSING_FINALIZED_INPUT',
      { gap_from: expectedNext, gap_to_exclusive: candidateBlock.ms, kind });
    emitStructured({ level: 'VERIFIED_MISSING', market, kind,
      block_start_ms: pendingBlock.block_start_ms,
      gap_range: { start_ms: expectedNext, end_ms_exclusive: candidateBlock.ms } });
    // cursor は進めず、pending を保持したまま return
    return { processed, errors, manifestUpdates: [], blocked: true,
      blockedReason: 'verified-missing', blockedState: 'verified-missing' };
  }
  // kind === 'trades': 既存の ASSUMED_EMPTY_GAP ロジック
  // ...
}
```

#### 5.2 book_updates verified-missing quarantine

P0-0 §13.4 の状態遷移表に従い、`verified-missing` は:
- commit: false
- cursor: retain
- quarantine: yes

quarantine record の形式:
```json
{
  "contract_version": "tfp_book_contract_v1",
  "market": "binance_spot",
  "kind": "book_updates",
  "block_start_ms": 1752192000000,
  "reason_code": "MISSING_FINALIZED_INPUT",
  "reason": "declared in frozen inventory but file absent",
  "raw_input_paths": ["book_updates/binance_spot/2026-07-11/00-00-00.jsonl"],
  "raw_sha256": null,
  "ts": "2026-07-11T10:00:00.000Z"
}
```

#### 5.3 P0-0 状態遷移表との整合確認

| P0-0 状態 | P0-1 での実装箇所 | kind 条件 |
|---|---|---|
| `assumed-empty-gap` | `processBlocks()` gap branch, trades only | kind=trades |
| `valid-empty` | 既存ロジック（変更なし） | any |
| `verified-missing` | `checkFinalizedHorizon()` + `processBlocks()` gap branch | kind=book_updates or default |
| `not-yet-arrived` | `checkFinalizedHorizon()` horizon 外判定 | any |
| `no-horizon-proof` | `checkFinalizedHorizon()` 証明なし | any |

### 6. テスト方針

#### 6.1 単体テスト (`test/burst-reducer/horizon.test.mjs` 拡張)

| テスト ID | 内容 | kind |
|---|---|---|
| HORIZON-001 | `--finalized-through` なし → `no-horizon-proof` blocked | trades |
| HORIZON-002 | `--finalized-through` あり、全ブロック horizon 内 → 正常 EOF | trades |
| HORIZON-003 | `--finalized-through` あり、horizon 境界で EOF | trades |
| HORIZON-004 | `--frozen-inventory` あり、inventory 終端で EOF | trades |
| HORIZON-005 | frozen inventory 内のブロック欠落 → `verified-missing` quarantine | book_updates |
| HORIZON-006 | frozen inventory で hash 不一致 → `hash-mismatch` quarantine | trades |
| HORIZON-007 | frozen inventory で kind 不一致 → `kind-mismatch` quarantine | book_updates |
| HORIZON-008 | kind=trades の gap → ASSUMED_EMPTY_GAP で commit | trades |
| HORIZON-009 | kind=book_updates の gap → `verified-missing` quarantine、commit しない | book_updates |
| HORIZON-010 | 未宣言ブロックが存在 → `undeclared-present` quarantine | book_updates |
| HORIZON-011 | inventory 空 → 全ブロック `no-horizon-proof` | any |
| HORIZON-012 | `--finalized-through` misaligned → E040 | any |
| HORIZON-013 | frozen inventory 内の `block_start_ms` が 30s-aligned でない → FATAL | any |
| HORIZON-014 | 同一 `(market, kind, block_start_ms)` 重複 → FATAL | any |

#### 6.2 統合テスト

- `scripts/tfp.mjs --kind book_updates --frozen-inventory <path>` のエンドツーエンド
- `block-scanner.mjs` の `scanBlocks(dataDir, 'book_updates', ...)` が正しいファイルを返すこと
- manifest に quarantine レコードが正しく書き込まれること

#### 6.3 テストデータ

`test/fixtures/burst-v1/` 以下に以下を追加:

```
tmp-horizon-p0-1/
├── trades/test_p0_1/1970-01-01/
│   ├── 00-00-00.jsonl
│   └── 00-00-30.jsonl
├── book_updates/test_p0_1/1970-01-01/
│   ├── 00-00-00.jsonl
│   └── 00-01-00.jsonl
└── frozen_inventory_valid.json
└── frozen_inventory_missing.json
└── frozen_inventory_hash_mismatch.json
└── frozen_inventory_kind_mismatch.json
```

#### 6.4 非回帰テスト

既存の `horizon.test.mjs` が全てパスすることを確認（kind パラメータのデフォルト `'trades'` により後方互換を確保）。

### 7. 範囲外明示

以下は P0-1 の範囲外である:

1. **book_updates の実際のパース・バリデーション・state 適用**: P0-1 は horizon proof の kind 別判定まで。book ブロックの中身の処理（BookStateMachine、sequence gap 検出、board MVP 計算）は P0-2 以降。
2. **book_updates の feature 計算・output shard 生成**: P0-2 以降。
3. **manifest スキーマへの kind フィールド追加**: manifest レコードに `kind` を追加するのは P0-2。P0-1 では quarantine レコードにのみ kind を含める。
4. **checkpoint への kind フィールド追加**: P0-2。
5. **recovery.mjs の kind 対応**: P0-2。P0-1 では recovery は trade-only のまま。
6. **live モードでの book_updates 受信・処理**: P0-0 で明示されている通り、book_updates の production 対応は P0-0 完了条件に含まれない。
7. **30s/5min 集約・rollup の kind 対応**: 別フェーズ。
8. **adapter 層（connector event → book_updates_v1 envelope 変換）**: P0-0 で定義されているが、P0-1 では実装しない。
9. **multi-market simultaneous backfill の並列化**: P0-1 は single-market 前提。multi-market の同時 backfill は cron スクリプト側で対応。
10. **`--finalized-through` の自動計算（Receiver からの取得）**: CLI 手動指定のみ。

---

## (B) 各変更ファイル修正点一覧

### B-1. `lib/burst-reducer/schema.mjs`

| 修正 | 内容 |
|---|---|
| 追加 | `INPUT_KIND` 定数オブジェクト (`{ TRADES: 'trades', BOOK_UPDATES: 'book_updates' }`) |
| 追加 | `VALID_INPUT_KINDS` Set |
| 追加 | `BLOCK_DURATION_MS` 定数（pipeline.mjs から移動し一元化） |

### B-2. `lib/burst-reducer/block-scanner.mjs`

| 修正 | 内容 |
|---|---|
| リファクタ | `scanTradeBlocks()` のコアロジックを `scanBlocks(dataDir, kind, market, fromMs, toMs)` に抽出 |
| 追加 | `scanTradeBlocks(dataDir, market, fromMs, toMs)` → `scanBlocks(dataDir, 'trades', ...)` のラッパー（後方互換） |
| 追加 | `scanBookUpdateBlocks(dataDir, market, fromMs, toMs)` → `scanBlocks(dataDir, 'book_updates', ...)` のラッパー |
| 変更 | `tradesDir` → `join(dataDir, kind, market)` に一般化 |

### B-3. `scripts/tfp.mjs`

| 修正 | 内容 |
|---|---|
| 追加 | `--kind <trades|book_updates>` オプション（デフォルト: `trades`） |
| 変更 | `loadFrozenInventory()` → `loadAndValidateFrozenInventory()` にリネーム・拡張 |
| 追加 | `validateInventoryEntry(entry)` 関数: market/kind/block_start_ms/path/sha256 の検証 |
| 追加 | `validateInventoryCrossReferences(entries)` 関数: 重複・整合性チェック |
| 追加 | 検証済み inventory の構造化: `{ byKindAndMarket: Map, entries: Array, errors: Array }` |
| 変更 | `runPipeline()` 呼び出しに `kind` パラメータを追加 |
| 変更 | `detectMarkets()` → `detectMarkets(dataDir, kind)` に kind パラメータ追加 |

### B-4. `lib/burst-reducer/pipeline.mjs`

| 修正 | 内容 |
|---|---|
| 変更 | `runPipeline()` シグネチャに `kind = 'trades'` 追加 |
| 変更 | `checkFinalizedHorizon()` シグネチャに `kind` 追加、返り値に `state` フィールド追加 |
| 追加 | `checkFinalizedHorizon()` 内の kind 別分岐（trades ASSUMED_EMPTY_GAP vs book_updates verified-missing） |
| 追加 | `checkFinalizedHorizon()` の frozen inventory 探索で kind 一致チェック |
| 追加 | `checkFinalizedHorizon()` の返り値に `state: 'no-horizon-proof' | 'verified-missing' | 'not-yet-arrived' | ...` |
| 変更 | `processBlocks()` gap branch（L286-361）に kind ガード追加: `kind === 'book_updates'` → quarantine return |
| 変更 | `processBlocks()` EOF section（L497-566）で `horizon.state === 'verified-missing'` の分岐追加 |
| 変更 | `emitStructured({ level: 'BLOCKED', ... })` に `kind` と `blocked_state` 追加 |
| 追加 | `writeQuarantineReport()` に `kind` フィールド追加 |
| 変更 | `scanTradeBlocks()` 呼び出し → `scanBlocks(dataDir, kind, market, ...)` |

### B-5. `test/burst-reducer/horizon.test.mjs`（新規追加分）

| 修正 | 内容 |
|---|---|
| 追加 | frozen inventory 用テストフィクスチャ |
| 追加 | HORIZON-005 ~ HORIZON-014 のテストケース |
| 追加 | kind='book_updates' のテストケース |
| 追加 | inventory validation エラーケース |

---

## (C) 受入条件

### C-1. Frozen Inventory 検証

- [ ] `--frozen-inventory` で指定された JSON ファイルが schema に従っていることを検証する
- [ ] 各エントリの `market`, `kind`, `block_start_ms`, `path`, `sha256` の存在・型・整合性をチェックする
- [ ] `kind` が `"trades"` または `"book_updates"` であることをチェックする
- [ ] `block_start_ms` が 30s-aligned であることをチェックする
- [ ] `path` から抽出される market と `block_start_ms` がエントリの値と一致することをチェックする
- [ ] 同一 `(market, kind, block_start_ms)` の重複エントリを FATAL エラーとする
- [ ] 不正な inventory は明確なエラーメッセージで exit code 1

### C-2. kind 別 Horizon 判定

- [ ] `kind='trades'` の場合、`checkFinalizedHorizon()` が既存と同一の挙動を示す（後方互換）
- [ ] `kind='book_updates'` の場合、`verified-missing` が quarantine を返し commit しない
- [ ] `kind='book_updates'` の gap が ASSUMED_EMPTY_GAP として処理されない
- [ ] `--finalized-through` なし + `--frozen-inventory` なし → `no-horizon-proof` blocked
- [ ] `--finalized-through` あり、horizon 内でファイル不在 + kind=book_updates → `verified-missing` quarantine
- [ ] `--finalized-through` あり、horizon 外 → `not-yet-arrived` blocked（quarantine ではない）

### C-3. Block Scanner 共通化

- [ ] `scanBlocks(dataDir, 'trades', ...)` が既存の `scanTradeBlocks()` と同一結果を返す
- [ ] `scanBlocks(dataDir, 'book_updates', ...)` が `dataDir/book_updates/<market>/...` を正しくスキャンする
- [ ] E006（00/30 境界チェック）が両 kind で機能する

### C-4. CLI 互換性

- [ ] 既存の `node scripts/tfp.mjs --from ... --to ...` が kind 未指定時に trades として動作する
- [ ] `--kind book_updates` を指定した場合、book_updates ディレクトリをスキャンする
- [ ] `--help` が新しいオプションを表示する

### C-5. Pipeline 出力

- [ ] BLOCKED structured log に `kind` と `blocked_state` が含まれる
- [ ] `verified-missing` 時に quarantine report が `<derivedDir>/quarantine/<market>/<block_start_ms>.json` に生成される
- [ ] quarantine report に `kind`, `reason_code: "MISSING_FINALIZED_INPUT"` が含まれる
- [ ] `verified-missing` 時に manifest commit と checkpoint advancement が行われない

### C-6. テスト

- [ ] 既存の `horizon.test.mjs` 全テストがパスする（後方互換）
- [ ] 新規 horizon テスト 14 ケースが全てパスする
- [ ] frozen inventory バリデーションのテストが全パターンをカバーする

### C-7. P0-0 契約との整合

- [ ] fixture `tfp-book-contract-vector-v1.json` の `frozen_inventory_expectation` で定義された期待値と一致する
- [ ] fixture の `commit_cursor_contract` の `no_horizon_proof` セマンティクスを満たす
- [ ] fixture の `assumed-empty-gap-trade-only` ケースの trade/book 差分を正しく処理する
- [ ] P0-0 §13.4 の状態遷移表の全状態を実装する

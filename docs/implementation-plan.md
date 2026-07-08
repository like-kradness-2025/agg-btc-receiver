# 実装計画: 1s Features Parquet→JSONL 移行

## 戦略

**段階的アプローチ**: 1フェーズ=1論理変更+テスト確認。
依存関係のあるフェーズ以外は並行可能。各フェーズ完了後に `npm test` で回帰確認。

**依存グラフ**:
```
Phase 1 (bugfix) ──→ Phase 2 (FE内部) ──→ Phase 3 (FE cleanup)
                                                  ↓
Phase 4 (aggregate) ←─────────────────────────────┘
     ↓
Phase 5 (integration)
     ↓
Phase 6 (docs)
```

---

## Phase 0: 準備

**目的**: 作業開始前の状態を記録、常駐を停止

- [ ] 現在の常駐を停止: `bash scripts/start-fairprice.sh stop`
- [ ] カレントブランチの状態を記録: `git log --oneline -3`
- [ ] 念のため `data/1s_features/` のバックアップ（確認用）

**検証**: 停止確認 `screen -ls | grep fairprice`

---

## Phase 1: 既存バグ修正（4件）

**目的**: ストレージ変更とは独立したバグ修正。最小リスク。

### 1a. `_lastL1.bidPrice` バグ (feature-accumulator.mjs:440)

```patch
- bidPrice: bidPrice !== null ? askPrice : null,
+ bidPrice: bidPrice !== null ? bidPrice : null,
```
→ 条件式は bidPrice をチェックしているのに代入値が askPrice。正しく bidPrice を代入。

### 1b. `best_deplete_count` プロパティ名 (feature-accumulator.mjs:417)

```patch
- best_deplete_count: (flow?.bidDeplete ?? 0) + (flow?.askDeplete ?? 0),
+ best_deplete_count: (flow?._depleteBid ?? 0) + (flow?._depleteAsk ?? 0),
```
→ flow オブジェクトの実際のプロパティは `_depleteBid` / `_depleteAsk`（lines 196,277,281）。

### 1c. `close()` 順序バグ (feature-accumulator.mjs:587-591)

```patch
-   this._closed = true;
-   const flushed = await this.flush();
+   const flushed = await this.flush();
+   this._closed = true;
```
→ `_closed = true` を先に立てると `flush()` が即0を返す。

### 1d. `feedSecond()` 未使用引数 (feature-accumulator.mjs:303)

```patch
- feedSecond(market, second, book, picked, fp) {
+ feedSecond(market, second, book, picked) {
```
→ `fp` (fairPrice/fair選定結果) は関数内で一度も参照されていない。

### 検証

- [ ] `npm test` 全件PASS
- [ ] `node --check lib/feature-accumulator.mjs` 構文OK

---

## Phase 2: FeatureAccumulator 内部変更（JSONL追記移行）

**目的**: Flush先を DuckDB Parquet から BufferedWriter JSONL に変更。
**影響範囲**: `lib/feature-accumulator.mjs` のみ。

### 2a. `utcDateStr` 追加

```js
function utcDateStr(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}
```
（fair-price-collector.mjs:52 からコピー。共通化は後日）

### 2b. `BufferedWriter` import + Writer キャッシュ追加

```js
import { BufferedWriter } from './buffered-writer.mjs';

// constructor に追加:
this._writers = new Map();
this._currentDate = null;
```

### 2c. `_getWriter(market, dateStr)` 追加

Writer キャッシュのlazy creation。既存の BufferedWriter パターンを踏襲。

```js
_getWriter(market, dateStr) {
  const key = `${dateStr}/${market}`;
  let w = this._writers.get(key);
  if (!w) {
    const dir = path.join(this._outputBase, dateStr);
    fs.mkdirSync(dir, { recursive: true });
    w = new BufferedWriter(path.join(dir, `${market}.jsonl`), {
      flushIntervalMs: 1000,
      idleCloseMs: 120000, // >60s flush interval avoids per-flush reopen
    });
    this._writers.set(key, w);
  }
  return w;
}
```

### 2d. `_ensureDate(dateStr)` 追加

日付パーティション変更時に旧 writer を close + 削除。
FairPriceCollector._ensureDate のパターンを踏襲。

```js
async _ensureDate(dateStr) {
  if (dateStr === this._currentDate) return;
  if (this._currentDate !== null) {
    const oldDate = this._currentDate;
    this._currentDate = dateStr;
    const promises = [];
    for (const [key, w] of this._writers) {
      if (key.startsWith(oldDate + '/')) promises.push(w.close());
    }
    await Promise.allSettled(promises);
    for (const key of this._writers.keys()) {
      if (key.startsWith(oldDate + '/')) this._writers.delete(key);
    }
  } else {
    this._currentDate = dateStr;
  }
}
```

### 2e. `flush()` 書き換え

DuckDB を使わず、BufferedWriter で JSONL 追記。

```js
async flush() {
  if (this._closed) return 0;
  let totalRows = 0;
  const byDate = new Map();

  for (const [market, rows] of this._buffers) {
    if (rows.size === 0) continue;
    for (const [second, row] of rows) {
      const dateStr = utcDateStr(new Date(second));
      if (!byDate.has(dateStr)) byDate.set(dateStr, new Map());
      if (!byDate.get(dateStr).has(market)) byDate.get(dateStr).set(market, []);
      byDate.get(dateStr).get(market).push(row);
      totalRows++;
    }
  }

  const sortedDates = Array.from(byDate.entries()).sort(([a], [b]) => a.localeCompare(b));
  for (const [dateStr, markets] of sortedDates) {
    await this._ensureDate(dateStr);
    for (const [market, rows] of markets) {
      const writer = this._getWriter(market, dateStr);
      for (const row of rows) {
        await writer.write(row);
      }
    }
  }

  this._buffers.clear();
  this._lastFlush = Date.now();
  return totalRows;
}
```

### 2f. `close()` 書き換え

```js
async close() {
  const flushed = await this.flush();
  this._closed = true;
  for (const w of this._writers.values()) await Promise.allSettled([w.close()]);
  this._writers.clear();
  return flushed;
}
```

### 2g. DuckDB import 削除 + 不要コード削除

- `import duckdb from 'duckdb'` 削除
- `q(db, sql)` / `e(db, sql)` ラッパー削除（他のモジュールで使ってないか確認）
- `_bpsLevels` / `_flushIntervalMs` は内部ロジックで引き続き使用（保持）

### 検証

- [ ] `node --check lib/feature-accumulator.mjs` 構文OK
- [ ] `npm test` 全件PASS
- [ ] fair-price-collector の変更なし（import interface が変わっていないことを確認）

---

## Phase 3: DuckDB 依存完全削除

**目的**: FeatureAccumulator から DuckDB 関連コードを一掃。

### 3a. 残存DuckDB参照を確認

- `feature-accumulator.mjs` から `import duckdb` を検索 → 残ってないか確認
- `feature-accumulator.mjs` から `lib/feature-accumulator.mjs:q(` / `lib/feature-accumulator.mjs:e(` を検索 → 残ってないか確認

### 3b. `fair-price-collector.mjs` への影響確認

`fair-price-collector.mjs` は DuckDB を直接使っていないことを確認。
FeatureAccumulator のインターフェース（`feedTrade`, `feedDepth`, `feedSecond`, `flush`, `close`）は変更なし。

### 検証

- [ ] `node --check lib/feature-accumulator.mjs` 構文OK
- [ ] `npm test` 全件PASS
- [ ] grep duckdb で該当ファイルからDuckDB参照が消えたことを確認

---

## Phase 4: aggregate-1s.mjs 変更

**目的**: 読み取り元を Parquetディレクトリ → JSONL全パーティション に変更。

### 4a. 定数変更

```js
const FEATURES_BASE = 'data/1s_features';
// (AGG_DIR, LOOKBACK_MS は変更なし)
```

### 4b. market 探索ロジック変更

```js
// Before: ls data/1s_features/{market}/*.parquet
// After: ls data/1s_features/{YYYY-MM-DD}/{market}.jsonl

function discoverFeatureFiles(market) {
  const partitions = fs.readdirSync(FEATURES_BASE)
    .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort();
  const paths = [];
  for (const dateStr of partitions) {
    const p = path.join(FEATURES_BASE, dateStr, `${market}.jsonl`);
    if (fs.existsSync(p)) paths.push(p);
  }
  return paths;
}
```

### 4c. SQL読み取り変更

```js
// Before:
// CREATE TABLE new_rows AS SELECT ... FROM read_parquet([files])

// After: UNION ALL
const unionSql = srcPaths.map(p =>
  `SELECT * FROM read_json_auto('${p.replace(/'/g, "''")}')`
).join(' UNION ALL ');
await e(db, `CREATE TABLE new_rows AS SELECT ${selectCols} FROM (${unionSql}) WHERE ts >= ${cutoff}`);
```

### 4d. メインループ変更

`processMarket()` の market 発見方法を変更:
- Before: `fs.readdirSync('data/1s_features/')` → market ディレクトリ
- After: `fs.readdirSync('data/1s_features/{date}')` → JSONL ファイル → market名抽出

```js
// 全パーティションから market 一覧を収集
function discoverMarkets() {
  const markets = new Set();
  const partitions = fs.readdirSync(FEATURES_BASE)
    .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d));
  for (const dateStr of partitions) {
    const dir = path.join(FEATURES_BASE, dateStr);
    for (const f of fs.readdirSync(dir)) {
      if (f.endsWith('.jsonl')) markets.add(f.replace('.jsonl', ''));
    }
  }
  return [...markets];
}
```

### 検証

- [ ] `node --check scripts/aggregate-1s.mjs` 構文OK
- [ ] `npm test` 全件PASS
- [ ] モックデータで手動動作確認

---

## Phase 5: 統合テスト（ライブ実走）

**目的**: 全パイプラインが正しく動作することを確認。

### 5a. クリーンスタート

```bash
rm -rf data/1s_features/  # 旧Parquet全削除
rm -f data/agg/*.parquet   # agg再構築用
```

### 5b. 常駐起動

```bash
bash scripts/start-fairprice.sh stop  # 念のため
bash scripts/start-fairprice.sh start
sleep 90  # 最少2回のflushを待つ
```

### 5c. JSONL 出力確認

```bash
ls -la data/1s_features/$(date -u +%Y-%m-%d)/
wc -l data/1s_features/$(date -u +%Y-%m-%d)/binance_spot.jsonl
```

→ 60行以上あること、JSONLの各行がvalid JSONであること

### 5d. trade データ確認（feedTrade修正の検証も兼ねる）

```bash
# trade_count > 0 の行があることを確認
node -e "
import duckdb from 'duckdb';
const db = new duckdb.Database(':memory:');
const p = 'data/1s_features/$(date -u +%Y-%m-%d)/binance_spot.jsonl';
db.all(\"SELECT count(*) as c FROM read_json_auto('\" + p + \"') WHERE trade_count > 0\", (e,r) => {
  console.log('rows with trades:', r[0].c);
  db.close();
});
"
```

### 5e. aggregate 動作確認

```bash
node scripts/aggregate-1s.mjs
ls -la data/agg/binance_spot.parquet
```

→ aggファイルが生成され、68列の新スキーマであること

### 5f. `npm test` 全件PASS

### 検証

- [ ] JSONL出力確認（日付パーティション + market単位）
- [ ] tradeデータがFeatureAccumulatorに入っている（trade_count > 0）
- [ ] aggregate正常動作
- [ ] npm test 全件PASS

---

## Phase 6: ドキュメント更新

### 6a. `docs/1s-features-schema.md`

- 出力形式を Parquet → JSONL に更新
- 出力パスを `data/1s_features/{YYYY-MM-DD}/{market}.jsonl` に更新
- 列名不一致修正: `bid_depth_1bps` → `bid_1bps`

### 6b. `docs/jsonl-append-redesign.md` 完了チェック

- 完了条件の全チェックボックスを ✅ に

### 検証

- [ ] docsの整合性確認（git diff）

---

## Phase 7: 常駐再開

```bash
bash scripts/start-fairprice.sh restart
bash scripts/start-fairprice.sh status
```

---

## リスクと対策

| リスク | 確率 | 影響 | 対策 |
|--------|------|------|------|
| feedTrade 接続漏れ再発 | 低 | 高 | Phase 5d で trade_count > 0 を確認 |
| BufferedWriter の O_APPEND 競合 | 低 | 中 | 60s cron + O_APPEND で実害なし。長期稼働で確認 |
| DuckDB依存削除漏れ | 低 | 中 | Phase 3a で grep 確認 |
| midnight 日付跨ぎ不具合 | 低 | 低 | ts基準パーティションで対応済み。Phase 5 で確認 |

## 完了条件

- [ ] npm test 252/252 PASS
- [ ] JSONL追記: 全marketで日付パーティション下に生成
- [ ] tradeデータが集約に含まれる
- [ ] aggregate: JSONL読み取り→68列agg Parquet出力
- [ ] ファイル数: 17market × N日分のみ（Parquet量産消滅）

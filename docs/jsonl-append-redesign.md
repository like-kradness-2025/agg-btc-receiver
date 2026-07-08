# 1s Features Storage: Parquet→JSONL 日付パーティション追記 再設計

## 背景

現在の 1s_features は60秒ごとにParquetファイルを量産（17market × 1440回/日 = 24,480ファイル/日）。
集約データは~30MB/日/全marketと十分に圧縮されており、このファイル分割はオーバーキル。
普通にJSONL追記に戻す。

## 変更内容

### Before

```
FeatureAccumulator
  → 60秒ごとに DuckDB flush
  → data/1s_features/{market}/{YYYYMMDD}_{HHmmss}.parquet
  → 17market × 1440ファイル/日
```

### After

```
FeatureAccumulator
  → flush時に BufferedWriter で追記
  → data/1s_features/{YYYY-MM-DD}/{market}.jsonl （日付パーティション + marketごと）
  → 17ファイル/日付パーティション、ファイル数は有限
```

### 変更ファイル

| ファイル | 変更内容 |
|---------|---------|
| `lib/feature-accumulator.mjs` | DuckDB flush 削除 → BufferedWriter JSONL追記に置き換え |
| `scripts/aggregate-1s.mjs` | 読み取り元を Parquetディレクトリ → JSONL全パーティションに変更 |
| `docs/1s-features-schema.md` | 出力形式を更新 |

## 設計判断

### なぜJSONLか

1. **集約データは小さい**: 1行68列 × 86,400秒 = ~30MB/日/全market
   → Parquet圧縮のカラムナ利点より追記の単純さが勝る
2. **追記が本質**: 1s集約は不変（追記専用、変更/削除が発生しない）
   → BufferedWriter の flush で十分
3. **読み取り互換性**: DuckDB `read_json_auto()` がJSONLを直接読める
4. **コード削減**: DuckDB import・CREATE TABLE・INSERT・COPY・型明示が不要に
   → FeatureAccumulator から DuckDB 依存が完全に消える

### なぜParquetをやめるか

- 60秒ごとの小ファイル量産が問題（24,480ファイル/日、inode消費）
- 集約データのサイズではParquet圧縮のトレードオフが悪い
- `type: '1s_feature'` のような固定文字列列が無駄
- 長期アーカイブは raw trade Parquet が別途担当

### なぜ日付パーティションか

- 単一JSONLが無制限成長するとaggregate読み取り性能が劣化する
- 既存の `raw_hot/{date}/trade/` と同じ日付パーティションパターンを踏襲
- 1日経過したパーティションは不変になるのでaggregateが安心して読める
- パーティション粒度は日（86400行/market = ~3MB/market）

## 実装手順

### Step 1: FeatureAccumulator の flush をJSONL追記に変更

```js
import { BufferedWriter } from './buffered-writer.mjs';

/** UTC date string YYYY-MM-DD */
function utcDateStr(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// FeatureAccumulator に Writer キャッシュ + 日付ローテーションを追加
constructor(outputBase, options = {}) {
  // ...既存の初期化...
  this._writers = new Map();     // Map<"YYYY-MM-DD/market", BufferedWriter>
  this._currentDate = null;      // 現在の日付パーティション文字列
}

_getWriter(market, dateStr) {
  const key = `${dateStr}/${market}`;
  let w = this._writers.get(key);
  if (!w) {
    const dir = path.join(this._outputBase, dateStr);
    fs.mkdirSync(dir, { recursive: true });
    w = new BufferedWriter(path.join(dir, `${market}.jsonl`), {
      flushIntervalMs: 1000,
      idleCloseMs: 120000,  // >60s flush interval avoids per-flush file open
    });
    this._writers.set(key, w);
  }
  return w;
}

async _ensureDate(dateStr) {
  if (dateStr === this._currentDate) return;
  // Close all writers from the old date partition
  if (this._currentDate !== null) {
    const oldDate = this._currentDate;
    this._currentDate = dateStr;
    const promises = [];
    for (const [key, w] of this._writers) {
      if (key.startsWith(oldDate + '/')) promises.push(w.close());
    }
    await Promise.allSettled(promises);
    // Remove closed writer entries
    for (const key of this._writers.keys()) {
      if (key.startsWith(oldDate + '/')) this._writers.delete(key);
    }
  } else {
    this._currentDate = dateStr;
  }
}

async flush() {
  let totalRows = 0;
  // Group rows by date partition (derived from each row's ts, not wall clock)
  const byDate = new Map(); // Map<dateStr, Map<market, rows[]>>
  for (const [market, rows] of this._buffers) {
    if (rows.size === 0) continue;
    for (const [second, row] of rows) {
      const dateStr = utcDateStr(new Date(second)); // partition by row ts
      if (!byDate.has(dateStr)) byDate.set(dateStr, new Map());
      if (!byDate.get(dateStr).has(market)) byDate.get(dateStr).set(market, []);
      byDate.get(dateStr).get(market).push(row);
      totalRows++;
    }
  }

  // Sort date partitions to ensure deterministic processing order
  const sortedDates = Array.from(byDate.entries()).sort(([a], [b]) => a.localeCompare(b));

  for (const [dateStr, markets] of sortedDates) {
    await this._ensureDate(dateStr);
    for (const [market, rows] of markets) {
      const writer = this._getWriter(market, dateStr);
      for (const row of rows) {
        await writer.write(row); // await for safe error handling
      }
    }
  }

  this._buffers.clear();
  this._lastFlush = Date.now();
  return totalRows;
}

async close() {
  const flushed = await this.flush(); // flush FIRST (before _closed guard)
  this._closed = true;
  for (const w of this._writers.values()) await w.close();
  this._writers.clear();
  return flushed;
}
```

**重要な設計ポイント**:
- **パーティションは行のts基準**（壁時計ではない）: フラッシュが00:00:30に実行されても、23:59:00のtsを持つ行は昨日のパーティションに、00:00:01以降のtsは今日のパーティションに正しく振り分けられる
- **日付ローテーション**: `_ensureDate()` が日付変更時に旧パーティションのwriterをクローズ・削除。FairPriceCollectorの既存パターンを踏襲
- **close() の順序**: `flush()` → `_closed = true`。逆だとflushがバッファを捨てるバグを修正
- **戻り値**: `flush()` は書き込んだ行数を返す。呼び出し元（fair-price-collector.mjs）のログ出力を維持

**クラッシュリカバリ**: 最大60秒分の未flushデータ（`_buffers`内）が消失する可能性がある。これは**Parquet版と同じトレードオフ**（flush間隔60秒は不変）。BufferedWriterの内部バッファ（~1s）はその一部。不完全な最終行は `read_json_auto` が自動スキップする（実測で確認済みの動作だが、本番投入前に要確認）。

### Step 2: aggregate-1s.mjs の読み取り元を変更

```js
const FEATURES_BASE = 'data/1s_features';

function processMarket(market) {
  // 全日付パーティションから対象marketのJSONLを収集
  const partitions = fs.readdirSync(FEATURES_BASE)
    .filter(d => /^\d{4}-\d{2}-\d{2}$/.test(d))
    .sort();
  const srcPaths = [];
  for (const dateStr of partitions) {
    const p = path.join(FEATURES_BASE, dateStr, `${market}.jsonl`);
    if (fs.existsSync(p)) srcPaths.push(p);
  }

  if (srcPaths.length === 0) return null;

  const unionSql = srcPaths.map(p =>
    `SELECT * FROM read_json_auto('${p}')`
  ).join(' UNION ALL ');

  await e(db, `
    CREATE TABLE new_rows AS
    SELECT ${selectCols}
    FROM (${unionSql})
    WHERE ts >= ${cutoff}
  `);
  // ...以下 dedup → merge → COPY は既存ロジック維持
}
```

**変更点**:
- 全パーティションスキャン（今日+昨日だけでなく全期間）。aggregatorが48h以上停止してもデータを拾える
- パーティションは `YYYY-MM-DD` 命名でフィルタ（他ファイル混入防止）
- `WHERE ts >= cutoff` で最新120sだけにフィルタ。これは新たに追加する最適化で、全パーティションスキャンしてもDuckDBがJSONLのts列に対して部分的なプッシュダウンを行える場合がある。プッシュダウンが効かなくても、ファイル数が1/1000に削減されているため総合性能は改善する。\n- `read_json_auto` の型推論問題: `selectCols` 動的ビルドの `NULL::type AS col` fallback は既存ロジック維持

### Step 3: バグ修正（ついでに直す）

1. **`_lastL1.bidPrice` に `askPrice` を代入**（feature-accumulator.mjs:440）
   `bidPrice: bidPrice !== null ? askPrice : null` → `bidPrice: bidPrice ?? null`
2. **`best_deplete_count` が常に0**（feature-accumulator.mjs:417）
   `flow?.bidDeplete` → `flow?._depleteBid`, `flow?.askDeplete` → `flow?._depleteAsk`
3. **close() のバッファ消失**（feature-accumulator.mjs:587-591）
   `_closed = true` → `flush()` の順序を `flush()` → `_closed = true` に変更
4. **`feedSecond()` の未使用引数 `fp`**（feature-accumulator.mjs:303）
   シグネチャから削除
5. **docs/1s-features-schema.md の列名不一致**: `bid_depth_1bps` → `bid_1bps`（実際のコードに合わせる）

### Step 4: 古いParquet形式の1s_featuresを削除

クリーンスタート。

## 完了条件

- [ ] FeatureAccumulator flush が JSONL追記で動作（Writer cache + ts基準パーティション + 日付ローテーション）
- [ ] aggregate-1s.mjs が JSONL読み取り（全パーティションスキャン + UNION ALL + dedup/merge）で動作
- [ ] npm test 全件PASS
- [ ] 実走確認: 60秒受信 → JSONL追記 → aggregate統合
- [ ] ファイル数が 17market × N日分 に収束（Parquet量産消滅）
- [ ] close() のバッファ消失バグ修正
- [ ] _lastL1 バグ修正、best_deplete_count バグ修正
- [ ] docs/1s-features-schema.md 列名修正

## 運用

- データサイズ: ~30MB/日（全market合計）
- リテンション: raw_hot と同じポリシー
- クラッシュリカバリ: 最大60秒（flush間隔）の未flushデータ消失。これはParquet版と同じ許容範囲。JSONL最終行の不完全行は read_json_auto が自動スキップする（実測に基づく。本番前に試験推奨）。
- aggregate cron: 60秒ごと、全パーティションスキャン + tsフィルタで最新120sの処理。

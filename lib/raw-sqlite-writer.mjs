// Market-split SQLite WAL sink for receiver raw events.
// One process owns the writers; other local processes may read each DB while
// it is being written because WAL keeps readers on a stable snapshot.

import { DatabaseSync } from 'node:sqlite';
import { mkdir, readdir } from 'node:fs/promises';
import { gunzipSync, gzipSync } from 'node:zlib';
import { PerformanceObserver } from 'node:perf_hooks';
import { createHash } from 'node:crypto';
import path from 'node:path';

export const RAW_SQLITE_SCHEMA = 'raw_v6_sqlite';
// Canonical raw (issues #10/#11): append-only, arrival-ordered, immutable
// source-frame log. Schema is deliberately separate from raw_v6_sqlite so the
// legacy normalized view (UPDATE-merge / event-time sort) and the canonical
// log never share rows or mutation semantics.
export const CANONICAL_SCHEMA = 'raw_v7_canonical';
export const DEFAULT_RAW_RETENTION_DAYS = 90;
// これ以上かかった append だけ内訳を報告する (異常時のみ記録)。
export const DEFAULT_SLOW_APPEND_MS = 1000;

const SQLITE_CONSTRAINT_UNIQUE = 2067;
const SQLITE_CONSTRAINT = 19;

// R-02: in-process write-side duplicate detection.  The raw_batches table has
// no UNIQUE constraint (batch_id AUTOINCREMENT only) and adding one would
// require a schema migration on live databases, so the writer keeps a bounded
// identity window of the rows it has already committed and drops exact
// re-deliveries instead of appending them twice.  Memory-only: a restart
// re-opens the window (a cross-restart replay needs a schema-level UNIQUE key,
// which is reported separately as an operator decision).
export const DEFAULT_DEDUPE_WINDOW_MS = 10 * 60 * 1000;
export const DEFAULT_DEDUPE_MAX_KEYS = 131072;

function isUniqueViolation(error) {
  if (error?.code === 'ERR_SQLITE_ERROR') {
    const primary = error?.errcode;
    if (primary === SQLITE_CONSTRAINT_UNIQUE) return true;
    if (primary === SQLITE_CONSTRAINT && /UNIQUE/i.test(String(error?.message ?? ''))) return true;
  }
  // Fallback for node versions that do not surface errcode on the error.
  return /UNIQUE constraint failed/i.test(String(error?.message ?? ''));
}

function finiteMs(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : fallback;
}

function scalarText(value) {
  return value === null || value === undefined ? null : String(value);
}

function isBusyError(error) {
  return error?.code === 'SQLITE_BUSY'
    || /database is locked|SQLITE_BUSY/i.test(String(error?.message ?? ''));
}

function normalizeEnvelope(envelope, nowMs, writerSessionId, ingestSeq) {
  if (!envelope || typeof envelope !== 'object') throw new TypeError('raw envelope must be an object');
  if (typeof envelope.market !== 'string' || !envelope.market) throw new TypeError('raw envelope market is required');
  if (typeof envelope.stream !== 'string' || !envelope.stream) throw new TypeError('raw envelope stream is required');
  const rawLine = typeof envelope.raw_line === 'string'
    ? envelope.raw_line.replace(/\n$/, '')
    : JSON.stringify(envelope);
  return {
    schema: typeof envelope.schema === 'string' ? envelope.schema : RAW_SQLITE_SCHEMA,
    market: envelope.market,
    stream: envelope.stream,
    event_ts_ms: finiteMs(envelope.event_ts_ms),
    recv_ts_ms: finiteMs(envelope.recv_ts_ms, nowMs),
    writer_session_id: scalarText(envelope.writer_session_id) ?? writerSessionId,
    ingest_seq: scalarText(envelope.ingest_seq) ?? String(ingestSeq),
    source_id: scalarText(envelope.source_id),
    raw_line: rawLine,
    written_at_ms: nowMs,
  };
}

function finiteInt(value) {
  if (value === null || value === undefined) return null;
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : null;
}

function boolToInt(value) {
  if (value === null || value === undefined) return null;
  return value ? 1 : 0;
}

// Canonical raw (issues #10/#11) row normalization. The stored frame is the
// EXACT source text captured at the socket boundary — nothing here parses,
// normalizes, reorders or interprets exchange payloads. frame_json stores one
// envelope whose `frame` value carries that text byte-identically; the outer
// JSON.stringify() escapes any embedded newlines, so frame_json always stays
// a single parseable JSON line. The envelope is canonical-specific and is NOT
// readable by the existing downstream raw parsers (agg-btc-downstream
// parseRawLine): consuming canonical rows requires a new adapter (future
// downstream work). The legacy dual path (raw_batches.raw_gzip) remains
// readable by the existing parsers exactly as before.
export function normalizeCanonicalEnvelope(envelope, nowMs, writerSessionId, ingestSeq) {
  if (!envelope || typeof envelope !== 'object') throw new TypeError('canonical envelope must be an object');
  if (typeof envelope.market !== 'string' || !envelope.market) throw new TypeError('canonical envelope market is required');
  if (typeof envelope.stream !== 'string' || !envelope.stream) throw new TypeError('canonical envelope stream is required');
  const frameText = envelope.frame_text ?? envelope.frame ?? envelope.raw_line;
  // Astra P2: only MISSING / non-string frame text is rejected. An empty
  // string ('') is a legitimate byte-exact source frame — e.g. an empty
  // socket message that failed JSON.parse at the boundary — and must be
  // stored verbatim (frame: "") instead of refused.
  if (typeof frameText !== 'string') {
    throw new TypeError('canonical envelope frame_text is required');
  }
  // Byte-exact source text is preserved verbatim as the `frame` value. There
  // is deliberately NO newline rejection: legitimate source frames may be
  // multi-line (pretty-printed JSON), and rejecting them here would surface
  // as reportRawDbFailure() in the main process and stop the whole receiver.
  const recvTs = finiteMs(envelope.recv_ts_ms);
  if (recvTs === null) throw new TypeError('canonical envelope recv_ts_ms is required');
  const receiveSeq = finiteInt(envelope.receive_seq);
  const schema = typeof envelope.schema === 'string' ? envelope.schema : CANONICAL_SCHEMA;
  const connectionId = scalarText(envelope.connection_id);
  const sourceTs = finiteMs(envelope.source_event_ts_ms);
  const line = JSON.stringify({
    schema,
    market: envelope.market,
    stream: envelope.stream,
    channel: scalarText(envelope.channel),
    connection_id: connectionId,
    receive_seq: receiveSeq,
    recv_ts_ms: recvTs,
    recv_mono_ns: finiteInt(envelope.recv_mono_ns),
    source_event_ts_ms: sourceTs,
    source_event_time_known: boolToInt(envelope.source_event_time_known),
    event_ts_ms: finiteMs(envelope.event_ts_ms),
    ingest_seq: scalarText(envelope.ingest_seq) ?? String(ingestSeq),
    writer_session_id: scalarText(envelope.writer_session_id) ?? writerSessionId,
    // #10: exact source frame, byte-identical to what the socket delivered.
    frame: frameText,
    written_at_ms: nowMs,
  });
  return {
    schema,
    market: envelope.market,
    stream: envelope.stream,
    channel: scalarText(envelope.channel),
    connection_id: connectionId,
    receive_seq: receiveSeq,
    recv_ts_ms: recvTs,
    recv_mono_ns: finiteInt(envelope.recv_mono_ns),
    source_event_ts_ms: sourceTs,
    source_event_time_known: boolToInt(envelope.source_event_time_known),
    event_ts_ms: finiteMs(envelope.event_ts_ms),
    ingest_seq: scalarText(envelope.ingest_seq) ?? String(ingestSeq),
    writer_session_id: scalarText(envelope.writer_session_id) ?? writerSessionId,
    frame_json: line,
    frame_sha256: createHash('sha256').update(line, 'utf8').digest('hex'),
    written_at_ms: nowMs,
  };
}

// 遅い append の内訳を取るための計測。
// process.hrtime.bigint() は単調クロックなので、時計補正 (NTP step) の影響を受けない。
function msSinceNs(t0Ns) {
  return Number(process.hrtime.bigint() - t0Ns) / 1e6;
}

function round3(value) {
  return Math.round(value * 1000) / 1000;
}

/**
 * 1 回の append 呼び出しの内訳。遅かった時だけ observer へ報告する
 * (正常時の記録はゼロ、という既存の観測方針を維持する)。
 */
// GC は append ごとに observer を作らずモジュールで 1 本だけ観測し、前後差分を取る
// (Astra P1: 通常 append で observer/timer が解放されず増殖していた)。
const gcStats = { count: 0, ms: 0 };
try {
  const gcObserver = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) { gcStats.count += 1; gcStats.ms += entry.duration; }
  });
  gcObserver.observe({ entryTypes: ['gc'] });
} catch { /* 観測できない環境では 0 のまま (gc は補助指標) */ }

// (stream,last_event_ts_ms) 索引の有無キャッシュ (DatabaseSync ごと)。
const lastEventIndexCache = new WeakMap();

class AppendTrace {
  constructor(id, events) {
    this.id = id;
    this.events = events;
    this.t0Ns = process.hrtime.bigint();
    this.cpu0 = process.cpuUsage();
    this.heap0 = process.memoryUsage().heapUsed;
    this.gcCount = 0;
    this.gcMs = 0;
    this.gc0 = { ...gcStats };
    // append 中に event loop が回った回数。連続ブロックなら 0 に近づく。
    this.loopTicks = 0;
    this.tickTimer = setInterval(() => { this.loopTicks += 1; }, 50);
    if (this.tickTimer.unref) this.tickTimer.unref();
    this._finalized = false;
    this.spans = [];
  }

  /** 未計測分の切り分け用サマリ (CPU/GC/loop 回数)。 */
  summary() {
    const cpu = process.cpuUsage(this.cpu0);
    this.gcCount = gcStats.count - this.gc0.count;
    this.gcMs = round3(gcStats.ms - this.gc0.ms);
    if (this.tickTimer) { clearInterval(this.tickTimer); this.tickTimer = null; }
    this._finalized = true;
    return {
      wall_ms: round3(this.totalMs()),
      cpu_ms: round3((cpu.user + cpu.system) / 1000),
      loop_ticks: this.loopTicks,
      gc_count: this.gcCount,
      gc_ms: round3(this.gcMs),
      heap_delta_mb: round3((process.memoryUsage().heapUsed - this.heap0) / 1048576),
    };
  }

  begin(name, details) {
    const t0Ns = process.hrtime.bigint();
    return (extra) => {
      const entry = {
        name,
        dur_ms: round3(msSinceNs(t0Ns)),
        details: { ...(details ?? {}), ...(extra ?? {}) },
      };
      this.spans.push(entry);
      return entry;
    };
  }

  totalMs() {
    return msSinceNs(this.t0Ns);
  }

  spansJson() {
    return this.spans.map((span) => ({ ...span, dur_ms: round3(span.dur_ms) }));
  }
}

function makeBatch(rows) {
  const rawText = `${rows.map((row) => row.raw_line).join('\n')}\n`;
  const eventTimes = rows.map((row) => row.event_ts_ms).filter((value) => value !== null);
  const recvTimes = rows.map((row) => row.recv_ts_ms).filter((value) => value !== null);
  return {
    schema: rows[0].schema,
    market: rows[0].market,
    stream: rows[0].stream,
    first_event_ts_ms: eventTimes.length ? Math.min(...eventTimes) : null,
    last_event_ts_ms: eventTimes.length ? Math.max(...eventTimes) : null,
    first_recv_ts_ms: Math.min(...recvTimes),
    last_recv_ts_ms: Math.max(...recvTimes),
    row_count: rows.length,
    raw_gzip: gzipSync(Buffer.from(rawText, 'utf8'), { level: 1 }),
    raw_bytes: Buffer.byteLength(rawText, 'utf8'),
    written_at_ms: rows.at(-1).written_at_ms,
  };
}

function eventOrderKey(row) {
  const eventTs = row.event_ts_ms;
  return eventTs === null ? Number.MAX_SAFE_INTEGER : eventTs;
}

function sortBatchRows(rows) {
  // IPC到着順をイベント順へ変換する。event_ts_ms欠落は末尾へ。
  return rows.map((row, index) => ({ row, index }))
    .sort((a, b) => eventOrderKey(a.row) - eventOrderKey(b.row)
      || Number(a.row.ingest_seq) - Number(b.row.ingest_seq)
      || a.index - b.index)
    .map(({ row }) => row);
}

// R-03: 遅着マージの冪等性キー。
// 同じ遅着イベントが再投入されても batch 内に同一行が二重に入らないよう、
// 挿入前に「同一イベント」判定用のキーを算出する (並び順キーとは別物):
//   - source_id (取引所イベントID = tradeId 等) を持つ行: source_id + event_ts_ms
//     (取引所IDは一意なので、ingest_seq が振り直された再送でも同一と判定できる)
//   - 持たない行 (depth / book_updates 等): event_ts_ms + ingest_seq + writer_session_id
//     ingest_seq はプロセス毎に 0 から振り直されるため、session を混ぜないと
//     再起動後の別イベント (reconnect で届く古いスナップショット等) が
//     「同一」と誤判定されて実データを捨てる。同一プロセス内の再投入
//     (flush 失敗で re-queue された envelope) は session も seq も一致するので
//     従来どおり重複として落ちる。
//   - どちらも欠ける行: raw_line の完全一致のみ同一とみなす
// 制約: source_id を持たない stream では、ingest_seq が振り直された再送は
// 行の内容から区別できない (writer 層では判定材料が無い) ため対象外。
function identityFromParsed(parsed, line) {
  const sourceId = scalarText(parsed.source_id);
  const ts = finiteMs(parsed.event_ts_ms);
  if (sourceId !== null && sourceId !== '') return `src\u0000${sourceId}\u0000${ts === null ? '' : ts}`;
  const seq = scalarText(parsed.ingest_seq);
  if (ts !== null && seq !== null && seq !== '') {
    const session = scalarText(parsed.writer_session_id) ?? '';
    return `tsseq\u0000${ts}\u0000${seq}\u0000${session}`;
  }
  return `line\u0000${line}`;
}

// 例外を投げない版 (mini-batch 経路用)。JSON でない行は完全一致キーに落とす。
function lineIdentityKey(line) {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch (_) {
    return `line\u0000${line}`;
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return `line\u0000${line}`;
  return identityFromParsed(parsed, line);
}

// R-02: fresh 経路 (watermark より新しい行) での再送検出キー。
// source_id (取引所イベントID = tradeId 等) を持つ行だけを対象にする:
//   - ingest_seq / writer_session_id 由来の key は配送ごとに異なるため追跡しても
//     一致せず、全 depth 行を窓に載せる無駄なメモリ消費になるだけ。
//   - 取引所IDは一意なので ingest_seq が振り直された再送でも同一と判定できる。
// market と stream を前置するのは、取引所をまたいだ source_id の衝突で
// 実データを誤除外しないため。
/**
 * 同一 flush (1 append) 内で同じ identity が複数行になることを「別約束」として
 * 許容する市場。Kraken v1 の trade フレームは [price, volume, time, side, orderType,
 * misc] の 6 要素で trade_id を持たない (live probe 31/31 で確認)。receiver が
 * 指紋を source_id にするため、1 フレームに同値の別約束が複数入り得る (実測 最大6重)。
 * ここに列挙されない市場の trade identity は取引所割当の一意 ID なので、同一
 * identity の重複は「同じ約束を別接続が配送した」重複配送とみなして落とす。
 */
const SAME_FLUSH_IDENTITY_SIBLING_MARKETS = new Set(['kraken_spot']);

function freshSourceIdentity(market, stream, row) {
  const sourceId = row.source_id;
  if (sourceId === null || sourceId === undefined || sourceId === '') return null;
  const ts = row.event_ts_ms === null || row.event_ts_ms === undefined ? '' : row.event_ts_ms;
  return `${market}\u0000${stream}\u0000${sourceId}\u0000${ts}`;
}

// 遅着マージ用: raw_line 1行からソートキーを抽出する。
// raw_line は正規化エンベロープJSONなので先頭付近に event_ts_ms / ingest_seq を持つ。
function lineSortKey(line) {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch (error) {
    throw new TypeError(`raw batch contains invalid JSON: ${error.message}`, { cause: error });
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new TypeError('raw batch line must be a JSON object');
  }
  const ts = finiteMs(parsed.event_ts_ms);
  const seqValue = parsed.ingest_seq;
  const seqNumber = seqValue === null || seqValue === undefined ? null : Number(seqValue);
  return {
    ts,
    seq: Number.isFinite(seqNumber) ? seqNumber : null,
    recv: finiteMs(parsed.recv_ts_ms),
    identity: identityFromParsed(parsed, line),
  };
}

function compareSortKey(a, b) {
  const aTs = a.ts === null ? Number.MAX_SAFE_INTEGER : a.ts;
  const bTs = b.ts === null ? Number.MAX_SAFE_INTEGER : b.ts;
  if (aTs !== bTs) return aTs - bTs;
  if (a.seq !== null && b.seq !== null && a.seq !== b.seq) return a.seq - b.seq;
  return 0;
}

// 遅着行を既存batchへマージする。
// meta: raw_batches行 (raw_gzip等), incoming: 正規化rowの配列
// 戻り値: 更新後のメタ + raw_gzip
//
// R-03: 冪等。同じ遅着行 (同一イベント) を再マージしても行は増えない。
// 挿入前に identityFromParsed の同一性キーで既存行と照合し、一致する行は
// duplicatesSkipped として数えて捨てる。同一呼び出し内の重複も同様に落とす。
export function mergeLinesIntoBatch(meta, incoming) {
  const text = gunzipSync(Buffer.from(meta.raw_gzip)).toString('utf8');
  const lines = text.length > 0 ? text.replace(/\n$/, '').split('\n') : [];
  const keys = lines.map(lineSortKey);

  // 既存行の同一性キー集合。挿入した行のキーも逐次追加する。
  const identities = new Set(keys.map((k) => k.identity));
  const inserted = [];
  let duplicatesSkipped = 0;

  for (const row of incoming) {
    // 正規化済みrowからraw_lineを取り出し、キーも抽出
    const line = typeof row.raw_line === 'string' ? row.raw_line : JSON.stringify(row);
    const key = lineSortKey(line);
    if (identities.has(key.identity)) {
      duplicatesSkipped += 1;
      continue;
    }
    identities.add(key.identity);
    inserted.push(row);
    let lo = 0;
    let hi = lines.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (compareSortKey(keys[mid], key) <= 0) lo = mid + 1;
      else hi = mid;
    }
    lines.splice(lo, 0, line);
    keys.splice(lo, 0, key);
  }

  const rawText = `${lines.join('\n')}\n`;
  const eventTimes = keys.map((k) => k.ts).filter((v) => v !== null);
  // recv timestamps were already extracted by lineSortKey — no second parse.
  // 実際に挿入した行だけを使う (棄却した重複行の受信時刻で batch の
  // 鮮度範囲を広げないため)。
  const recvTimes = keys.map((k) => k.recv).filter((v) => v !== null);
  const firstRecv = Math.min(...inserted.map((r) => r.recv_ts_ms),
    ...(recvTimes.length ? [Math.min(...recvTimes)] : []));
  const lastRecv = Math.max(...inserted.map((r) => r.recv_ts_ms),
    ...(recvTimes.length ? [Math.max(...recvTimes)] : []));

  return {
    first_event_ts_ms: eventTimes.length ? Math.min(...eventTimes) : null,
    last_event_ts_ms: eventTimes.length ? Math.max(...eventTimes) : null,
    first_recv_ts_ms: Number.isFinite(firstRecv) ? firstRecv : meta.first_recv_ts_ms,
    last_recv_ts_ms: Number.isFinite(lastRecv) ? lastRecv : meta.last_recv_ts_ms,
    row_count: lines.length,
    mergedRows: inserted.length,
    duplicatesSkipped,
    raw_gzip: gzipSync(Buffer.from(rawText, 'utf8'), { level: 1 }),
    raw_bytes: Buffer.byteLength(rawText, 'utf8'),
  };
}

function safeMarketName(market) {
  const name = String(market).replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!name) throw new TypeError('market must produce a non-empty filename');
  return name;
}

function configureDatabase(db) {
  db.exec(`
    PRAGMA busy_timeout=5000;
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA wal_autocheckpoint=1000;
    CREATE TABLE IF NOT EXISTS raw_batches (
      batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
      schema TEXT NOT NULL,
      market TEXT NOT NULL,
      stream TEXT NOT NULL,
      first_event_ts_ms INTEGER,
      last_event_ts_ms INTEGER,
      first_recv_ts_ms INTEGER NOT NULL,
      last_recv_ts_ms INTEGER NOT NULL,
      row_count INTEGER NOT NULL,
      raw_gzip BLOB NOT NULL,
      raw_bytes INTEGER NOT NULL,
      written_at_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS raw_batches_recv_idx
      ON raw_batches(last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_idx
      ON raw_batches(stream, last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_last_event_idx
      ON raw_batches(stream, last_event_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_first_event_idx
      ON raw_batches(stream, first_event_ts_ms);
    -- Canonical raw (issues #10/#11): immutable append-only source-frame log.
    -- frame_id (AUTOINCREMENT) is the physical arrival order: rows are never
    -- updated, re-sorted, merged or deleted by the writer. The UNIQUE
    -- (connection_id, receive_seq) index is the append-safe duplicate guard:
    -- a frame already recorded for a connection is skipped, never merged.
    -- receive_seq = NULL rows (non-socket observations) are exempt because
    -- SQLite UNIQUE treats NULLs as distinct.
    -- TTL-exempt: pruneExpired() deletes legacy raw_batches rows only; this
    -- table grows monotonically until an explicit operator archive removes it.
    CREATE TABLE IF NOT EXISTS canonical_frames (
      frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
      schema TEXT NOT NULL,
      market TEXT NOT NULL,
      stream TEXT NOT NULL,
      channel TEXT,
      connection_id TEXT,
      receive_seq INTEGER,
      recv_ts_ms INTEGER NOT NULL,
      recv_mono_ns INTEGER,
      source_event_ts_ms INTEGER,
      source_event_time_known INTEGER,
      event_ts_ms INTEGER,
      ingest_seq TEXT,
      writer_session_id TEXT,
      frame_json TEXT NOT NULL,
      frame_sha256 TEXT NOT NULL,
      written_at_ms INTEGER NOT NULL,
      UNIQUE (connection_id, receive_seq)
    );
    CREATE INDEX IF NOT EXISTS canonical_frames_stream_id_idx
      ON canonical_frames(stream, frame_id);
    CREATE INDEX IF NOT EXISTS canonical_frames_recv_idx
      ON canonical_frames(recv_ts_ms);
  `);
}

export class RawSqliteWriter {
  constructor({
    databaseDir,
    retentionDays = DEFAULT_RAW_RETENTION_DAYS,
    now = () => Date.now(),
    freshDedupeWindowMs,
    freshDedupeMaxKeys,
    observer = null,
    slowAppendMs = DEFAULT_SLOW_APPEND_MS,
  } = {}) {
    if (!databaseDir) throw new TypeError('databaseDir is required');
    if (!Number.isInteger(retentionDays) || retentionDays < 1) {
      throw new TypeError('retentionDays must be a positive integer');
    }
    this.databaseDir = path.resolve(databaseDir);
    this.retentionDays = retentionDays;
    this.now = now;
    this.databases = new Map();
    this.queue = Promise.resolve();
    this.closed = false;
    this.writerSessionId = `sqlite:${process.pid}:${Date.now()}`;
    this.ingestSeq = 0;
    // 遅い append の内訳報告 (observer は report(record) を持つ任意の観測器)。
    this.observer = observer;
    // 閾値 (ms)。0 = 毎回報告 (検証用) / 負値 = 内訳計測を無効化。
    this.slowAppendMs = Number.isFinite(slowAppendMs) ? slowAppendMs : DEFAULT_SLOW_APPEND_MS;
    this.appendSeq = 0;
    // 遅着イベントの可視性カウンタ (market\0stream -> {merged, minibatches, mergedRows, overlaps})
    this.lateEventStats = new Map();
    // Canonical raw counters (issues #10/#11): appended rows and duplicates
    // skipped by the append-safe (connection_id, receive_seq) guard.
    this.canonicalStats = { appended: 0, skippedDuplicates: 0 };
    // R-06: retention prune visibility.  A busy-deferred prune is counted and
    // logged here instead of being reported as a raw-DB failure.
    this.pruneStats = { runs: 0, deletedRows: 0, failures: 0, deferredBusy: 0, lastError: null };
    // R-02: コミット済み (market,stream,source_id,event_ts) の bounded 窓。
    // 再接続スナップショットの再送が watermark より新しいと遅着ルートに入らず
    // 「新規行」として二重に追記されていた分を fresh 経路で棄却する。
    // 上書き値は信頼境界の外なので sanitize する (NaN は比較が常に false になり
    // 実質無効化、0/負値は窓が即時失効する)。
    const windowOverride = Number(freshDedupeWindowMs);
    this.freshDedupeWindowMs = Number.isFinite(windowOverride) && windowOverride >= 1
      ? Math.floor(windowOverride)
      : DEFAULT_DEDUPE_WINDOW_MS;
    const keysOverride = Number(freshDedupeMaxKeys);
    this.freshDedupeMaxKeys = Number.isFinite(keysOverride) && keysOverride >= 1
      ? Math.floor(keysOverride)
      : DEFAULT_DEDUPE_MAX_KEYS;
    this.freshDedupe = new Map();
    this.freshDedupeStats = { skipped: 0, sameFlushSkipped: 0, lastSkippedAtMs: null };
  }

  _bumpLateStats(key, field, amount = 1) {
    if (!this.lateEventStats.has(key)) {
      this.lateEventStats.set(key, {
        merged: 0, mergedRows: 0, minibatches: 0, overlaps: 0, duplicatesSkipped: 0,
      });
    }
    const stats = this.lateEventStats.get(key);
    stats[field] += amount;
    return stats;
  }

  lateEventSummary() {
    const summary = {};
    for (const [key, value] of this.lateEventStats) {
      const [market, stream] = key.split('\u0000');
      summary[`${market}.${stream}`] = { ...value };
    }
    return summary;
  }

  /**
   * R-02: fresh 経路で「重複配送」を棄却する。
   *
   * 2 種類を扱う:
   *   1. コミット済み identity の再送 (再接続スナップショット等) — 窓で判定。
   *   2. **同じ flush (1 append) に載った同一 identity** — 取引所 ID が一意な市場では
   *      「同じ約束を 2 つの接続が配送した」重複配送である (実測: hyperliquid_perp で
   *      再接続時に旧接続 conn:7 と新接続 conn:9 が同一約定を再配送し、同一 append に
   *      17 組が入って両方保存された 2026-09-21 07:44 JST)。
   *
   * SAME_FLUSH_IDENTITY_SIBLING_MARKETS だけは例外で、同一 identity の複数行を
   * 「別約束」として保持する (Kraken v1 は trade_id を返さないため receiver が
   * 指紋を source_id にしており、1 フレームに同値の別約束が複数入り得る)。
   * 窓が空でも同一 flush の判定は必要なので、早期 return はしない。
   */
  _dropFreshRedeliveries(key, rows) {
    const [market, stream] = key.split('\u0000');
    let skipped = 0;
    let sameFlush = 0;
    let kept = rows;
    if (!SAME_FLUSH_IDENTITY_SIBLING_MARKETS.has(market)) {
      const seen = new Set();
      const deduped = [];
      for (const row of rows) {
        const identity = freshSourceIdentity(market, stream, row);
        if (identity === null) {
          deduped.push(row);
          continue;
        }
        if (seen.has(identity)) {
          skipped += 1;
          sameFlush += 1;
          continue;
        }
        seen.add(identity);
        deduped.push(row);
      }
      if (sameFlush > 0) kept = deduped;
    }
    if (this.freshDedupe.size > 0) {
      const out = [];
      for (const row of kept) {
        const identity = freshSourceIdentity(market, stream, row);
        if (identity !== null && this.freshDedupe.has(identity)) {
          skipped += 1;
          continue;
        }
        out.push(row);
      }
      if (skipped > sameFlush) kept = out;
    }
    if (skipped === 0) return rows;
    this.freshDedupeStats.skipped += skipped;
    this.freshDedupeStats.sameFlushSkipped += sameFlush;
    this.freshDedupeStats.lastSkippedAtMs = this.now();
    console.error(
      `[RawSqliteWriter] ${market}/${stream}: ${skipped} duplicate row(s) skipped on the fresh path`
      + ` (same-flush=${sameFlush}, already-committed=${skipped - sameFlush}; R-02)`
      + ` [total skipped=${this.freshDedupeStats.skipped}]`,
    );
    return kept;
  }

  /**
   * R-02: 窓への登録は「コミット成功後」に限る。
   * 同じ append 呼び出しに含まれる同一 identity の双子 (Kraken v1 の
   * trade_id 非保持による指紋同一の別約定など) を落とさないため。
   */
  _rememberFreshIdentities(key, rows, committedAtMs) {
    const [market, stream] = key.split('\u0000');
    for (const row of rows) {
      const identity = freshSourceIdentity(market, stream, row);
      if (identity === null) continue;
      this.freshDedupe.set(identity, committedAtMs);
    }
    this._sweepFreshDedupe(committedAtMs);
  }

  /** TTL と件数上限の両方で窓を有界に保つ (挿入順 = 古い順に落とす)。 */
  _sweepFreshDedupe(nowMs) {
    const cutoff = nowMs - this.freshDedupeWindowMs;
    for (const [identity, at] of this.freshDedupe) {
      if (at < cutoff) this.freshDedupe.delete(identity);
    }
    while (this.freshDedupe.size > this.freshDedupeMaxKeys) {
      const oldest = this.freshDedupe.keys().next().value;
      this.freshDedupe.delete(oldest);
    }
  }

  freshDedupeSummary() {
    return {
      ...this.freshDedupeStats,
      windowMs: this.freshDedupeWindowMs,
      maxKeys: this.freshDedupeMaxKeys,
      trackedIdentities: this.freshDedupe.size,
    };
  }

  async open() {
    await mkdir(this.databaseDir, { recursive: true });
    const entries = await readdir(this.databaseDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isFile() && entry.name.endsWith('.sqlite')) {
        this._openMarket(path.basename(entry.name, '.sqlite'));
      }
    }
    return this;
  }

  _openMarket(market) {
    if (this.databases.has(market)) return this.databases.get(market);
    const databasePath = path.join(this.databaseDir, `${safeMarketName(market)}.sqlite`);
    const db = new DatabaseSync(databasePath);
    try {
      configureDatabase(db);
    } catch (error) {
      try { db.close(); } catch (_) {}
      throw error;
    }
    const entry = { market, databasePath, db };
    this.databases.set(market, entry);
    return entry;
  }

  append(envelopes) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    // 遅い append の内訳計測 (observer 未設定なら trace は null で一切の追加コスト無し)。
    const trace = this.observer === null ? null : new AppendTrace(++this.appendSeq, envelopes.length);
    const enqueuedNs = process.hrtime.bigint();
    const endNormalize = trace?.begin('writer.normalize', { events: envelopes.length });
    const rows = envelopes.map((envelope) => normalizeEnvelope(
      envelope, this.now(), this.writerSessionId, ++this.ingestSeq,
    ));
    endNormalize?.({ rows: rows.length });
    if (rows.length === 0) return Promise.resolve();

    // Keep watermark reads and writes in the same serialized queue.  This is
    // required for callers that issue append() without awaiting each promise.
    //
    // R-06: run on a SETTLED queue (then(run, run)) and only store the
    // catch-settled tail.  A failed batch must reject for its own caller
    // (fail-closed) without poisoning every later append/close, which is what
    // turned one transient SQLITE_BUSY into "all raw persistence fails".
    const run = () => {
      const queueWaitMs = round3(msSinceNs(enqueuedNs));
      const endPartition = trace?.begin('writer.partition', { rows: rows.length });
      // R-02: 窓の TTL 失効は append ごとに評価する (全行が再送で棄却された
      // append が続くと、コミット時だけの sweep では古い identity が残るため)。
      if (this.freshDedupe.size > 0) this._sweepFreshDedupe(this.now());
      const groups = new Map();
      for (const row of rows) {
        const key = `${row.market}\u0000${row.stream}`;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
      }
      const freshGroups = [];
      const lateGroups = [];
      const committed = [];
      for (const [key, groupRows] of groups) {
        const sorted = sortBatchRows(groupRows);
        const fresh = this._partitionLate(key, sorted, lateGroups);
        // R-02: コミット済み identity の再送 (再接続スナップショット等) を追記前に棄却。
        const kept = this._dropFreshRedeliveries(key, fresh);
        if (kept.length > 0) {
          freshGroups.push(kept);
          committed.push({ key, rows: kept });
        }
      }
      endPartition?.({
        groups: groups.size,
        fresh_groups: freshGroups.length,
        late_groups: lateGroups.length,
      });
      // gzip は同期実行で event loop を止めるため、時間を分けて記録する。
      const endGzip = trace?.begin('writer.gzip_fresh', { groups: freshGroups.length });
      const freshBatches = freshGroups.map(makeBatch);
      endGzip?.();
      // 計測器の解放は報告するかに関係なく必ず行う (Astra P1)。
      const report = (outcome) => {
        if (trace === null) return;
        const metrics = trace.summary();
        if (this.slowAppendMs < 0 || metrics.wall_ms < this.slowAppendMs) return;
        if (typeof this.observer?.report !== 'function') return;
        this.observer.report({
          kind: 'slow_append',
          outcome,
          append: trace.id,
          events: rows.length,
          groups: groups.size,
          queue_wait_ms: queueWaitMs,
          total_ms: metrics.wall_ms,
          ...metrics,
          spans: trace.spansJson(),
        });
      };
      return this._insertBatches(freshBatches, lateGroups, trace).then((result) => {
        // 窓への登録はコミット成功後のみ (失敗しても再投入で失われない)。
        const committedAtMs = this.now();
        const endRemember = trace?.begin('writer.remember_fresh', { groups: committed.length });
        for (const group of committed) {
          this._rememberFreshIdentities(group.key, group.rows, committedAtMs);
        }
        endRemember?.();
        report('ok');
        return result;
      }, (error) => {
        // 失敗した append も、遅かったなら内訳を残す (原因追跡のため)。
        report('error');
        throw error;
      });
    };
    const operation = this.queue.then(run, run);
    this.queue = operation.catch(() => {});
    return operation;
  }

  /**
   * Canonical raw (issues #10/#11): append source frames as IMMUTABLE rows.
   *
   * Contract:
   *   - INSERT only — existing rows are never updated, merged, re-sorted or
   *     deleted. Physical order is the append order (frame_id AUTOINCREMENT).
   *   - Arrival order is the caller's array order; event_ts_ms / recv_ts_ms
   *     are metadata only and never reorder rows (unlike the legacy path).
   *   - Append-safe dedupe: a row whose (connection_id, receive_seq) already
   *     exists is SKIPPED inside the transaction (no UPDATE of the old row).
   *     Rows without receive_seq (non-socket observations) always append.
   *   - Fail-closed: unexpected errors roll back the whole market batch.
   * @param {Array<Object>} envelopes
   * @returns {Promise<void>}
   */
  appendCanonical(envelopes) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    const rows = envelopes.map((envelope) => normalizeCanonicalEnvelope(
      envelope, this.now(), this.writerSessionId, ++this.ingestSeq,
    ));
    if (rows.length === 0) return Promise.resolve();
    // run on settled queue: a failed batch rejects for its caller (fail-closed)
    // but never poisons the queue for later batches.
    const run = () => this._insertCanonicalRows(rows);
    const operation = this.queue.then(run, run);
    this.queue = operation.catch(() => {});
    return operation;
  }

  async _insertCanonicalRows(rows) {
    const byMarket = new Map();
    for (const row of rows) {
      if (!byMarket.has(row.market)) byMarket.set(row.market, []);
      byMarket.get(row.market).push(row);
    }
    for (const [market, marketRows] of byMarket) {
      await this._withMarketDb(market, async ({ db }) => {
        const insert = db.prepare(`
          INSERT INTO canonical_frames (
            schema, market, stream, channel, connection_id, receive_seq,
            recv_ts_ms, recv_mono_ns, source_event_ts_ms,
            source_event_time_known, event_ts_ms, ingest_seq,
            writer_session_id, frame_json, frame_sha256, written_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        await this._runWithBusyRetry(db, () => {
          db.exec('BEGIN IMMEDIATE');
          try {
            for (const row of marketRows) {
              try {
                insert.run(
                  row.schema, row.market, row.stream, row.channel,
                  row.connection_id, row.receive_seq, row.recv_ts_ms,
                  row.recv_mono_ns, row.source_event_ts_ms,
                  row.source_event_time_known, row.event_ts_ms,
                  row.ingest_seq, row.writer_session_id,
                  row.frame_json, row.frame_sha256, row.written_at_ms,
                );
                this.canonicalStats.appended += 1;
              } catch (error) {
                if (isUniqueViolation(error)) {
                  this.canonicalStats.skippedDuplicates += 1;
                  continue; // append-safe dedupe: skip, never update
                }
                throw error;
              }
            }
            db.exec('COMMIT');
          } catch (error) {
            try { db.exec('ROLLBACK'); } catch (_) {}
            throw error;
          }
        });
      });
    }
  }
  // groupRows (event順ソート済み) の先頭が既存データより古いか判定し、
  // 遅着分と通常分に分割する。戻り値は通常分 (makeBatch 済み配列へpushされる)。
  _partitionLate(key, sortedRows, lateGroups) {
    const [market, stream] = key.split('\u0000');
    const entry = this.databases.get(market);
    let lastEventTs = null;
    if (entry) {
      try {
        const row = entry.db.prepare(`
          SELECT max(last_event_ts_ms) AS ts
          FROM raw_batches WHERE stream = ?
        `).get(stream);
        lastEventTs = row?.ts === null || row?.ts === undefined ? null : Number(row.ts);
      } catch (error) {
        throw new Error(
          `failed to read raw batch watermark for ${market}/${stream}`,
          { cause: error },
        );
      }
    }
    // Equal-to-watermark timestamps stay on the fresh path.  This avoids
    // duplicate boundary rows in a new backfill batch; strictly older rows
    // are the late-event path below.
    if (lastEventTs === null || sortedRows.length === 0
      || eventOrderKey(sortedRows[0]) >= lastEventTs) {
      return sortedRows; // 通常フロー: 全部新着
    }
    // 遅着と新着の境界を見つける (sortedRows は event_ts 昇順)
    let split = 0;
    while (split < sortedRows.length && eventOrderKey(sortedRows[split]) < lastEventTs) split++;
    const [lateRows, freshRows] = [sortedRows.slice(0, split), sortedRows.slice(split)];
    lateGroups.push({ market, stream, rows: lateRows });
    console.error(
      `[RawSqliteWriter] ${market}/${stream}: ${lateRows.length} late event(s) `
      + `(oldest=${eventOrderKey(lateRows[0])}, watermark=${lastEventTs}) routed to backfill`,
    );
    return freshRows;
  }

  _insertBatches(batches, lateGroups = [], trace = null) {
    const byMarket = new Map();
    for (const batch of batches) {
      if (!byMarket.has(batch.market)) byMarket.set(batch.market, []);
      byMarket.get(batch.market).push(batch);
    }
    return (async () => {
    for (const [market, marketBatches] of byMarket) {
      const endMarket = trace?.begin('writer.market', {
        market,
        batches: marketBatches.length,
        rows: marketBatches.reduce((sum, batch) => sum + (batch.row_count ?? 0), 0),
      });
      try {
      await this._withMarketDb(market, async ({ db }) => {
        const insert = db.prepare(`
          INSERT INTO raw_batches (
            schema, market, stream, first_event_ts_ms, last_event_ts_ms,
            first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
            written_at_ms
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        await this._runWithBusyRetry(db, () => {
          // 書きロック取得待ち (busy_timeout) と INSERT/COMMIT を分けて測る。
          const endLock = trace?.begin('writer.lockWait', { market });
          db.exec('BEGIN IMMEDIATE');
          endLock?.();
          const endInsert = trace?.begin('writer.insert', { market, batches: marketBatches.length });
          try {
            for (const batch of marketBatches) {
              insert.run(
                batch.schema, batch.market, batch.stream,
                batch.first_event_ts_ms, batch.last_event_ts_ms,
                batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
                batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
              );
            }
            db.exec('COMMIT');
          } catch (error) {
            try { db.exec('ROLLBACK'); } catch (_) {}
            throw error;
          } finally {
            endInsert?.();
          }
        });
      }, trace);
      } finally {
        endMarket?.();
      }
    }
    for (const group of lateGroups) {
      // sub-span の隙間も見えるよう遅着処理全体を囲む (Astra P1)。
      const endBackfill = trace?.begin('writer.backfill', {
        market: group.market, stream: group.stream, rows: group.rows.length,
      });
      try {
        await this._backfillLateGroup(group, trace);
      } finally {
        endBackfill?.();
      }
    }
    })();
  }

  async _withMarketDb(market, fn, trace = null) {
    let entry;
    // DB オープン自体がファイルロックで待つことがある。
    const endOpen = trace?.begin('writer.openDb', { market });
    for (let attempt = 0; ; attempt++) {
      try {
        entry = this._openMarket(market);
        break;
      } catch (error) {
        if (!isBusyError(error) || attempt >= 2) throw error;
        await new Promise((resolve) => setTimeout(resolve, [100, 250, 500][attempt]));
      }
    }
    endOpen?.();
    return fn(entry);
  }

  async _runWithBusyRetry(db, fn) {
    for (let attempt = 0; ; attempt++) {
      try {
        await fn();
        return;
      } catch (error) {
        if (!isBusyError(error) || attempt >= 2) throw error;
        await new Promise((resolve) => setTimeout(resolve, [100, 250, 500][attempt]));
      }
    }
  }

  /**
   * 遅着候補バッチの選択。述語・並び・返す列は従来と完全に同一で、
   * SQLite に選ばせるアクセスパスのみを固定する。
   *
   * 計測 (2026-09-21): 候補が 1〜3 件でも 1 回 110〜233ms かかっていた。
   * 原因は `first_event_ts_ms <= maxTs` を索引で前方走査し、返った各行の
   * last_event を判定するために表へ戻るため = 「対象時刻以前の全バッチ」を
   * 舐めるコストで、履歴長に比例して伸びる。本番の append 内訳でも 3 市場で
   * 792ms + 609ms + 206ms を計上していた (186k バッチ/市場)。
   *
   * `last_event_ts_ms >= minTs` 側は、遅着が通常ここ最近の再接続リプレイ
   * (秒〜分前) であるため索引範囲が短く、read-only 実測で 0.0ms だった。
   * 述語集合は変わらないので選択結果は同一 (どの行が該当するかは SQL が決める)。
   * 索引が無い DB では従来のクエリへフォールバックする。
   */
  _selectLateCandidates(db, stream, minTs, maxTs) {
    if (this._hasLastEventIndex(db)) {
      const candidates = db.prepare(`
        SELECT batch_id, first_event_ts_ms, last_event_ts_ms, raw_gzip,
               first_recv_ts_ms, last_recv_ts_ms, written_at_ms
        FROM raw_batches INDEXED BY raw_batches_stream_last_event_idx
        WHERE stream = ? AND first_event_ts_ms <= ? AND last_event_ts_ms >= ?
        ORDER BY first_event_ts_ms ASC
      `).all(stream, maxTs, minTs);
      return { candidates, path: 'last_event_idx' };
    }
    const candidates = db.prepare(`
      SELECT batch_id, first_event_ts_ms, last_event_ts_ms, raw_gzip,
             first_recv_ts_ms, last_recv_ts_ms, written_at_ms
      FROM raw_batches
      WHERE stream = ? AND first_event_ts_ms <= ? AND last_event_ts_ms >= ?
      ORDER BY first_event_ts_ms ASC
    `).all(stream, maxTs, minTs);
    return { candidates, path: 'default' };
  }

  /** (stream, last_event_ts_ms) 索引の有無 (DB ごとに 1 回だけ確認)。 */
  _hasLastEventIndex(db) {
    let available = lastEventIndexCache.get(db);
    if (available === undefined) {
      try {
        available = db.prepare(
          "SELECT count(*) AS n FROM sqlite_master WHERE type = 'index' AND name = 'raw_batches_stream_last_event_idx'",
        ).get().n > 0;
      } catch {
        available = false;
      }
      lastEventIndexCache.set(db, available);
    }
    return available;
  }

  // 遅着イベントの本来位置への差し込み。
  // - 既存batchの [first,last_event_ts_ms] 範囲内に収まる行はそのbatchへマージ
  //   (batch_id 不変なので下流の batch_id cursor は安全)
  // - どのbatchにも属しない行(時間ギャップ内)は mini-batch として新規作成
  async _backfillLateGroup({ market, stream, rows }, trace = null) {
    if (!rows || rows.length === 0) return;
    const key = `${market}\u0000${stream}`;
    await this._withMarketDb(market, async ({ db }) => {
      const minTs = eventOrderKey(rows[0]);
      const maxKey = eventOrderKey(rows[rows.length - 1]);
      const maxTs = maxKey === Number.MAX_SAFE_INTEGER ? minTs : maxKey;
      const stats = {
        mergedBatches: 0, mergedRows: 0, minibatches: 0, overlaps: 0, duplicatesSkipped: 0,
      };
      await this._runWithBusyRetry(db, () => {
        const endBackfillLock = trace?.begin('writer.backfill.lockWait', { market, stream });
        db.exec('BEGIN IMMEDIATE');
        endBackfillLock?.();
        const endBackfillSelect = trace?.begin('writer.backfill.select', { market, stream, rows: rows.length });
        try {
          // Re-read candidate metadata after acquiring the write lock.  This
          // keeps selection and mutation on one SQLite snapshot and retries
          // both together when the database is busy.
          const selection = this._selectLateCandidates(db, stream, minTs, maxTs);
          const candidates = selection.candidates;
          endBackfillSelect?.({ candidates: candidates.length, path: selection.path });
          const endBackfillMatch = trace?.begin('writer.backfill.match', { rows: rows.length });
          const byBatch = new Map(); // batch_id -> rows
          const leftovers = [];
          for (const row of rows) {
            const ts = eventOrderKey(row);
            if (ts === Number.MAX_SAFE_INTEGER) { leftovers.push(row); continue; }
            const matches = candidates.filter((c) => ts >= Number(c.first_event_ts_ms)
              && ts <= Number(c.last_event_ts_ms));
            let target;
            if (matches.length > 1) {
              // Legacy writers could leave overlapping [first,last] ranges in
              // raw_batches.  Fail-open with a deterministic pick (tightest
              // range wins, ties broken by lowest batch_id) so ingestion keeps
              // flowing; the anomaly stays observable via stats + warn log.
              target = matches.reduce((best, c) => {
                const w = Number(c.last_event_ts_ms) - Number(c.first_event_ts_ms);
                const bw = Number(best.last_event_ts_ms) - Number(best.first_event_ts_ms);
                if (w !== bw) return w < bw ? c : best;
                return Number(c.batch_id) < Number(best.batch_id) ? c : best;
              });
              stats.overlaps += 1;
              console.error(
                `[RawSqliteWriter] OVERLAP ${market}/${stream}: `
                + `${matches.length} batches cover event_ts_ms=${ts} `
                + `(${matches.map((c) => `batch_id=${c.batch_id}[${c.first_event_ts_ms},${c.last_event_ts_ms}]`).join(', ')}); `
                + `picked batch_id=${target.batch_id}`,
              );
            } else {
              target = matches[0];
            }
            if (target) {
              if (!byBatch.has(target.batch_id)) byBatch.set(target.batch_id, []);
              byBatch.get(target.batch_id).push(row);
            } else {
              leftovers.push(row);
            }
          }

          endBackfillMatch?.({ batches: byBatch.size, leftovers: leftovers.length });
          // gunzip→行挿入→gzip を含む区間 (遅着マージの本体)。
          const endBackfillMerge = trace?.begin('writer.backfill.merge', { batches: byBatch.size });
          for (const [batchId, incoming] of byBatch) {
            const meta = candidates.find((c) => Number(c.batch_id) === Number(batchId));
            const updated = mergeLinesIntoBatch(meta, incoming);
            stats.duplicatesSkipped += updated.duplicatesSkipped ?? 0;
            // R-03: 全行が既存行の再投入だった場合は batch を書き換えない
            // (行数も gzip も不変なので UPDATE は無駄。冪等の要)。
            if ((updated.mergedRows ?? incoming.length) === 0) continue;
            db.prepare(`
              UPDATE raw_batches SET
                first_event_ts_ms = ?, last_event_ts_ms = ?,
                first_recv_ts_ms = ?, last_recv_ts_ms = ?,
                row_count = ?, raw_gzip = ?, raw_bytes = ?
              WHERE batch_id = ?
            `).run(
              updated.first_event_ts_ms, updated.last_event_ts_ms,
              updated.first_recv_ts_ms, updated.last_recv_ts_ms,
              updated.row_count, updated.raw_gzip, updated.raw_bytes,
              Number(batchId),
            );
            stats.mergedBatches++;
            stats.mergedRows += updated.mergedRows ?? incoming.length;
          }
          endBackfillMerge?.({
            merged_batches: stats.mergedBatches,
            merged_rows: stats.mergedRows,
            duplicates_skipped: stats.duplicatesSkipped,
          });
          if (leftovers.length > 0) {
            // R-03: mini-batch 内の重複も落とす (同一行の再投入で行数を水増ししない)。
            const unique = [];
            const seen = new Set();
            for (const row of leftovers) {
              const line = typeof row.raw_line === 'string' ? row.raw_line : JSON.stringify(row);
              const identity = lineIdentityKey(line);
              if (seen.has(identity)) {
                stats.duplicatesSkipped += 1;
                continue;
              }
              seen.add(identity);
              unique.push(row);
            }
            if (unique.length > 0) {
              const endMini = trace?.begin('writer.backfill.minibatch', { rows: unique.length });
              stats.minibatches = 1;
              const insert = db.prepare(`
                INSERT INTO raw_batches (
                  schema, market, stream, first_event_ts_ms, last_event_ts_ms,
                  first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
                  written_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              `);
              // leftovers全体を1つのソート済みmini-batchとして登録する
              const batch = makeBatch(sortBatchRows(unique));
              insert.run(
                batch.schema, batch.market, batch.stream,
                batch.first_event_ts_ms, batch.last_event_ts_ms,
                batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
                batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
              );
              endMini?.();
            }
          }
          const endBackfillCommit = trace?.begin('writer.backfill.commit', { market, stream });
          db.exec('COMMIT');
          endBackfillCommit?.();
        } catch (error) {
          try { db.exec('ROLLBACK'); } catch (_) {}
          throw error;
        }
      });

      const s = this._bumpLateStats(key, 'merged', stats.mergedBatches);
      this._bumpLateStats(key, 'mergedRows', stats.mergedRows);
      this._bumpLateStats(key, 'minibatches', stats.minibatches);
      if (stats.overlaps > 0) this._bumpLateStats(key, 'overlaps', stats.overlaps);
      // R-03: 再投入された同一行を棄却した件数 (可観測性)。
      if (stats.duplicatesSkipped > 0) {
        this._bumpLateStats(key, 'duplicatesSkipped', stats.duplicatesSkipped);
      }
      console.error(
        `[RawSqliteWriter] ${market}/${stream}: backfilled ${rows.length} late row(s) `
        + `into ${stats.mergedBatches} batch(es) (${stats.mergedRows} merged, `
        + `${stats.minibatches} mini-batch${stats.minibatches === 1 ? '' : 'es'}`
        + (stats.overlaps > 0 ? `, overlaps=${stats.overlaps}` : '')
        + (stats.duplicatesSkipped > 0 ? `, duplicates skipped=${stats.duplicatesSkipped}` : '')
        + `) [total merged=${s.merged}]`,
      );
    });
  }

  // Retention TTL applies to the legacy mutable raw_batches layer ONLY.
  // canonical_frames (issues #10/#11) is the immutable full-history source
  // log and is deliberately TTL-exempt: the receiver never decides canonical
  // history is disposable. Committed canonical rows must stay reconstructible
  // from the saved source frames, so removing expired rows is an explicit
  // operator action (manual DELETE / archive job) — never an automatic
  // background prune. This trades unbounded storage growth for full-history
  // reconstructability (Issue #9/#10/#11 Done conditions).
  //
  // R-06: a prune must never poison ingestion.  It runs on a settled queue
  // (then(run, run)) with the same busy retry as the write paths, and a lock
  // that survives the retries is logged + counted and deferred to the next
  // scheduled run instead of surfacing as a fail-closed shutdown of the whole
  // receiver.  Non-busy errors (real I/O faults) still reject for the caller.
  pruneExpired(nowMs = this.now()) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    const cutoff = Math.trunc(nowMs - this.retentionDays * 24 * 60 * 60 * 1000);
    const run = async () => {
      let failure = null;
      for (const { market, db } of this.databases.values()) {
        try {
          await this._runWithBusyRetry(db, () => {
            // Legacy normalized batches only — canonical_frames rows are never
            // deleted here (TTL-exempt append-only log).
            const info = db.prepare('DELETE FROM raw_batches WHERE last_recv_ts_ms < ?').run(cutoff);
            this.pruneStats.deletedRows += Number(info?.changes ?? 0);
            db.exec('PRAGMA wal_checkpoint(PASSIVE)');
          });
          this.pruneStats.runs += 1;
        } catch (error) {
          this.pruneStats.failures += 1;
          this.pruneStats.lastError = error?.message ?? String(error);
          if (isBusyError(error)) {
            // Deferrable: the next scheduled prune retries the same rows.
            this.pruneStats.deferredBusy += 1;
            console.error(
              `[RawSqliteWriter] pruneExpired deferred for ${market}: ${error.message} `
              + `(retention stays pending, ingestion unaffected)`,
            );
            continue;
          }
          console.error(`[RawSqliteWriter] pruneExpired failed for ${market}: ${error.message}`);
          failure ??= error;
        }
      }
      if (failure) throw failure;
    };
    const operation = this.queue.then(run, run);
    this.queue = operation.catch(() => {});
    return operation;
  }

  /** @returns {{runs: number, deletedRows: number, failures: number, deferredBusy: number, lastError: string|null}} */
  getPruneStats() {
    return { ...this.pruneStats };
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await this.queue;
    for (const { db } of this.databases.values()) db.close();
    this.databases.clear();
  }
}

export function decodeRawBatch(rawGzip) {
  return gunzipSync(rawGzip).toString('utf8').trim().split('\n').filter(Boolean);
}

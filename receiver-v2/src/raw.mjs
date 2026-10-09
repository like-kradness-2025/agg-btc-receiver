/**
 * The canonical raw writer: the v1-compatible `raw_batches` sink, one database per market.
 *
 * What it is for (docs: Set 7a of the split). Reception is the only place that sees every frame at
 * the socket boundary, so reception is where the canonical record of what was received is written -
 * before anything downstream is allowed to judge the frame. The raw is therefore written on
 * reception and is deliberately independent of whether the board later accepts or refuses the frame:
 * "received" is a fact about reception, not a verdict downstream.
 *
 * The shape is v1's, on purpose. The downstream reader (`agg-btc-downstream`) opens these files with
 * unchanged SQL and an unchanged parser: one file per market, one `raw_batches` table, the same
 * eleven columns and the same index names, and `raw_gzip` holding one v1 envelope per line, gzip
 * level 1. A deployment can point the existing downstream at `<rawDir>/<market>.sqlite` and nothing
 * changes.
 *
 * The envelope line is v1's field set:
 *   { schema, market, stream, event_ts_ms, recv_ts_ms, recv_mono_ns, receive_seq,
 *     source_event_ts_ms, source_event_time_known, writer_session_id, connection_id,
 *     sequence_order, source_id, payload }
 * Downstream requires `market`, `stream`, `event_ts_ms` > 0 and, per stream, `payload.price/qty/side`
 * (trades) or `payload.type/bids/asks/seq` (book) - so a record missing any of the three is refused
 * here rather than written as a row nothing can read.
 *
 * Batching follows v1: rows accumulate for a market and stream and are confirmed either when the
 * window closes (10 s) or when the group reaches 16,384 rows, whichever comes first. One group is one
 * row, `batch_id` is AUTOINCREMENT (monotonic within the market), and the decompressed line count of
 * `raw_gzip` is exactly `row_count` - the invariant downstream leans on.
 *
 * Ownership and failure: one market has one writer, in one process (the same single-owner discipline
 * as `ingest/store.mjs`). A write that fails throws and is never swallowed: the raw is this service's
 * primary product, so a raw that cannot be written stops the process loudly instead of leaving a run
 * that looks healthy while recording nothing. A failed group stays pending, so the caller's retry (or
 * the process's stop path) can try again rather than losing the rows.
 */

import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
import path from 'node:path';

export const RAW_BATCH_SCHEMA = 'raw_v6_sqlite';
export const DEFAULT_RAW_BATCH_WINDOW_MS = 10_000;
export const DEFAULT_RAW_BATCH_MAX_ROWS = 16_384;

/**
 * The index set. The first four are byte-for-byte v1's; `raw_batches_stream_batch_idx` is the one the
 * Set 7 spec adds (stream + batch_id) so a downstream cursor can walk a stream by batch id without a
 * table scan. An extra index changes no column and no reader, so a v1 reader is unaffected by it.
 */
export const RAW_BATCH_INDEXES = Object.freeze([
  'raw_batches_recv_idx',
  'raw_batches_stream_idx',
  'raw_batches_stream_batch_idx',
  'raw_batches_stream_first_event_idx',
  'raw_batches_stream_last_event_idx',
]);

/** v1's table definition, verbatim: same columns, same types, same order, same NOT NULLs. */
export const RAW_BATCH_TABLE_SQL = `
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
`;

function safeMarketName(market) {
  const name = String(market).replace(/[^a-zA-Z0-9_-]/g, '_');
  if (!name) throw new TypeError('market must produce a non-empty filename');
  return name;
}

function configureDatabase(db) {
  db.exec(`
    PRAGMA busy_timeout=0;
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=FULL;
  `);
  db.exec(RAW_BATCH_TABLE_SQL);
  db.exec(`
    CREATE INDEX IF NOT EXISTS raw_batches_recv_idx
      ON raw_batches(last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_idx
      ON raw_batches(stream, last_recv_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_batch_idx
      ON raw_batches(stream, batch_id);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_first_event_idx
      ON raw_batches(stream, first_event_ts_ms);
    CREATE INDEX IF NOT EXISTS raw_batches_stream_last_event_idx
      ON raw_batches(stream, last_event_ts_ms);
  `);
}

function requirePositiveMs(value, name) {
  if (!Number.isInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive integer, got ${JSON.stringify(value)}`);
  }
  return value;
}

function optionalInt(value) {
  if (value === null || value === undefined) return null;
  if (!Number.isInteger(value)) {
    throw new TypeError(`expected an integer or null, got ${JSON.stringify(value)}`);
  }
  return value;
}

/**
 * Normalize one raw record into the envelope line the batch will carry. Everything that downstream
 * requires is enforced here: a record the reader could not parse is refused before it becomes a row.
 */
export function envelopeLineFor(record, { writerSessionId = null, schema = RAW_BATCH_SCHEMA } = {}) {
  if (!record || typeof record !== 'object') throw new TypeError('raw record must be an object');
  if (typeof record.market !== 'string' || record.market.length === 0) {
    throw new TypeError('raw record needs a non-empty market');
  }
  if (typeof record.stream !== 'string' || record.stream.length === 0) {
    throw new TypeError('raw record needs a non-empty stream');
  }
  requirePositiveMs(record.event_ts_ms, 'raw record event_ts_ms');
  requirePositiveMs(record.recv_ts_ms, 'raw record recv_ts_ms');
  if (record.payload === null || typeof record.payload !== 'object' || Array.isArray(record.payload)) {
    throw new TypeError('raw record needs a payload object');
  }
  const receiveSeq = optionalInt(record.receive_seq);
  const envelope = {
    schema: typeof record.schema === 'string' && record.schema.length > 0 ? record.schema : schema,
    market: record.market,
    stream: record.stream,
    event_ts_ms: record.event_ts_ms,
    recv_ts_ms: record.recv_ts_ms,
    recv_mono_ns: optionalInt(record.recv_mono_ns),
    receive_seq: receiveSeq,
    source_event_ts_ms: optionalInt(record.source_event_ts_ms),
    source_event_time_known: record.source_event_time_known === true ? 1 : 0,
    writer_session_id: record.writer_session_id ?? writerSessionId,
    connection_id: record.connection_id ?? null,
    // sequence_order is the reception-side order the frame arrived in; it defaults to the receive
    // sequence, which is monotonic per connection.
    sequence_order: optionalInt(record.sequence_order) ?? receiveSeq,
    source_id: record.source_id ?? null,
    // v1 put the worker's metadata into the payload as well as the envelope (`buildRawDbEnvelope`),
    // and the downstream reads `payload.connection_id` as a fallback: the payload must carry them
    // too, or a receiver change looks like no change at all.
    payload: {
      ...record.payload,
      worker_seq: record.worker_seq ?? receiveSeq,
      connection_id: record.connection_id ?? null,
      sequence_order: optionalInt(record.sequence_order) ?? receiveSeq,
    },
  };
  return JSON.stringify(envelope);
}

/** The single-line JSON text of one record, as it will be stored (no trailing newline). */
function encodeLine(record, writerSessionId) {
  return envelopeLineFor(record, { writerSessionId });
}

/**
 * Build one batch row from normalized rows. Kept beside the writer so the gzip invariant (one line
 * per row) has exactly one author.
 */
function makeBatchRow(rows, { schema, writerSessionId, writtenAtMs }) {
  const lines = rows.map((row) => encodeLine(row, writerSessionId));
  const rawText = `${lines.join('\n')}\n`;
  const eventTimes = rows.map((row) => row.event_ts_ms).filter((value) => Number.isInteger(value));
  const recvTimes = rows.map((row) => row.recv_ts_ms).filter((value) => Number.isInteger(value));
  return {
    schema,
    market: rows[0].market,
    stream: rows[0].stream,
    first_event_ts_ms: eventTimes.length ? Math.min(...eventTimes) : null,
    last_event_ts_ms: eventTimes.length ? Math.max(...eventTimes) : null,
    first_recv_ts_ms: Math.min(...recvTimes),
    last_recv_ts_ms: Math.max(...recvTimes),
    row_count: rows.length,
    raw_gzip: gzipSync(Buffer.from(rawText, 'utf8'), { level: 1 }),
    raw_bytes: Buffer.byteLength(rawText, 'utf8'),
    written_at_ms: writtenAtMs,
  };
}

/**
 * Open a raw writer rooted at `dir`. Databases are opened lazily, one per market, the first time a
 * row for that market is confirmed; a market that is never written leaves no file behind.
 */
export function openRawWriter({
  dir,
  nowMs = () => Date.now(),
  setTimer = setTimeout,
  clearTimer = clearTimeout,
  batchWindowMs = DEFAULT_RAW_BATCH_WINDOW_MS,
  maxBatchRows = DEFAULT_RAW_BATCH_MAX_ROWS,
  writerSessionId = null,
  onError = null,
  Database = DatabaseSync,
} = {}) {
  if (typeof dir !== 'string' || dir.length === 0) throw new TypeError('the raw writer needs a directory');
  if (!Number.isFinite(batchWindowMs) || batchWindowMs <= 0) {
    throw new TypeError('batchWindowMs must be a positive number of milliseconds');
  }
  if (!Number.isInteger(maxBatchRows) || maxBatchRows <= 0) {
    throw new TypeError('maxBatchRows must be a positive integer');
  }
  mkdirSync(dir, { recursive: true });

  const databases = new Map();
  // Pending rows, keyed by market and stream. Insertion order is preserved so a batch's rows are the
  // order they were received in.
  const pending = new Map();
  let flushTimer = null;
  let closed = false;

  function groupKeyOf(market, stream) {
    return `${market}\u0000${stream}`;
  }

  function openMarket(market) {
    const existing = databases.get(market);
    if (existing) return existing;
    const databasePath = path.join(dir, `${safeMarketName(market)}.sqlite`);
    const db = new Database(databasePath);
    try {
      configureDatabase(db);
    } catch (error) {
      try { db.close(); } catch { /* the original failure is the one to report */ }
      throw error;
    }
    const entry = { market, databasePath, db };
    databases.set(market, entry);
    return entry;
  }

  function clearFlushTimer() {
    if (flushTimer !== null) {
      clearTimer(flushTimer);
      flushTimer = null;
    }
  }

  function armFlushTimer() {
    if (closed || flushTimer !== null) return;
    flushTimer = setTimer(() => {
      flushTimer = null;
      try {
        flush();
      } catch (error) {
        // A failure raised from a timer has no caller to throw to. It is handed to the owner
        // (which stops reception loudly) rather than becoming an uncaught exception while the
        // rows sit in memory that a process death would erase.
        if (typeof onError === 'function') onError(error);
        else throw error;
      }
    }, batchWindowMs);
    if (typeof flushTimer?.unref === 'function') flushTimer.unref();
  }

  /**
   * Confirm every pending group as one batch row. The groups are written in the order they were first
   * seen, so batch_id rises with arrival. A failure rolls the whole transaction back and leaves the
   * rows pending, so nothing is lost and nothing half-written is kept.
   */
  function flush() {
    if (closed) return 0;
    clearFlushTimer();
    if (pending.size === 0) return 0;
    const entries = [...pending.entries()].map(([key, rows]) => ({ key, rows }));
    const writtenAtMs = nowMs();
    const batches = entries.map(({ rows }) => makeBatchRow(rows, { schema: RAW_BATCH_SCHEMA, writerSessionId, writtenAtMs }));

    // Group the batch rows by market; each market's batches go into that market's database in one
    // transaction, so a market is all-or-nothing.
    const byMarket = new Map();
    batches.forEach((batch, index) => {
      const rows = entries[index].rows;
      if (!byMarket.has(batch.market)) byMarket.set(batch.market, { batches: [], keys: [] });
      byMarket.get(batch.market).batches.push(batch);
      byMarket.get(batch.market).keys.push(entries[index].key);
    });

    let written = 0;
    for (const [market, group] of byMarket) {
      const entry = openMarket(market);
      const insert = entry.db.prepare(`
        INSERT INTO raw_batches (
          schema, market, stream, first_event_ts_ms, last_event_ts_ms,
          first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
          written_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      entry.db.exec('BEGIN IMMEDIATE');
      try {
        for (const batch of group.batches) {
          insert.run(
            batch.schema, batch.market, batch.stream,
            batch.first_event_ts_ms, batch.last_event_ts_ms,
            batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
            batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
          );
          written += 1;
        }
        entry.db.exec('COMMIT');
      } catch (error) {
        try { entry.db.exec('ROLLBACK'); } catch { /* the original failure is the one to report */ }
        throw error;
      }
      for (const key of group.keys) pending.delete(key);
    }
    return written;
  }

  return {
    /**
     * Buffer one record. It is confirmed when the window closes or the group reaches its row cap,
     * whichever comes first. A record that cannot be encoded is refused here (a programming error the
     * caller must fix); a write that fails throws after `flush` has left the rows pending.
     */
    append(record) {
      if (closed) throw new Error('the raw writer is closed');
      // Encode eagerly so a malformed record is refused at the call that produced it, not later from
      // inside a timer where the caller cannot see it.
      encodeLine(record, writerSessionId);
      const key = groupKeyOf(record.market, record.stream);
      let rows = pending.get(key);
      if (rows === undefined) {
        rows = [];
        pending.set(key, rows);
      }
      rows.push(record);
      if (rows.length >= maxBatchRows) {
        flush();
        return;
      }
      armFlushTimer();
    },

    /** Confirm everything now; a failure throws and leaves the rows pending. */
    flush,

    close() {
      if (closed) return;
      try {
        flush();
      } finally {
        closed = true;
        clearFlushTimer();
        for (const { db } of databases.values()) db.close();
        databases.clear();
      }
    },

    get dir() {
      return dir;
    },
    get pendingRows() {
      let total = 0;
      for (const rows of pending.values()) total += rows.length;
      return total;
    },
    get pendingGroups() {
      return pending.size;
    },
  };
}

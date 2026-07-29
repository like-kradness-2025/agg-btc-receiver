// Market-split SQLite WAL sink for receiver raw events.
// One process owns the writers; other local processes may read each DB while
// it is being written because WAL keeps readers on a stable snapshot.

import { DatabaseSync } from 'node:sqlite';
import { mkdir, readdir } from 'node:fs/promises';
import { gunzipSync, gzipSync } from 'node:zlib';
import path from 'node:path';

export const RAW_SQLITE_SCHEMA = 'raw_v6_sqlite';
export const DEFAULT_RAW_RETENTION_DAYS = 90;

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
  `);
}

export class RawSqliteWriter {
  constructor({ databaseDir, retentionDays = DEFAULT_RAW_RETENTION_DAYS, now = () => Date.now() } = {}) {
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
    const rows = envelopes.map((envelope) => normalizeEnvelope(
      envelope, this.now(), this.writerSessionId, ++this.ingestSeq,
    ));
    if (rows.length === 0) return Promise.resolve();
    const groups = new Map();
    for (const row of rows) {
      const key = `${row.market}\u0000${row.stream}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(row);
    }
    const batches = [...groups.values()].map(makeBatch);
    this.queue = this.queue.then(() => this._insertBatches(batches));
    return this.queue;
  }

  _insertBatches(batches) {
    const byMarket = new Map();
    for (const batch of batches) {
      if (!byMarket.has(batch.market)) byMarket.set(batch.market, []);
      byMarket.get(batch.market).push(batch);
    }
    return (async () => {
    for (const [market, marketBatches] of byMarket) {
      let entry;
      for (let attempt = 0; ; attempt++) {
        try {
          entry = this._openMarket(market);
          break;
        } catch (error) {
          if (!isBusyError(error) || attempt >= 2) throw error;
          await new Promise(resolve => setTimeout(resolve, [100, 250, 500][attempt]));
        }
      }
      const { db } = entry;
      const insert = db.prepare(`
        INSERT INTO raw_batches (
          schema, market, stream, first_event_ts_ms, last_event_ts_ms,
          first_recv_ts_ms, last_recv_ts_ms, row_count, raw_gzip, raw_bytes,
          written_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      for (let attempt = 0; ; attempt++) {
       try {
        db.exec('BEGIN IMMEDIATE');
        for (const batch of marketBatches) {
          insert.run(
            batch.schema, batch.market, batch.stream,
            batch.first_event_ts_ms, batch.last_event_ts_ms,
            batch.first_recv_ts_ms, batch.last_recv_ts_ms, batch.row_count,
            batch.raw_gzip, batch.raw_bytes, batch.written_at_ms,
          );
        }
        db.exec('COMMIT');
        break;
      } catch (error) {
        try { db.exec('ROLLBACK'); } catch (_) {}
        if (!isBusyError(error) || attempt >= 2) throw error;
        await new Promise(resolve => setTimeout(resolve, [100, 250, 500][attempt]));
      }
    }
    }
    })();
  }

  pruneExpired(nowMs = this.now()) {
    if (this.closed) return Promise.reject(new Error('raw SQLite writer is closed'));
    const cutoff = Math.trunc(nowMs - this.retentionDays * 24 * 60 * 60 * 1000);
    this.queue = this.queue.then(() => {
      for (const { db } of this.databases.values()) {
        db.prepare('DELETE FROM raw_batches WHERE last_recv_ts_ms < ?').run(cutoff);
        db.exec('PRAGMA wal_checkpoint(PASSIVE)');
      }
    });
    return this.queue;
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

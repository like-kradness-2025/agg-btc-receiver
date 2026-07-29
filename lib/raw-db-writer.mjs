// Single-file DuckDB sink for receiver raw events.
// One process owns the connection; workers send envelopes to that process.
// Raw JSON is kept losslessly in gzip batches to avoid per-event DB overhead.

import duckdb from 'duckdb';
import { mkdir } from 'node:fs/promises';
import { gunzipSync, gzipSync } from 'node:zlib';
import path from 'node:path';

export const RAW_DB_SCHEMA = 'raw_v5_duckdb';
export const DEFAULT_RAW_RETENTION_DAYS = 90;

function openDatabase(databasePath) {
  return new Promise((resolve, reject) => {
    let db;
    try {
      db = new duckdb.Database(databasePath, (error) => error ? reject(error) : resolve(db));
    } catch (error) {
      reject(error);
    }
  });
}

function exec(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.exec(sql, (error) => error ? reject(error) : resolve());
  });
}

function run(connection, sql, ...params) {
  return new Promise((resolve, reject) => {
    connection.run(sql, ...params, (error) => error ? reject(error) : resolve());
  });
}

function all(connection, sql, ...params) {
  return new Promise((resolve, reject) => {
    connection.all(sql, ...params, (error, rows) => error ? reject(error) : resolve(rows));
  });
}

function scalarText(value) {
  return value === null || value === undefined ? null : String(value);
}

function finiteMs(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : fallback;
}

function normalizeEnvelope(envelope, nowMs) {
  if (!envelope || typeof envelope !== 'object') throw new TypeError('raw envelope must be an object');
  if (typeof envelope.market !== 'string' || !envelope.market) throw new TypeError('raw envelope market is required');
  if (typeof envelope.stream !== 'string' || !envelope.stream) throw new TypeError('raw envelope stream is required');

  const rawLine = typeof envelope.raw_line === 'string'
    ? envelope.raw_line.replace(/\n$/, '')
    : JSON.stringify(envelope);

  return {
    schema: typeof envelope.schema === 'string' ? envelope.schema : RAW_DB_SCHEMA,
    market: envelope.market,
    stream: envelope.stream,
    event_ts_ms: finiteMs(envelope.event_ts_ms),
    recv_ts_ms: finiteMs(envelope.recv_ts_ms, nowMs),
    writer_session_id: scalarText(envelope.writer_session_id),
    ingest_seq: scalarText(envelope.ingest_seq),
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

export class RawDbWriter {
  constructor({ databasePath, retentionDays = DEFAULT_RAW_RETENTION_DAYS, now = () => Date.now() } = {}) {
    if (!databasePath) throw new TypeError('databasePath is required');
    if (!Number.isInteger(retentionDays) || retentionDays < 1) {
      throw new TypeError('retentionDays must be a positive integer');
    }
    this.databasePath = path.resolve(databasePath);
    this.retentionDays = retentionDays;
    this.now = now;
    this.db = null;
    this.connection = null;
    this.queue = Promise.resolve();
    this.closed = false;
  }

  async open() {
    await mkdir(path.dirname(this.databasePath), { recursive: true });
    this.db = await openDatabase(this.databasePath);
    this.connection = this.db.connect();
    await exec(this.connection, 'CREATE SEQUENCE IF NOT EXISTS raw_batch_id_seq START 1');
    const tables = new Set((await all(this.connection, `
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'main' AND table_name IN ('raw_batches', 'raw_events')
    `)).map((row) => row.table_name));

    if (tables.has('raw_events')) {
      await this._migrateLegacyRows();
    } else if (!tables.has('raw_batches')) {
      await this._createBatchTable('raw_batches');
    }
    return this;
  }

  async _createBatchTable(tableName, withSequenceDefault = true) {
    await exec(this.connection, `
      CREATE TABLE ${tableName} (
        batch_id UBIGINT${withSequenceDefault ? " DEFAULT nextval('raw_batch_id_seq')" : ''},
        schema VARCHAR NOT NULL,
        market VARCHAR NOT NULL,
        stream VARCHAR NOT NULL,
        first_event_ts_ms BIGINT,
        last_event_ts_ms BIGINT,
        first_recv_ts_ms BIGINT NOT NULL,
        last_recv_ts_ms BIGINT NOT NULL,
        row_count BIGINT NOT NULL,
        raw_gzip BLOB NOT NULL,
        raw_bytes BIGINT NOT NULL,
        written_at_ms BIGINT NOT NULL
      );
    `);
  }

  async _migrateLegacyRows() {
    const columns = new Set((await all(this.connection, `
      SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_events'
    `)).map((row) => row.column_name));
    const migrationTable = 'raw_batches_duckdb_migration';
    await exec(this.connection, `DROP TABLE IF EXISTS ${migrationTable}`);
    await this._createBatchTable(migrationTable, true);

    const selectRaw = columns.has('raw_line') ? 'raw_line' : 'raw_gzip';
    const legacyRows = await all(this.connection, `
      SELECT schema, market, stream, event_ts_ms, recv_ts_ms,
             writer_session_id, ingest_seq, source_id, ${selectRaw} AS raw_value, written_at_ms
      FROM raw_events ORDER BY event_id
    `);
    const groups = new Map();
    for (const row of legacyRows) {
      const key = `${row.market}\u0000${row.stream}`;
      if (!groups.has(key)) groups.set(key, []);
      const rawLine = columns.has('raw_line')
        ? String(row.raw_value).replace(/\n$/, '')
        : gunzipSync(row.raw_value).toString('utf8').replace(/\n$/, '');
      groups.get(key).push({
        schema: row.schema,
        market: row.market,
        stream: row.stream,
        event_ts_ms: row.event_ts_ms === null ? null : Number(row.event_ts_ms),
        recv_ts_ms: Number(row.recv_ts_ms),
        writer_session_id: row.writer_session_id,
        ingest_seq: row.ingest_seq,
        source_id: row.source_id,
        raw_line: rawLine,
        written_at_ms: Number(row.written_at_ms),
      });
    }

    for (const rows of groups.values()) {
      for (let start = 0; start < rows.length; start += 512) {
        await this._insertBatches([makeBatch(rows.slice(start, start + 512))], migrationTable, false);
      }
    }

    await exec(this.connection, `
      DROP TABLE raw_events;
      ALTER TABLE ${migrationTable} RENAME TO raw_batches;
    `);
  }

  append(envelopes) {
    if (this.closed) return Promise.reject(new Error('raw DB writer is closed'));
    const rows = envelopes.map((envelope) => normalizeEnvelope(envelope, this.now()));
    if (rows.length === 0) return Promise.resolve();
    const groups = new Map();
    for (const row of rows) {
      const key = `${row.market}\u0000${row.stream}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(row);
    }
    const batches = [...groups.values()].map(makeBatch);
    this.queue = this.queue.then(() => this._insertBatches(batches, 'raw_batches', false));
    return this.queue;
  }

  async _insertBatches(batches, tableName, includeBatchId) {
    const placeholders = batches.map(() => `(${includeBatchId ? '?, ' : ''}?, ?, ?, ?, ?, ?, ?, ?, from_hex(?), ?, ?)`).join(', ');
    const params = batches.flatMap((batch) => [
      ...(includeBatchId ? [batch.batch_id] : []),
      batch.schema,
      batch.market,
      batch.stream,
      batch.first_event_ts_ms,
      batch.last_event_ts_ms,
      batch.first_recv_ts_ms,
      batch.last_recv_ts_ms,
      batch.row_count,
      batch.raw_gzip.toString('hex'),
      batch.raw_bytes,
      batch.written_at_ms,
    ]);
    await run(
      this.connection,
      `INSERT INTO ${tableName} (
        ${includeBatchId ? 'batch_id, ' : ''}schema, market, stream,
        first_event_ts_ms, last_event_ts_ms, first_recv_ts_ms, last_recv_ts_ms,
        row_count, raw_gzip, raw_bytes, written_at_ms
      ) VALUES ${placeholders}`,
      ...params,
    );
  }

  pruneExpired(nowMs = this.now()) {
    if (this.closed) return Promise.reject(new Error('raw DB writer is closed'));
    const cutoff = Math.trunc(nowMs - this.retentionDays * 24 * 60 * 60 * 1000);
    this.queue = this.queue.then(async () => {
      await run(this.connection, 'DELETE FROM raw_batches WHERE last_recv_ts_ms < ?', cutoff);
      await exec(this.connection, 'CHECKPOINT');
    });
    return this.queue;
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await this.queue;
    if (this.connection) {
      await exec(this.connection, 'CHECKPOINT');
      await new Promise((resolve) => this.connection.close(() => resolve()));
      this.connection = null;
    }
    if (this.db) {
      await new Promise((resolve) => this.db.close(() => resolve()));
      this.db = null;
    }
  }
}

import { test } from 'node:test';
import assert from 'node:assert/strict';
import duckdb from 'duckdb';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { gunzipSync } from 'node:zlib';
import { RawDbWriter } from '../lib/raw-db-writer.mjs';

function query(databasePath, sql) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(databasePath);
    db.all(sql, (error, rows) => {
      db.close();
      if (error) reject(error);
      else resolve(rows);
    });
  });
}

function envelope(market, stream, recvTs, payload) {
  return {
    schema: 'raw_v5_duckdb',
    market,
    stream,
    event_ts_ms: recvTs - 2,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:1',
    ingest_seq: null,
    source_id: null,
    payload,
  };
}

test('RawDbWriter appends compressed raw batches without time-partitioned paths', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-db-'));
  const databasePath = path.join(root, 'agg-btc-receiver.duckdb');
  const writer = await new RawDbWriter({ databasePath, retentionDays: 90 }).open();

  await writer.append([
    envelope('binance_perp', 'trades', 1_000, { price: 100 }),
    envelope('kraken_spot', 'book_updates', 2_000, { type: 'update' }),
  ]);
  await writer.close();

  const rows = await query(databasePath, 'SELECT market, stream, row_count, raw_bytes, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 2);
  assert.deepEqual(rows.map((row) => [row.market, row.stream, row.row_count]), [
    ['binance_perp', 'trades', 1n],
    ['kraken_spot', 'book_updates', 1n],
  ]);
  const decoded = rows.map((row) => JSON.parse(gunzipSync(row.raw_gzip).toString('utf8').trim()));
  assert.equal(decoded[0].payload.price, 100);
  assert.equal(decoded[1].payload.type, 'update');
  assert.equal((await fs.readdir(root)).filter((name) => name.includes('2026')).length, 0);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawDbWriter prunes rows older than the retention window', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-db-retention-'));
  const databasePath = path.join(root, 'agg-btc-receiver.duckdb');
  const writer = await new RawDbWriter({ databasePath, retentionDays: 90 }).open();
  const nowMs = 10_000_000_000;

  await writer.append([
    envelope('m', 'trades', nowMs - 91 * 24 * 60 * 60 * 1000, { old: true }),
  ]);
  await writer.append([
    envelope('m', 'trades', nowMs - 89 * 24 * 60 * 60 * 1000, { old: false }),
  ]);
  await writer.pruneExpired(nowMs);
  await writer.close();

  const rows = await query(databasePath, 'SELECT row_count, raw_gzip FROM raw_batches');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].row_count, 1n);
  assert.equal(JSON.parse(gunzipSync(rows[0].raw_gzip).toString('utf8').trim()).payload.old, false);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawDbWriter migrates the previous duplicated JSON schema', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-db-migration-'));
  const databasePath = path.join(root, 'agg-btc-receiver.duckdb');
  const db = new duckdb.Database(databasePath);
  await new Promise((resolve, reject) => db.exec(`
    CREATE SEQUENCE raw_event_id_seq START 1;
    CREATE TABLE raw_events (
      event_id UBIGINT DEFAULT nextval('raw_event_id_seq'), schema VARCHAR, market VARCHAR,
      stream VARCHAR, event_ts_ms BIGINT, recv_ts_ms BIGINT, writer_session_id VARCHAR,
      ingest_seq VARCHAR, source_id VARCHAR, payload_json VARCHAR, raw_line VARCHAR, written_at_ms BIGINT
    );
    INSERT INTO raw_events (schema, market, stream, event_ts_ms, recv_ts_ms, writer_session_id, payload_json, raw_line, written_at_ms)
    VALUES ('raw_v5_duckdb', 'm', 'trades', 100, 101, 'legacy', '{"x":1}', '{"schema":"raw_v5_duckdb","market":"m","stream":"trades","event_ts_ms":100,"recv_ts_ms":101,"payload":{"x":1}}', 101);
  `, (error) => error ? reject(error) : resolve()));
  db.close();

  const writer = await new RawDbWriter({ databasePath }).open();
  await writer.close();
  const columns = await query(databasePath, "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_batches' ORDER BY ordinal_position");
  assert.deepEqual(columns.map((row) => row.column_name), [
    'batch_id', 'schema', 'market', 'stream', 'first_event_ts_ms', 'last_event_ts_ms',
    'first_recv_ts_ms', 'last_recv_ts_ms', 'row_count', 'raw_gzip', 'raw_bytes', 'written_at_ms',
  ]);
  await fs.rm(root, { recursive: true, force: true });
});

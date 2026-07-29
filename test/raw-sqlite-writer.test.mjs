import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { gunzipSync } from 'node:zlib';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';

function envelope(market, stream, recvTs, payload) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream,
    event_ts_ms: recvTs - 2,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:sqlite',
    payload,
  };
}

function query(databasePath, sql) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try { return db.prepare(sql).all(); } finally { db.close(); }
}

test('RawSqliteWriter splits databases by market and allows concurrent readers', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    envelope('binance_perp', 'trades', 1_000, { price: 100 }),
    envelope('bybit_perp', 'book_updates', 2_000, { type: 'update' }),
  ]);

  const binancePath = path.join(root, 'binance_perp.sqlite');
  const bybitPath = path.join(root, 'bybit_perp.sqlite');
  assert.equal((await fs.stat(binancePath)).isFile(), true);
  assert.equal((await fs.stat(bybitPath)).isFile(), true);
  assert.equal(query(binancePath, 'SELECT sum(row_count) AS n FROM raw_batches')[0].n, 1);
  const raw = query(binancePath, 'SELECT raw_gzip FROM raw_batches')[0].raw_gzip;
  assert.equal(JSON.parse(gunzipSync(raw).toString('utf8').trim()).payload.price, 100);

  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter prunes by receive time without deleting other market data', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-retention-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, retentionDays: 90 }).open();
  const nowMs = 10_000_000_000;
  await writer.append([envelope('m1', 'trades', nowMs - 91 * 24 * 60 * 60 * 1000, { old: true })]);
  await writer.append([envelope('m2', 'trades', nowMs - 89 * 24 * 60 * 60 * 1000, { old: false })]);
  await writer.pruneExpired(nowMs);
  assert.equal(query(path.join(root, 'm1.sqlite'), 'SELECT count(*) AS n FROM raw_batches')[0].n, 0);
  assert.equal(query(path.join(root, 'm2.sqlite'), 'SELECT sum(row_count) AS n FROM raw_batches')[0].n, 1);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

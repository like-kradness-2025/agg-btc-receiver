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

test('RawSqliteWriter stores batch rows in event_ts_ms order', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-order-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const base = envelope('order_test', 'trades', 10_000);
  const missingTs = envelope('order_test', 'trades', 6000);
  delete missingTs.event_ts_ms;
  await writer.append([
    { ...base, event_ts_ms: 3000, recv_ts_ms: 5000 },
    { ...base, event_ts_ms: 1000, recv_ts_ms: 3000 },
    { ...base, event_ts_ms: 2000, recv_ts_ms: 4000 },
    missingTs,
  ]);
  await writer.close();

  const row = query(path.join(root, 'order_test.sqlite'),
    'SELECT raw_gzip, row_count, first_event_ts_ms, last_event_ts_ms FROM raw_batches')[0];
  assert.equal(Number(row.row_count), 4);
  assert.equal(Number(row.first_event_ts_ms), 1000);
  assert.equal(Number(row.last_event_ts_ms), 3000);
  const lines = gunzipSync(row.raw_gzip).toString('utf8').trim().split('\n').map((line) => JSON.parse(line));
  assert.deepEqual(lines.map((line) => line.event_ts_ms), [1000, 2000, 3000, undefined]);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter keeps ingest_seq order for equal event_ts_ms', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-tie-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    { ...envelope('tie_test', 'trades', 2000), event_ts_ms: 1000, ingest_seq: '5' },
    { ...envelope('tie_test', 'trades', 2000), event_ts_ms: 1000, ingest_seq: '2' },
    { ...envelope('tie_test', 'trades', 3000), event_ts_ms: 1000, ingest_seq: '9' },
  ]);
  await writer.close();

  const row = query(path.join(root, 'tie_test.sqlite'), 'SELECT raw_gzip FROM raw_batches')[0];
  const lines = gunzipSync(row.raw_gzip).toString('utf8').trim().split('\n').map((line) => JSON.parse(line));
  assert.deepEqual(lines.map((line) => line.ingest_seq), ['2', '5', '9']);
  await fs.rm(root, { recursive: true, force: true });
});

// ---- Late-event backfill ----------------------------------------------------

function lateEnvelope(market, stream, eventTs, recvTs, payload) {
  return {
    schema: 'raw_v6_sqlite',
    market,
    stream,
    event_ts_ms: eventTs,
    recv_ts_ms: recvTs,
    writer_session_id: 'test:sqlite',
    payload,
  };
}

test('RawSqliteWriter merges late events into the batch covering their time range', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-merge-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('late_merge', 'trades', 1000, 5000, { i: 1 }),
    lateEnvelope('late_merge', 'trades', 2000, 5100, { i: 2 }),
    lateEnvelope('late_merge', 'trades', 3000, 5200, { i: 3 }),
  ]);
  // Late arrival inside the existing [1000, 3000] range
  await writer.append([lateEnvelope('late_merge', 'trades', 2500, 9000, { late: true })]);
  await writer.close();

  const rows = query(path.join(root, 'late_merge.sqlite'),
    'SELECT batch_id, row_count, first_event_ts_ms, last_event_ts_ms, first_recv_ts_ms, last_recv_ts_ms, raw_gzip FROM raw_batches');
  assert.equal(rows.length, 1, 'late event must be merged into the existing batch, not appended');
  const row = rows[0];
  assert.equal(Number(row.row_count), 4);
  assert.equal(Number(row.first_event_ts_ms), 1000);
  assert.equal(Number(row.last_event_ts_ms), 3000);
  assert.equal(Number(row.first_recv_ts_ms), 5000);
  assert.equal(Number(row.last_recv_ts_ms), 9000);
  const lines = gunzipSync(row.raw_gzip).toString('utf8').trim().split('\n').map((line) => JSON.parse(line));
  assert.deepEqual(lines.map((line) => line.event_ts_ms), [1000, 2000, 2500, 3000],
    'merged line must sit at its event-time position');
  assert.equal(lines[2].payload.late, true);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter creates a mini-batch for late events outside every batch range', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-gap-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([lateEnvelope('gap_test', 'trades', 10_000, 10_050, { i: 1 })]);
  // Both fall into the time gap before the only batch — one combined mini-batch
  await writer.append([
    lateEnvelope('gap_test', 'trades', 5_000, 20_000, { gap: true }),
    lateEnvelope('gap_test', 'trades', 7_000, 20_010, { gap2: true }),
  ]);
  await writer.close();

  const rows = query(path.join(root, 'gap_test.sqlite'),
    'SELECT batch_id, row_count, first_event_ts_ms, last_event_ts_ms, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 2, 'gap events must share a single mini-batch');
  assert.equal(Number(rows[0].row_count), 1);
  assert.equal(Number(rows[0].first_event_ts_ms), 10_000, 'original batch metadata must stay intact');
  assert.equal(Number(rows[1].row_count), 2);
  assert.equal(Number(rows[1].first_event_ts_ms), 5_000);
  assert.equal(Number(rows[1].last_event_ts_ms), 7_000);
  const gapLines = gunzipSync(rows[1].raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(gapLines.map((l) => l.event_ts_ms), [5_000, 7_000]);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter splits a mixed append into late backfill plus fresh batch', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-mixed-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('mixed_test', 'trades', 1000, 5000, { i: 1 }),
    lateEnvelope('mixed_test', 'trades', 3000, 5200, { i: 3 }),
  ]);
  await writer.append([
    lateEnvelope('mixed_test', 'trades', 1500, 9000, { late: true }), // inside [1000,3000]
    lateEnvelope('mixed_test', 'trades', 3500, 9010, { fresh: true }), // ahead of watermark
  ]);
  await writer.close();

  const rows = query(path.join(root, 'mixed_test.sqlite'),
    'SELECT batch_id, row_count, first_event_ts_ms, last_event_ts_ms, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 2);
  const merged = rows[0];
  assert.equal(Number(merged.row_count), 3);
  assert.equal(Number(merged.first_event_ts_ms), 1000);
  assert.equal(Number(merged.last_event_ts_ms), 3000);
  const mergedLines = gunzipSync(merged.raw_gzip).toString('utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.deepEqual(mergedLines.map((l) => l.event_ts_ms), [1000, 1500, 3000]);
  const fresh = rows[1];
  assert.equal(Number(fresh.row_count), 1);
  assert.equal(Number(fresh.first_event_ts_ms), 3500);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter exposes late-event statistics', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-stats-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('stats_test', 'trades', 10_000, 10_050, { i: 1 }),
    lateEnvelope('stats_test', 'trades', 11_000, 10_150, { i: 2 }),
    lateEnvelope('stats_test', 'trades', 12_000, 10_250, { i: 3 }),
  ]);
  await writer.append([
    lateEnvelope('stats_test', 'trades', 8_000, 20_000, { gap: true }),
    lateEnvelope('stats_test', 'trades', 11_500, 20_010, { merge: true }),
  ]);
  const summary = writer.lateEventSummary();
  assert.deepEqual(summary['stats_test.trades'], { merged: 1, mergedRows: 1, minibatches: 1 });
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter uses top-level sort keys even when payload fields come first', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-key-order-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('key_order', 'trades', 1000, 5000, { i: 1 }),
    lateEnvelope('key_order', 'trades', 3000, 5100, { i: 3 }),
  ]);
  const incoming = lateEnvelope('key_order', 'trades', 1500, 9000,
    { event_ts_ms: -999, recv_ts_ms: -999, i: 2 });
  incoming.raw_line = JSON.stringify({
    payload: incoming.payload,
    event_ts_ms: incoming.event_ts_ms,
    ingest_seq: incoming.ingest_seq,
    recv_ts_ms: incoming.recv_ts_ms,
    market: incoming.market,
    stream: incoming.stream,
  });
  await writer.append([incoming]);
  await writer.close();

  const row = query(path.join(root, 'key_order.sqlite'), 'SELECT raw_gzip FROM raw_batches')[0];
  const lines = gunzipSync(row.raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines.map((line) => line.event_ts_ms), [1000, 1500, 3000]);
  const meta = query(path.join(root, 'key_order.sqlite'),
    'SELECT first_recv_ts_ms, last_recv_ts_ms FROM raw_batches')[0];
  assert.equal(Number(meta.first_recv_ts_ms), 5000);
  assert.equal(Number(meta.last_recv_ts_ms), 9000);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter serializes concurrent append calls before watermark routing', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-concurrent-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('concurrent', 'trades', 1000, 5000, { i: 1 }),
    lateEnvelope('concurrent', 'trades', 3000, 5100, { i: 3 }),
  ]);
  const late = writer.append([lateEnvelope('concurrent', 'trades', 1500, 9000, { late: true })]);
  const fresh = writer.append([lateEnvelope('concurrent', 'trades', 4000, 9010, { fresh: true })]);
  await Promise.all([late, fresh]);
  await writer.close();

  const rows = query(path.join(root, 'concurrent.sqlite'),
    'SELECT row_count, first_event_ts_ms, last_event_ts_ms, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 2);
  const merged = gunzipSync(rows[0].raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(merged.map((line) => line.event_ts_ms), [1000, 1500, 3000]);
  assert.equal(Number(rows[1].first_event_ts_ms), 4000);
  assert.equal(Number(rows[1].last_event_ts_ms), 4000);
  await fs.rm(root, { recursive: true, force: true });
});

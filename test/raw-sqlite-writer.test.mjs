import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';
import { RawSqliteWriter, mergeLinesIntoBatch } from '../lib/raw-sqlite-writer.mjs';

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
  assert.deepEqual(summary['stats_test.trades'],
    { merged: 1, mergedRows: 1, minibatches: 1, overlaps: 0, duplicatesSkipped: 0 });
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter resolves overlapping legacy batch ranges deterministically', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-overlap-'));
  // Seed two non-overlapping batches through the normal writer path...
  {
    const seed = await new RawSqliteWriter({ databaseDir: root }).open();
    await seed.append([envelope('overlap_m', 'trades', 6_000, { i: 'a' })]);
    await seed.append([envelope('overlap_m', 'trades', 6_200, { i: 'b' })]);
    await seed.close();
  }
  // ...then rewrite batch 2's range so both cover the same instant, mimicking
  // the overlap bands left behind by the legacy independent-append writer.
  {
    const raw = new DatabaseSync(path.join(root, 'overlap_m.sqlite'));
    try {
      raw.prepare('UPDATE raw_batches SET first_event_ts_ms = ?, last_event_ts_ms = ? WHERE batch_id = ?')
        .run(5_900, 6_199, 2);
    } finally { raw.close(); }
  }

  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  // event_ts_ms = 5998 falls inside BOTH batch ranges. Must not throw; must
  // pick the tightest range (batch 1, width 0) deterministically.
  await writer.append([envelope('overlap_m', 'trades', 6_000, { i: 'late' })]);
  await writer.close();

  const rows = query(path.join(root, 'overlap_m.sqlite'),
    'SELECT batch_id, row_count FROM raw_batches ORDER BY batch_id');
  assert.deepEqual(rows.map((r) => [Number(r.batch_id), Number(r.row_count)]),
    [[1, 2], [2, 1]]);
  const pickedRaw = query(path.join(root, 'overlap_m.sqlite'),
    'SELECT raw_gzip FROM raw_batches WHERE batch_id = 1')[0].raw_gzip;
  const pickedLines = gunzipSync(pickedRaw).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(pickedLines.map((line) => line.payload.i), ['a', 'late']);
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

// ---- R-03: idempotent late merge -------------------------------------------
// Regression: re-merging the SAME late row used to append a second copy of it
// (row_count 2 -> 3, two identical lines inside one batch). The merge must be
// identity-checked: a line already present in the batch is skipped and counted.

test('mergeLinesIntoBatch skips lines already present in the batch (R-03)', () => {
  const lineA = JSON.stringify({ event_ts_ms: 5000, ingest_seq: 7, source_id: 'T7' });
  const lineB = JSON.stringify({ event_ts_ms: 6000, ingest_seq: 8, source_id: 'T8' });
  const meta = {
    raw_gzip: gzipSync(Buffer.from(`${lineA}\n`, 'utf8')),
    first_recv_ts_ms: 100,
    last_recv_ts_ms: 100,
  };

  const again = mergeLinesIntoBatch(meta, [{ raw_line: lineA, recv_ts_ms: 200 }]);
  assert.equal(again.row_count, 1, 'a line already in the batch must not be inserted twice');
  assert.equal(again.mergedRows, 0);
  assert.equal(again.duplicatesSkipped, 1);
  assert.equal(again.first_recv_ts_ms, 100, 'a rejected duplicate must not widen the recv window');
  assert.equal(again.last_recv_ts_ms, 100);

  // A genuinely new line still merges at its event-time position.
  const merged = mergeLinesIntoBatch(meta, [{ raw_line: lineB, recv_ts_ms: 300 }]);
  assert.equal(merged.row_count, 2);
  assert.equal(merged.mergedRows, 1);
  assert.equal(merged.duplicatesSkipped, 0);
  assert.equal(merged.last_recv_ts_ms, 300);
});

test('RawSqliteWriter re-merging the same late row is idempotent (R-03)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-idem-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('late_idem', 'trades', 1000, 5000, { i: 'A' }),
    lateEnvelope('late_idem', 'trades', 3000, 5100, { i: 'C' }),
  ]);
  const reDelivered = () => ({
    ...lateEnvelope('late_idem', 'trades', 2500, 9000, { i: 'L1' }),
    source_id: 'L1',
  });
  await writer.append([reDelivered()]);
  await writer.append([reDelivered()]); // same event delivered a second time
  await writer.close();

  const rows = query(path.join(root, 'late_idem.sqlite'),
    'SELECT batch_id, row_count, first_recv_ts_ms, last_recv_ts_ms, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 1, 'the re-delivered row must merge into the covering batch');
  assert.equal(Number(rows[0].row_count), 3, 're-delivery must not grow row_count');
  const lines = gunzipSync(rows[0].raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines.map((line) => line.payload.i), ['A', 'L1', 'C']);
  assert.equal(Number(rows[0].last_recv_ts_ms), 9000, 'the accepted merge still extends the recv window');
  const summary = writer.lateEventSummary()['late_idem.trades'];
  assert.deepEqual(summary,
    { merged: 1, mergedRows: 1, minibatches: 0, overlaps: 0, duplicatesSkipped: 1 });
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter uses source_id to detect a re-delivered trade with a new ingest_seq (R-03)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-srcid-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('late_src', 'trades', 1000, 5000, { i: 'A' }),
    lateEnvelope('late_src', 'trades', 3000, 5100, { i: 'C' }),
  ]);
  // The re-delivered trade keeps its exchange id (source_id) but the worker
  // assigns a fresh ingest_seq, so a byte comparison alone would miss it.
  const reDelivered = (ingestSeq) => ({
    ...lateEnvelope('late_src', 'trades', 2500, 9000, { i: 'T1' }),
    source_id: 'T1',
    raw_line: JSON.stringify({
      event_ts_ms: 2500, ingest_seq: ingestSeq, source_id: 'T1', payload: { i: 'T1' },
    }),
  });
  await writer.append([reDelivered(41)]);
  await writer.append([reDelivered(99)]);
  await writer.close();

  const row = query(path.join(root, 'late_src.sqlite'),
    'SELECT row_count, raw_gzip FROM raw_batches ORDER BY batch_id')[0];
  assert.equal(Number(row.row_count), 3);
  const lines = gunzipSync(row.raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines.map((line) => line.ingest_seq), [undefined, 41, undefined],
    'only the first delivery of the trade id may be kept');
  assert.equal(writer.lateEventSummary()['late_src.trades'].duplicatesSkipped, 1);
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter de-duplicates a re-queued late row that forms a mini-batch (R-03)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-gap-idem-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([lateEnvelope('gap_idem', 'trades', 10_000, 10_050, { i: 'base' })]);
  // Both copies fall before the only batch range and are re-queued together.
  const gap = () => ({
    ...lateEnvelope('gap_idem', 'trades', 5_000, 20_000, { i: 'G1' }),
    source_id: 'G1',
  });
  await writer.append([gap(), gap()]);
  await writer.close();

  const rows = query(path.join(root, 'gap_idem.sqlite'),
    'SELECT batch_id, row_count, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 2, 'the gap event still gets its own mini-batch');
  assert.equal(Number(rows[0].row_count), 1, 'the original batch must stay intact');
  assert.equal(Number(rows[1].row_count), 1, 'the mini-batch must hold the event once');
  const gapLines = gunzipSync(rows[1].raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(gapLines.map((line) => line.payload.i), ['G1']);
  assert.equal(writer.lateEventSummary()['gap_idem.trades'].duplicatesSkipped, 1);
  await fs.rm(root, { recursive: true, force: true });
});

test('late merge keys a source-less row by ts+seq+session so a restart cannot drop a real event (R-03)', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-late-session-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  await writer.append([
    lateEnvelope('sess_idem', 'book_updates', 1000, 5000, { i: 'A' }),
    lateEnvelope('sess_idem', 'book_updates', 3000, 5100, { i: 'C' }),
  ]);
  // book_updates carries no source_id, so the identity key is (ts, seq, session).
  const reQueued = (session) => ({
    ...lateEnvelope('sess_idem', 'book_updates', 2000, 9000, { i: 'D' }),
    ingest_seq: 4321,
    writer_session_id: session,
  });
  await writer.append([reQueued('w1')]);
  await writer.append([reQueued('w1')]); // the very same envelope re-queued after a flush failure
  await writer.append([reQueued('w2')]); // new process: same ts+seq is a different event
  await writer.close();

  const rows = query(path.join(root, 'sess_idem.sqlite'),
    'SELECT row_count, raw_gzip FROM raw_batches ORDER BY batch_id');
  assert.equal(rows.length, 1);
  assert.equal(Number(rows[0].row_count), 4,
    'a fresh writer session must never be mistaken for a re-delivery');
  const lines = gunzipSync(rows[0].raw_gzip).toString('utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines.map((line) => line.writer_session_id),
    ['test:sqlite', 'w1', 'w2', 'test:sqlite']);
  assert.equal(writer.lateEventSummary()['sess_idem.book_updates'].duplicatesSkipped, 1);
  await fs.rm(root, { recursive: true, force: true });
});

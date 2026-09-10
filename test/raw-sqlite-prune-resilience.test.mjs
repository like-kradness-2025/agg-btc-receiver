// test/raw-sqlite-prune-resilience.test.mjs — R-06 regression tests
//
// Round-3 audit R-06 (P1): pruneExpired had no busy retry and reused
// `this.queue` directly, so one transient SQLITE_BUSY (a 6-hourly retention
// prune hitting another connection's write lock) left the queue rejected and
// every later append/close failed with "database is locked" — ingestion died
// from a deferrable retention task.
//
// Fixed contract pinned here:
//   - pruneExpired retries SQLITE_BUSY and, when the lock survives the retries,
//     logs + counts it (deferredBusy) and RESOLVES, so the scheduled retention
//     task cannot turn into a fail-closed shutdown of the receiver.
//   - a failed prune or append never poisons the writer queue: later appends
//     still persist (fail-closed per batch, not per process).
//
// The writer connection's busy_timeout is lowered in these tests so the real
// SQLITE_BUSY path is exercised quickly (the retry logic, not the timeout, is
// under test).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RawSqliteWriter } from '../lib/raw-sqlite-writer.mjs';

function envelope(market, stream, recvTs, id) {
  return {
    schema: 'raw_v6_sqlite', market, stream,
    event_ts_ms: recvTs, recv_ts_ms: recvTs,
    raw_line: JSON.stringify({ id, recv_ts_ms: recvTs }),
  };
}

function query(databasePath, sql) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try { return db.prepare(sql).all(); } finally { db.close(); }
}

/** Hold an EXCLUSIVE write lock so the writer's own connection gets BUSY. */
function holdExclusiveLock(databasePath) {
  const blocker = new DatabaseSync(databasePath);
  blocker.exec('PRAGMA busy_timeout=0; BEGIN EXCLUSIVE');
  return blocker;
}

test('RawSqliteWriter defers a busy pruneExpired instead of poisoning ingestion', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-prune-busy-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, retentionDays: 1 }).open();
  const oldTs = Date.now() - 3 * 24 * 60 * 60 * 1000;
  await writer.append([envelope('busy_test', 'trades', oldTs, 'old')]);

  const databasePath = path.join(root, 'busy_test.sqlite');
  writer.databases.get('busy_test').db.exec('PRAGMA busy_timeout=200');
  const blocker = holdExclusiveLock(databasePath);
  let pruneOutcome = 'resolved';
  try {
    await writer.pruneExpired(Date.now());
  } catch (error) {
    pruneOutcome = `rejected: ${error.message}`;
  }
  assert.equal(pruneOutcome, 'resolved', 'a busy prune must defer, not fail the receiver');

  const stats = writer.getPruneStats();
  assert.equal(stats.deferredBusy, 1, 'the deferred prune must be counted');
  assert.equal(stats.failures, 1);
  assert.match(String(stats.lastError), /locked|BUSY/i);

  // Ingestion survives: the retry is deferred, the writer is still usable.
  blocker.exec('ROLLBACK');
  blocker.close();
  await writer.append([envelope('busy_test', 'trades', Date.now(), 'new')]);

  // The deferred retention work is still performed by the next run.
  await writer.pruneExpired(Date.now());
  assert.equal(writer.getPruneStats().deferredBusy, 1, 'the second prune must succeed');
  assert.ok(writer.getPruneStats().deletedRows >= 1, 'the expired row must be deleted');
  assert.equal(query(databasePath, 'SELECT row_count FROM raw_batches')[0].row_count, 1);
  const raw = query(databasePath, 'SELECT raw_gzip FROM raw_batches')[0].raw_gzip;
  // The surviving row must keep a real gzip payload. Asserting on
  // `raw.toString('utf8')` was meaningless: gzip bytes are not ASCII text, so
  // the match could never succeed. Check the container instead.
  assert.ok(raw.length > 0, 'the surviving row keeps its payload');
  assert.equal(raw[0], 0x1f, 'gzip magic byte 1');
  assert.equal(raw[1], 0x8b, 'gzip magic byte 2');
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter keeps appending after a busy append fails', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-append-busy-'));
  const writer = await new RawSqliteWriter({ databaseDir: root }).open();
  const databasePath = path.join(root, 'append_busy.sqlite');
  await writer.append([envelope('append_busy', 'trades', Date.now() - 10_000, 'first')]);
  writer.databases.get('append_busy').db.exec('PRAGMA busy_timeout=200');

  const blocker = holdExclusiveLock(databasePath);
  await assert.rejects(
    () => writer.append([envelope('append_busy', 'trades', Date.now(), 'blocked')]),
    /locked|BUSY/i,
  );
  blocker.exec('ROLLBACK');
  blocker.close();

  // Pre-fix this append rejected with the *previous* batch's error (poisoned queue).
  await writer.append([envelope('append_busy', 'trades', Date.now() + 1000, 'after')]);
  await writer.close();
  const rows = query(databasePath, 'SELECT sum(row_count) AS n FROM raw_batches');
  assert.equal(Number(rows[0].n), 2, 'both the earlier and the post-failure row must persist');
  await fs.rm(root, { recursive: true, force: true });
});

test('RawSqliteWriter survives a non-busy prune failure with a healthy queue', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'raw-sqlite-prune-hard-'));
  const writer = await new RawSqliteWriter({ databaseDir: root, retentionDays: 1 }).open();
  await writer.append([envelope('hard_fail', 'trades', Date.now(), 'x')]);

  // Force a non-busy failure: the market's database handle is unusable.
  const entry = writer.databases.get('hard_fail');
  entry.db.close();
  await assert.rejects(() => writer.pruneExpired(Date.now()));

  // The failure is reported for that call, but the queue stays usable.
  writer.databases.delete('hard_fail');
  await writer.append([envelope('healthy', 'trades', Date.now() + 1, 'y')]);
  assert.equal(query(path.join(root, 'healthy.sqlite'), 'SELECT count(*) AS n FROM raw_batches')[0].n, 1);
  await writer.close();
  await fs.rm(root, { recursive: true, force: true });
});

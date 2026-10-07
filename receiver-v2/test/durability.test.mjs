import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { DatabaseSync } from 'node:sqlite';

import { openDurability } from '../src/durability.mjs';

import { internalsOf } from '../src/internal/wiring.mjs';

async function withStore(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'durability-'));
  try {
    return await fn(join(dir, 'state.sqlite'));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('a failure while invalidating a previous run reports itself, not the cleanup that follows', async () => {
  await withStore(async (dbPath) => {
    // A store with an unfinished run in it, so opening it again has something to invalidate.
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    first.beginRun();
    first.close();

    // The invalidation write fails, and so does the rollback that tries to undo it. What the caller must
    // hear about is the write: a failure that hides behind its own cleanup is a failure nobody can fix.
    class RollbackThatFails extends DatabaseSync {
      prepare(sql, ...rest) {
        const statement = super.prepare(sql, ...rest);
        if (/UPDATE run_marker/.test(sql)) {
          return {
            run: () => {
              throw new Error('the invalidation could not be written');
            },
            get: (...args) => statement.get(...args),
            all: (...args) => statement.all(...args),
          };
        }
        return statement;
      }

      exec(sql, ...rest) {
        if (/ROLLBACK/i.test(String(sql))) throw new Error('and the rollback failed as well');
        return super.exec(sql, ...rest);
      }
    }

    assert.throws(
      () => openDurability({ path: dbPath, runId: 'run-2', Database: RollbackThatFails }),
      /could not be written/,
    );
  });
});

test('a run that closes cleanly is the one a restart can trust', async () => {
  await withStore(async (dbPath) => {
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    first.beginRun();
    assert.equal(first.lastCompleteRun(), null, 'nothing has completed while it is still running');
    first.completeRun();
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    assert.deepEqual(second.lastCompleteRun()?.runId, 'run-1');
    second.close();
  });
});

test('starting a new run invalidates the previous marker before anything else', async () => {
  await withStore(async (dbPath) => {
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    first.beginRun();
    first.close();

    // The previous run never completed: opening again must not leave it looking clean.
    const second = openDurability({ path: dbPath, runId: 'run-2' });
    assert.equal(second.lastCompleteRun(), null, 'an unfinished run is not a clean one');
    const states = internalsOf(second).db.prepare('SELECT run_id, state FROM run_marker ORDER BY at_ms').all();
    assert.equal(states.find((row) => row.run_id === 'run-1').state, 'invalidated');
    second.close();
  });
});

test('the obsolete pending-boundary API is absent from the legacy test-only store', async () => {
  await withStore(async (dbPath) => {
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const db = internalsOf(store).db;
    assert.equal(typeof store.advanceWithBoundary, 'undefined');
    assert.equal(typeof store.clearBoundary, 'undefined');
    assert.equal(typeof store.pendingBoundaries, 'undefined');
    assert.equal(
      db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pending_boundary'").get(),
      undefined,
      'the redundant table is not recreated',
    );
    store.close();
  });
});


test('the received tail is what this process can prove, and it is per connection', async () => {
  await withStore(async (dbPath) => {
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    store.beginRun();
    store.updateReceivedTail({
      connectionId: 'conn-1',
      market: 'kraken_spot',
      lastReceivedSeq: 1_000,
      lastRecvMonoNs: 5_000,
    });
    store.updateReceivedTail({
      connectionId: 'conn-2',
      market: 'kraken_spot',
      lastReceivedSeq: 12,
      lastRecvMonoNs: 900,
    });
    assert.equal(store.readReceivedTail('conn-1').lastReceivedSeq, 1_000);
    assert.equal(store.readReceivedTail('conn-2').lastReceivedSeq, 12);
    assert.equal(store.readReceivedTail('conn-9'), null);
    // An update replaces the previous value: it is a lower bound, and it moves forward with reality.
    store.updateReceivedTail({
      connectionId: 'conn-1',
      market: 'kraken_spot',
      lastReceivedSeq: 1_040,
      lastRecvMonoNs: 5_100,
    });
    assert.equal(store.readReceivedTail('conn-1').lastReceivedSeq, 1_040);
    store.close();
  });
});

test('an unprovable range is recorded and stays recorded', async () => {
  await withStore(async (dbPath) => {
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    store.beginRun();
    store.recordSuspectedGap({
      market: 'okx_perp',
      stream: 'trades',
      fromMs: 1_792_000_000_000,
      toMs: 1_792_000_030_000,
      reason: 'abnormal exit: tail after the last durable acknowledgement',
    });
    const gaps = store.suspectedGaps({ market: 'okx_perp' });
    assert.equal(gaps.length, 1);
    assert.equal(gaps[0].fromMs, 1_792_000_000_000);
    assert.match(gaps[0].reason, /abnormal exit/);
    store.close();

    const reopened = openDurability({ path: dbPath, runId: 'run-2' });
    assert.equal(reopened.suspectedGaps().length, 1, 'the loss is still visible after a restart');
    reopened.close();
  });
});

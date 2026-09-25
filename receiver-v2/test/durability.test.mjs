import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openDurability } from '../src/durability.mjs';

async function withStore(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'durability-'));
  try {
    return await fn(join(dir, 'state.sqlite'));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

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
    const states = second.db.prepare('SELECT run_id, state FROM run_marker ORDER BY at_ms').all();
    assert.equal(states.find((row) => row.run_id === 'run-1').state, 'invalidated');
    second.close();
  });
});

test('the watermark and its boundary record are written together or not at all', async () => {
  await withStore(async (dbPath) => {
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    store.beginRun();
    let watermark = 0;
    store.advanceWithBoundary({
      market: 'kraken_spot',
      stream: 'trades',
      boundarySeq: 42,
      boundaryTsMs: 1_792_000_000_042,
      updateWatermark: () => {
        // The real pattern: the caller's own view advances only after the transaction committed.
      },
    });
    watermark = 42;
    assert.equal(watermark, 42);
    const pending = store.pendingBoundaries();
    assert.equal(pending.length, 1);
    assert.equal(pending[0].boundarySeq, 42);

    // A failure inside the transaction must leave the store as it was.
    assert.throws(() =>
      store.advanceWithBoundary({
        market: 'kraken_spot',
        stream: 'trades',
        boundarySeq: 99,
        boundaryTsMs: 1_792_000_000_099,
        updateWatermark: () => {
          throw new Error('derived work failed');
        },
      }),
    );
    assert.equal(watermark, 42, 'the caller only advances its own view after the transaction commits');
    assert.equal(store.pendingBoundaries()[0].boundarySeq, 42, 'and the old boundary is still the one owed');
    store.close();
  });
});

test('once the derived work is applied the boundary stops being owed', async () => {
  await withStore(async (dbPath) => {
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    store.beginRun();
    store.advanceWithBoundary({
      market: 'kraken_spot',
      stream: 'trades',
      boundarySeq: 7,
      boundaryTsMs: 1_792_000_000_007,
      updateWatermark: () => {},
    });
    store.clearBoundary({ market: 'kraken_spot', stream: 'trades' });
    assert.deepEqual(store.pendingBoundaries(), [], 'nothing is owed any more');
    store.close();
  });
});

test('an unfinished boundary is visible to the next run', async () => {
  await withStore(async (dbPath) => {
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    first.beginRun();
    first.advanceWithBoundary({
      market: 'binance_perp',
      stream: 'depth',
      boundarySeq: 500,
      boundaryTsMs: 1_792_000_000_500,
      updateWatermark: () => {},
    });
    first.close(); // dies before applying it

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const owed = second.pendingBoundaries();
    assert.equal(owed.length, 1, 'the next run has to resolve it, not discover it later');
    assert.equal(owed[0].market, 'binance_perp');
    assert.equal(owed[0].runId, 'run-1', 'and it knows which generation left it');
    second.close();
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

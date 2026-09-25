import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openBook } from '../src/book/state.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq},"side":"bid","price":${100 + seq},"size":${seq}}`,
  });

async function withBook(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'book-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('a range applies in order and the position follows it', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    for (const seq of [1, 2, 3]) {
      const result = book.apply(envelope(seq), () => book.board.apply({ side: 'bid', price: 100 + seq, size: seq }));
      assert.equal(result.applied, true);
    }
    assert.equal(book.appliedBoundary.upToSeq, 3);
    assert.equal(book.isRunning, true, 'serving again once a range has landed');
    assert.equal(book.board.size('bid', 103), 3);
    store.close();
  });
});

test('a resend of something already applied is a no-op', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.apply(envelope(1), () => book.board.apply({ side: 'bid', price: 101, size: 1 }));
    book.apply(envelope(2), () => book.board.apply({ side: 'bid', price: 102, size: 2 }));
    const again = book.apply(envelope(2), () => book.board.apply({ side: 'bid', price: 102, size: 999 }));
    assert.equal(again.applied, false);
    assert.equal(again.reason, 'already applied');
    assert.equal(book.board.size('bid', 102), 2, 'the board did not take the second copy');
    assert.equal(book.appliedBoundary.upToSeq, 2);
    store.close();
  });
});

test('a restart resumes from the position the board actually reached', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: first });
    for (const seq of [1, 2, 3, 4]) book.apply(envelope(seq), () => {});
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'trades', durability: second });
    assert.equal(reopened.appliedBoundary.upToSeq, 4, 'the position came back from the store');
    assert.equal(reopened.isRunning, false, 'a reopened book proves its boundary before serving');
    assert.deepEqual(reopened.resumeFrom(), { connectionId: 'conn-1', upToSeq: 4 });
    const replay = reopened.apply(envelope(4), () => { throw new Error('must not run'); });
    assert.equal(replay.applied, false, 'a replay of covered data does not run the apply function');
    second.close();
  });
});

test('a superseded connection is refused by name, and only a newer one replaces it', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.apply(envelope(1, 'conn-1'), () => {});

    const stale = book.apply(envelope(1, 'conn-old'), () => { throw new Error('must not run'); });
    assert.equal(stale.applied, false);
    assert.equal(stale.reason, 'superseded connection', 'the book does not guess which one is current');
    assert.ok(book.lastRefusal, 'and the refusal is recorded rather than swallowed');

    const newer = book.apply({ ...envelope(1, 'conn-2'), generation: 2 }, () => {});
    assert.equal(newer.applied, true, 'a newer generation takes over');
    assert.equal(book.appliedBoundary.connectionId, 'conn-2');
    store.close();
  });
});

test('no boundary, no service: a syncing book does not claim to be running', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    assert.equal(book.isRunning, false, 'a fresh book has proven nothing');
    assert.equal(book.proveBoundary().proven, false, 'with no connection there is nothing to prove');
    book.apply(envelope(1), () => {});
    assert.equal(book.isRunning, true);
    book.beginSync();
    assert.equal(book.isRunning, false, 'a sync in progress means the board is not to be trusted');
    assert.equal(book.proveBoundary().proven, true);
    store.close();
  });
});

test('the position never runs ahead of the state it describes', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.apply(envelope(1), () => book.board.apply({ side: 'bid', price: 101, size: 1 }));

    // The state change fails: the transaction must leave the position where it was, so the same
    // record is applied again later instead of being skipped as already done.
    assert.throws(() =>
      book.apply(envelope(2), () => {
        throw new Error('state change failed');
      }),
    );
    assert.equal(book.appliedBoundary.upToSeq, 1, 'the position did not move without the state');

    const retry = book.apply(envelope(2), () => book.board.apply({ side: 'bid', price: 102, size: 2 }));
    assert.equal(retry.applied, true, 'and it applies cleanly the second time');
    assert.equal(book.appliedBoundary.upToSeq, 2);
    store.close();
  });
});

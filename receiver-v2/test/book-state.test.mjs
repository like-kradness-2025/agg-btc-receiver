import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openBook } from '../src/book/state.mjs';

const envelope = (seq, connectionId = 'conn-1', extra = {}) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 }, // reception stamps the connection's first sequence; without it there is
    // nothing to anchor the boundary, which the book refuses rather than guessing.
    ...extra,
  });

const change = (seq) => ({ side: 'bid', price: 100 + seq, size: seq });

async function withBook(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'book-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('a contiguous range applies, and the board travels with the position', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    for (const seq of [1, 2, 3]) {
      const result = book.apply({ envelope: envelope(seq), changes: [change(seq)] });
      assert.equal(result.applied, true);
    }
    assert.equal(book.appliedBoundary.upToSeq, 3);
    assert.equal(book.board.size('bid', 103), 3);
    // The same transaction wrote both, so the store and the board agree.
    const levels = store.db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n;
    assert.equal(levels, 3, 'every applied range is in the store too');
    store.close();
  });
});

test('a resend of something already applied is a no-op', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    book.apply({ envelope: envelope(1), changes: [change(1)] });
    book.apply({ envelope: envelope(2), changes: [change(2)] });
    const again = book.apply({ envelope: envelope(2), changes: [{ side: 'bid', price: 102, size: 999 }] });
    assert.equal(again.applied, false);
    assert.equal(again.reason, 'already applied');
    assert.equal(book.board.size('bid', 102), 2, 'the board did not take the second copy');
    store.close();
  });
});

test('a frame with a hole before it is refused, and the hole is recorded', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    book.apply({ envelope: envelope(1), changes: [change(1)] });

    const jumped = book.apply({ envelope: envelope(3), changes: [change(3)] });
    assert.equal(jumped.applied, false, 'applying this would make 2 permanently unapplicable');
    assert.equal(jumped.reason, 'gap before this sequence');
    assert.equal(jumped.waitingFor, 2);
    assert.equal(book.appliedBoundary.upToSeq, 1, 'the position did not move over the hole');
    assert.equal(book.openGaps().length, 1, 'and the hole is a recorded fact');

    const missing = book.apply({ envelope: envelope(2), changes: [change(2)] });
    assert.equal(missing.applied, true);
    assert.equal(missing.alsoApplied, 1, 'the frame that arrived early is applied with it');
    assert.equal(book.appliedBoundary.upToSeq, 3);
    assert.equal(book.board.size('bid', 103), 3, 'and its change reached the board');
    assert.deepEqual(book.openGaps(), [], 'nothing is left waiting');
    store.close();
  });
});

test('without a first sequence there is nothing to anchor the boundary to', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1'); // a connection is taken over only by an explicit accept
    const bare = makeEnvelope({
      market: 'kraken_spot',
      stream: 'trades',
      connectionId: 'conn-1',
      receiveSeq: 7,
      recvTsMs: 1_792_000_000_007,
      recvMonoNs: 7,
      raw: '{"seq":7}',
    });
    const refused = book.apply({ envelope: bare, changes: [change(7)] });
    assert.equal(refused.applied, false, 'starting in the middle would be a guess, not a boundary');
    assert.equal(refused.reason, 'first sequence unknown');
    assert.equal(book.board.size('bid', 107), null, 'and nothing reached the board');
    store.close();
  });
});

test('a restart restores the board and the position together', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: first });
      book.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    for (const seq of [1, 2, 3, 4]) {
      book.apply({ envelope: envelope(seq), changes: [change(seq)] });
    }
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'trades', durability: second });
    reopened.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    assert.equal(reopened.appliedBoundary.upToSeq, 4, 'the position came back from the store');
    assert.equal(reopened.isRunning, false, 'a reopened book proves its boundary before serving');
    assert.equal(reopened.board.size('bid', 104), 4, 'and so did the board the position describes');
    assert.equal(reopened.board.depth, 4);
    const replay = reopened.apply({ envelope: envelope(4), changes: [change(4)] });
    assert.equal(replay.applied, false, 'a replay of covered data is still a no-op');
    second.close();
  });
});

test('only a strictly newer generation replaces the connection', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1', { firstSeq: 1 }); // a connection is taken over only by an explicit accept
    book.apply({ envelope: envelope(1, 'conn-1'), changes: [change(1)] });

    // A numbered generation takes over from an unnumbered one, which is how the first reconnect
    // after a restart looks.
    const newer = book.accept('conn-2', { generation: 2 });
    assert.equal(newer.accepted, true, 'a strictly newer generation takes over');
    const newerApplied = book.apply({ envelope: { ...envelope(1, 'conn-2'), generation: 2 }, changes: [] });
    assert.equal(newerApplied.applied, true);
    assert.equal(book.appliedBoundary.connectionId, 'conn-2');

    // Now a lower number is genuinely stale, whichever connection it names.
    const older = book.accept('conn-old', { generation: 1 });
    assert.equal(older.accepted, false, 'a positive generation is not automatically a newer one');
    assert.equal(older.reason, 'superseded connection');
    assert.ok(book.lastRefusal, 'and the refusal is recorded rather than swallowed');

    const newest = book.accept('conn-3', { generation: 3 });
    assert.equal(newest.accepted, true, 'only something strictly newer takes over');
    store.close();
  });
});

test('the accepted generation is written down before any data is applied', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.apply({ envelope: envelope(1, 'conn-1'), changes: [] });
    book.accept('conn-2', { generation: 5 }); // accepted, nothing applied from it yet
    store.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'trades', durability: second });
    assert.equal(reopened.appliedBoundary.connectionId, 'conn-2');
    assert.equal(reopened.appliedBoundary.generation, 5, 'the generation survived the restart');
    const stale = reopened.accept('conn-1', { generation: 1 });
    assert.equal(stale.accepted, false, 'so a restart cannot fall back to an older generation');
    second.close();
  });
});

test('applying data does not put the book into service, and a proof does', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'trades', durability: store });
    book.accept('conn-1'); // a connection is taken over only by an explicit accept
    assert.equal(book.isRunning, false, 'a fresh book has proven nothing');
    assert.equal(book.proveBoundary().proven, false, 'with no connection there is nothing to prove');

    book.apply({ envelope: envelope(1), changes: [change(1)] });
    assert.equal(book.isRunning, false, 'frames arriving are not a boundary proof');

    // ここで初めて錨が与えられ、境界が証明される
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.proveBoundary().proven, true);
    assert.equal(book.isRunning, true);
    book.beginSync();
    assert.equal(book.isRunning, false, 'a sync in progress means the board is not to be trusted');
    store.close();
  });
});

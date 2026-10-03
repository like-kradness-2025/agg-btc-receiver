/**
 * Set 7: the receive tail is keyed by the board, not just the connection.
 *
 * A connection name is made of run, venue, market and generation and does not carry the stream (C2), so the
 * same connection legitimately has a book tail and a trades tail. Keyed only by the connection, the two
 * overwrote each other - a lower bound attributed to the wrong board. The key gains the stream; rows written
 * before that have no stream recorded and are kept under the empty marker rather than guessed at.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { openDurability } from '../src/durability.mjs';

async function withDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'tail-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('two streams of one connection keep separate receive tails, and no stream does not overwrite either', async () => {
  await withDir(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    try {
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', stream: 'trades', lastReceivedSeq: 5, lastRecvMonoNs: 10 });
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', stream: 'book', lastReceivedSeq: 9, lastRecvMonoNs: 20 });

      assert.equal(store.readReceivedTail('c', 'trades').lastReceivedSeq, 5);
      assert.equal(store.readReceivedTail('c', 'book').lastReceivedSeq, 9);
      assert.equal(store.readReceivedTail('c', 'trades').stream, 'trades');

      // A caller with no stream is recorded under the empty marker: it is not attributed to a board that
      // never wrote it, and it does not overwrite either real board's tail.
      store.updateReceivedTail({ connectionId: 'c', market: 'kraken_spot', lastReceivedSeq: 1, lastRecvMonoNs: 1 });
      assert.equal(store.readReceivedTail('c', 'trades').lastReceivedSeq, 5, 'the trades tail is untouched');
      assert.equal(store.readReceivedTail('c', 'book').lastReceivedSeq, 9, 'and so is the book tail');
      assert.equal(store.readReceivedTail('c', '').lastReceivedSeq, 1, 'the unknown board has its own row');
    } finally {
      store.close();
    }
  });
});

test('an old single-key receive tail is migrated without guessing its stream, and survives reopen', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // An old store: the tail keyed by the connection alone, one row already written.
    const old = new DatabaseSync(path);
    old.exec(`CREATE TABLE received_tail (
      connection_id TEXT NOT NULL PRIMARY KEY,
      market TEXT NOT NULL,
      last_received_seq INTEGER NOT NULL,
      last_recv_mono_ns INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL
    )`);
    old.exec(`INSERT INTO received_tail (connection_id, market, last_received_seq, last_recv_mono_ns, updated_at_ms)
              VALUES ('conn-old', 'kraken_spot', 42, 1000, 7)`);
    old.close();

    const store = openDurability({ path, runId: 'run-1' });
    const migrated = store.readReceivedTail('conn-old');
    assert.equal(migrated.lastReceivedSeq, 42, 'the old row is kept');
    assert.equal(migrated.stream, '', 'and its stream is left unguessed');
    // The new key works beside the migrated row.
    store.updateReceivedTail({ connectionId: 'conn-old', market: 'kraken_spot', stream: 'book', lastReceivedSeq: 3, lastRecvMonoNs: 5 });
    store.close();

    // Re-open: the migration is idempotent and the rows are still there.
    const again = openDurability({ path, runId: 'run-2' });
    assert.equal(again.readReceivedTail('conn-old').lastReceivedSeq, 42);
    assert.equal(again.readReceivedTail('conn-old', 'book').lastReceivedSeq, 3);
    assert.equal(again.readReceivedTail('conn-old', 'trades'), null);
    again.close();
  });
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { makeEnvelope } from '../src/envelope.mjs';

const envelope = (seq, connectionId, generation, runId) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'book',
    connectionId,
    runId,
    venue: 'kraken',
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

async function withBook(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'run-auth-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('generations only order connections inside one run', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });

    const first = book.accept('run-1:kraken:kraken_spot:1', { generation: 1, runId: 'run-1', firstSeq: 1 });
    assert.equal(first.accepted, true);

    // A different run is not older or newer; without an explicit takeover it is refused.
    const other = book.accept('run-2:kraken:kraken_spot:1', { generation: 2, runId: 'run-2' });
    assert.equal(other.accepted, false, 'a different run is not ordered by its generation');
    assert.match(other.reason, /explicit takeover/);

    // Declared, it is accepted - that is what a restart looks like from here.
    const taken = book.accept('run-2:kraken:kraken_spot:1', {
      generation: 1, runId: 'run-2', firstSeq: 1, takeover: true,
    });
    assert.equal(taken.accepted, true, 'a new run may take over when it says so');

    // And the old run cannot come back in.
    const back = book.accept('run-1:kraken:kraken_spot:5', { generation: 5, runId: 'run-1', takeover: true });
    assert.equal(back.accepted, false, 'the run that was replaced does not return with a bigger number');
    store.close();
  });
});

test('an unresolved hole keeps the book out of service', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 1 });

    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-1'), changes: [] });
    const jumped = book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-1'), changes: [] });
    assert.equal(jumped.reason, 'gap before this sequence');

    const unproven = book.proveBoundary();
    assert.equal(unproven.proven, false, 'a hole in the middle is not a proven boundary');
    assert.match(unproven.reason, /unresolved hole/);
    assert.equal(book.isRunning, false, 'and the book is not serving');

    book.apply({ envelope: envelope(2, 'conn-1', 1, 'run-1'), changes: [] });
    const proven = book.proveBoundary();
    assert.equal(proven.proven, true, 'closing the hole makes the boundary provable');
    assert.equal(book.isRunning, true);
    store.close();
  });
});

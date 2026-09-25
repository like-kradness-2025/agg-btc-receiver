import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

async function withStructure(fn, { rawWriterBehaviour = 'durable', spool = false } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'structure-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  const rawWritten = [];
  const acks = [];
  const gaps = [];
  const stops = [];
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (envelope) => [{ side: 'bid', price: 100 + envelope.receive_seq, size: envelope.receive_seq }],
    },
    durability: store,
    webSocketImpl: function unused() {
      throw new Error('this test feeds frames directly');
    },
    rawWriter: (envelope) => {
      if (rawWriterBehaviour === 'refusing') return false;
      if (rawWriterBehaviour === 'throwing') throw new Error('disk gone');
      rawWritten.push(envelope.receive_seq);
      return true;
    },
    spoolDir: spool ? join(dir, 'spool') : null,
    onAck: (ack) => acks.push(ack),
    onGap: (gap) => gaps.push(gap),
    onStop: (stop) => stops.push(stop),
  });
  try {
    return await fn({ structure, store, rawWritten, acks, gaps, stops, dir });
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test('frames flow from organization into the book and are acknowledged in order', async () => {
  await withStructure(async ({ structure, rawWritten, acks }) => {
    for (const seq of [1, 2, 3]) {
      const result = structure.feed(envelope(seq));
      assert.equal(result.applied, true);
    }
    assert.deepEqual(rawWritten, [1, 2, 3], 'every frame was made durable');
    assert.equal(acks.at(-1).upToSeq, 3, 'and the last acknowledgement covers them all');
    assert.equal(structure.stats.applied, 3);
    assert.equal(structure.book.board.depth, 3, 'the book has what it was told');
    assert.equal(structure.stats.gaps, 0);
  });
});

test('a hole is held by the book and filled by the frame that arrives late', async () => {
  await withStructure(async ({ structure }) => {
    structure.feed(envelope(1));
    const ahead = structure.feed(envelope(3));
    assert.equal(ahead.applied, false, 'the book will not apply past a hole');
    assert.equal(structure.stats.applied, 1);
    assert.equal(structure.stats.gaps, 1, 'and the hole is recorded rather than skipped');

    const late = structure.feed(envelope(2));
    assert.equal(late.applied, true);
    assert.equal(structure.stats.applied, 3, 'the held frame was applied with it');
    assert.equal(structure.book.board.depth, 3);
    assert.equal(structure.stats.gaps, 0);
  });
});

test('a raw write that is refused spills to the spool rather than dropping the frame', async () => {
  await withStructure(
    async ({ structure }) => {
      const result = structure.feed(envelope(1));
      assert.equal(result.spooled, true, 'nothing is dropped: the frame waits in the spool');
      assert.equal(structure.stats.spooledFrames, 1);
      assert.equal(structure.stats.stopped, false, 'and reception is still allowed to continue');
      assert.equal(structure.stats.applied, null, 'nothing was acknowledged, because nothing is safe');
    },
    { rawWriterBehaviour: 'refusing', spool: true },
  );
});

test('when nothing can hold the frame, reception stops and the gap is written down', async () => {
  await withStructure(
    async ({ structure, gaps, stops }) => {
      const result = structure.feed(envelope(1));
      assert.equal(result.stopped, true, 'continuing would mean pretending the frame was handled');
      assert.equal(structure.stats.stopped, true);
      assert.equal(gaps.length, 1);
      assert.match(gaps[0].reason, /spool could not hold it/);
      assert.equal(stops.length, 1);
      const after = structure.feed(envelope(2));
      assert.equal(after.accepted, false, 'and nothing more is accepted');
      assert.equal(structure.stats.refusedFrames, 1, 'the refusal is counted, not hidden');
    },
    { rawWriterBehaviour: 'refusing' },
  );
});

test('a failure while handling a frame stops the structure instead of passing unnoticed', async () => {
  await withStructure(
    async ({ structure, gaps }) => {
      const result = structure.feed(envelope(1));
      assert.equal(result.accepted, false);
      assert.equal(result.reason, 'failure');
      assert.equal(structure.stats.stopped, true);
      assert.match(gaps[0].reason, /disk gone/);
    },
    { rawWriterBehaviour: 'throwing' },
  );
});

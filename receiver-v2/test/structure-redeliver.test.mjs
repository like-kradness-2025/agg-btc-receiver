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

test('a durable frame the board refused is held, reported once, and later applied', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const gaps = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (envelope) => [{ side: 'bid', price: 100 + envelope.receive_seq, size: 1 }],
      },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: () => true,
      spoolDir: null,
      onAck: () => {},
      onGap: (gap) => gaps.push(gap),
      onStop: () => {},
    });

    // The board has not been told about this connection, so the frame is durable and not applied.
    const refused = structure.feed(envelope(1));
    assert.equal(refused.durable, true);
    assert.equal(refused.applied, false);
    assert.equal(gaps.length, 1, 'the difference between the two positions is reported');

    // The same frame arriving again is the same fact, not a new one.
    structure.feed(envelope(1));
    assert.equal(gaps.length, 1, 'a resend of the same frame does not report it again');

    // Accepting the connection is enough: the held frame is offered again by the structure itself,
    // without the caller having to remember to ask.
    structure.accept('conn-1');
    assert.equal(
      structure.book.appliedBoundary.upToSeq,
      1,
      'the frame that was waiting is on the board as soon as the connection is accepted',
    );
    const after = structure.redeliverPending();
    assert.equal(after.applied, 0, 'and there is nothing left waiting to be applied');
    assert.equal(after.stillPending, 0);

    // A frame the book is holding on purpose is not a loss and must not be reported as one.
    structure.feed(envelope(3));
    assert.equal(gaps.filter((g) => String(g.reason).includes('gap before')).length, 0,
      'a hole the book is holding is not a lost frame');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

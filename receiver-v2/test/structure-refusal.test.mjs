import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

const envelope = (seq, connectionId = 'conn-1', meta = { first_seq: 1 }) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta,
  });

test('a frame the board refuses for an unexpected reason is written down', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'refusal-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const gaps = [];
    const acks = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: () => true,
      spoolDir: null,
      onAck: (ack) => acks.push(ack),
      onGap: (gap) => gaps.push(gap),
      onStop: () => {},
    });
    // Nothing is accepted here on purpose: the organizer takes the connection on its own, the book was
    // never told, and that difference is the state being tested.
    const refused = structure.feed(envelope(1));
    assert.equal(refused.durable, true, 'still durable in the raw');
    assert.equal(refused.applied, false);
    assert.match(refused.reason, /connection has been accepted/);
    assert.equal(
      gaps.filter((g) => String(g.reason).includes('the board refused')).length,
      1,
      'and the drift is recorded rather than returned to nobody',
    );
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

/** A frame with no declared first sequence: the raw takes it, the board has nothing to anchor to. */
const bare = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });

test('a frame the board cannot anchor is held and reported, not quietly dropped', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'holds-'));
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
    structure.accept('conn-1');

    // This is the state that must never be filtered out: durable in the raw, not on the board, and the
    // board's reason is not one of the two it uses for a deliberate hold.
    const refused = structure.feed(bare(1));
    assert.equal(refused.durable, true, 'the raw took it');
    assert.equal(refused.applied, false);
    assert.match(refused.reason, /first sequence unknown/);
    assert.equal(
      gaps.filter((g) => String(g.reason).includes('the board refused')).length,
      1,
      'it is reported as the loss it is',
    );

    // The frame is offered again as soon as the connection is accepted - but it is still refused,
    // because the board does not take an anchor for a connection it already knows. That is a real gap
    // and it is pinned here rather than papered over: the frame stays held, still reported, and the
    // count of what is pending stays honest.
    structure.accept('conn-1', { firstSeq: 1 });
    const pending = structure.redeliverPending();
    assert.equal(pending.applied, 0, 'accepting the same connection again does not give the board an anchor');
    assert.equal(pending.stillPending, 1, 'so the frame is still held, and still counted');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

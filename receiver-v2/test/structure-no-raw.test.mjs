import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

/** A frame that declares no origin: the store alone takes it, the board has nothing to anchor to yet. */
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

/**
 * One structure over one store, with a raw writer only when the caller gives one. The no-raw shape is
 * exactly the entrance's: `rawWriter` is left out, so the organizer takes a frame as durable on the
 * store's own record and every result has to say so.
 */
function build(store, { rawWriter } = {}) {
  return createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (envelope) => [{ side: 'bid', price: 100 + envelope.receive_seq, size: 1 }],
    },
    durability: store,
    webSocketImpl: () => {
      throw new Error('this test feeds frames directly');
    },
    ...(rawWriter ? { rawWriter } : {}),
    spoolDir: null,
    onAck: () => {},
    onGap: () => {},
    onStop: () => {},
    onDiagnostic: () => {},
  });
}

test('a resend served straight from the ledger keeps the no-raw display it had when it arrived', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'no-raw-resend-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const structure = build(store);
    structure.accept('conn-1', { firstSeq: 1 });

    // A frame above the first sequence is durable in the store alone, and it says so.
    const first = structure.feed(bare(3));
    assert.equal(first.accepted, true);
    assert.equal(first.durable, true, 'without a raw writer the store alone makes it durable');
    assert.equal(first.rawSkipped, true, 'the arrival says there is no raw');

    // The same frame arriving again is served from the ledger's own record, not from the raw. The display
    // has to be the same one: a caller must not be told a raw holds a frame this process has no raw for.
    const again = structure.feed(bare(3));
    assert.equal(again.accepted, true);
    assert.equal(again.alreadyDurable, true, 'it is already durable on the store’s record');
    assert.equal(again.rawSkipped, true, 'the resend still says there is no raw');

    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('with a raw writer the display of a ledger-served resend is unchanged', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'raw-resend-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const structure = build(store, {
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
    });
    structure.accept('conn-1', { firstSeq: 1 });

    structure.feed(bare(3));
    assert.deepEqual(written, [3], 'the frame reached the raw');

    const again = structure.feed(bare(3));
    assert.equal(again.alreadyDurable, true);
    assert.equal('rawSkipped' in again, false, 'a raw writer means the raw stage was not skipped');
    assert.deepEqual(written, [3], 'the resend does not go near the raw again');

    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

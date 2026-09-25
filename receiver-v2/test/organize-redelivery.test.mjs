import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openDurability } from '../src/durability.mjs';
import { openOrganizer } from '../src/organize/watermark.mjs';
import { makeEnvelope } from '../src/envelope.mjs';

const envelope = (seq) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'book',
    connectionId: 'conn-1',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

test('a resend of durable data is still offered to the board', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'book',
      durability: store,
      writeRaw: (e) => {
        written.push(e.receive_seq);
        return true;
      },
    });

    const first = organizer.note(envelope(1));
    assert.equal(first.durable, true, 'the first copy is written');

    // What a crash between the raw write and the board application leaves behind.
    const resend = organizer.note(envelope(1));
    assert.equal(resend.duplicate, true, 'nothing is written again');
    assert.equal(resend.alreadyDurable, true, 'but the caller learns the raw is safe, so it can be applied');
    assert.deepEqual(written, [1], 'the raw was written exactly once');

    // A frame the raw never held is a different answer: it is not durable and must not be applied.
    const fresh = organizer.note(envelope(2));
    assert.equal(fresh.durable, true);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

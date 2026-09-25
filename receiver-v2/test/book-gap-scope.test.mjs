import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { makeEnvelope } from '../src/envelope.mjs';

const envelope = (seq, connectionId, generation = 1) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'book',
    connectionId,
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

test('a hole belongs to the connection that opened it', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'gap-scope-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { firstSeq: 1 });

    book.apply({ envelope: envelope(1, 'conn-1'), changes: [] });
    const jumped = book.apply({ envelope: envelope(3, 'conn-1'), changes: [] });
    assert.equal(jumped.reason, 'gap before this sequence', 'sequence 2 is missing');

    // A newer connection takes over and numbers from its own start.
    const accepted = book.accept('conn-2', { generation: 2, firstSeq: 1 });
    assert.equal(accepted.accepted, true);
    book.apply({ envelope: envelope(1, 'conn-2', 2), changes: [] });
    book.apply({ envelope: envelope(2, 'conn-2', 2), changes: [] });

    const open = store.db
      .prepare('SELECT COUNT(*) AS n FROM book_gap WHERE connection_id = ? AND filled_at_ms IS NULL')
      .get('conn-1').n;
    assert.equal(open, 1, 'the new connection numbering past the hole did not fill it');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

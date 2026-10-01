import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openDeliveryLedger } from '../src/supervisor/delivery.mjs';

const envelope = (seq, connectionId = 'conn-1', payload = `{"seq":${seq}}`) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    runId: 'run-1',
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: payload,
    meta: { first_seq: 1 },
  });

async function withLedger(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'ledger-'));
  try {
    return await fn({ path: join(dir, 'state.sqlite') });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('what the raw holds and the board does not is written down, and outlives the process', async () => {
  await withLedger(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const ledger = openDeliveryLedger({ durability: store, market: 'kraken_spot', stream: 'trades' });
    assert.deepEqual(ledger.pending(), [], 'nothing is owed before anything is durable');
    assert.equal(ledger.record(envelope(7), 'durable in the raw and not applied yet').recorded, true);
    assert.equal(ledger.size(), 1);
    store.close();

    const reopened = openDurability({ path, runId: 'run-2' });
    const later = openDeliveryLedger({ durability: reopened, market: 'kraken_spot', stream: 'trades' });
    const owed = later.pending();
    assert.equal(owed.length, 1, 'the debt is not this process to forget');
    assert.equal(owed[0].connectionId, 'conn-1');
    assert.equal(owed[0].receiveSeq, 7);
    assert.equal(owed[0].raw.toString('utf8'), '{"seq":7}', 'the bytes the frame arrived with');
    assert.deepEqual(owed[0].meta, { first_seq: 1 }, 'and everything else the envelope needs');
    assert.equal(owed[0].runId, 'run-1');
    assert.equal(owed[0].venue, 'kraken');
    assert.equal(owed[0].generation, 1);
    assert.equal(owed[0].recvTsMs, 1_792_000_000_007);
    assert.equal(owed[0].recvMonoNs, 1_000_007);
    reopened.close();
  });
});

test('the same frame is written down once, and keeps the reason it was first owed for', async () => {
  await withLedger(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const ledger = openDeliveryLedger({ durability: store, market: 'kraken_spot', stream: 'trades' });
    assert.equal(ledger.record(envelope(3), 'the board was holding it').recorded, true);
    assert.equal(ledger.record(envelope(3), 'the board was holding it still').recorded, false);
    assert.equal(ledger.size(), 1, 'a resend is not a second debt');
    assert.equal(ledger.pending()[0].reason, 'the board was holding it');
    store.close();
  });
});

test('the board taking a frame releases it, and only what the board reached', async () => {
  await withLedger(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const ledger = openDeliveryLedger({ durability: store, market: 'kraken_spot', stream: 'trades' });
    for (const seq of [1, 2, 3]) ledger.record(envelope(seq), 'owed');
    ledger.record(envelope(1, 'conn-2'), 'owed');

    // Before the board has applied anything there is no position to release up to, and without a known
    // start there is no range that can be called delivered: null is not zero.
    assert.equal(ledger.release({ connectionId: 'conn-1', firstSeq: 1, upToSeq: null }).released, 0);
    assert.equal(ledger.release({ connectionId: null, firstSeq: 1, upToSeq: 5 }).released, 0);
    assert.equal(ledger.release({ connectionId: 'conn-1', firstSeq: null, upToSeq: 5 }).released, 0);
    assert.equal(ledger.size(), 4);

    assert.equal(ledger.release({ connectionId: 'conn-1', firstSeq: 1, upToSeq: 2 }).released, 2);
    assert.deepEqual(
      ledger.pending().map((entry) => `${entry.connectionId}:${entry.receiveSeq}`),
      ['conn-1:3', 'conn-2:1'],
      'what the board has not reached - and another connection entirely - is still owed',
    );

    // A frame below the connection's first sequence can never be applied, so reaching the ceiling is not
    // the same as having delivered it: it stays as the record of a permanent loss.
    assert.equal(ledger.release({ connectionId: 'conn-1', firstSeq: 4, upToSeq: 3 }).released, 0);
    assert.deepEqual(
      ledger.pending().map((entry) => `${entry.connectionId}:${entry.receiveSeq}`),
      ['conn-1:3', 'conn-2:1'],
      'and a permanent loss is not released by a ceiling that passed it',
    );
    store.close();
  });
});

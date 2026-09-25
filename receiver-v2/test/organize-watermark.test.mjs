import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openOrganizer } from '../src/organize/watermark.mjs';

const envelope = (seq, connectionId = 'conn-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
  });

async function withOrganizer(fn, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: (envelope) => written.push(envelope.receive_seq),
      ...options,
    });
    try {
      return await fn({ organizer, store, written });
    } finally {
      store.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('the raw is written before anything is acknowledged', async () => {
  await withOrganizer(async ({ organizer, written }) => {
    const result = organizer.note(envelope(1));
    assert.deepEqual(written, [1], 'the canonical record is safe first');
    assert.equal(result.ack.upToSeq, 1, 'and only then may an acknowledgement carry it');
  });
});

test('data above a hole is kept, and the hole is written down', async () => {
  await withOrganizer(async ({ organizer, written }) => {
    organizer.note(envelope(1));
    const above = organizer.note(envelope(3)); // 2 never arrived
    assert.deepEqual(written, [1, 3], 'the frame above the hole is still written down');
    assert.equal(above.ack.upToSeq, 1, 'the acknowledgement stops at the hole, it does not cross it');
    assert.equal(above.ack.connectionId, 'conn-1');
    const gaps = organizer.openGaps();
    assert.equal(gaps.length, 1);
    assert.deepEqual({ from: gaps[0].missingFrom, to: gaps[0].missingTo }, { from: 2, to: 2 });
  });
});

test('filling a hole releases everything waiting behind it, at once', async () => {
  await withOrganizer(async ({ organizer }) => {
    organizer.note(envelope(1));
    organizer.note(envelope(3));
    organizer.note(envelope(5));
    assert.equal(organizer.ackState.upToSeq, 1);
    const filled = organizer.note(envelope(2));
    assert.equal(filled.ack.upToSeq, 3, '3 was already durable, so it comes out with the hole');
    const rest = organizer.note(envelope(4));
    assert.equal(rest.ack.upToSeq, 5);
    assert.deepEqual(organizer.openGaps(), [], 'nothing is left open');
  });
});

test('a resend of durable data writes nothing and acknowledges nothing new', async () => {
  await withOrganizer(async ({ organizer, written }) => {
    organizer.note(envelope(1));
    organizer.note(envelope(2));
    const before = written.length;
    const resend = organizer.note(envelope(2));
    assert.equal(resend.duplicate, true);
    assert.equal(resend.ack, null);
    assert.equal(written.length, before, 'a resend is not written a second time');
    assert.equal(organizer.ackState.upToSeq, 2, 'and the ceiling does not move');
  });
});

test('the watermark survives a restart, so a replay is not acknowledged twice', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const dbPath = join(dir, 'state.sqlite');
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    const one = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: first,
      writeRaw: () => {},
    });
    one.note(envelope(1));
    one.note(envelope(2));
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const written = [];
    const two = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: second,
      writeRaw: (envelope) => written.push(envelope.receive_seq),
    });
    const accepted = two.accept('conn-1');
    assert.equal(accepted.upToSeq, 2, 'the watermark came back from the store');
    const replay = two.note(envelope(2));
    assert.equal(replay.duplicate, true);
    assert.deepEqual(written, [], 'replayed data is not written again');
    second.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('another connection is not this one, and is refused by name', async () => {
  await withOrganizer(async ({ organizer, written }) => {
    organizer.note(envelope(1));
    const other = organizer.note(envelope(1, 'conn-2'));
    assert.equal(other.accepted, false);
    assert.equal(other.reason, 'not the accepted connection');
    assert.deepEqual(written, [1], 'nothing from the refused connection was written');
  });
});

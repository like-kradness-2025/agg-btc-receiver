import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openOrganizer } from '../src/organize/watermark.mjs';
import { withInjectableWrites, WATERMARK_WRITE } from '../test-support/failing-store.mjs';

import { internalsOf } from '../src/internal/wiring.mjs';

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

async function withOrganizer(fn, options = {}, { acceptConnection = true } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: (envelope) => {
        written.push(envelope.receive_seq);
        return true; // durable: the contract this module relies on
      },
      ...options,
    });
    // Organization takes a connection explicitly: nothing is accepted by sight (C2), so every test that
    // needs a working connection says so here. The test that is about this rule builds its own organizer.
    if (acceptConnection) organizer.accept('conn-1');
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
    assert.equal(above.durable, true, 'and it is durable: the raw is safe even though nothing covers it yet');
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
      writeRaw: () => true, // durable: the contract this module relies on
    });
    one.accept('conn-1'); // an accepted connection, not one taken on sight
    one.note(envelope(1));
    one.note(envelope(2));
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-2' });
    const written = [];
    const two = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: second,
      writeRaw: (envelope) => {
        written.push(envelope.receive_seq);
        return true; // durable: the contract this module relies on
      },
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

test('a raw write that is not durable moves neither the watermark nor the acknowledgement', async () => {
  let durable = false;
  await withOrganizer(
    async ({ organizer }) => {
      const blocked = organizer.note(envelope(1));
      assert.equal(blocked.durable, false);
      assert.equal(blocked.ack, null, 'nothing is acknowledged on the strength of a write that failed');
      assert.equal(organizer.ackState.upToSeq, null, 'and the watermark did not move');

      durable = true;
      const after = organizer.note(envelope(1));
      assert.equal(after.durable, true, 'the same frame is accepted once the raw is safe');
      assert.equal(after.ack.upToSeq, 1);
    },
    { writeRaw: () => durable },
  );
});

test('nothing is organized before a connection is accepted, and no acknowledgement is invented', async () => {
  await withOrganizer(
    async ({ organizer, written }) => {
      const refused = organizer.note(envelope(1));
      assert.equal(refused.accepted, false, 'a frame from a connection nobody accepted is not taken');
      assert.equal(refused.ack, null, 'and nothing is acknowledged for it');
      assert.deepEqual(written, [], 'the canonical record is not written on the strength of its own arrival');
      assert.equal(organizer.ackState.upToSeq, null);

      // Once the connection is accepted, the same frame is written and acknowledged.
      organizer.accept('conn-1');
      const after = organizer.note(envelope(1));
      assert.equal(after.durable, true);
      assert.equal(after.ack.upToSeq, 1);
      assert.deepEqual(written, [1]);
    },
    {},
    { acceptConnection: false },
  );
});

test('while the start of a connection is unknown the raw is safe and no ceiling is claimed', async () => {
  await withOrganizer(
    async ({ organizer, written }) => {
      // The connection is accepted, but nobody has said where its numbering starts.
      organizer.accept('conn-1', { firstSeq: null });
      const held = organizer.note(envelope(1));
      assert.equal(held.durable, true, 'the canonical record is written');
      assert.equal(held.ack, null, 'but no ceiling is claimed for it');
      assert.equal(organizer.ackState.upToSeq, null);

      organizer.note(envelope(3));
      const completed = organizer.accept('conn-1', { firstSeq: 1 });
      assert.equal(completed.advanced, true, 'the completion moved the ceiling it could prove');
      assert.equal(completed.upToSeq, 1, 'up to the contiguous frame, and not past the hole');
      assert.equal(organizer.openGaps().length, 1, 'the hole it stopped at is written down');

      const hole = organizer.note(envelope(2));
      assert.equal(hole.ack.upToSeq, 3, '3 was already durable, so it comes out with the hole');
      assert.deepEqual(organizer.openGaps(), [], 'and nothing is left open');
      assert.deepEqual(written, [1, 3, 2]);
    },
    {},
    { acceptConnection: false },
  );
});

test('an origin arriving above a hole still releases what is held, and stops where the hole is', async () => {
  await withOrganizer(
    async ({ organizer }) => {
      // The start of this connection's numbering has not arrived, but the frames above it are durable.
      organizer.accept('conn-1', { firstSeq: null });
      organizer.note(envelope(2));
      organizer.note(envelope(3));

      const completed = organizer.accept('conn-1', { firstSeq: 1 });
      assert.equal(completed.advanced, false, 'there is nothing contiguous to claim yet');
      assert.equal(completed.upToSeq, null, 'so no ceiling is claimed');
      assert.deepEqual(
        organizer.openGaps().map((gap) => ({ from: gap.missingFrom, to: gap.missingTo })),
        [{ from: 1, to: 1 }],
        'and the hole they are waiting for is written down rather than left in this process',
      );

      // The frame they were waiting for arrives: everything that was held comes out with it.
      const arrival = organizer.note(envelope(1));
      assert.equal(arrival.ack.upToSeq, 3, 'the frames held above the hole are released with it');
      assert.deepEqual(organizer.openGaps(), [], 'and the hole is closed');
    },
    {},
    { acceptConnection: false },
  );
});

test('a completion whose write fails leaves the organizer as it was, and can be asked for again', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability,
      writeRaw: () => true,
    });
    organizer.accept('conn-1', { firstSeq: null });
    organizer.note(envelope(1));

    // The write that would record the completion fails. Nothing may move - and the completion has to stay
    // outstanding, because an answer nobody could write down is an answer the next caller has to be able
    // to give again.
    armWriteFailure(WATERMARK_WRITE);
    assert.throws(() => organizer.accept('conn-1', { firstSeq: 1 }), /injected write failure/);
    assert.equal(organizer.ackState.upToSeq, null, 'the ceiling did not move');
    assert.equal(
      internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark WHERE up_to_receive_seq IS NOT NULL').get().n,
      0,
      'and nothing was written down',
    );

    const retry = organizer.accept('conn-1', { firstSeq: 1 });
    assert.equal(retry.advanced, true, 'asking again finishes it');
    assert.equal(retry.upToSeq, 1);
    assert.equal(organizer.currentAck().upToSeq, 1);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame from another run or generation is not written down, and does not hide the real one', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: store,
      writeRaw: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
    });
    // The identity the board recorded for this connection: the organizer measures frames against it, because
    // "the connection id matches" is not "this frame is this connection's".
    organizer.accept('conn-1', { firstSeq: 1, runId: 'run-A', generation: 3 });

    const frame = (seq, { runId = 'run-A', generation = 3 } = {}) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: 'conn-1',
        runId,
        venue: 'kraken',
        generation,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });

    const otherRun = organizer.note(frame(1, { runId: 'run-B' }));
    assert.equal(otherRun.accepted, false, 'another run is not this connection');
    assert.match(otherRun.reason, /another run or generation/);
    const otherGeneration = organizer.note(frame(1, { generation: 4 }));
    assert.equal(otherGeneration.accepted, false, 'nor is another generation');
    assert.deepEqual(written, [], 'nothing of theirs reached the canonical record');
    assert.equal(organizer.ackState.upToSeq, null, 'and nothing was acknowledged');

    // Their frame did not take the sequence our own frame needs: the real one is still durable.
    const ours = organizer.note(frame(1));
    assert.equal(ours.durable, true);
    assert.equal(ours.ack.upToSeq, 1);
    assert.deepEqual(written, [1]);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame the store cannot describe leaves the watermark where it was', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'organize-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const written = [];
    const organizer = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability,
      writeRaw: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
    });
    organizer.accept('conn-1', { firstSeq: 1 });

    armWriteFailure(WATERMARK_WRITE);
    assert.throws(() => organizer.note(envelope(1)), /injected write failure/);
    assert.equal(organizer.ackState.upToSeq, null, 'the ceiling did not move');
    assert.equal(
      internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n,
      0,
      'and nothing was written',
    );

    // The same frame sent again is durable: the raw writer is append-safe on (connection, sequence), so the
    // second write of the same frame is the contract working, not a second copy of it.
    const retry = organizer.note(envelope(1));
    assert.equal(retry.durable, true);
    assert.equal(retry.ack.upToSeq, 1);
    assert.deepEqual(written, [1, 1]);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('an origin above a sequence already made durable is refused, and nothing is acknowledged', async () => {
  await withOrganizer(
    async ({ organizer, written }) => {
      organizer.accept('conn-1', { firstSeq: null });
      organizer.note(envelope(1));
      organizer.note(envelope(3));
      assert.deepEqual(written, [1, 3], 'both frames are durable');

      // The start cannot be above a sequence this process already holds: acknowledging it would claim a range
      // nobody received, and the frames below it would never be sent again.
      const jumped = organizer.accept('conn-1', { firstSeq: 3 });
      assert.equal(jumped.refusedOrigin, 3, 'the claim is refused rather than obeyed');
      assert.equal(jumped.advanced, false);
      assert.equal(organizer.ackState.upToSeq, null, 'and no ceiling was claimed');

      const fitting = organizer.accept('conn-1', { firstSeq: 1 });
      assert.equal(fitting.advanced, true, 'the origin that fits the frames is accepted');
      assert.equal(fitting.upToSeq, 1, 'and the ceiling stops where the data does');
      assert.equal(organizer.currentAck().upToSeq, 1);
    },
    {},
    { acceptConnection: false },
  );
});

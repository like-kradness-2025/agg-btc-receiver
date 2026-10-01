import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

/** A frame that declares no origin: the raw takes it, the board has nothing to anchor to yet. */
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

test('a durable frame the board cannot anchor waits for its origin, and moves the moment it arrives', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
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
        changesFor: (envelope) => [{ side: 'bid', price: 100 + envelope.receive_seq, size: 1 }],
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

    // The connection is accepted explicitly, and its origin is not known yet - which is the state this
    // test is about: the raw is safe, the board cannot anchor, and the difference is visible.
    const accepted = structure.accept('conn-1');
    assert.equal(accepted.accepted, true);
    assert.equal(accepted.reason, 'first connection');
    assert.equal(structure.book.appliedBoundary.firstSeq, null);

    const refused = structure.feed(bare(1));
    assert.equal(refused.durable, true, 'the raw took it');
    assert.equal(refused.applied, false);
    assert.match(refused.reason, /first sequence unknown/);
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('the board refused')).length,
      1,
      'the difference between the two positions is reported',
    );
    assert.equal(acks.length, 0, 'and nothing is acknowledged while the origin is unknown');

    // The same frame arriving again is the same fact, not a new one.
    structure.feed(bare(1));
    assert.equal(gaps.length, 1, 'a resend of the same frame does not report it again');

    // The origin arrives, and the frame that was waiting for it is applied - the structure offers it
    // again by itself, without the caller having to remember to ask.
    structure.accept('conn-1', { firstSeq: 1 });
    assert.equal(
      structure.book.appliedBoundary.upToSeq,
      1,
      'the frame that was waiting is on the board as soon as its origin is known',
    );
    const after = structure.redeliverPending();
    assert.equal(after.applied, 0, 'and there is nothing left waiting to be applied');
    assert.equal(after.stillPending, 0);
    assert.equal(acks.at(-1).upToSeq, 1, 'the origin released exactly the ceiling it could prove');

    // A frame the book is holding on purpose is not a loss and must not be reported as one.
    structure.feed(bare(3));
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('gap before')).length,
      0,
      'a hole the book is holding is not a lost frame',
    );
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('an acknowledgement whose notification failed is delivered when the completion is asked for again', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const acks = [];
    let failNextAck = true;
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
      onAck: (ack) => {
        if (failNextAck) {
          failNextAck = false;
          throw new Error('the acknowledgement could not be delivered');
        }
        acks.push(ack);
      },
      onGap: () => {},
      onStop: () => {},
    });

    structure.accept('conn-1');
    structure.feed(bare(1)); // durable in the raw, not on the board: nothing to acknowledge yet
    assert.equal(acks.length, 0);

    // The completion itself is written; only its notification fails.
    assert.throws(() => structure.accept('conn-1', { firstSeq: 1 }), /could not be delivered/);
    assert.equal(acks.length, 0, 'nothing was delivered');
    assert.equal(structure.book.appliedBoundary.firstSeq, 1, 'but the completion is recorded');

    // Asking again delivers what was left owed: a repair that only works the first time is a repair that
    // is lost with the failure.
    structure.accept('conn-1', { firstSeq: 1 });
    assert.equal(acks.length, 1, 'the acknowledgement is delivered on the retry');
    assert.equal(acks[0].upToSeq, 1, 'and it carries only the ceiling the completion proved');
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'while the frame that was waiting is applied');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame from another run is not written to the canon, and does not hide the real one', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-A' });
    const written = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-A',
      venue: 'kraken',
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
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
      spoolDir: null,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });

    const connection = 'run-A:kraken:kraken_spot:1';
    structure.accept(connection, { runId: 'run-A', generation: 1, firstSeq: 1 });
    const frame = (seq, runId) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: connection,
        runId,
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });

    // A frame that names this connection but not this run: a connection id that happens to match is not an
    // identity, and writing this one to the canonical record would make it the record of a frame nobody
    // accepted - while the real frame with the same sequence would then look already durable.
    const foreign = structure.feed(frame(1, 'run-B'));
    assert.equal(foreign.accepted, false, 'another run is not organized');
    assert.deepEqual(written, [], 'so nothing of theirs is written down');
    assert.equal(structure.stats.stopped, false, 'and it does not stop reception');

    const ours = structure.feed(frame(1, 'run-A'));
    assert.equal(ours.durable, true, 'the frame that does belong here is written');
    assert.deepEqual(written, [1]);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame above the origin is durable, and does not stop reception', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
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
      spoolDir: null, // nothing to spill into: a frame that is not durable here stops reception
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });

    // The origin is known and the first frame has not arrived: the raw takes what comes, and no ceiling
    // covers it yet. That is a state, not a reason to stop receiving.
    structure.accept('conn-1', { firstSeq: 1 });
    const ahead = structure.feed(bare(3));
    assert.equal(ahead.durable, true, 'the raw took it');
    assert.equal(ahead.applied, false, 'and the board has nowhere to put it yet');
    assert.equal(structure.stats.stopped, false, 'so reception carries on');
    assert.equal(structure.stats.spooledFrames, 0, 'nothing had to be spilled');

    const origin = structure.feed(bare(1));
    assert.equal(origin.applied, true);
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the origin lands where it should');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('an origin above the frames already received is refused, and they are applied once it fits', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
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
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });

    structure.accept('conn-1');
    assert.equal(structure.feed(bare(1)).durable, true);
    assert.equal(structure.feed(bare(3)).durable, true);

    // An origin above what has already arrived would skip the frame at 1, and the position it wrote would then
    // refuse the frames below it for ever as already applied.
    const jumped = structure.accept('conn-1', { firstSeq: 3 });
    assert.equal(jumped.accepted, false, 'the board refuses an origin above what it has been handed');
    assert.match(jumped.reason, /beyond frames that have already arrived/);
    assert.equal(structure.book.appliedBoundary.firstSeq, null, 'nothing was recorded');
    const second = structure.feed(bare(2));
    assert.equal(second.reason, 'first sequence unknown', 'the frame at 2 is not written off as already applied');

    // The origin that fits: every frame that had already arrived is applied, contiguously and in order.
    structure.accept('conn-1', { firstSeq: 1 });
    assert.equal(
      structure.book.appliedBoundary.upToSeq,
      3,
      'the frames that were waiting are applied once the origin fits',
    );
    const after = structure.redeliverPending();
    assert.equal(after.applied, 0, 'offering them again applies nothing a second time');
    assert.equal(after.held, 0, 'and nothing new is waiting behind a hole');
    assert.deepEqual(structure.book.openGaps(), [], 'with no hole left behind');
    // The entry that the book itself drained is still offered once more and refused as already applied: sorting
    // what a redelivery finds into applied / held / never-applicable is set 4 of the plan, and until then the
    // one thing that must hold is that nothing is lost - the position and the board above say it is not.
    assert.equal(structure.book.appliedBoundary.upToSeq, 3);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

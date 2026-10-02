import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openDeliveryLedger } from '../src/supervisor/delivery.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';

/** The parts of a structure, for a test that plants a ledger entry directly: the wiring's private side. */
const partsOf = (structure) => internalsOf(structure);

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
    // The board drained them and the release took them: nothing is owed, so a pass is offered nothing. The
    // four-way sort says what a redelivery reached, and here it reached everything during admission - the
    // position and the board above are the proof that nothing was lost.
    assert.equal(structure.book.appliedBoundary.upToSeq, 3);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a debt no connection has been accepted for is not a loss, and is delivered once its connection is', async () => {
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

    // A frame the raw already holds, with no connection accepted for the board yet. It is owed, and it may
    // yet be admitted - which is a wait, not a loss, and must not be written down as one.
    assert.equal(
      partsOf(structure).ledger.record(bare(1), 'durable in the raw and never delivered').recorded,
      true,
    );
    const waiting = structure.redeliverPending();
    assert.equal(waiting.unadmitted, 1, 'the frame waits for a connection instead of being written off');
    assert.equal(waiting.applied, 0);
    assert.equal(waiting.held, 0);
    assert.equal(waiting.skipped, 0, 'nothing is written down as a loss');
    assert.equal(
      waiting.applied + waiting.held + waiting.skipped + waiting.unadmitted,
      1,
      'and it leaves the pass as exactly one outcome',
    );
    assert.equal(structure.ledger.find('conn-1', 1).state, 'owed', 'the entry is left owed');
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      0,
      'a wait is not reported as a loss',
    );

    // Its connection is admitted, and the structure's own redelivery hands the frame to the board.
    const accepted = structure.accept('conn-1', { firstSeq: 1 });
    assert.equal(accepted.accepted, true);
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the frame the debt stands for reaches the board');
    assert.equal(structure.book.board.size('bid', 101), 1);

    // The entry goes once the raw's own record vouches for the frame as well: recovery is what brings the
    // two positions together, and the release follows it.
    assert.equal(structure.resume().applied, 1, 'a recovery pass counts the delivery it finds already made');
    assert.equal(structure.ledger.find('conn-1', 1), null, 'and the entry is released');
    assert.equal(structure.stats.owed, 0);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame whose connection is gone is written down as a loss once, and a restart does not repeat it', async () => {
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
        changesFor: () => [],
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
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // The raw holds a frame of this connection that never reached the board.
    assert.equal(
      partsOf(structure).ledger.record(bare(1), 'durable in the raw and never delivered').recorded,
      true,
    );

    // A newer generation takes the board over: the connection the frame belongs to never comes back, so
    // the frame is a permanent loss - and the decision is written down in the entry, once.
    assert.equal(structure.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }).accepted, true);
    const loss = structure.ledger.find('conn-1', 1);
    assert.equal(loss.state, 'skipped', 'the loss is written down in the entry itself');
    assert.match(loss.reason, /the connection this frame belonged to is gone/);
    assert.match(loss.reason, /durable in the raw and never delivered/, 'and it carries why the frame was owed');
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      1,
      'and it is reported once',
    );

    // The decision is kept: offering the debt again decides nothing and reports nothing.
    const again = structure.redeliverPending();
    assert.equal(again.skipped, 0, 'a decided loss is not offered again');
    assert.equal(again.unadmitted, 0);
    assert.equal(structure.ledger.find('conn-1', 1).state, 'skipped');
    assert.equal(gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length, 1);

    // A restart repeats neither the decision nor the report: the entry already carries both.
    structure.close();
    store.close();

    const reopened = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-2' });
    const fresh = [];
    const after = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: () => [],
      },
      durability: reopened,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: () => true,
      spoolDir: null,
      onAck: () => {},
      onGap: (gap) => fresh.push(gap),
      onStop: () => {},
    });
    assert.equal(after.ledger.find('conn-1', 1).state, 'skipped', 'the record of the loss outlives the process');
    assert.equal(
      fresh.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      0,
      'and opening it says nothing new',
    );
    assert.equal(after.redeliverPending().skipped, 0, 'nor does a pass over it');
    assert.equal(fresh.filter((gap) => String(gap.reason).includes('can never be applied')).length, 0);
    after.close();
    reopened.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('one pass sorts every owed frame into exactly one of applied, held, skipped and unadmitted', async () => {
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
    structure.accept('conn-1', { firstSeq: 1 });

    // Three debts: one the board can take at once, one it must hold until the hole before it is filled, and
    // one whose connection was replaced before it could be delivered.
    assert.equal(partsOf(structure).ledger.record(bare(1), 'owed').recorded, true);
    assert.equal(partsOf(structure).ledger.record(bare(5), 'owed').recorded, true);
    assert.equal(partsOf(structure).ledger.record(bare(1, 'conn-2'), 'owed').recorded, true);
    const offered = structure.ledger.pending({ state: 'owed' }).length;
    assert.equal(offered, 3, 'three debts are owed');

    const result = structure.redeliverPending();
    assert.equal(result.applied, 1, 'the frame at the start of the numbering is applied');
    assert.equal(result.held, 1, 'the frame above the hole stays owed');
    assert.equal(result.skipped, 1, 'the frame of the replaced connection is written down as a loss');
    assert.equal(result.unadmitted, 0, 'with an owner established nothing waits for one');
    assert.equal(
      result.applied + result.held + result.skipped + result.unadmitted,
      offered,
      'the four outcomes are exclusive and exhaustive',
    );
    assert.equal(structure.book.appliedBoundary.upToSeq, 1);
    assert.equal(structure.ledger.find('conn-1', 5).state, 'owed', 'the held frame is still owed');
    assert.equal(structure.ledger.find('conn-2', 1).state, 'skipped', 'and the loss is in the ledger');

    // A second pass over the same board: the frame the board already holds is counted as applied, not
    // dropped, and the held frame is still held - the buckets account for every owed entry each pass.
    const stillOwed = structure.ledger.pending({ state: 'owed' }).length;
    const second = structure.redeliverPending();
    assert.equal(second.applied, 1, 'already applied is a delivery the pass can account for');
    assert.equal(second.held, 1);
    assert.equal(second.skipped, 0, 'a decided loss is not decided again');
    assert.equal(second.applied + second.held + second.skipped + second.unadmitted, stillOwed);
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('owed frames are offered back in the order they arrived, not by sequence', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-'));
  try {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const written = [];
    const offered = [];
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: {
        url: 'ws://venue.test/ws',
        stream: 'trades',
        parse: () => ({ kind: 'data' }),
        changesFor: (envelope) => {
          offered.push(envelope.receive_seq);
          return [{ side: 'bid', price: 100 + envelope.receive_seq, size: 1 }];
        },
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

    // The origin is not known yet, so every frame is durable in the raw and owed to the board - and they
    // arrive out of sequence order.
    structure.accept('conn-1');
    for (const seq of [5, 3, 1]) {
      assert.match(structure.feed(bare(seq)).reason, /first sequence unknown/);
    }
    assert.deepEqual(written, [5, 3, 1], 'the writer was called in arrival order');
    assert.deepEqual(offered, [5, 3, 1], 'and so was the board, as each frame arrived');

    // The origin arrives and the structure offers the debts back by itself. The order is the order they were
    // written down - never the clock, and never sorted by sequence, which would offer the frame at 1 before
    // the frame at 5 that arrived first.
    offered.length = 0;
    assert.equal(structure.accept('conn-1', { firstSeq: 1 }).accepted, true);
    assert.deepEqual(offered, [5, 3, 1], 'the redelivery keeps the arrival order');
    assert.deepEqual(offered, written, 'the order the writer is called is the order the frames come back in');
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the frame at the start is applied');
    assert.deepEqual(
      structure.ledger.pending({ state: 'owed' }).map((entry) => entry.receiveSeq),
      [5, 3],
      'and the frames behind the hole wait in arrival order too',
    );
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a board that predates the ownership columns leaves its owed frame a wait, not a loss', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'redeliver-legacy-'));
  try {
    const dbPath = join(dir, 'state.sqlite');
    // A boundary row from before the ownership columns existed: it carries a connection name, but nothing
    // has ever said who owns this board. The name is not an owner - the explicit accept that claims the
    // board is, and it may be the very connection the frame below belongs to.
    const legacy = new DatabaseSync(dbPath);
    legacy.exec(`
      CREATE TABLE applied_boundary (
        market TEXT NOT NULL,
        stream TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        generation INTEGER,
        up_to_receive_seq INTEGER,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (market, stream)
      );
      INSERT INTO applied_boundary (market, stream, connection_id, generation, up_to_receive_seq, updated_at_ms)
      VALUES ('kraken_spot', 'trades', 'legacy-c', 5, 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const ledger = openDeliveryLedger({ durability: store, market: 'kraken_spot', stream: 'trades' });
    // The raw already holds a frame of a connection nobody has accepted yet. Writing it off as a loss on the
    // strength of the old row's connection name would lose a frame the accept below admits.
    assert.equal(
      ledger.record(
        makeEnvelope({
          market: 'kraken_spot',
          stream: 'trades',
          connectionId: 'new-c',
          runId: 'new-run',
          generation: 1,
          receiveSeq: 1,
          recvTsMs: 1_792_000_000_001,
          recvMonoNs: 1_000_001,
          raw: '{"seq":1}',
        }),
        'durable in the raw and never delivered',
      ).recorded,
      true,
    );

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
    // The recovery the construction runs is where the frame is offered back, and it leaves it waiting.
    const waiting = structure.redeliverPending();
    assert.equal(waiting.unadmitted, 1, 'the frame waits for its connection');
    assert.equal(waiting.skipped, 0, 'and is not written down as a loss');
    assert.equal(structure.book.ownerEstablished, false, 'the board has no owner');
    assert.equal(ledger.find('new-c', 1).state, 'owed', 'the entry is left owed');
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      0,
      'nothing is reported as lost',
    );

    // The accept claims the board - there is nobody to take it from - and the frame, of exactly the
    // connection it names, reaches the board on the redelivery the accept itself runs.
    assert.equal(structure.accept('new-c', { runId: 'new-run', generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(structure.book.ownerEstablished, true);
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the owed frame is applied once the board has an owner');
    assert.equal(structure.book.board.size('bid', 101), 1);
    assert.notEqual(structure.ledger.find('new-c', 1)?.state, 'skipped', 'the frame was never written off');
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a decided loss cannot be undone by confirming or recording the frame again', async () => {
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
        changesFor: () => [],
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
    structure.accept('conn-1', { firstSeq: 5 });
    // A frame below this connection's numbering: it was never applied and never will be, so the redelivery
    // writes the decision down.
    assert.equal(
      partsOf(structure).ledger.record(bare(3), 'durable in the raw and never delivered').recorded,
      true,
    );
    assert.equal(structure.redeliverPending().skipped, 1);
    const loss = () => structure.ledger.find('conn-1', 3);
    assert.equal(loss().state, 'skipped');
    const reason = loss().reason;

    // The public routes another writer has, over the same store: an entry whose loss is decided is not a
    // state to move from - confirming it again, or recording it again, leaves the decision standing.
    const hand = openDeliveryLedger({ durability: store, market: 'kraken_spot', stream: 'trades' });
    hand.confirm(bare(3));
    assert.equal(loss().state, 'skipped', 'confirmation does not resurrect a decided loss');
    assert.equal(loss().reason, reason, 'and does not touch the decision the entry keeps');
    hand.record(bare(3), 'repeat');
    assert.equal(loss().state, 'skipped');
    assert.equal(loss().reason, reason);

    // And the decision is not re-decided or re-reported by the next pass.
    const again = structure.redeliverPending();
    assert.equal(again.skipped, 0);
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      1,
      'the loss was reported exactly once',
    );
    store.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openOrganizer } from '../src/organize/watermark.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import {
  withInjectableWrites,
  APPLIED_BOUNDARY_WRITE,
  LEDGER_INTENT_WRITE,
  LEDGER_CONFIRM_WRITE,
} from '../test-support/failing-store.mjs';

import { internalsOf } from '../src/internal/wiring.mjs';

/** The parts of a structure, for a test that drives one of them directly: the wiring's private side. */
const partsOf = (structure) => internalsOf(structure);

const envelope = (seq, { connectionId = 'conn-1', generation = 1, payload, meta = { first_seq: 1 } } = {}) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'trades',
    connectionId,
    runId: 'run-1',
    venue: 'kraken',
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: payload ?? `{"seq":${seq},"size":${seq}}`,
    ...(meta ? { meta } : {}),
  });

/**
 * One structure over one store. The level it writes comes out of the frame's own bytes, so a delivery is
 * only correct if the bytes that reached the book are the bytes that arrived.
 */
function build(
  store,
  {
    runId = 'run-1',
    rawWritten = [],
    acks = [],
    gaps = [],
    stops = [],
    opened = [],
    closes = [],
    onRawWrite = null,
    rawWriter = null,
    spoolDir = null,
    onAck = null,
  } = {},
) {
  const holder = {};
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    runId,
    venue: 'kraken',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (frame) => {
        const { seq, size } = JSON.parse(frame.raw.toString('utf8'));
        return [{ side: 'bid', price: 100 + seq, size }];
      },
    },
    durability: store,
    webSocketImpl: function fakeSocket(url) {
      opened.push({ url, applied: holder.structure.book.appliedBoundary.upToSeq, owed: holder.structure.stats.owed });
      return {
        url,
        onopen: null,
        onmessage: null,
        onclose: null,
        onerror: null,
        send() {},
        close() {
          closes.push(url);
        },
      };
    },
    rawWriter:
      rawWriter ??
      ((frame) => {
        rawWritten.push(frame.receive_seq);
        if (onRawWrite) onRawWrite(frame, holder.structure);
        return true;
      }),
    spoolDir,
    onAck: onAck ?? ((ack) => acks.push(ack)),
    onGap: (gap) => gaps.push(gap),
    onStop: (stop) => stops.push(stop),
    onDiagnostic: () => {},
  });
  holder.structure = structure;
  return structure;
}

/**
 * The same structure over a raw writer that refuses every write: what the raw already holds cannot be
 * decided by an attempt that fails, which is the whole point of the two tests that use this.
 */
function buildWithRefusingRawWriter(store, { rawWritten = [], ...options } = {}) {
  const holder = {};
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    runId: 'run-1',
    venue: 'kraken',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (frame) => {
        const { seq, size } = JSON.parse(frame.raw.toString('utf8'));
        return [{ side: 'bid', price: 100 + seq, size }];
      },
    },
    durability: store,
    webSocketImpl: function unused() {
      throw new Error('this test feeds frames directly');
    },
    rawWriter: (frame) => {
      rawWritten.push(frame.receive_seq);
      return false;
    },
    spoolDir: null,
    onAck: options.onAck ?? (() => {}),
    onGap: options.onGap ?? (() => {}),
    onStop: options.onStop ?? (() => {}),
    onDiagnostic: () => {},
  });
  holder.structure = structure;
  return structure;
}

async function withStore(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'resume-'));
  try {
    return await fn({ path: join(dir, 'state.sqlite'), dir });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('a frame the board could not take is delivered again after a restart', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    // The connection's numbering has no known start, so its frames are durable and owed to the board.
    assert.equal(
      before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: null }).accepted,
      true,
    );
    const refused = before.feed(envelope(1, { meta: null }));
    assert.equal(refused.applied, false);
    assert.equal(refused.reason, 'first sequence unknown');
    assert.equal(before.stats.owed, 1, 'the debt is in the store, not only in this process');
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    assert.equal(after.stats.owed, 1, 'and it is still there after a restart');
    assert.equal(after.book.appliedBoundary.upToSeq, null, 'the board has applied nothing');
    assert.equal(after.resume().offered, 1, 'so the restart offers it again');
    assert.equal(after.stats.owed, 1, 'the board still cannot take it: the start is unknown');

    // The start arrives: the frame that was owed is released and applied, rather than lost with the
    // process that received it.
    assert.equal(
      after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 }).accepted,
      true,
    );
    assert.equal(after.book.appliedBoundary.upToSeq, 1);
    assert.equal(after.stats.owed, 0, 'nothing is owed once the board has it');
    assert.equal(after.book.board.size('bid', 101), 1, 'and the level came out of the frame it owed');
    second.close();
  });
});

test('a resend delivers the frame without writing it to the raw a second time', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const written = [];
    const before = build(first, { rawWritten: written });
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(before.feed(envelope(1)).applied, true);
    // A frame above a hole waits in the board, and it is owed while it waits.
    assert.equal(before.feed(envelope(3)).applied, false);
    assert.equal(before.stats.owed, 1);
    assert.deepEqual(written, [1, 3], 'the raw was written once per frame that arrived');
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second, { rawWritten: written });
    assert.equal(after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(after.resume().held, 1, 'the frame waiting above the hole is restored from the store');
    assert.deepEqual(written, [1, 3], 'the raw already holds it: a delivery is not a second write');

    // The hole fills, and the frame that waited across the restart is applied with it.
    assert.equal(after.feed(envelope(2)).applied, true);
    assert.equal(after.book.appliedBoundary.upToSeq, 3);
    assert.equal(after.stats.owed, 0);
    assert.equal(after.book.board.depth, 3, 'including the frame nothing would have applied otherwise');
    second.close();
  });
});

test('a restart closes the difference between the two positions before reception starts', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    for (const seq of [1, 2, 3, 4]) assert.equal(before.feed(envelope(seq)).applied, true);
    assert.equal(before.stats.owed, 0);
    // Exactly the crash the ledger exists for: the raw holds a frame the board never saw, and the process
    // is gone before the board could be told.
    assert.equal(partsOf(before).ledger.record(envelope(5), 'the raw held it and the board never saw it').recorded, true);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const opened = [];
    const after = build(second, { runId: 'run-2', opened });
    // Opening the store is already a resume: the frame the raw held reaches the book now, and no caller
    // gets a chance to accept a connection in between.
    assert.equal(after.stats.owed, 0, 'the frame the raw held is no longer owed');
    assert.equal(after.stats.applied, 5, 'the board resumes where it was and then takes it');

    after.start();
    assert.equal(opened.length, 1, 'the socket opened');
    assert.equal(opened[0].owed, 0, 'with nothing left owed when reception starts');
    assert.equal(
      after.book.board.size('bid', 105),
      5,
      'the delivered frame is in the board, with the size its own bytes carried',
    );
    second.close();
  });
});

test('nothing is acknowledged for a frame whose debt to the board could not be written down', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const acks = [];
    const stops = [];
    const written = [];
    const structure = build(durability, { acks, stops, rawWritten: written });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });

    armWriteFailure(/INSERT OR IGNORE INTO delivery_ledger/);
    const result = structure.feed(envelope(1));

    assert.equal(result.accepted, false);
    assert.equal(result.reason, 'failure', 'the frame is not reported as handled');
    assert.deepEqual(acks, [], 'and nothing may be acknowledged: the claim did not land with it');
    assert.equal(structure.book.appliedBoundary.upToSeq, null, 'nothing reached the board');
    assert.equal(structure.ledger.size(), 0, 'and no half-written entry was left behind');
    assert.equal(structure.stats.framesWrittenDown, 0, 'nothing was counted as written down either');
    assert.equal(stops.length, 1, 'reception stops rather than carrying on with a frame of unknown fate');
    assert.equal(
      internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n,
      0,
      'the claim and the debt are one commit: neither of them landed',
    );
    store.close();
  });
});

test('a frame the raw keeps and the board can never take is owed, and stays owed', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const gaps = [];
    const before = build(first, { gaps });
    // This connection's numbering starts at 5, so a frame below it is durable in the raw and can never be
    // applied to the board. That is a loss, and the store is where a loss is written down.
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 });
    const below = before.feed(envelope(3, { meta: null }));
    assert.equal(below.applied, false);
    assert.equal(below.reason, 'below the first sequence');
    assert.equal(before.stats.owed, 1, 'the frame is accounted for rather than dropped');
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      1,
      'and it is reported once as a permanent loss',
    );
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    assert.equal(after.stats.owed, 1, 'the record of the loss outlives the process that saw it');
    second.close();
  });
});

test('the frame is written down as an intent before the raw is touched', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const atWrite = [];
    const structure = build(store, {
      onRawWrite: (frame, subject) =>
        atWrite.push({
          seq: frame.receive_seq,
          // What the store says about this frame at the moment the raw write is attempted: a crash here
          // must leave an entry behind, so the entry cannot be written after the write returns.
          intents: subject.ledger.pending({ state: 'intent' }).map((entry) => entry.receiveSeq),
          watermarks: internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n,
        }),
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true);
    assert.deepEqual(atWrite, [{ seq: 1, intents: [1], watermarks: 0 }], 'the intent was already durable');
    assert.equal(structure.stats.owed, 0, 'and the delivered frame is not owed any more');
    assert.equal(structure.stats.framesWrittenDown, 1, 'one frame was written down for the first time');
    store.close();
  });
});

test('a frame whose raw write was refused leaves nothing owed', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    // A raw writer that refuses the write: the intent written before the attempt has to come back out,
    // or a restart would deliver a frame the canonical record never took.
    const refusing = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      runId: 'run-1',
      venue: 'kraken',
      adapter: { url: 'ws://venue.test/ws', stream: 'trades', parse: () => ({ kind: 'data' }), changesFor: () => [] },
      durability: store,
      webSocketImpl: function unused() {
        throw new Error('this test feeds frames directly');
      },
      rawWriter: () => false,
      spoolDir: null,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });
    refusing.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const result = refusing.feed(envelope(2));
    assert.equal(result.durable, false, 'the raw did not take it');
    assert.equal(refusing.ledger.size(), 0, 'so nothing is owed for it');
    assert.equal(
      refusing.stats.framesWrittenDown,
      1,
      'the entry was written down once, and taken back when the write it stood for did not happen',
    );
    store.close();
  });
});

test('an intent left behind by a crash is decided again from its own frame', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // The crash the intent exists for: the frame was written down as about to be made durable, and the
    // process died before the raw write could be attempted or completed.
    assert.equal(partsOf(before).ledger.record(envelope(4), 'about to be written', 'intent').recorded, true);
    assert.equal(before.ledger.pending()[0].state, 'intent', 'and it is not a frame the raw holds');
    assert.equal(before.ledger.size(), 1);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const after = build(second, { rawWritten: written });
    assert.deepEqual(written, [4], 'opening the store decides the raw write again, from the frame itself');
    assert.equal(after.ledger.pending()[0].state, 'owed', 'and the frame is confirmed: the raw holds it now');
    assert.equal(after.stats.owed, 1);

    // The board cannot take it yet - the frames below it have not arrived - so it is held rather than
    // dropped, and the frame the intent stood for is applied once they do.
    for (const seq of [1, 2, 3]) assert.equal(after.feed(envelope(seq)).applied, true);
    assert.equal(after.book.appliedBoundary.upToSeq, 4);
    assert.equal(after.stats.owed, 0, 'and nothing is owed once the board has it');
    assert.equal(after.book.board.size('bid', 104), 4, 'the level its own bytes describe');
    second.close();
  });
});

test('an origin cannot be completed above a frame the store still owes', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    // No known start, so the frame is durable and owed, and the board has been handed it.
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: null });
    assert.equal(before.feed(envelope(3, { meta: null })).reason, 'first sequence unknown');
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    // Completing the origin above sequence 3 would skip it for ever: the frame is in the raw, the board
    // would never be able to apply it, and the entry would be released as if it had been delivered. The
    // frames the store owes are offered to the board before any caller can accept, which is what puts this
    // ceiling in place (§2.2).
    const completed = after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 });
    assert.equal(completed.accepted, false);
    assert.match(completed.reason, /beyond frames that have already arrived/);
    assert.equal(after.stats.owed, 1, 'and the frame is still owed, not released');

    assert.equal(after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 3 }).accepted, true);
    assert.equal(after.stats.owed, 0, 'an origin that fits the frames releases them');
    assert.equal(after.book.board.size('bid', 103), 3);
    second.close();
  });
});

test('reception is closed, not left running, when the structure stops taking frames', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const closes = [];
    const stops = [];
    const structure = build(durability, { closes, stops });
    // The name reception derives for this run, so that the connection is admitted and the socket really
    // opens: what is being checked is what happens to an open socket when the structure stops.
    const connectionId = 'run-1:kraken:kraken_spot:1';
    structure.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.start();
    assert.equal(closes.length, 0);

    armWriteFailure(/INSERT OR IGNORE INTO delivery_ledger/);
    const failed = structure.feed(envelope(1, { connectionId }));
    assert.equal(failed.accepted, false);
    // A stopped structure refuses every further frame, so a socket left open would deliver frames that go
    // nowhere: they are reported and dropped, which is the one outcome this design never allows.
    assert.equal(closes.length, 1, 'the socket was closed with it');
    assert.equal(stops.length, 1);
    store.close();
  });
});

test('a frame the raw may not hold is not delivered to the board', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const before = build(store);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // An intent is a frame the raw may not hold. Delivering it would put the board ahead of the canonical
    // record, so it is not the board's to take until the write it stands for has happened.
    assert.equal(partsOf(before).ledger.record(envelope(1), 'about to be written', 'intent').recorded, true);
    const result = before.redeliverPending();
    assert.equal(result.applied, 0, 'nothing was delivered');
    assert.equal(result.stillPending, 1, 'and the entry is still there');
    assert.equal(before.ledger.pending()[0].state, 'intent');
    assert.equal(before.book.appliedBoundary.upToSeq, null, 'the board has applied nothing');
    store.close();
  });
});

test('a refused attempt does not forget a frame the raw may already hold', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // The crash this entry survives: the raw write happened, and the claim that would have confirmed it
    // never committed, so the entry is still an intent.
    assert.equal(partsOf(before).ledger.record(envelope(1), 'about to be written', 'intent').recorded, true);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const refusing = buildWithRefusingRawWriter(second);
    // Deciding it again refuses the write, and that refusal says nothing about the frame: the raw may hold
    // it from the earlier attempt, so the entry is not this attempt's to delete.
    assert.equal(refusing.ledger.size(), 1, 'the record of a frame that may be in the raw survives');
    assert.equal(refusing.ledger.pending()[0].state, 'intent', 'and it is still an intent, not confirmed');
    second.close();
  });
});

test('a resend of a frame the raw already holds does not go near the raw again', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(before.feed(envelope(1)).applied, true);
    // Above a hole: durable in the raw, owed to the board, waiting for the frame before it.
    assert.equal(before.feed(envelope(3)).applied, false);
    assert.equal(before.stats.owed, 1);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const after = build(second, {
      // The raw refuses to write sequence 3 again - it already holds it - and takes everything else.
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return frame.receive_seq !== 3;
      },
    });
    after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const again = after.feed(envelope(3));

    assert.equal(again.accepted, true, 'the resent frame is taken');
    assert.equal(again.alreadyDurable, true, 'because the raw holds it');
    assert.deepEqual(written, [], 'and the raw was not asked to write it again');
    assert.equal(after.stats.stopped, false, 'so a refusal that decided nothing stopped nothing');
    assert.equal(after.stats.owed, 1, 'the frame is still owed');

    // The hole fills: the frame the raw already held is applied with it.
    assert.equal(after.feed(envelope(2)).applied, true);
    assert.equal(after.book.appliedBoundary.upToSeq, 3);
    assert.equal(after.stats.owed, 0);
    assert.deepEqual(written, [2], 'only the frame that still needed writing was written');

    // And the raw's own record reaches it now, so the next resend is a duplicate rather than a rewrite: a
    // rewrite would be refused (the raw has the frame) and that refusal would stop reception over a frame
    // nothing is wrong with.
    const onceMore = after.feed(envelope(3));
    assert.equal(onceMore.alreadyDurable, true);
    assert.deepEqual(written, [2], 'the raw was not asked a second time');
    assert.equal(after.stats.stopped, false);
    assert.equal(after.feed(envelope(4)).durable, true, 'and reception carries on');
    second.close();
  });
});

test('an intent cannot be re-decided into a start that skips a frame the store holds', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: null });
    assert.equal(before.feed(envelope(3, { meta: null })).reason, 'first sequence unknown');
    // And the frame that was being written when the process died, declaring a start above it.
    assert.equal(
      partsOf(before).ledger.record(envelope(5, { meta: { first_seq: 5 } }), 'about to be written', 'intent')
        .recorded,
      true,
    );
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    // What the raw holds is offered first, so the board has already been handed sequence 3 when the intent's
    // declaration arrives - and a start above a frame already handed over is refused (§2.2).
    assert.equal(after.book.appliedBoundary.firstSeq, null, 'no start was adopted from the intent');
    assert.equal(after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 3 }).accepted, true);
    assert.equal(after.book.appliedBoundary.firstSeq, 3, 'the start that fits the frames is the one taken');
    second.close();
  });
});

test('a structure that stopped does not open its socket again', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const opened = [];
    const closes = [];
    const structure = build(durability, { opened, closes });
    const connectionId = 'run-1:kraken:kraken_spot:1';
    structure.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    structure.start();
    assert.equal(opened.length, 1);

    armWriteFailure(LEDGER_INTENT_WRITE);
    assert.equal(structure.feed(envelope(1, { connectionId })).accepted, false);
    assert.equal(closes.length, 1, 'reception was closed with the stop');

    const restarted = structure.start();
    assert.deepEqual(restarted, { started: false, reason: 'this structure has stopped' });
    assert.equal(opened.length, 1, 'and the socket was not opened again: the frames would only be refused');
    store.close();
  });
});

test('the claim on the raw and the confirmation are one commit', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const acks = [];
    const stops = [];
    const structure = build(durability, { acks, stops });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });

    armWriteFailure(LEDGER_CONFIRM_WRITE);
    const result = structure.feed(envelope(1));

    assert.equal(result.accepted, false, 'the frame is not reported as handled');
    assert.deepEqual(acks, [], 'nothing is acknowledged for a claim that did not land');
    assert.equal(
      internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n,
      0,
      'the claim was rolled back with it: neither half of the commit is left behind',
    );
    assert.equal(structure.ledger.pending()[0].state, 'intent', 'and the entry is still an intent');
    assert.equal(structure.book.appliedBoundary.upToSeq, null, 'nothing reached the board');
    assert.equal(stops.length, 1);
    store.close();
  });
});

test('a stop that happens while recovering keeps the socket shut', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const opened = [];
    const stops = [];
    // A frame that was being written when the process died, and a raw that refuses it again: recovery runs
    // its re-decision on start, and nothing can hold the frame.
    const after = build(second, { opened, stops, rawWriter: () => false });
    after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(partsOf(after).ledger.record(envelope(1), 'about to be written', 'intent').recorded, true);
    assert.equal(after.stats.stopped, false);

    const started = after.start();
    assert.deepEqual(started, { started: false, reason: 'this structure has stopped' });
    assert.equal(after.stats.stopped, true, 'recovery stopped it');
    assert.equal(stops.length, 1);
    assert.equal(opened.length, 0, 'and the socket was never opened to deliver frames it can only refuse');
    second.close();
  });
});

test('a resend is judged by what it claims, and the stored frame is what the board gets', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(before.feed(envelope(1)).applied, true);
    assert.equal(before.feed(envelope(3)).applied, false);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const after = build(second, { rawWritten: written });
    after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });

    // Another run's frame carrying the same key is refused, whatever the store holds under that key: being
    // served from the ledger must not become a way for another identity to be told about this board.
    const foreign = makeEnvelope({
      market: 'kraken_spot',
      stream: 'trades',
      connectionId: 'conn-1',
      runId: 'run-9',
      venue: 'kraken',
      generation: 1,
      receiveSeq: 3,
      recvTsMs: 1_792_000_000_003,
      recvMonoNs: 1_000_003,
      raw: '{"seq":3,"size":99}',
    });
    const refused = after.feed(foreign);
    assert.equal(refused.accepted, false);
    assert.match(refused.reason, /another run or generation/);
    assert.deepEqual(written, [], 'and nothing was written for it');

    // A resend of the right frame with different bytes: the frame the raw holds is what goes on the board.
    assert.equal(after.feed(envelope(3, { payload: '{"seq":3,"size":99}' })).alreadyDurable, true);
    assert.equal(after.feed(envelope(2)).applied, true);
    assert.equal(after.book.board.size('bid', 103), 3, 'the level is the one the stored frame describes');
    second.close();
  });
});

test('a resend of a replaced connection does not drag the organizer onto it', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const written = [];
    const structure = build(store, { rawWritten: written });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true);
    assert.equal(structure.feed(envelope(3)).applied, false, 'held above the hole, and owed');
    // A newer generation takes the board's connection over.
    assert.equal(structure.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }).accepted, true);

    // The replaced connection's frame arrives again. The board refuses it - and if it were allowed to name
    // the organizer's connection, the frames of the connection that IS the board's would be refused before
    // the raw, so they would not even reach the spool.
    const stale = structure.feed(envelope(3));
    assert.equal(stale.accepted, false);
    assert.match(stale.reason, /not the accepted connection/);

    const current = structure.feed(envelope(1, { connectionId: 'conn-2', generation: 2 }));
    assert.equal(current.durable, true, 'the connection the board holds is still received');
    assert.deepEqual(written, [1, 3, 1], 'and its frame reached the raw');
    store.close();
  });
});

test('a hand-over attempted from inside a delivery is refused, and the board takes it between frames', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const written = [];
    const refusals = [];
    const state = { structure: null };
    const structure = build(store, {
      rawWritten: written,
      onAck: () => {
        // The acknowledgement is delivered from inside the frame's own processing, so a hand-over attempted
        // there is a change operation arriving while another one is running. It is refused, and refused as a
        // normal answer: an error here would be read as a store failure and stop reception.
        if (state.structure === null) return;
        refusals.push(state.structure.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }));
      },
    });
    state.structure = structure;
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true, 'the frame is applied');

    assert.equal(refusals.length, 1, 'the hand-over was attempted from inside the delivery');
    assert.equal(refusals[0].accepted, false, 'and it was refused');
    assert.equal(refusals[0].code, 'REENTRANT_OPERATION', 'as a change operation already in progress');
    assert.equal(structure.stats.stopped, false, 'a re-entrant hand-over does not stop reception');
    assert.equal(structure.book.appliedBoundary.connectionId, 'conn-1', 'the board is still the first connection');

    // Once that frame's operation has finished, the same hand-over is accepted, and the new connection's
    // first frame is received and applied normally.
    const accepted = structure.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 });
    assert.equal(accepted.accepted, true, 'the hand-over is taken between frames');
    const current = structure.feed(envelope(1, { connectionId: 'conn-2', generation: 2 }));
    assert.equal(current.durable, true, 'the connection the board holds is still received');
    assert.equal(structure.book.appliedBoundary.connectionId, 'conn-2', 'and the board is the new one');
    assert.equal(structure.stats.stopped, false);
    store.close();
  });
});
test('a retried intent writes and delivers the frame that was written down, not the resend', async () => {
  await withStore(async ({ dir, path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    // The frame was written down before the raw write (size 3) and the process died there.
    assert.equal(partsOf(before).ledger.record(envelope(3), 'about to be written', 'intent').recorded, true);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    let refusing = true;
    const rawSeen = [];
    const after = build(second, {
      spoolDir: join(dir, 'spool'),
      rawWriter: (frame) => {
        rawSeen.push({ seq: frame.receive_seq, size: JSON.parse(frame.raw.toString('utf8')).size });
        return !refusing;
      },
    });
    after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.deepEqual(rawSeen, [{ seq: 3, size: 3 }], 'the retry at open tried the stored frame and was refused');
    assert.equal(after.stats.stopped, false, 'the spool held it, so nothing had to stop');

    // The raw can take it now, and the venue sends the same key with different bytes. What goes to the raw
    // and to the board is the frame that was written down, because that is the frame the key means.
    refusing = false;
    assert.equal(after.feed(envelope(3, { payload: '{"seq":3,"size":99}' })).durable, true);
    assert.equal(rawSeen.at(-1).size, 3, 'the raw took the stored frame');
    for (const seq of [1, 2]) assert.equal(after.feed(envelope(seq)).applied, true, 'the holes are filled');
    assert.equal(after.book.board.size('bid', 103), 3, 'and the board has the stored frame, size 3');
    second.close();
  });
});

test('the stored frame decides what is declared, not the resend that arrives', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: null });
    // The frame the raw holds under this key declares nothing about where the numbering starts.
    assert.equal(partsOf(before).ledger.record(envelope(3, { meta: null }), 'owed').recorded, true);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    assert.equal(after.book.appliedBoundary.firstSeq, null, 'nothing was declared by the stored frame');

    // A resend of the same key that claims a start: the frame in the raw is what the board is told about, so
    // the claim is not adopted from the arrival.
    assert.equal(after.feed(envelope(3, { meta: { first_seq: 1 } })).alreadyDurable, true);
    assert.equal(after.book.appliedBoundary.firstSeq, null, 'the arrival did not speak for the frame');
    second.close();
  });
});

test('a frame is not released while the raw itself cannot vouch for it', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const structure = build(store);
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(structure.feed(envelope(1)).applied, true);
    // The raw holds sequence 2 and its own contiguous position has not reached it: the entry is the only
    // record of that, so reaching it on the board is not a reason to forget it.
    assert.equal(partsOf(structure).ledger.record(envelope(2), 'owed').recorded, true);
    assert.equal(
      partsOf(structure).book.apply({ envelope: envelope(2), changes: [{ side: 'bid', price: 102, size: 2 }] }).applied,
      true,
      'the board has it',
    );
    assert.equal(structure.organizer.ackState.upToSeq, 1, "and the raw's own position has not");

    // A resend of it is served from the store, and that is the moment the release is decided.
    assert.equal(structure.feed(envelope(2)).alreadyDurable, true);
    assert.equal(structure.stats.owed, 1, 'so the record stays until the raw can vouch for the frame');
    store.close();
  });
});

test('a recovery that establishes the start gives it to the raw too, so nothing is written twice', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(first);
    const before = build(durability);
    const connectionId = 'run-1:kraken:kraken_spot:1';
    before.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: null });
    // The raw takes the frame and the debt is written down, but the board's own record of the origin never
    // lands: what a crash leaves is a frame that is durable and owed, with no start on the board.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.equal(before.feed(envelope(1, { connectionId })).accepted, false);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const after = build(second, {
      // The raw refuses a rewrite of sequence 1 - it already holds it.
      rawWriter: (frame) => {
        written.push(frame.receive_seq);
        return frame.receive_seq !== 1;
      },
    });
    // At open the raw is told what it holds, the board adopts the start the frame declared, and the raw's own
    // position moves with it - so the entry is delivered and released, not left as the only record.
    assert.equal(after.book.appliedBoundary.firstSeq, 1);
    assert.deepEqual(written, [], 'nothing was written at open');
    assert.equal(after.stats.owed, 0, 'the frame is no longer owed');

    const resend = after.feed(envelope(1, { connectionId }));
    assert.equal(resend.alreadyDurable, true, 'the resend is a duplicate, not a rewrite');
    assert.deepEqual(written, [], 'so the raw was not asked again');
    assert.equal(after.stats.stopped, false);
    assert.equal(after.feed(envelope(2, { connectionId })).durable, true, 'and reception carries on');
    second.close();
  });
});

test('what is spilled is the frame the key means, not the resend that arrived', async () => {
  await withStore(async ({ dir, path }) => {
    // A well-formed name, because what is spilled is read back as a frame and a frame's name has to describe
    // its own identity.
    const connectionId = 'run-1:kraken:kraken_spot:1';
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(
      partsOf(before).ledger.record(envelope(3, { connectionId }), 'about to be written', 'intent').recorded,
      true,
    );
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second, { spoolDir: join(dir, 'spool'), rawWriter: () => false });
    after.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    // The retry is refused and spilled, and so is a resend of the same key carrying different bytes.
    assert.equal(
      after.feed(envelope(3, { connectionId, payload: '{"seq":3,"size":99}', meta: { first_seq: 9 } })).spooled,
      true,
    );
    assert.equal(after.stats.stopped, false);

    partsOf(after).spool.sync();
    const spilled = [...after.spool.drain()].filter((entry) => entry && typeof entry === 'object');
    const forThree = spilled.filter((entry) => entry.receive_seq === 3);
    assert.equal(forThree.length, 2, 'both attempts were spilled rather than dropped');
    for (const entry of forThree) {
      assert.equal(
        entry.raw.toString('utf8'),
        '{"seq":3,"size":3}',
        'and what was spilled is the frame the key means',
      );
      assert.deepEqual(entry.meta, { first_seq: 1 }, 'down to the meta it carried');
    }
    second.close();
  });
});

test('a frame is not released on the raw\'s silence', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const structure = build(store);
    // The board holds a position while the raw's own record says nothing about this connection: the entry is
    // the only statement that the raw holds the frame, so nothing may release it.
    partsOf(structure).book.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(partsOf(structure).book.apply({ envelope: envelope(1), changes: [] }).applied, true);
    assert.equal(partsOf(structure).ledger.record(envelope(1), 'owed').recorded, true);
    assert.equal(structure.organizer.ackState.upToSeq, null, 'the raw claims nothing contiguously');

    // A resend of it is served from the store, and that is the moment the release is decided.
    assert.equal(structure.feed(envelope(1)).alreadyDurable, true);
    assert.equal(structure.stats.owed, 1, 'so the record stays: the raw has not vouched for this frame');
    store.close();
  });
});

test('owed frames are recovered in the order they were written down, not by the clock', async () => {
  await withStore(async ({ path }) => {
    const realNow = Date.now;
    let clock = 20_000;
    Date.now = () => clock;
    try {
      const first = openDurability({ path, runId: 'run-1' });
      const before = build(first);
      before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: null });
      // A frame that declares nothing, written down while the clock read 20000.
      assert.equal(before.feed(envelope(3, { meta: null })).reason, 'first sequence unknown');
      // The clock steps back, and a frame declaring a start above it is refused - but it was written down
      // after the frame above it, and that order is what a recovery has to keep.
      clock = 10_000;
      const refused = before.feed(envelope(5, { meta: { first_seq: 5 } }));
      assert.match(refused.reason, /beyond frames that have already arrived/);
      first.close();
    } finally {
      Date.now = realNow;
    }

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second);
    // Sequence 3 is offered first, so the start 5 was never adopted out of order.
    assert.equal(after.book.appliedBoundary.firstSeq, null, 'no start was adopted from the later frame');
    assert.equal(after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 3 }).accepted, true);
    assert.equal(after.book.appliedBoundary.firstSeq, 3, 'the start that fits the frames is still available');
    second.close();
  });
});

test('two boards of one market do not read each other\'s durable position', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    // A connection is named by the run, the venue, the market and the generation (C2) - the stream is not part
    // of the name, so both boards legitimately hold it. What separates them is the board, not the name.
    const connectionId = 'run-1:kraken:kraken_spot:1';
    const bookWritten = [];
    const tradesWritten = [];
    const board = (stream, written) =>
      createStructure({
        market: 'kraken_spot',
        stream,
        runId: 'run-1',
        venue: 'kraken',
        adapter: {
          url: `ws://venue.test/${stream}`,
          stream,
          parse: () => ({ kind: 'data' }),
          changesFor: () => [{ side: 'bid', price: 1, size: 1 }],
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
        onDiagnostic: () => {},
      });
    const frame = (stream, seq) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream,
        connectionId,
        runId: 'run-1',
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });

    const books = board('book', bookWritten);
    books.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    assert.equal(books.feed(frame('book', 1)).applied, true);
    assert.deepEqual(bookWritten, [1]);

    const trades = board('trades', tradesWritten);
    trades.accept(connectionId, { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = trades.feed(frame('trades', 1));
    assert.equal(first.durable, true, 'this board has no position of its own, so the raw was asked');
    assert.deepEqual(tradesWritten, [1], 'and it took the frame');
    assert.equal(trades.book.appliedBoundary.upToSeq, 1);
    assert.equal(books.book.appliedBoundary.upToSeq, 1, 'each board keeps its own position');
    store.close();
  });
});

test('a raw writer that already holds a frame takes the retry, so a failed confirmation recovers', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const held = new Set();
    // The writer this contract is about: it never refuses a frame it already has. A refusal has to mean "not
    // durable", or nothing downstream can tell a duplicate from a frame that was never written.
    const rawWriter = (frame) => {
      held.add(`${frame.connection_id}:${frame.receive_seq}`);
      return true;
    };
    const { durability, armWriteFailure } = withInjectableWrites(first);
    const before = build(durability, { rawWriter });
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    armWriteFailure(LEDGER_CONFIRM_WRITE);
    assert.equal(before.feed(envelope(1)).accepted, false, 'the confirmation failed');
    assert.equal(before.ledger.pending()[0].state, 'intent');
    assert.deepEqual([...held], ['conn-1:1'], 'and the raw holds the frame');
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const after = build(second, { rawWriter });
    assert.equal(after.ledger.size(), 0, 'opening decides the write again, and the writer confirms it holds it');
    assert.equal(after.book.appliedBoundary.upToSeq, 1, 'so the frame reaches the board');
    second.close();
  });
});

test('a frame below the connection first sequence is written, and recorded as a permanent loss', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const written = [];
    const gaps = [];
    const structure = build(store, { rawWritten: written, gaps });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 });
    assert.equal(structure.feed(envelope(5)).durable, true);

    // A sequence below where this connection starts. It was never written, and calling it already durable
    // would leave the canonical record without it for ever - and the board would be told a frame it cannot
    // ever apply is safe.
    const below = structure.feed(envelope(3));
    assert.equal(below.durable, true, 'it is written down like any other frame');
    assert.equal(below.alreadyDurable, undefined, 'and not answered as a duplicate');
    assert.deepEqual(written, [5, 3], 'the raw was asked for it');
    assert.equal(below.applied, false, 'the board can never apply it');
    assert.equal(below.reason, 'below the first sequence');
    assert.equal(
      gaps.filter((gap) => String(gap.reason).includes('can never be applied')).length,
      1,
      'and that is recorded as the loss it is',
    );
    store.close();
  });
});

test('a hole filled on one board does not close the other board hole', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    // One connection name, two boards - a market's book and its trades - each with its own hole.
    const connectionId = 'run-1:kraken:kraken_spot:1';
    const frame = (stream, seq) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream,
        connectionId,
        runId: 'run-1',
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });
    const organizer = (stream) =>
      openOrganizer({ market: 'kraken_spot', stream, durability: store, writeRaw: () => true });
    const books = organizer('book');
    const trades = organizer('trades');
    for (const [one, stream] of [
      [books, 'book'],
      [trades, 'trades'],
    ]) {
      one.accept(connectionId, { firstSeq: 1, runId: 'run-1', generation: 1 });
      one.note(frame(stream, 2));
      assert.equal(one.openGaps().length, 1, `${stream} sees its own hole`);
    }

    // Filling the hole on one board fills that board's hole, and only that one.
    books.note(frame('book', 1));
    assert.equal(books.openGaps().length, 0, 'the hole on this board is filled');
    assert.equal(trades.openGaps().length, 1, "the other board's hole is not");
    const rows = internalsOf(store).db
      .prepare('SELECT stream, filled_at_ms FROM organize_gap ORDER BY id')
      .all()
      .map((row) => ({ stream: row.stream, filled: row.filled_at_ms !== null }));
    assert.deepEqual(rows, [
      { stream: 'book', filled: true },
      { stream: 'trades', filled: false },
    ]);
    store.close();
  });
});

test('the start a connection was established with survives a restart', async () => {
  await withStore(async ({ path }) => {
    const first = openDurability({ path, runId: 'run-1' });
    const before = build(first);
    before.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 });
    assert.equal(before.feed(envelope(5)).durable, true);
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const gaps = [];
    const after = build(second, { rawWritten: written, gaps });
    // The board remembers the start, and the restore takes it from there rather than assuming 1: with 1, every
    // sequence would look like one this connection covers.
    assert.equal(after.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 }).accepted, true);
    const below = after.feed(envelope(3));
    assert.equal(below.durable, true, 'a frame below the start is written down');
    assert.deepEqual(written, [3], 'the raw was asked for it');
    assert.equal(below.reason, 'below the first sequence', 'and it is a loss, not a duplicate');
    second.close();
  });
});

test('a store written before the board owned its tables is migrated, not crashed into', async () => {
  await withStore(async ({ path }) => {
    // The store as this module wrote it before: keyed by connection id alone, with no board.
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE organized_watermark (
        connection_id TEXT NOT NULL PRIMARY KEY,
        market TEXT NOT NULL,
        up_to_receive_seq INTEGER,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE organize_gap (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        connection_id TEXT NOT NULL,
        market TEXT NOT NULL,
        missing_from INTEGER NOT NULL,
        missing_to INTEGER NOT NULL,
        detected_at_ms INTEGER NOT NULL,
        filled_at_ms INTEGER,
        reason TEXT NOT NULL
      );
      INSERT INTO organized_watermark (connection_id, market, up_to_receive_seq, updated_at_ms)
      VALUES ('run-1:kraken:kraken_spot:1', 'kraken_spot', 7, 1);
      INSERT INTO organize_gap (connection_id, market, missing_from, missing_to, detected_at_ms, reason)
      VALUES ('run-1:kraken:kraken_spot:1', 'kraken_spot', 3, 4, 1, 'a legacy hole');
    `);
    legacy.close();

    const store = openDurability({ path, runId: 'run-1' });
    const structure = build(store);
    // The old rows cannot be attributed to a board - one connection id names two of them - so the position
    // starts from what this board can prove. The old rows name this very connection, which is what makes
    // "nothing was inherited" a real test rather than a check of some other connection.
    const legacyConnection = 'run-1:kraken:kraken_spot:1';
    assert.equal(
      structure.accept(legacyConnection, { runId: 'run-1', generation: 1, firstSeq: 1 }).accepted,
      true,
    );
    assert.equal(structure.organizer.ackState.upToSeq, null, 'nothing was inherited from an ambiguous name');
    assert.deepEqual(
      internalsOf(store).db.prepare('SELECT up_to_receive_seq FROM organized_watermark_legacy').all().map((row) => row.up_to_receive_seq),
      [7],
      'and what the old store said is kept, not discarded',
    );
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organize_gap_legacy').get().n, 1);
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organize_gap').get().n, 0);
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM organized_watermark').get().n, 0);
    assert.equal(
      structure.feed(envelope(1, { connectionId: legacyConnection })).durable,
      true,
      'a frame is written again rather than called durable',
    );
    store.close();
  });
});

test('a start restored from the store decides what is a duplicate', async () => {
  await withStore(async ({ path }) => {
    const id = 'run-1:kraken:kraken_spot:1';
    const frame = (seq) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: id,
        runId: 'run-1',
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });
    const written = [];
    const writer = (one) => {
      written.push(one.receive_seq);
      return true;
    };

    const first = openDurability({ path, runId: 'run-1' });
    const before = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: first,
      writeRaw: writer,
    });
    before.accept(id, { firstSeq: 5, runId: 'run-1', generation: 1 });
    assert.equal(before.note(frame(5)).durable, true);
    first.close();

    // Started again with no start to offer: what the store recorded is what decides, not an assumption of 1.
    const second = openDurability({ path, runId: 'run-2' });
    const after = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: second,
      writeRaw: writer,
    });
    after.accept(id, { firstSeq: null, runId: 'run-1', generation: 1 });
    const below = after.note(frame(3));
    assert.equal(below.durable, true, 'the frame below the start is written down');
    assert.equal(below.alreadyDurable, undefined, 'and is not a duplicate of anything');
    assert.deepEqual(written, [5, 3], 'the raw was asked for it');
    second.close();
  });
});

test('the very first frame after a start is established is judged by that start too', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const written = [];
    const gaps = [];
    const structure = build(store, { rawWritten: written, gaps });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 5 });
    // No frame of this connection has arrived yet, so there is no ceiling to compare against - and the frame
    // below the start is still not a duplicate.
    const below = structure.feed(envelope(3));
    assert.equal(below.durable, true, 'it is written down');
    assert.equal(below.duplicate, undefined);
    assert.equal(below.alreadyDurable, undefined);
    assert.deepEqual(written, [3], 'the raw was asked for it');
    assert.equal(below.reason, 'below the first sequence');
    store.close();
  });
});

test('a second migration keeps what the first set aside', async () => {
  await withStore(async ({ path }) => {
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE organized_watermark (
        connection_id TEXT NOT NULL PRIMARY KEY,
        market TEXT NOT NULL,
        up_to_receive_seq INTEGER,
        updated_at_ms INTEGER NOT NULL
      );
      CREATE TABLE organized_watermark_legacy (
        connection_id TEXT NOT NULL PRIMARY KEY,
        market TEXT NOT NULL,
        up_to_receive_seq INTEGER,
        updated_at_ms INTEGER NOT NULL
      );
      INSERT INTO organized_watermark (connection_id, market, up_to_receive_seq, updated_at_ms)
      VALUES ('run-1:kraken:kraken_spot:1', 'kraken_spot', 9, 2);
      INSERT INTO organized_watermark_legacy (connection_id, market, up_to_receive_seq, updated_at_ms)
      VALUES ('run-1:kraken:kraken_spot:1', 'kraken_spot', 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path, runId: 'run-1' });
    build(store);
    // What an earlier migration set aside is still there, under a name of its own - a re-migration must not
    // overwrite the very evidence it exists to keep.
    const rows = internalsOf(store).db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'organized_watermark_legacy%' ORDER BY name",
      )
      .all()
      .map((row) => row.name);
    assert.deepEqual(rows, ['organized_watermark_legacy', 'organized_watermark_legacy_2']);
    const positions = internalsOf(store).db
      .prepare('SELECT up_to_receive_seq FROM organized_watermark_legacy_2')
      .all()
      .map((row) => row.up_to_receive_seq);
    assert.deepEqual(positions, [9], 'and the newer one was set aside too');
    assert.deepEqual(
      internalsOf(store).db.prepare('SELECT up_to_receive_seq FROM organized_watermark_legacy').all().map((row) => row.up_to_receive_seq),
      [7],
      'and what the earlier migration set aside is still there, unchanged',
    );
    store.close();
  });
});

test('a connection whose frames are still held keeps its start across a restart', async () => {
  await withStore(async ({ path }) => {
    const id = 'run-1:kraken:kraken_spot:1';
    const make = (stream, seq) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream,
        connectionId: id,
        runId: 'run-1',
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });
    const first = openDurability({ path, runId: 'run-1' });
    const before = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: first,
      writeRaw: () => true,
    });
    before.accept(id, { firstSeq: 5, runId: 'run-1', generation: 1 });
    // A frame above the start is held while the frames between are missing: the store has a start and no
    // position, which is the state a restore must not read as "start unknown".
    assert.equal(before.note(make('trades', 7)).ack, null);
    assert.deepEqual(
      internalsOf(first).db.prepare('SELECT up_to_receive_seq, first_seq FROM organized_watermark').all().map((row) => [row.up_to_receive_seq, row.first_seq]),
      [[null, 5]],
    );
    first.close();

    const second = openDurability({ path, runId: 'run-2' });
    const written = [];
    const after = openOrganizer({
      market: 'kraken_spot',
      stream: 'trades',
      durability: second,
      writeRaw: (frame) => {
        written.push(frame.receive_seq);
        return true;
      },
    });
    assert.equal(after.accept(id, { firstSeq: null, runId: 'run-1', generation: 1 }).firstSeq, 5, 'the start comes back');
    // Nor does a late declaration move the start past the frames that are still missing.
    assert.equal(after.accept(id, { firstSeq: 7, runId: 'run-1', generation: 1 }).advanced, false);
    assert.equal(after.ackState.upToSeq, null, 'nothing was acknowledged that was not written');
    const start = after.note(make('trades', 5));
    assert.equal(start.durable, true);
    assert.equal(start.ack.upToSeq, 5, 'and the position starts where the numbering does');
    assert.deepEqual(written, [5]);
    second.close();
  });
});

test('opening one board does not move a live board holes aside', async () => {
  await withStore(async ({ path }) => {
    const id = 'run-1:kraken:kraken_spot:1';
    const make = (stream, seq) =>
      makeEnvelope({
        market: 'kraken_spot',
        stream,
        connectionId: id,
        runId: 'run-1',
        venue: 'kraken',
        generation: 1,
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: `{"seq":${seq}}`,
      });
    const store = openDurability({ path, runId: 'run-1' });
    const trades = openOrganizer({ market: 'kraken_spot', stream: 'trades', durability: store, writeRaw: () => true });
    trades.accept(id, { firstSeq: 1, runId: 'run-1', generation: 1 });
    trades.note(make('trades', 3));
    assert.equal(trades.openGaps().length, 1, 'a hole is open on this board');

    // The migration asks each table for the columns that table needs - the holes never carried a start, and
    // asking them for one moved a live board's holes aside every time any other board was opened.
    const books = openOrganizer({ market: 'kraken_spot', stream: 'book', durability: store, writeRaw: () => true });
    assert.deepEqual(books.openGaps(), [], 'the other board has no holes of its own');
    assert.equal(trades.openGaps().length, 1, "and this board's hole is still there");
    assert.equal(
      internalsOf(store).db.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE name LIKE 'organize_gap_legacy%'").get().n,
      0,
      'nothing was moved aside',
    );
    trades.note(make('trades', 1));
    trades.note(make('trades', 2));
    assert.equal(trades.openGaps().length, 0, 'and it can still be filled');
    store.close();
  });
});

test('a hand-over attempted inside the raw write is refused, and the new connection is received afterwards', async () => {
  await withStore(async ({ path }) => {
    const store = openDurability({ path, runId: 'run-1' });
    const written = [];
    const acks = [];
    const refusals = [];
    let handedOver = false;
    const structure = build(store, {
      onAck: (ack) => acks.push(ack),
      onRawWrite: (frame, subject) => {
        written.push(frame.connection_id);
        if (handedOver) return;
        handedOver = true;
        // The raw writer is the caller's hook, so this is where a hand-over can be attempted from inside a
        // frame's own write - and it is refused, because that write is part of the operation in progress.
        refusals.push(subject.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }));
      },
    });
    structure.accept('conn-1', { runId: 'run-1', generation: 1, firstSeq: 1 });
    const first = structure.feed(envelope(1));
    assert.equal(first.durable, true, 'the frame that was written is durable');
    assert.equal(first.applied, true, 'and it reaches the board it belongs to');
    assert.equal(refusals.length, 1, 'the hand-over was attempted from inside the write');
    assert.equal(refusals[0].code, 'REENTRANT_OPERATION', 'and refused as a change operation already running');
    assert.deepEqual(
      acks.map((ack) => ack.connectionId),
      ['conn-1'],
      'the acknowledgement names the connection that sent the frame',
    );

    // Between frames the hand-over is taken, and the new connection's own frame is written, applied and
    // acknowledged: it is not answered "already durable" on the strength of the earlier frame.
    assert.equal(structure.accept('conn-2', { runId: 'run-1', generation: 2, firstSeq: 1 }).accepted, true);
    const second = structure.feed(envelope(1, { connectionId: 'conn-2', generation: 2 }));
    assert.deepEqual(written, ['conn-1', 'conn-2'], 'the second connection frame reached the raw');
    assert.notEqual(second.alreadyDurable, true, 'it is not a duplicate of the first connection frame');
    assert.equal(second.applied, true, 'and the board took it');
    assert.equal(second.ack.connectionId, 'conn-2');
    assert.equal(structure.stats.stopped, false);
    store.close();
  });
});

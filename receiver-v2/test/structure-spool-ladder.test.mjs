/**
 * Set 5: the spool as a transfer path, and the two bounds that keep it and the ledger honest.
 *
 * The ladder under test, end to end: a raw that refuses -> the frame is spilled -> the raw is healthy
 * again -> the walk either heals itself or is asked to, and the frame reaches the board exactly once.
 * Beside it, the retention bound: an entry past the earlier of five minutes or a gigabyte is declared
 * missing through the same decision a permanent loss goes through, and an entry inside the bound is
 * left alone.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';

// A well-formed connection name, because a record written to the spool is read back as a frame and a
// frame's name has to describe its own identity.
const CONNECTION = 'run-1:kraken:kraken_spot:1';

const envelope = (seq, { connectionId = CONNECTION, raw, meta = { first_seq: 1 } } = {}) =>
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
    raw: raw ?? `{"seq":${seq},"size":${seq}}`,
    ...(meta ? { meta } : {}),
  });

/**
 * One structure whose raw writer can be switched from refusing to accepting, so the same process can
 * spill a frame and later take it. Everything else is the fixture the other resume tests use.
 */
async function withStructure(
  fn,
  { spool = true, nowMs, ledgerRetentionMs, ledgerRetentionBytes, refuseSeqs = [], socket = false } = {},
) {
  const dir = await mkdtemp(join(tmpdir(), 'ladder-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  const rawWritten = [];
  const gaps = [];
  const stops = [];
  const health = { accepting: false };
  let fake = null;
  const structure = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    runId: 'run-1',
    venue: 'kraken',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: 'trades',
      parse: () => ({ kind: 'data' }),
      changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
    },
    durability: store,
    webSocketImpl: socket
      ? function fakeSocket(url) {
          // A socket this test can drive, so a frame's road in is the reception road rather than feed().
          fake = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
          return fake;
        }
      : function unused() {
          throw new Error('this test feeds frames directly');
        },
    rawWriter: (frame) => {
      if (!health.accepting) return false;
      // One frame can be refused while the rest are taken: a walk has to stop at the record it could not
      // consume, and a fixture that could only refuse everything could not show that it does.
      if (refuseSeqs.includes(frame.receive_seq)) return false;
      rawWritten.push(frame.receive_seq);
      return true;
    },
    spoolDir: spool ? join(dir, 'spool') : null,
    ...(nowMs === undefined ? {} : { nowMs }),
    ...(ledgerRetentionMs === undefined ? {} : { ledgerRetentionMs }),
    ...(ledgerRetentionBytes === undefined ? {} : { ledgerRetentionBytes }),
    onAck: () => {},
    onGap: (gap) => gaps.push(gap),
    onStop: (stop) => stops.push(stop),
    onDiagnostic: () => {},
  });
  structure.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
  try {
    return await fn({
      structure,
      store,
      rawWritten,
      gaps,
      stops,
      health,
      dir,
      parts: internalsOf(structure),
      socket: () => fake,
    });
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test('the ladder heals: a spilled frame is walked back out and reaches the board once the raw accepts', async () => {
  await withStructure(async ({ structure, rawWritten, health }) => {
    const spilled = structure.feed(envelope(1));
    assert.equal(spilled.spooled, true, 'the raw refused it, so it waits in the spool');
    assert.equal(structure.stats.spooledFrames, 1);
    assert.ok(structure.spool.bytes > 0, 'the spool is holding it');
    assert.deepEqual(structure.spool.cursor, { segment: null, offset: 0 }, 'nothing is confirmed yet');

    health.accepting = true; // the raw is healthy again
    const drained = structure.drainSpool();
    assert.equal(drained.consumed, 1, 'the record was walked out and made durable');
    assert.equal(drained.advanced, true, 'and the cursor moved over it');

    assert.deepEqual(rawWritten, [1], 'the raw took the frame exactly once');
    assert.equal(structure.book.appliedBoundary.upToSeq, 1, 'the board has it');
    assert.equal(structure.book.board.size('bid', 101), 1, 'with the level the frame carried');
    assert.equal(structure.stats.owed, 0, 'the ledger released the debt');
    assert.equal(structure.stats.spooledFrames, 1, 'the spill is counted once, not once per walk');
    assert.equal(structure.spool.bytes, 0, 'and the consumed segment is gone');
    assert.deepEqual(structure.spool.segments, []);
  });
});

test('a drain that meets a still-refusing raw stops there and copies nothing', async () => {
  await withStructure(async ({ structure }) => {
    assert.equal(structure.feed(envelope(1)).spooled, true);
    const held = structure.spool.bytes;
    const segments = structure.spool.segments.map((segment) => segment.name);

    const drained = structure.drainSpool(); // the raw is still refusing
    assert.equal(drained.walked, 1, 'the record was offered back to the raw');
    assert.equal(drained.consumed, 0, 'and did not become durable');
    assert.equal(drained.advanced, false, 'so the cursor does not move');
    assert.match(String(drained.stopped), /still refused/);
    assert.equal(structure.spool.bytes, held, 'the record is not appended a second time');
    assert.deepEqual(
      structure.spool.segments.map((segment) => segment.name),
      segments,
      'and the same single segment still holds it',
    );
    assert.deepEqual(structure.spool.cursor, { segment: null, offset: 0 });
    assert.equal(structure.stats.owed, 0, 'no half-written debt is left behind');
    assert.equal(structure.stats.applied, null, 'and nothing was applied');
  });
});

test('a successful frame write drains what the spool holds by itself', async () => {
  await withStructure(async ({ structure, rawWritten, health }) => {
    assert.equal(structure.feed(envelope(1)).spooled, true, 'the raw refuses the first frame');
    health.accepting = true;
    // Nothing asks for a drain: the write that succeeds is the opportunity, and the frame that was
    // waiting behind it is climbed out in the same operation.
    const second = structure.feed(envelope(2));
    assert.equal(second.durable, true, 'the raw takes the new frame');
    assert.equal(structure.book.appliedBoundary.upToSeq, 2, 'and the board ends up holding both');
    assert.equal(structure.book.board.size('bid', 101), 1, 'the spilled frame is on the board');
    assert.deepEqual(rawWritten, [2, 1], 'the raw took the new frame, then the one it had refused');
    assert.equal(structure.stats.owed, 0, 'nothing is left owed');
    assert.equal(structure.spool.bytes, 0, 'and the spool is empty');
  });
});

test('an entry past the time bound is declared missing once, and a fresh entry survives', async () => {
  let clock = 1_000_000;
  await withStructure(
    async ({ structure, gaps, parts }) => {
      // Frames the board cannot anchor are durable and owed: exactly what the retention bound acts on.
      assert.equal(parts.ledger.record(envelope(1, { meta: null }), 'durable and held').recorded, true);
      clock += 10 * 60 * 1000; // ten minutes later, well past the five-minute bound
      assert.equal(parts.ledger.record(envelope(2, { meta: null }), 'durable and held').recorded, true);

      const swept = structure.drainSpool(); // no spool here: the sweep is the drain's other repair
      assert.equal(swept.swept, 1, 'only the entry past the bound was declared missing');

      const states = new Map(structure.ledger.pending().map((entry) => [entry.receiveSeq, entry.state]));
      assert.equal(states.get(1), 'skipped', 'the old entry is written down as a loss');
      assert.equal(states.get(2), 'owed', 'and the fresh one is untouched');
      assert.equal(structure.ledger.size(), 2, 'a decided loss keeps its row');
      const reports = gaps.filter((gap) => String(gap.reason).includes('retention bound'));
      assert.equal(reports.length, 1, 'the report is made once, where the decision is made');
      assert.equal(reports[0].seq, 1);

      // Asking again changes nothing: the first decision keeps the row and its reason.
      assert.equal(structure.drainSpool().swept, 0, 'a decided loss is not decided again');
    },
    { spool: false, nowMs: () => clock, ledgerRetentionMs: 5 * 60 * 1000 },
  );
});

test('the byte half of the bound declares the oldest entries missing until what is held fits', async () => {
  await withStructure(
    async ({ structure, gaps, parts }) => {
      // Four entries of 40 bytes each; a 100-byte bound means the two oldest must go and the two
      // newest (80 bytes) fit. The size is carried by the raw the frame holds.
      const raw = 'x'.repeat(40);
      for (const seq of [1, 2, 3, 4]) {
        assert.equal(parts.ledger.record(envelope(seq, { meta: null, raw }), 'durable and held').recorded, true);
      }
      const swept = structure.drainSpool();
      assert.equal(swept.swept, 2, 'the oldest entries beyond the bound were declared missing');
      assert.deepEqual(
        structure.ledger.pending().map((entry) => `${entry.receiveSeq}:${entry.state}`),
        ['1:skipped', '2:skipped', '3:owed', '4:owed'],
        'oldest first, and the ones inside the bound are untouched',
      );
      assert.equal(gaps.filter((gap) => String(gap.reason).includes('retention bound')).length, 2);
      const stillHeld = structure.ledger
        .oldestEntries()
        .filter((entry) => entry.state !== 'skipped')
        .reduce((sum, entry) => sum + entry.bytes, 0);
      assert.ok(stillHeld <= 100, 'what is still held for delivery fits the bound');
    },
    { spool: false, ledgerRetentionBytes: 100 },
  );
});

test('a decided loss is not counted against the bound again', async () => {
  await withStructure(
    async ({ structure, gaps, parts }) => {
      // A loss decided earlier keeps its row and its bytes. What the byte bound measures is what the
      // ledger is still holding for delivery - counting the decided row as well would leave the bound
      // unsatisfiable and sweep entries that are well inside it.
      const decided = 'x'.repeat(80);
      assert.equal(parts.ledger.record(envelope(1, { meta: null, raw: decided }), 'durable and held').recorded, true);
      assert.equal(parts.ledger.skip(CONNECTION, 1, 'a loss decided earlier').decided, true);
      assert.equal(
        parts.ledger.record(envelope(2, { meta: null, raw: 'y'.repeat(40) }), 'durable and held').recorded,
        true,
      );

      const swept = structure.drainSpool();
      assert.equal(swept.swept, 0, 'nothing inside the bound is declared missing');
      const states = new Map(structure.ledger.pending().map((entry) => [entry.receiveSeq, entry.state]));
      assert.equal(states.get(1), 'skipped', 'the decided loss is left as it was');
      assert.equal(states.get(2), 'owed', 'and the fresh entry survives: the decided bytes are not counted');
      assert.equal(structure.ledger.size(), 2, 'both rows are still written down');
      assert.equal(
        structure.ledger.heldSize().bytes,
        40,
        'and what is held for delivery does not include the decided row',
      );
      assert.equal(
        gaps.filter((gap) => String(gap.reason).includes('retention bound')).length,
        0,
        'and nothing is reported as newly lost',
      );
    },
    { spool: false, ledgerRetentionBytes: 60 },
  );
});

test('a drain stops at a record the raw refuses, and does not consume past it', async () => {
  await withStructure(
    async ({ structure, health }) => {
      // Two frames are spilled, then the raw takes them again - except the first. Carrying the walk past
      // the record it could not consume would move the cursor over it, and the position a caller confirms
      // is what releases a segment: the unread record would be deleted with the one that was read.
      assert.equal(structure.feed(envelope(1)).spooled, true);
      assert.equal(structure.feed(envelope(2)).spooled, true);
      health.accepting = true;

      const drained = structure.drainSpool();
      assert.equal(drained.walked, 1, 'the walk stopped at the first record it could not consume');
      assert.equal(drained.consumed, 0, 'and consumed nothing past it');
      assert.equal(drained.advanced, false, 'so the cursor does not move');
      assert.match(String(drained.stopped), /still refused/);

      const held = [...structure.spool.drain()].filter((entry) => entry && typeof entry === 'object');
      assert.deepEqual(
        held.map((entry) => entry.receive_seq),
        [1, 2],
        'both records are still in the spool: nothing copied out, nothing dropped',
      );
      assert.deepEqual(structure.spool.cursor, { segment: null, offset: 0 });
      assert.equal(structure.ledger.find(CONNECTION, 1), null, 'no debt was left for the refused record');
      assert.equal(structure.ledger.find(CONNECTION, 2), null, 'and none for the one behind it');
    },
    { refuseSeqs: [1] },
  );
});

test('a frame that arrives on the socket climbs the ladder too, not only one a caller hands in', async () => {
  await withStructure(
    async ({ structure, socket, health, rawWritten }) => {
      structure.start();
      socket().onopen?.();
      // The raw refuses, so the frame is spilled rather than dropped.
      socket().onmessage?.({ data: JSON.stringify({ seq: 1, size: 1 }) });
      assert.equal(structure.stats.spooledFrames, 1, 'the frame that arrived on the socket was spilled');
      assert.ok(structure.spool.bytes > 0, 'and the spool holds it');

      // The raw is healthy again, and the frame that finds this out arrives on the socket - not from a
      // caller. The repair has to hang off the road in, or the spilled frame waits for a caller who is
      // never coming.
      health.accepting = true;
      socket().onmessage?.({ data: JSON.stringify({ seq: 2, size: 2 }) });
      assert.equal(structure.book.appliedBoundary.upToSeq, 2, 'both frames reached the board');
      assert.deepEqual(rawWritten, [2, 1], 'the raw took the arrival, then the frame it had refused');
      assert.equal(structure.spool.bytes, 0, 'and the spool was drained by that arrival itself');
    },
    { socket: true },
  );
});

test('the bound is re-applied while frames keep arriving, with no spool and no explicit drain', async () => {
  await withStructure(
    async ({ structure, health, gaps, parts }) => {
      health.accepting = true;
      // Three entries the ledger holds for delivery, 40 bytes each.
      for (const seq of [2, 3, 4]) {
        assert.equal(
          parts.ledger.record(envelope(seq, { meta: null, raw: 'x'.repeat(40) }), 'durable and held').recorded,
          true,
        );
      }
      assert.equal(structure.ledger.size(), 3);

      // The next frame arrives normally: its write is the moment the bound is applied. Nothing asks for a
      // drain, and there is no spool in play - an ordinary frame has to be enough.
      assert.equal(structure.feed(envelope(5, { meta: null })).durable, true);
      const states = new Map(structure.ledger.pending().map((entry) => [entry.receiveSeq, entry.state]));
      assert.equal(states.get(2), 'skipped', 'the oldest entry past the bound was declared missing');
      assert.equal(states.get(3), 'owed', 'and what is left is back inside it');
      assert.equal(states.get(4), 'owed');
      assert.equal(states.get(5), 'owed');
      assert.equal(
        gaps.filter((gap) => String(gap.reason).includes('retention bound')).length,
        1,
        'the loss is reported once, where the decision is made',
      );
    },
    { spool: false, ledgerRetentionBytes: 100 },
  );
});

test('reaching the bound is not passing it, in either half', async () => {
  let clock = 1_000_000;
  await withStructure(
    async ({ structure, gaps, parts }) => {
      // Exactly at the byte bound and exactly at the age bound: neither half has been passed, so nothing
      // is declared missing. One byte - or one millisecond - further, and the oldest entry goes.
      assert.equal(
        parts.ledger.record(envelope(1, { meta: null, raw: 'x'.repeat(100) }), 'durable and held').recorded,
        true,
      );
      clock += 5 * 60 * 1000;
      assert.equal(structure.drainSpool().swept, 0, 'exactly at the bound is inside it');

      assert.equal(
        parts.ledger.record(envelope(2, { meta: null, raw: 'y' }), 'durable and held').recorded,
        true,
      );
      assert.equal(structure.drainSpool().swept, 1, 'one byte past it, and the oldest is declared missing');
      const states = new Map(structure.ledger.pending().map((entry) => [entry.receiveSeq, entry.state]));
      assert.equal(states.get(1), 'skipped');
      assert.equal(states.get(2), 'owed', 'and the one inside the bound survives');
      assert.equal(gaps.filter((gap) => String(gap.reason).includes('retention bound')).length, 1);
    },
    { spool: false, nowMs: () => clock, ledgerRetentionMs: 5 * 60 * 1000, ledgerRetentionBytes: 100 },
  );
});

test('the age half of the bound is re-applied on the reception road too', async () => {
  let clock = 1_000_000;
  await withStructure(
    async ({ structure, health, gaps, parts }) => {
      health.accepting = true;
      // One entry, well inside the byte bound: only its age can act on it.
      assert.equal(
        parts.ledger.record(envelope(2, { meta: null, raw: 'x'.repeat(40) }), 'durable and held').recorded,
        true,
      );
      const states = () =>
        new Map(structure.ledger.pending().map((entry) => [entry.receiveSeq, entry.state]));

      clock += 5 * 60 * 1000;
      assert.equal(structure.feed(envelope(3, { meta: null })).durable, true, 'a frame arrives at the bound');
      assert.equal(states().get(2), 'owed', 'exactly at the age bound is still inside it');

      clock += 1; // one millisecond further, and the age half has been passed
      assert.equal(structure.feed(envelope(4, { meta: null })).durable, true, 'and another frame arrives');
      assert.equal(states().get(2), 'skipped', 'the oldest entry is declared missing by the arrival itself');
      assert.equal(states().get(3), 'owed');
      assert.equal(states().get(4), 'owed');
      assert.equal(
        gaps.filter((gap) => String(gap.reason).includes('retention bound')).length,
        1,
        'reported once, where the decision is made',
      );
    },
    { spool: false, nowMs: () => clock, ledgerRetentionMs: 5 * 60 * 1000, ledgerRetentionBytes: 10_000 },
  );
});

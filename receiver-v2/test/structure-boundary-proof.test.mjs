/**
 * Set 6, stage A: the proof reaches the structure.
 *
 * A frame that fails its proof is refused before it is committed, so the ledger it was written down in
 * still holds it as owed and the two positions do not drift. And a range the retention sweep declares
 * missing for the connection the board follows drops the board's proof - even across a restart - so the
 * board waits for a re-anchor instead of proving a range with a hole in it, while a loss of another
 * connection is history that must not block the board running now (C7, C11).
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
import { withInjectableWrites, BOOK_MISSING_WRITE } from '../test-support/failing-store.mjs';

const CONNECTION = 'conn-1';
const MARKET = 'kraken_spot';
const STREAM = 'trades';

const envelope = (seq, venueSeq, { meta = { first_seq: 1, venue_seq: venueSeq }, connectionId = CONNECTION } = {}) =>
  makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId,
    runId: 'run-1',
    venue: 'kraken',
    generation: 1,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq},"venue_seq":${venueSeq}}`,
    ...(meta ? { meta } : {}),
  });

const exactSuccessor = ({ previous, current, replace }) =>
  replace === true || previous === null || current.meta.venue_seq === previous.meta.venue_seq + 1;

let clock = 1_000_000;

async function withStructure(fn, { connects = exactSuccessor, replaceSeqs = [], injectable = false, gapThrows = false, rawRefuses = false, refetchThrowsOnce = false } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'proof-structure-'));
  const base = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  const inject = injectable ? withInjectableWrites(base) : null;
  const store = inject ? inject.durability : base;
  const refetch = [];
  const gaps = [];
  let rawCalls = 0;
  let refetchThrew = false;
  const structure = createStructure({
    market: MARKET,
    stream: STREAM,
    runId: 'run-1',
    venue: 'kraken',
    adapter: {
      url: 'ws://venue.test/ws',
      stream: STREAM,
      parse: () => ({ kind: 'data' }),
      boundary: 'sequence',
      connects,
      changesFor: (frame) =>
        replaceSeqs.includes(frame.receive_seq)
          ? { replace: true, levels: [{ side: 'bid', price: 500 + frame.receive_seq, size: 1 }] }
          : [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
    },
    durability: store,
    webSocketImpl: function unused() {
      throw new Error('this test feeds frames directly');
    },
    rawWriter: () => {
      rawCalls += 1;
      return !rawRefuses;
    },
    nowMs: () => clock,
    onGap: (gap) => {
      if (gapThrows) throw new Error('the gap hook refused');
      gaps.push(gap);
    },
    onDiagnostic: () => {},
    onRefetch: (request) => {
      if (refetchThrowsOnce && !refetchThrew) {
        refetchThrew = true;
        throw new Error('the refetch hook refused');
      }
      refetch.push(request);
    },
  });
  structure.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
  try {
    return await fn({ structure, store, refetch, gaps, dir, rawWrites: () => rawCalls, parts: internalsOf(structure), ...(inject ? { inject } : {}) });
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test('a frame whose proof fails leaves the levels, the position and the ledger untouched', async () => {
  await withStructure(async ({ structure, refetch }) => {
    assert.equal(structure.feed(envelope(1, 1)).applied, true);
    const before = { ...structure.book.appliedBoundary };

    const refused = structure.feed(envelope(2, 50));
    assert.equal(refused.applied, false);
    assert.match(String(refused.reason), /proof is broken/);
    assert.deepEqual(structure.book.appliedBoundary, before, 'the position did not move');
    assert.equal(structure.book.board.size('bid', 102), null, 'the level did not land');
    assert.equal(structure.book.board.size('bid', 101), 1, 'and the board still holds what it did');
    assert.equal(structure.book.phase, 'syncing');

    const entry = structure.ledger.find(CONNECTION, 2);
    assert.notEqual(entry, null, 'the frame is written down as durable');
    assert.equal(entry.state, 'owed', 'and is neither released, applied, nor decided a loss');
    assert.equal(structure.stats.owed, 1, 'it is still owed to the board');
    assert.equal(refetch.length, 1, 'and the structure asks for a re-anchor');
  });
});

test('the retention sweep of the followed connection drops the proof and asks for a re-anchor', async () => {
  await withStructure(async ({ structure, refetch, parts, store }) => {
    assert.equal(structure.feed(envelope(1, 1)).applied, true);
    assert.equal(parts.book.proveBoundary().proven, true, 'the board is serving before the loss');

    // An entry of this very connection, old enough for the bound to declare it missing.
    assert.equal(parts.ledger.record(envelope(7, 7, { meta: null }), 'durable and held').recorded, true);
    clock += 10 * 60 * 1000;
    assert.equal(structure.drainSpool().swept, 1, 'the old entry is declared missing');

    assert.equal(structure.ledger.find(CONNECTION, 7).state, 'skipped');
    const proof = parts.book.proveBoundary();
    assert.equal(proof.proven, false, 'the proof is dropped, not merely the phase');
    assert.match(proof.reason, /proof is broken/);
    assert.equal(structure.book.phase, 'syncing');
    assert.equal(refetch.length, 1, 'and a re-anchor is requested once');

    // The record is what a restart and an operator read, so it carries the reason the range was lost.
    const record = internalsOf(store).db
      .prepare('SELECT reason FROM book_missing_record WHERE market = ? AND stream = ?')
      .get(MARKET, STREAM);
    assert.match(String(record?.reason ?? ''), /retention bound/, 'the record says why the range is gone');
  });
});

test('a missing range of another connection does not block the board', async () => {
  await withStructure(async ({ structure, refetch, parts }) => {
    assert.equal(structure.feed(envelope(1, 1)).applied, true);
    assert.equal(parts.book.proveBoundary().proven, true);

    assert.equal(
      parts.ledger.record(envelope(5, 5, { meta: null, connectionId: 'conn-2' }), 'durable and held').recorded,
      true,
    );
    clock += 10 * 60 * 1000;
    assert.equal(structure.drainSpool().swept, 1, 'the other connection loss is declared missing');
    assert.equal(parts.book.proveBoundary().proven, true, 'and the running board is untouched');
    assert.equal(refetch.length, 0, 'nothing asks for a re-anchor');
  });
});

test('a replacement frame re-anchors the board after a broken proof', async () => {
  await withStructure(
    async ({ structure, refetch }) => {
      assert.equal(structure.feed(envelope(1, 1)).applied, true);
      assert.equal(structure.feed(envelope(2, 50)).applied, false, 'the jump breaks the proof');
      assert.equal(structure.book.phase, 'syncing');
      assert.equal(refetch.length, 1);

      const replaced = structure.feed(envelope(3, 100));
      assert.equal(replaced.applied, true);
      assert.equal(replaced.replaced, true);
      assert.equal(structure.book.appliedBoundary.upToSeq, 3, 'the snapshot re-anchored the position');
      assert.equal(structure.book.board.size('bid', 503), 1, 'and replaced the board');
    },
    { replaceSeqs: [3] },
  );
});

test('a proof broken while the waiting queue drains asks for a re-anchor', async () => {
  // A break found while draining is the same event as a break found on a frame that arrived on its own, and
  // it has to reach the same place: the queue stops where it is, and the structure asks for a re-anchor
  // rather than leaving a board that cannot serve with nobody told (C7).
  await withStructure(async ({ structure, refetch }) => {
    assert.equal(structure.feed(envelope(1, 1)).applied, true);
    assert.equal(structure.feed(envelope(3, 30)).applied, false, 'held behind the hole');

    const filled = structure.feed(envelope(2, 2));
    assert.equal(filled.applied, true, 'the hole is filled by the frame that was missing');
    assert.equal(filled.proofBroken, true, 'and the frame behind it failed its proof on the way in');
    assert.equal(structure.book.appliedBoundary.upToSeq, 2, 'the position stops where the queue stopped');
    assert.equal(structure.book.phase, 'syncing');
    assert.equal(refetch.length, 1, 'the structure asks for a re-anchor');
    assert.equal(structure.ledger.find(CONNECTION, 3).state, 'owed', 'and the refused frame is still owed to the board');
  });
});

test('a loss the ledger cannot write down takes the proof-dropping with it', async () => {
  // The row that says a range is gone and the proof that range invalidates are one fact about this store,
  // so they are written together: a store holding the row and the proof at once is the state a restart
  // would misread, and it must not be reachable through a half-failure.
  await withStructure(
    async ({ structure, refetch, parts, inject }) => {
      assert.equal(structure.feed(envelope(1, 1)).applied, true);
      assert.equal(parts.book.proveBoundary().proven, true, 'the board is serving before the loss');
      assert.equal(parts.ledger.record(envelope(7, 7, { meta: null }), 'durable and held').recorded, true);
      clock += 10 * 60 * 1000;

      inject.armWriteFailure(BOOK_MISSING_WRITE);
      assert.throws(() => structure.drainSpool(), /injected write failure/, 'the write was refused');
      assert.equal(structure.ledger.find(CONNECTION, 7).state, 'owed', 'the loss was not decided');
      assert.equal(parts.book.proveBoundary().proven, true, 'and the proof still stands');
      assert.equal(refetch.length, 0, 'nothing is reported, because nothing was decided');
    },
    { injectable: true },
  );
});

test('a frame the rule accepts puts the board in service through the structure, without a private call', async () => {
  // The book does not put itself in service when it applies - a proof is asked for, never assumed - and the
  // structure is the one that asks. A board that never leaves syncing is a board nobody may read from, and
  // the way back into service must not depend on a caller remembering to ask for it (C6, C7).
  await withStructure(async ({ structure, refetch }) => {
    assert.equal(structure.feed(envelope(1, 1)).applied, true);
    assert.equal(structure.book.phase, 'running', 'the structure asked, and the rule covered what was applied');
    assert.equal(refetch.length, 0, 'nothing needed a re-anchor');
  });
});

test('a commit that never lands leaves the board serving, because the store still holds its proof', async () => {
  // The proof follows the commit, never the write: the sweep's writes are taken back together, and an
  // in-memory fact cannot be taken back with them. A board left refusing to serve over a loss the store
  // does not hold is a board stopped by a fact that never happened.
  await withStructure(
    async ({ structure, refetch, parts, inject, store }) => {
      assert.equal(structure.feed(envelope(1, 1)).applied, true);
      assert.equal(parts.book.proveBoundary().proven, true, 'the board is serving before the loss');
      assert.equal(parts.ledger.record(envelope(7, 7, { meta: null }), 'durable and held').recorded, true);
      clock += 10 * 60 * 1000;

      inject.armCommitFailure();
      assert.throws(() => structure.drainSpool(), /injected write failure/, 'the commit was refused');
      assert.equal(structure.ledger.find(CONNECTION, 7).state, 'owed', 'the loss was not decided');
      assert.equal(
        internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_missing_record').get().n,
        0,
        'and no record of it was left behind',
      );
      assert.equal(parts.book.proveBoundary().proven, true, 'so the board is still serving');
      assert.equal(refetch.length, 0, 'and nothing asks for a re-anchor');
    },
    { injectable: true },
  );
});

test('a report that throws does not leave the board serving on a range the store wrote off', async () => {
  // The state follows the commit and a caller's hook follows the state. A hook that throws must not be able to
  // leave the proof standing over a range the ledger has already decided is gone: the row is decided, so a
  // later sweep skips it and nothing would ever come back to drop that proof.
  await withStructure(
    async ({ structure, refetch, parts, store }) => {
      assert.equal(structure.feed(envelope(1, 1)).applied, true);
      assert.equal(parts.book.proveBoundary().proven, true, 'the board is serving before the loss');
      assert.equal(parts.ledger.record(envelope(7, 7, { meta: null }), 'durable and held').recorded, true);
      clock += 10 * 60 * 1000;

      assert.throws(() => structure.drainSpool(), /the gap hook refused/, 'the report could not be made');
      assert.equal(structure.ledger.find(CONNECTION, 7).state, 'skipped', 'the loss is still decided');
      assert.equal(
        internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_missing_record').get().n,
        1,
        'and its record is on the store',
      );
      const proof = parts.book.proveBoundary();
      assert.equal(proof.proven, false, 'so the board is not serving on it');
      assert.match(proof.reason, /proof is broken/);
      assert.equal(refetch.length, 1, 'and a re-anchor was asked for');
    },
    { gapThrows: true },
  );
});

test('a stop that could not be reported is still a stop', async () => {
  // When nothing can hold the frame, stopping is the state that matters and the report is only news of it.
  // A hook that throws must not leave reception running: the next frame would be taken by a process with
  // nowhere to put it, and nothing would say so (measured before the fix: nothing owed, not stopped).
  await withStructure(
    async ({ structure, rawWrites }) => {
      assert.throws(() => structure.feed(envelope(1, 1)), /the gap hook refused/, 'the report could not be made');
      assert.equal(structure.stats.stopped, true, 'reception is stopped all the same');

      const reached = rawWrites();
      // The stopped reception reports the frame the same way, and the hook throws here too - the point is
      // that the raw is not reached again: reception is not taking frames it has nowhere to put.
      assert.throws(() => structure.feed(envelope(2, 2)), /the gap hook refused/);
      assert.equal(rawWrites(), reached, 'the raw is not reached again');
    },
    { rawRefuses: true, gapThrows: true },
  );
});

test('a re-anchor request that could not be delivered is asked again', async () => {
  // The request *is* the notification, so it is recorded as made only once a caller has heard it: a request
  // recorded as made but never delivered leaves the board waiting for an answer to a question nobody asked.
  await withStructure(
    async ({ structure, refetch, parts }) => {
      assert.equal(structure.feed(envelope(1, 1)).applied, true);
      assert.equal(parts.book.proveBoundary().proven, true, 'the board is serving');
      assert.equal(parts.ledger.record(envelope(7, 7, { meta: null }), 'durable and held').recorded, true);
      clock += 10 * 60 * 1000;

      // The sweep decides the loss and asks for a re-anchor; the caller's hook refuses to hear it.
      assert.throws(() => structure.drainSpool(), /the refetch hook refused/);
      assert.equal(refetch.length, 0, 'the request was never delivered');

      // The next frame breaks the proof the same way, and the request is made this time - because the first
      // one never was.
      const broken = structure.feed(envelope(2, 50));
      assert.equal(broken.applied, false);
      assert.match(String(broken.reason), /proof is broken/);
      assert.equal(refetch.length, 1, 'and this time the request lands');
    },
    { refetchThrowsOnce: true },
  );
});

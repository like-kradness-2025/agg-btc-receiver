/**
 * Set 6, stage A: the boundary proof is the adapter's, and it is judged before anything is committed.
 *
 * The rule under test is one sentence: a board serves because a proof covers the range it applied since
 * an anchor, not because frames kept arriving and a venue number kept going up. So the adapter declares
 * the kind of proof and decides, per frame, whether the frame connects to what came before; the book
 * binds that proof to the board, the connection, the anchor and the verified range; and a frame that
 * does not connect is refused before the levels, the position or anything durable move.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { openBook } from '../src/book/state.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';

const envelope = (seq, venueSeq, connectionId = 'conn-1', extra = {}) =>
  makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq},"venue_seq":${venueSeq}}`,
    meta: { first_seq: 1, venue_seq: venueSeq },
    ...extra,
  });

const change = (seq) => ({ side: 'bid', price: 100 + seq, size: seq });

/** The rule the review asked for: the adapter decides whether a frame connects, never the number. */
const exactSuccessor = ({ previous, current, replace }) =>
  replace === true || previous === null || current.meta.venue_seq === previous.meta.venue_seq + 1;

const sequenceAdapter = (connects = exactSuccessor) => ({ boundary: 'sequence', connects });

async function withBook(fn, adapter) {
  const dir = await mkdtemp(join(tmpdir(), 'book-proof-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
  const book = openBook({
    market: MARKET,
    stream: STREAM,
    durability: store,
    ...(adapter ? { adapter } : {}),
  });
  try {
    return await fn({ book, store, dir });
  } finally {
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
}

test('a continuing run applies, and the proof names the kind it verified', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    for (const seq of [1, 2, 3]) {
      assert.equal(book.apply({ envelope: envelope(seq, seq), changes: [change(seq)] }).applied, true);
    }
    const proof = book.proveBoundary();
    assert.equal(proof.proven, true, 'the rule held over the whole run');
    assert.equal(proof.kind, 'sequence', 'and the result names the kind of proof it was');
    assert.equal(proof.verified, true);
    assert.equal(book.isRunning, true);
  }, sequenceAdapter());
});

test('a frame the connection rule rejects breaks the proof, and the next ordinary frame is refused', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);
    assert.equal(book.apply({ envelope: envelope(2, 2), changes: [change(2)] }).applied, true);

    const broken = book.apply({ envelope: envelope(3, 9), changes: [change(3)] });
    assert.equal(broken.applied, false, 'the frame did not connect to what came before');
    assert.match(broken.reason, /proof is broken/);
    assert.equal(book.phase, 'syncing', 'a broken proof puts the board back to syncing');
    assert.equal(book.appliedBoundary.upToSeq, 2, 'and the position did not move over it');
    assert.equal(book.board.size('bid', 103), null, 'nor did the level it carried');
    assert.equal(book.proveBoundary().proven, false, 'a broken proof is not a boundary');

    const next = book.apply({ envelope: envelope(4, 3), changes: [change(4)] });
    assert.equal(next.applied, false, 'the next ordinary frame is refused while the proof is broken');
    assert.match(next.reason, /proof is broken/);
    assert.equal(book.appliedBoundary.upToSeq, 2);
    assert.equal(book.board.size('bid', 104), null);
  }, sequenceAdapter());
});

test('a bare increase the rule does not accept is refused, and one it accepts is not', async () => {
  // The same jump of +2: refused by a rule that requires the exact successor...
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    const bare = book.apply({ envelope: envelope(3, 4), changes: [change(3)] });
    assert.equal(bare.applied, false, 'a bigger number is not a connection by itself');
    assert.equal(book.appliedBoundary.upToSeq, 2);
  }, sequenceAdapter());

  // ...and accepted by a rule that accepts any increase, so the number alone never decides.
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    const bare = book.apply({ envelope: envelope(3, 4), changes: [change(3)] });
    assert.equal(bare.applied, true, 'what makes it a proof is the rule, not the number');
  }, sequenceAdapter(({ previous, current }) => previous === null || current.meta.venue_seq > previous.meta.venue_seq));
});

test('a frame whose proof fails writes nothing: not the levels, not the position', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    const before = { ...book.appliedBoundary };

    const refused = book.apply({ envelope: envelope(2, 50), changes: [change(2)] });
    assert.equal(refused.applied, false);
    assert.deepEqual(book.appliedBoundary, before, 'the position did not move');
    assert.equal(book.board.size('bid', 102), null, 'the level the frame carried did not land');
    assert.equal(book.board.size('bid', 101), 1, 'and what the board held is untouched');
    assert.equal(book.phase, 'syncing');
  }, sequenceAdapter());
});

test('a frame held behind a hole that fails its proof when it drains stops the queue', async () => {
  const rejected = new Set([3]);
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);
    assert.equal(book.apply({ envelope: envelope(3, 3), changes: [change(3)] }).applied, false, 'held by the hole');

    const filled = book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    assert.equal(filled.applied, true, 'the frame that fills the hole applies');
    assert.equal(filled.alsoApplied, 0, 'but the held frame failed its proof on the way out');
    assert.equal(book.appliedBoundary.upToSeq, 2, 'so the position stops before it');
    assert.equal(book.board.size('bid', 103), null);

    const behind = book.apply({ envelope: envelope(4, 4), changes: [change(4)] });
    assert.equal(behind.applied, false, 'and the frames behind it are not applied either');
    assert.equal(book.board.size('bid', 104), null);
  }, sequenceAdapter(({ previous, current }) => previous === null || !rejected.has(current.meta.venue_seq)));
});

test('a replacement behind a hole re-anchors after its own proof, and the past record is kept', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);

    // A hole: the frame at 3 is held, and the record of the missing 2 exists.
    const held = book.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    assert.equal(held.reason, 'gap before this sequence');
    assert.equal(book.openGaps().length, 1, 'the hole is a record');
    assert.equal(book.appliedBoundary.upToSeq, 1);

    // The frame that would fill the hole is contiguous, but it does not connect: the proof breaks.
    const breaks = book.apply({ envelope: envelope(2, 99), changes: [change(2)] });
    assert.equal(breaks.applied, false);
    assert.equal(book.phase, 'syncing');
    assert.equal(book.proveBoundary().proven, false, 'a broken proof waits for a re-anchor');
    assert.equal(book.openGaps().length, 1, 'and the missing record is still there');

    // The replacement is self-contained: from behind the hole, after its own proof, it re-anchors.
    const replaced = book.apply({
      envelope: envelope(5, 100),
      changes: { replace: true, levels: [{ side: 'bid', price: 500, size: 5 }] },
    });
    assert.equal(replaced.applied, true);
    assert.equal(replaced.replaced, true);
    assert.equal(book.appliedBoundary.upToSeq, 5, 'the snapshot moved the position to its own numbering');
    assert.equal(book.board.size('bid', 500), 5, 'the whole board was replaced');
    assert.equal(book.board.size('bid', 101), null, 'levels the snapshot did not carry are gone');
    assert.equal(book.openGaps().length, 1, 'C7: the earlier missing record is kept');
    assert.equal(book.proveBoundary().proven, true, 'the re-anchor cleared the broken proof');
    assert.equal(book.board.size('bid', 103), null, 'and the frame it overtook was dropped');
  }, sequenceAdapter());
});

test('an ordinary diff behind a hole is still held', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    const held = book.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    assert.equal(held.applied, false);
    assert.equal(held.reason, 'gap before this sequence', 'unchanged: a diff is held, not applied');
    assert.equal(held.waitingFor, 2);
    assert.equal(book.appliedBoundary.upToSeq, 1);
    assert.equal(book.openGaps().length, 1);
    assert.equal(book.phase, 'syncing', 'C7: the hole puts the board back to syncing');
  });
});

test('an unverifiable venue runs, and its result says nothing was proven', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    const proof = book.proveBoundary();
    assert.equal(proof.proven, true, 'a venue with no means of proof may still run');
    assert.equal(proof.kind, 'unverifiable', 'but the kind is recorded');
    assert.equal(proof.verified, false, 'and nothing is described as proven');
    assert.match(proof.reason, /unverifiable|without a boundary proof/);
    assert.equal(book.isRunning, true);
  });
});

test('a hole puts the board back to syncing, and the proof returns once the hole is filled', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    assert.equal(book.proveBoundary().proven, true);
    assert.equal(book.isRunning, true);

    const jumped = book.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    assert.equal(jumped.reason, 'gap before this sequence');
    assert.equal(book.phase, 'syncing', 'C7: a hole is detected and the board stops serving');
    assert.equal(book.isRunning, false);

    const filled = book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    assert.equal(filled.applied, true);
    assert.equal(filled.alsoApplied, 1, 'the held frame comes out with the hole');
    assert.equal(book.proveBoundary().proven, true, 'and the proof comes back without a snapshot');
    assert.equal(book.isRunning, true);
  });
});

test('proveBoundary still refuses with no connection, nothing applied, or an open hole', async () => {
  await withBook(async ({ book }) => {
    assert.equal(book.proveBoundary().proven, false, 'no connection has been accepted');
    book.accept('conn-1');
    assert.equal(book.proveBoundary().proven, false, 'nothing from this connection has applied');
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    book.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    const hole = book.proveBoundary();
    assert.equal(hole.proven, false);
    assert.match(hole.reason, /unresolved hole/);
  });
});

test('beginSync pauses the phase, but only a dropped proof stops the book serving again', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    assert.equal(book.proveBoundary().proven, true);
    book.beginSync();
    assert.equal(book.phase, 'syncing', 'beginSync pauses the phase');
    // The proof itself is untouched: this is exactly why a re-take has to drop it rather than call
    // beginSync and hope - a stored proof would put the book straight back into service.
    assert.equal(book.proveBoundary().proven, true, 'the old proof would put the book straight back to running');
  });
});

test('a missing record of the followed connection invalidates the proof, and a restart comes back unproven', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'book-proof-'));
  const dbPath = join(dir, 'state.sqlite');
  try {
    const first = openDurability({ path: dbPath, runId: 'run-1' });
    const book = openBook({ market: MARKET, stream: STREAM, durability: first });
    book.accept('conn-1', { firstSeq: 1 });
    book.apply({ envelope: envelope(1, 1), changes: [change(1)] });
    book.apply({ envelope: envelope(2, 2), changes: [change(2)] });
    assert.equal(book.proveBoundary().proven, true);

    // A loss belonging to another connection is history: it must not block the board running now.
    assert.equal(book.invalidateProof('conn-2', 'a loss of another connection').invalidated, false);
    assert.equal(book.proveBoundary().proven, true, 'the board running now is untouched');

    // A loss of the connection the board follows drops the proof, not merely the phase.
    assert.equal(book.invalidateProof('conn-1', 'the delivery ledger declared this frame missing').invalidated, true);
    assert.equal(book.phase, 'syncing');
    assert.equal(book.proveBoundary().proven, false, 'the old proof does not come back by itself');
    first.close();

    // And it is written down, so a restart waits for a re-anchor instead of proving a range with a hole.
    const second = openDurability({ path: dbPath, runId: 'run-1' });
    const reopened = openBook({ market: MARKET, stream: STREAM, durability: second });
    assert.equal(reopened.isRunning, false);
    const proof = reopened.proveBoundary();
    assert.equal(proof.proven, false, 'a stored missing record keeps the board unproven');
    assert.match(proof.reason, /proof is broken/);
    // Unproven is not the same as waiting: the board has to refuse an ordinary frame until a replacement
    // re-anchors it, or frames would be applied over the range it knows is incomplete.
    const refused = reopened.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    assert.equal(refused.applied, false, 'an ordinary frame is refused while the stored proof is broken');
    assert.match(refused.reason, /proof is broken/);
    assert.equal(reopened.appliedBoundary.upToSeq, 2, 'and the position did not move');
    second.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frame the rule refuses is not applied merely because the board is unanchored', async () => {
  // The first frame of a range has nothing before it, and that is exactly the frame that would build the
  // anchor - but having nothing before it is not permission to believe it. The rule is consulted, with no
  // `previous` to lean on, so a board with nothing to anchor on stays unanchored rather than anchored on a
  // frame nobody verified (C6).
  await withBook(
    async ({ book }) => {
      book.accept('conn-1', { firstSeq: 1 });
      const refused = book.apply({ envelope: envelope(1, 2), changes: [change(1)] });
      assert.equal(refused.applied, false, 'the rule refused the frame that would have built the anchor');
      assert.match(refused.reason, /proof is broken/);
      assert.equal(book.appliedBoundary.upToSeq, null, 'nothing was applied');
      assert.equal(book.board.size('bid', 101), null, 'and no level landed');
      assert.equal(book.proveBoundary().proven, false, 'an unjudged range is not a proof');
    },
    sequenceAdapter(({ previous, current }) => previous !== null && current.meta.venue_seq === previous.meta.venue_seq + 1),
  );
});

test('a proof that broke is written down, and the frame it refused is not applied after a restart', async () => {
  // Fail-closed across a restart: a break kept only in memory comes back as a merely unanchored board, and
  // the very frame the rule refused would then be applied with nothing left to say it was refused (C6).
  await withBook(async ({ book, store, dir }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);
    assert.match(book.apply({ envelope: envelope(2, 9), changes: [change(2)] }).reason, /proof is broken/);
    store.close();

    const second = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const reopened = openBook({ market: MARKET, stream: STREAM, durability: second, adapter: sequenceAdapter() });
    assert.equal(reopened.appliedBoundary.upToSeq, 1, 'the position came back');
    const again = reopened.apply({ envelope: envelope(2, 9), changes: [change(2)] });
    assert.equal(again.applied, false, 'the frame the rule refused is still not applied');
    assert.match(again.reason, /proof is broken/);
    assert.equal(reopened.board.size('bid', 102), null);
    assert.equal(reopened.proveBoundary().proven, false);
    second.close();
  }, sequenceAdapter());
});

test('a replacement behind a hole re-anchors the proof past it, and the past hole stays history', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);
    assert.equal(book.apply({ envelope: envelope(3, 3), changes: [change(3)] }).applied, false, 'held behind the hole');
    assert.equal(book.openGaps().length, 1, 'the hole is recorded');

    const replaced = book.apply({ envelope: envelope(5, 20), changes: { replace: true, levels: [change(5)] } });
    assert.equal(replaced.applied, true);
    assert.equal(replaced.replaced, true);
    const proof = book.proveBoundary();
    assert.equal(proof.proven, true, 'the replacement re-anchored past the hole');
    assert.equal(proof.anchorSeq, 5, 'the anchor is where the replacement stood');
    assert.equal(book.openGaps().length, 1, 'and the hole is still recorded: a loss is history');
  }, sequenceAdapter());
});

test('a hole that is filled puts the board back in service by itself', async () => {
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    for (const seq of [1, 2]) assert.equal(book.apply({ envelope: envelope(seq, seq), changes: [change(seq)] }).applied, true);
    assert.equal(book.proveBoundary().proven, true);
    assert.equal(book.isRunning, true);

    assert.equal(book.apply({ envelope: envelope(4, 4), changes: [change(4)] }).applied, false, 'a hole holds it');
    assert.equal(book.isRunning, false, 'and takes the board out of service (C7)');

    const filling = book.apply({ envelope: envelope(3, 3), changes: [change(3)] });
    assert.equal(filling.applied, true);
    assert.equal(filling.alsoApplied, 1, 'the frame the hole held follows it');
    assert.equal(book.isRunning, true, 'the board serves again without anyone asking');
    assert.equal(book.appliedBoundary.upToSeq, 4, 'the position is whole again');
  }, sequenceAdapter());
});

test('a frame the proof refuses does not write the origin it declared', async () => {
  // The declaration is a fact about the connection, and a refused frame establishes nothing - an origin
  // written anyway is a fact the connection never established, which a restart would then believe.
  await withBook(
    async ({ book }) => {
      book.accept('conn-1', {});
      const refused = book.apply({
        envelope: envelope(5, 9, 'conn-1', { meta: { first_seq: 5, venue_seq: 9 } }),
        changes: [change(5)],
      });
      assert.equal(refused.applied, false);
      assert.match(refused.reason, /proof is broken/);
      assert.equal(book.appliedBoundary.firstSeq, null, 'the origin it declared was not written');
      assert.equal(book.appliedBoundary.upToSeq, null);
    },
    sequenceAdapter(({ previous }) => previous !== null),
  );
});

test('dropping a proof names the connection the board follows, and no other', async () => {
  // The book enforces this itself rather than trusting its caller: a loss of another connection is history,
  // and a board that stopped serving over one would be stopped by something that never concerned it (C11).
  await withBook(async ({ book }) => {
    book.accept('conn-1', { firstSeq: 1 });
    assert.equal(book.apply({ envelope: envelope(1, 1), changes: [change(1)] }).applied, true);
    assert.equal(book.proveBoundary().proven, true);

    const internal = internalsOf(book);
    assert.equal(internal.dropProof('conn-2').dropped, false, 'another connection is not the board\u2019s');
    assert.equal(book.proveBoundary().proven, true, 'so the board is still serving');
    assert.equal(internal.dropProof('conn-1').dropped, true);
    assert.equal(book.proveBoundary().proven, false, 'its own loss stops it');
  }, sequenceAdapter());
});

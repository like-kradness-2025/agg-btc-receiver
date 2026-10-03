import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { withInjectableWrites, APPLIED_BOUNDARY_WRITE, RETIRED_RUN_WRITE } from '../test-support/failing-store.mjs';
import { bindInternals, internalsOf } from '../src/internal/wiring.mjs';


/** The parts of a structure, for a test that drives one of them directly: the wiring's private side. */
const partsOf = (structure) => internalsOf(structure);

const envelope = (seq, connectionId, generation = null, runId = null, extra = {}) =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'book',
    connectionId,
    runId,
    venue: runId === null ? null : 'kraken',
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
    ...extra,
  });

async function withBook(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'run-auth-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('generations only order connections inside one run', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });

    const first = book.accept('run-1:kraken:kraken_spot:1', { generation: 1, runId: 'run-1', firstSeq: 1 });
    assert.equal(first.accepted, true);

    // A different run is not older or newer; without an explicit takeover it is refused.
    const other = book.accept('run-2:kraken:kraken_spot:1', { generation: 2, runId: 'run-2' });
    assert.equal(other.accepted, false, 'a different run is not ordered by its generation');
    assert.match(other.reason, /explicit takeover/);

    // Declared, it is accepted - that is what a restart looks like from here.
    const taken = book.accept('run-2:kraken:kraken_spot:1', {
      generation: 1, runId: 'run-2', firstSeq: 1, takeover: true,
    });
    assert.equal(taken.accepted, true, 'a new run may take over when it says so');

    // And the old run cannot come back in.
    const back = book.accept('run-1:kraken:kraken_spot:5', { generation: 5, runId: 'run-1', takeover: true });
    assert.equal(back.accepted, false, 'the run that was replaced does not return with a bigger number');
    store.close();
  });
});

test('an unresolved hole keeps the book out of service', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 1 });

    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-1'), changes: [] });
    const jumped = book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-1'), changes: [] });
    assert.equal(jumped.reason, 'gap before this sequence');

    const unproven = book.proveBoundary();
    assert.equal(unproven.proven, false, 'a hole in the middle is not a proven boundary');
    assert.match(unproven.reason, /unresolved hole/);
    assert.equal(book.isRunning, false, 'and the book is not serving');

    book.apply({ envelope: envelope(2, 'conn-1', 1, 'run-1'), changes: [] });
    const proven = book.proveBoundary();
    assert.equal(proven.proven, true, 'closing the hole makes the boundary provable');
    assert.equal(book.isRunning, true);
    store.close();
  });
});

test('a store that predates the ownership columns is migrated, and then keeps its owner across restarts', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');

    // A store written before the ownership columns existed: the boundary row has six columns, and there
    // is nothing in it that says which run owns the board.
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
      VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1);
      CREATE TABLE book_level (
        market TEXT NOT NULL, stream TEXT NOT NULL, side TEXT NOT NULL, price REAL NOT NULL,
        size REAL NOT NULL, PRIMARY KEY (market, stream, side, price)
      );
      INSERT INTO book_level (market, stream, side, price, size) VALUES ('kraken_spot', 'book', 'bid', 99, 3);
    `);
    legacy.close();

    const first = openDurability({ path: dbPath, runId: 'run-A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: first });
    const columns = internalsOf(first).db
      .prepare('PRAGMA table_info(applied_boundary)')
      .all()
      .map((column) => column.name);
    assert.ok(columns.includes('run_id'), 'the store was migrated in place');
    assert.ok(columns.includes('first_seq'), 'and keeps the origin it can now record');
    assert.equal(book.board.size('bid', 99), 3, 'the board a legacy store holds is still a board');

    // The first explicit accept establishes the owner: there is nobody to take it from, and the
    // generation it brings is the baseline rather than a comparison against a number nobody wrote down.
    const claimed = book.accept('run-A:kraken:kraken_spot:5', { generation: 5, runId: 'run-A' });
    assert.equal(claimed.accepted, true);
    assert.match(claimed.reason, /predates it/);
    assert.equal(book.appliedBoundary.runId, 'run-A');
    assert.equal(book.appliedBoundary.firstSeq, null, 'the origin is not known yet');

    // The origin arrives for the connection the board already knows: that is a completion, and it moves
    // nothing but the anchor.
    const completed = book.accept('run-A:kraken:kraken_spot:5', { generation: 5, runId: 'run-A', firstSeq: 1 });
    assert.equal(completed.accepted, true);
    assert.equal(book.appliedBoundary.firstSeq, 1);
    assert.equal(book.appliedBoundary.upToSeq, null, 'knowing where the stream starts is not data');
    assert.equal(book.phase, 'syncing');

    // The completion is a write, not a change of mind: it is in the store before a single frame is applied,
    // so a restart in between cannot lose the anchor it has just established. It is read through this
    // handle - one file is held by one handle at a time - and the restart below confirms it outlived the
    // process that wrote it.
    assert.equal(
      internalsOf(first).db
        .prepare('SELECT first_seq FROM applied_boundary WHERE market = ? AND stream = ?')
        .get('kraken_spot', 'book').first_seq,
      1,
      'the origin is in the store as soon as it is known',
    );
    assert.equal(book.appliedBoundary.upToSeq, null, 'and it is still not a position');

    // One frame applied, so that position, origin and board all exist to be checked after a restart.
    const applied = book.apply({
      envelope: envelope(1, 'run-A:kraken:kraken_spot:5', 5, 'run-A'),
      changes: [{ side: 'bid', price: 100, size: 2 }],
    });
    assert.equal(applied.applied, true);
    first.close();

    const second = openDurability({ path: dbPath, runId: 'run-A' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.deepEqual(reopened.appliedBoundary, {
      connectionId: 'run-A:kraken:kraken_spot:5',
      generation: 5,
      upToSeq: 1,
      runId: 'run-A',
      firstSeq: 1,
    });
    assert.equal(reopened.board.size('bid', 100), 2, 'the board came back with the position describing it');
    assert.equal(reopened.isRunning, false, 'and a reopened book proves its boundary before serving');

    // A frame carried by no run at all is not "any run": this board belongs to run-A.
    const nullRun = reopened.apply({ envelope: envelope(2, 'run-A:kraken:kraken_spot:5', 5, null), changes: [] });
    assert.equal(nullRun.applied, false);
    assert.match(nullRun.reason, /another run/);
    assert.equal(reopened.appliedBoundary.upToSeq, 1, 'and nothing moved');

    // A different run that does not say it is taking over is refused.
    const unauthorised = reopened.accept('run-B:kraken:kraken_spot:1', { generation: 1, runId: 'run-B', firstSeq: 1 });
    assert.equal(unauthorised.accepted, false);
    assert.match(unauthorised.reason, /explicit takeover/);
    assert.equal(reopened.appliedBoundary.runId, 'run-A', 'and the board did not change hands');

    // Authorised, run-B takes the board: its generation is not compared, and run-A is retired.
    const taken = reopened.accept('run-B:kraken:kraken_spot:1', {
      generation: 1, runId: 'run-B', firstSeq: 1, takeover: true,
    });
    assert.equal(taken.accepted, true);
    assert.equal(reopened.appliedBoundary.runId, 'run-B');
    assert.deepEqual(
      internalsOf(second).db
        .prepare('SELECT run_id FROM retired_run WHERE market = ? AND stream = ?')
        .all('kraken_spot', 'book')
        .map((row) => row.run_id),
      ['run-A'],
      'the run that was replaced is written down, not remembered in this process',
    );
    second.close();

    // Reopened again: the retirement did not vanish with the process that wrote it, and run-A is refused
    // whatever number it quotes - including a number higher than the run that replaced it.
    const third = openDurability({ path: dbPath, runId: 'run-A' });
    const afterRestart = openBook({ market: 'kraken_spot', stream: 'book', durability: third });
    const comeback = afterRestart.accept('run-A:kraken:kraken_spot:9', {
      generation: 9, runId: 'run-A', takeover: true,
    });
    assert.equal(comeback.accepted, false);
    assert.match(comeback.reason, /already replaced/);
    assert.equal(afterRestart.appliedBoundary.runId, 'run-B', 'and the board still belongs to its owner');
    third.close();
  });
});

test('a frame that disagrees with the accepted connection in one field is refused, and changes nothing', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 2, runId: 'run-1', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'conn-1', 2, 'run-1'), changes: [{ side: 'bid', price: 100, size: 1 }] });
    const before = { ...book.appliedBoundary };

    const variants = [
      ['market', makeEnvelope({ market: 'okx_spot', stream: 'book', connectionId: 'conn-1', runId: 'run-1', venue: 'kraken', generation: 2, receiveSeq: 2, recvTsMs: 1_792_000_000_002, recvMonoNs: 1_000_000_002, raw: '{"seq":2}' }), /another board/],
      ['stream', makeEnvelope({ market: 'kraken_spot', stream: 'trades', connectionId: 'conn-1', runId: 'run-1', venue: 'kraken', generation: 2, receiveSeq: 2, recvTsMs: 1_792_000_000_002, recvMonoNs: 1_000_000_002, raw: '{"seq":2}' }), /another board/],
      ['run', envelope(2, 'conn-1', 2, 'run-2'), /another run/],
      ['generation', envelope(2, 'conn-1', 3, 'run-1'), /another generation/],
    ];

    for (const [field, frame, expected] of variants) {
      const refused = book.apply({ envelope: frame, changes: [{ side: 'bid', price: 200, size: 9 }] });
      assert.equal(refused.applied, false, `a frame with a different ${field} is not applied`);
      assert.match(refused.reason, expected);
      assert.deepEqual(book.appliedBoundary, before, `nothing moved for the ${field} mismatch`);
      assert.equal(book.board.size('bid', 200), null, `and the board took nothing from it (${field})`);
    }

    // The frame that agrees in every field is the one that applies, which is what makes the four
    // refusals above a test of the identity rather than of a check that refuses everything.
    const agreeing = book.apply({ envelope: envelope(2, 'conn-1', 2, 'run-1'), changes: [] });
    assert.equal(agreeing.applied, true);
    store.close();
  });
});

test('a write that fails leaves the store and the book exactly as they were', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability });

    book.accept('conn-1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 100, size: 1 }] });
    book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 103, size: 3 }] });
    const settled = {
      boundary: { ...book.appliedBoundary },
      phase: book.phase,
      levels: internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n,
      gaps: book.openGaps().length,
    };
    assert.equal(settled.gaps, 1, 'the frame above a hole is held, which is the state being kept');

    const unchanged = () => {
      assert.deepEqual(book.appliedBoundary, settled.boundary, 'the position did not move');
      assert.equal(book.phase, settled.phase, 'the phase did not move');
      assert.equal(book.board.size('bid', 100), 1, 'the board still holds what it held');
      assert.equal(book.board.size('bid', 200), null, 'and nothing it did not hold');
      assert.equal(book.openGaps().length, settled.gaps, 'the holes are the same holes');
      const row = internalsOf(store).db
        .prepare('SELECT connection_id, generation, up_to_receive_seq, run_id, first_seq FROM applied_boundary WHERE market = ? AND stream = ?')
        .get('kraken_spot', 'book');
      assert.deepEqual(
        { connectionId: row.connection_id, generation: row.generation, upToSeq: row.up_to_receive_seq, runId: row.run_id, firstSeq: row.first_seq },
        settled.boundary,
        'and the store says exactly what the book says',
      );
    };

    // ① the takeover: the retirement of the old run is written first, so a failure on the new owner's
    // record has to take that retirement back with it - a store that says "run-A was replaced" while
    // still naming run-A as the owner is the one state nobody could reason about.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(
      () => book.accept('conn-2', { generation: 1, runId: 'run-B', firstSeq: 1, takeover: true }),
      /injected write failure/,
    );
    assert.equal(book.appliedBoundary.runId, 'run-A', 'the board was not handed over');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM retired_run').get().n, 0, 'and no run was retired');
    unchanged();

    // ①b the retirement record itself: a takeover that cannot write it down is not a takeover.
    armWriteFailure(RETIRED_RUN_WRITE);
    assert.throws(
      () => book.accept('conn-2', { generation: 1, runId: 'run-B', firstSeq: 1, takeover: true }),
      /injected write failure/,
    );
    assert.equal(book.appliedBoundary.runId, 'run-A');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM retired_run').get().n, 0);
    unchanged();

    // ② the owner's own record: the new connection's acceptance cannot land halfway.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(() => book.accept('conn-2', { generation: 5, runId: 'run-A', firstSeq: 1 }), /injected write failure/);
    unchanged();

    // ③ the boundary written together with the board: the levels were already written in that
    // transaction, so a failure here has to take them back with it.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(
      () => book.apply({ envelope: envelope(2, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 200, size: 9 }] }),
      /injected write failure/,
    );
    unchanged();
    assert.equal(
      internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM book_level WHERE price = 200').get().n,
      0,
      'the level written in the same transaction was rolled back with it',
    );

    // And a restart after all three sees the same book: nothing of a failed write is in the store.
    store.close();
    const second = openDurability({ path: dbPath, runId: 'run-1' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.deepEqual(reopened.appliedBoundary, settled.boundary, 'the reopened book has the same owner');
    assert.equal(reopened.board.size('bid', 100), 1);
    assert.equal(reopened.board.size('bid', 200), null, 'and none of the failed writes are in it');
    assert.equal(reopened.openGaps().length, settled.gaps);
    second.close();
  });
});

test('a NULL owner written by this version is an owner, and one legacy board does not speak for the store', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');

    // Two boards whose rows predate the ownership columns: one of them is claimed below, the other is left
    // alone. Whether a board's NULL means "nobody was ever asked" is a fact about that board, so the board
    // that is left alone must not speak for the one that was claimed.
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
      VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1),
             ('okx_spot', 'book', 'okx_spot:1', 3, 4, 1);
    `);
    legacy.close();

    const first = openDurability({ path: dbPath, runId: 'run-A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: first });
    // Claimed by a process that runs as no run at all: the claim is recorded, NULL and all.
    const claimed = book.accept('kraken_spot:1', { generation: 5, runId: null, firstSeq: 1 });
    assert.match(claimed.reason, /predates it/);
    assert.equal(book.appliedBoundary.runId, null);
    first.close();

    // Reopened: "no run" is now a recorded owner rather than an unanswered question, so a named run has
    // to take the board over explicitly - the legacy board does not drag the whole store back with it.
    const second = openDurability({ path: dbPath, runId: 'run-B' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.equal(reopened.appliedBoundary.firstSeq, 1, 'the claim came back from the store');
    const uninvited = reopened.accept('run-B:kraken:kraken_spot:1', { generation: 1, runId: 'run-B', firstSeq: 1 });
    assert.equal(uninvited.accepted, false, 'a recorded NULL owner is an owner, not an empty slot');
    assert.match(uninvited.reason, /explicit takeover/);

    const taken = reopened.accept('run-B:kraken:kraken_spot:1', {
      generation: 1, runId: 'run-B', firstSeq: 1, takeover: true,
    });
    assert.equal(taken.accepted, true, 'and it can still be handed over when somebody says so');
    assert.equal(reopened.appliedBoundary.runId, 'run-B');
    second.close();
  });
});

test('the origin a frame declares is written down at once, and cannot be re-completed past a hole', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
    const store = openDurability({ path: dbPath, runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    // Accepted with no origin, which is the state a restart leaves behind.
    book.accept('conn-1', { generation: 1, runId: 'run-1' });

    // A frame that declares where this connection's numbering starts. The origin is a fact about the
    // connection, so it is relied on and written down even though the frame itself cannot be applied yet.
    const ahead = book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-1'), changes: [] });
    assert.equal(ahead.reason, 'waiting for the first sequence');
    assert.equal(book.appliedBoundary.firstSeq, 1, 'the declared origin is recorded');
    assert.equal(book.appliedBoundary.upToSeq, null, 'and it is not a position');
    store.close();

    const second = openDurability({ path: dbPath, runId: 'run-1' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.equal(reopened.appliedBoundary.firstSeq, 1, 'the origin came back from the store');

    // An origin already relied on cannot be re-completed to a sequence that has arrived: that would close
    // the hole the board recorded instead of filling it.
    const renegotiated = reopened.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 3 });
    assert.equal(renegotiated.accepted, false);
    assert.match(renegotiated.reason, /already established/);
    assert.equal(reopened.appliedBoundary.firstSeq, 1, 'the origin did not move');
    assert.equal(reopened.openGaps().length, 1, 'and the hole the real origin opened is still a hole');
    assert.equal(reopened.appliedBoundary.upToSeq, null, 'while the frame above it is still waiting');
    second.close();
  });
});

test('a takeover that fails takes nothing with it, including what the board was holding', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability });

    book.accept('conn-1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 100, size: 1 }] });
    const held = book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 103, size: 3 }] });
    assert.equal(held.reason, 'gap before this sequence', 'the frame above the hole is held');

    // A takeover that cannot be written down must leave the book as it was - including what it is holding,
    // because a takeover that did not happen cannot have dropped the frames waiting for a hole.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(
      () => book.accept('conn-2', { generation: 1, runId: 'run-B', firstSeq: 1, takeover: true }),
      /injected write failure/,
    );
    assert.equal(book.appliedBoundary.runId, 'run-A');
    assert.equal(internalsOf(store).db.prepare('SELECT COUNT(*) AS n FROM retired_run').get().n, 0);

    const filled = book.apply({
      envelope: envelope(2, 'conn-1', 1, 'run-A'),
      changes: [{ side: 'bid', price: 102, size: 2 }],
    });
    assert.equal(filled.applied, true);
    assert.equal(filled.alsoApplied, 1, 'the frame held above the hole was applied with it');
    assert.equal(book.appliedBoundary.upToSeq, 3, 'so the position reaches the held frame');
    assert.equal(book.board.size('bid', 103), 3, 'and its change reached the board');
    store.close();
  });
});

test('a store that is only missing one ownership column keeps the owner it already records', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');

    // A store that already recorded who owns its board and is only missing the origin column. The owner is
    // a fact somebody wrote down, so the migration must not treat that board as one nobody was ever asked
    // about: a NULL is only evidence of an unrecorded owner when there was no column to record it in.
    const partial = new DatabaseSync(dbPath);
    partial.exec(`
      CREATE TABLE applied_boundary (
        market TEXT NOT NULL,
        stream TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        generation INTEGER,
        up_to_receive_seq INTEGER,
        run_id TEXT,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (market, stream)
      );
      INSERT INTO applied_boundary (market, stream, connection_id, generation, up_to_receive_seq, run_id, updated_at_ms)
      VALUES ('kraken_spot', 'book', 'run-A:kraken:kraken_spot:5', 5, 7, 'run-A', 1);
    `);
    partial.close();

    const store = openDurability({ path: dbPath, runId: 'run-B' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    assert.equal(book.appliedBoundary.runId, 'run-A', 'the owner the store recorded is the authority');
    assert.equal(
      internalsOf(store).db.prepare('PRAGMA table_info(applied_boundary)').all().some((column) => column.name === 'first_seq'),
      true,
      'and the missing column was added',
    );

    const uninvited = book.accept('run-B:kraken:kraken_spot:1', { generation: 1, runId: 'run-B', firstSeq: 1 });
    assert.equal(uninvited.accepted, false, 'so another run cannot walk in without saying so');
    assert.match(uninvited.reason, /explicit takeover/);
    assert.equal(book.appliedBoundary.runId, 'run-A');
    store.close();
  });
});

test('a connection name that disagrees with its own generation is refused, not applied a second time', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 100, size: 1 }] });

    // A run-scoped name is derived from the generation it belongs to, so a name arriving with a different
    // generation is a contradiction - and letting it through as a "newer connection" would reset the
    // position and apply the same range twice.
    const contradiction = book.accept('conn-1', { generation: 2, runId: 'run-A', firstSeq: 1 });
    assert.equal(contradiction.accepted, false);
    assert.match(contradiction.reason, /disagree/);
    assert.equal(book.appliedBoundary.connectionId, 'conn-1');
    assert.equal(book.appliedBoundary.upToSeq, 1, 'the position did not move');
    const again = book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-A'), changes: [] });
    assert.equal(again.reason, 'already applied', 'and the range is not applied a second time');
    store.close();
  });
});

test('an origin above a sequence that has already arrived is refused, and the frames are kept', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    // Accepted without an origin, which is where a restart leaves a connection.
    book.accept('conn-1', { generation: 1, runId: 'run-1' });

    // Frames arrive that declare no origin either: they cannot be applied, and the lowest of them is now the
    // ceiling on where this connection's numbering can start.
    const bare = (seq) => envelope(seq, 'conn-1', 1, 'run-1', { meta: undefined });
    assert.equal(book.apply({ envelope: bare(1), changes: [] }).reason, 'first sequence unknown');
    assert.equal(book.apply({ envelope: bare(3), changes: [] }).reason, 'first sequence unknown');

    // An origin above what has arrived would skip the frame at 1 - and the frames below it would then be
    // refused for ever as already applied while nothing ever applied them.
    const jumped = book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 3 });
    assert.equal(jumped.accepted, false);
    assert.match(jumped.reason, /beyond frames that have already arrived/);
    assert.equal(book.appliedBoundary.firstSeq, null, 'nothing was recorded');
    assert.equal(book.appliedBoundary.upToSeq, null, 'and no position was invented');

    // The origin that does fit the frames is accepted, and the frame at 1 can then be applied. The frame at 3
    // stays held, because the hole at 2 is real.
    const fitting = book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 1 });
    assert.equal(fitting.accepted, true);
    assert.equal(book.apply({ envelope: bare(1), changes: [] }).applied, true);
    const ahead = book.apply({ envelope: bare(3), changes: [] });
    assert.equal(ahead.reason, 'gap before this sequence', 'the frame above the hole is still waiting');
    assert.equal(book.appliedBoundary.upToSeq, 1, 'and the position stops where the data does');
    store.close();
  });
});

test('a frame that declares an origin above what has arrived is refused rather than believed', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1' });
    assert.equal(
      book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: undefined }), changes: [] }).reason,
      'first sequence unknown',
    );

    // Now a frame declaring origin 2 arrives - above the frame at 1 that has already been seen. One of the two
    // is wrong, and the frame at 1 would disappear if the declaration won.
    const declared = book.apply({
      envelope: envelope(4, 'conn-1', 1, 'run-1', { meta: { first_seq: 2 } }),
      changes: [],
    });
    assert.equal(declared.applied, false);
    assert.match(declared.reason, /beyond frames that have already arrived/);
    assert.equal(book.appliedBoundary.firstSeq, null, 'the declaration was not recorded');
    assert.equal(book.appliedBoundary.upToSeq, null);
    store.close();
  });
});

test('a connection name that another run holds is refused, even when it says it is taking over', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('m:1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'm:1', 1, 'run-A'), changes: [{ side: 'bid', price: 100, size: 1 }] });

    // A name without the run in it is a name two runs can both hold. Handing this board over while leaving
    // the organizer numbering under the same name would put the board and the raw in different worlds: the
    // new run's frame is refused here rather than being allowed to reset a position the raw does not share.
    const held = book.accept('m:1', { generation: 1, runId: 'run-B', firstSeq: 1, takeover: true });
    assert.equal(held.accepted, false);
    assert.match(held.reason, /disagrees/);
    assert.equal(book.appliedBoundary.runId, 'run-A', 'the board did not change hands');
    assert.equal(book.appliedBoundary.upToSeq, 1, 'and nothing was reset');

    // The run that does own the name continues where it was.
    const again = book.accept('m:1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    assert.equal(again.accepted, true);
    assert.equal(book.appliedBoundary.upToSeq, 1);
    store.close();
  });
});

test('a store another process migrates mid-way keeps the owner that process recorded', async () => {
  await withBook(async (dir) => {
    // Another process migrates the same store between this process's first look and its own transaction. What
    // it adds is `addedBy` - both columns, or only the first of them. Each case gets a store of its own.
    const raceWith = async (label, addedBy) => {
      const path = join(dir, `${label}.sqlite`);
      const legacy = new DatabaseSync(path);
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
        VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1);
      `);
      legacy.close();

      const store = openDurability({ path, runId: 'run-A' });
      let raced = false;
      // The interception is installed through the wiring: the modules run their statements through
      // `internalsOf(store).db`, so a wrapper that only replaced the public `db` would not be read.
      const racing = {
        ...store,
      };
      const racingDb = {
          exec: (sql, ...rest) => internalsOf(store).db.exec(sql, ...rest),
          prepare: (sql, ...rest) => {
            const statement = internalsOf(store).db.prepare(sql, ...rest);
            if (!/PRAGMA table_info/.test(sql)) return statement;
            return {
              get: (...args) => statement.get(...args),
              all: (...args) => {
                const rows = statement.all(...args);
                if (!raced) {
                  raced = true;
                  const other = new DatabaseSync(path);
                  for (const column of addedBy) other.exec(`ALTER TABLE applied_boundary ADD COLUMN ${column}`);
                  other.close();
                }
                return rows;
              },
            };
          },
      };
      bindInternals(racing, { ...internalsOf(store), db: racingDb });
      const book = openBook({ market: 'kraken_spot', stream: 'book', durability: racing });
      return { store, book };
    };

    // Both columns already there: this process has nothing left to add, and must not treat the row it finds as
    // a board nobody recorded an owner for.
    const both = await raceWith('both', ['run_id TEXT', 'first_seq INTEGER']);
    assert.equal(both.book.appliedBoundary.connectionId, 'kraken_spot:1', 'the store opened');
    assert.equal(
      internalsOf(both.store).db.prepare('SELECT COUNT(*) AS n FROM legacy_owner').get().n,
      0,
      'nothing was re-marked as a board nobody recorded an owner for',
    );
    const uninvited = both.book.accept('run-B:kraken:kraken_spot:1', { generation: 1, runId: 'run-B', firstSeq: 1 });
    assert.equal(uninvited.accepted, false, 'so another run cannot walk in without saying so');
    assert.match(uninvited.reason, /explicit takeover/);
    both.store.close();

    // Only run_id there: the migration still has to add first_seq, which is the whole reason the missing set
    // is read inside the transaction rather than remembered from before it.
    const partial = await raceWith('partial', ['run_id TEXT']);
    const columns = internalsOf(partial.store).db
      .prepare('PRAGMA table_info(applied_boundary)')
      .all()
      .map((column) => column.name);
    assert.ok(columns.includes('run_id') && columns.includes('first_seq'), 'both columns are there');
    assert.equal(partial.book.appliedBoundary.connectionId, 'kraken_spot:1', 'and the store opened');
    assert.equal(partial.book.appliedBoundary.runId, null, 'the row is the one that was already there');
    partial.store.close();
  });
});

test('an acceptance that fails changes nothing: not the phase, not what the board is holding', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const { durability, armWriteFailure } = withInjectableWrites(store);
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability });

    book.accept('conn-1', { generation: 1, runId: 'run-A', firstSeq: 1 });
    book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-A'), changes: [{ side: 'bid', price: 100, size: 1 }] });
    assert.equal(book.proveBoundary().proven, true, 'the book is serving before the acceptance that fails');

    // A newer generation of the same run that cannot be written down: the book keeps serving the connection
    // it has rather than starting a sync it cannot finish.
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(
      () => book.accept('conn-2', { generation: 5, runId: 'run-A', firstSeq: 1 }),
      /injected write failure/,
    );
    assert.equal(book.phase, 'running', 'the phase did not move');
    assert.equal(book.appliedBoundary.connectionId, 'conn-1');

    // And what it is holding survives the same failure. A frame above a hole is still applied when the hole
    // is filled, which is the only thing that shows the holding queue was not emptied by the attempt.
    const held = book.apply({
      envelope: envelope(3, 'conn-1', 1, 'run-A'),
      changes: [{ side: 'bid', price: 103, size: 3 }],
    });
    assert.equal(held.reason, 'gap before this sequence');
    // C7: detecting the hole put the board back to syncing, and that is the phase the acceptance that
    // fails must leave exactly where it found it.
    assert.equal(book.phase, 'syncing', 'a hole takes the board out of service');
    const phaseBeforeFailure = book.phase;
    armWriteFailure(APPLIED_BOUNDARY_WRITE);
    assert.throws(
      () => book.accept('conn-2', { generation: 5, runId: 'run-A', firstSeq: 1 }),
      /injected write failure/,
    );
    assert.equal(book.phase, phaseBeforeFailure, 'the failed acceptance did not move the phase');
    const filled = book.apply({ envelope: envelope(2, 'conn-1', 1, 'run-A'), changes: [] });
    assert.equal(filled.applied, true);
    assert.equal(filled.alsoApplied, 1, 'the frame it was holding came out with the hole');
    assert.equal(book.appliedBoundary.upToSeq, 3);
    store.close();
  });
});

test('a frame that declares an origin above its own sequence is refused, and its frame is not lost', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1' });

    // A frame at sequence 1 that claims the connection starts at 3 contradicts itself: its own arrival says the
    // numbering begins at or before 1. Believing it would write a position above the frame that declared it, and
    // that frame would then be refused for ever as already applied.
    const selfContradicting = book.apply({
      envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: { first_seq: 3 } }),
      changes: [{ side: 'bid', price: 100, size: 1 }],
    });
    assert.equal(selfContradicting.applied, false);
    assert.match(selfContradicting.reason, /beyond frames that have already arrived/);
    assert.equal(book.appliedBoundary.firstSeq, null, 'nothing was recorded');
    assert.equal(book.appliedBoundary.upToSeq, null);

    // The frame can still be applied, and its change still reach the board, once an origin that fits it arrives.
    const fitting = book.apply({
      envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: { first_seq: 1 } }),
      changes: [{ side: 'bid', price: 100, size: 1 }],
    });
    assert.equal(fitting.applied, true);
    assert.equal(book.board.size('bid', 100), 1, 'the change that would have been lost is in the board');
    store.close();
  });
});

test('a new connection is not measured against the sequences of the old one', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1' });
    assert.equal(book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: undefined }), changes: [] }).reason, 'first sequence unknown');

    // A new generation takes the board over. Its numbering is its own, so what was seen under the previous
    // connection says nothing about where this one starts.
    assert.equal(book.accept('conn-2', { generation: 2, runId: 'run-1' }).accepted, true);
    assert.equal(book.apply({ envelope: envelope(10, 'conn-2', 2, 'run-1', { meta: undefined }), changes: [] }).reason, 'first sequence unknown');

    const completed = book.accept('conn-2', { generation: 2, runId: 'run-1', firstSeq: 10 });
    assert.equal(completed.accepted, true, 'the origin of the new connection is not refused by the old one');
    assert.equal(book.appliedBoundary.firstSeq, 10);
    assert.equal(book.apply({ envelope: envelope(10, 'conn-2', 2, 'run-1', { meta: undefined }), changes: [] }).applied, true);
    store.close();
  });
});

test('a store that predates the ownership columns is claimed by the connection it already names', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
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
      VALUES ('kraken_spot', 'book', 'A:v:m:5', 5, 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path: dbPath, runId: 'run-A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    // The store already carries this name, and nobody recorded which run it belonged to: this accept is the one
    // that says so, and it is not a disagreement with anything.
    const claimed = book.accept('A:v:m:5', { generation: 5, runId: 'run-A', firstSeq: 1 });
    assert.equal(claimed.accepted, true);
    assert.match(claimed.reason, /predates it/);
    assert.equal(book.appliedBoundary.runId, 'run-A');
    assert.equal(book.appliedBoundary.firstSeq, 1);
    store.close();
  });
});

test('the missing columns are decided while the migration holds the write lock', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
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
      VALUES ('kraken_spot', 'book', 'kraken_spot:1', 5, 7, 1);
    `);
    legacy.close();

    // The store watches its own lock: which column set the migration decides on is only safe if that question is
    // asked while nothing else can be writing (§6.2).
    let inWriteLock = false;
    const reads = [];
    class LockWatching extends DatabaseSync {
      exec(sql, ...rest) {
        const result = super.exec(sql, ...rest);
        if (/BEGIN IMMEDIATE/i.test(String(sql))) inWriteLock = true;
        if (/\b(COMMIT|ROLLBACK)\b/i.test(String(sql))) inWriteLock = false;
        return result;
      }

      prepare(sql, ...rest) {
        const statement = super.prepare(sql, ...rest);
        if (!/PRAGMA table_info/.test(sql)) return statement;
        return {
          get: (...args) => statement.get(...args),
          all: (...args) => {
            const rows = statement.all(...args);
            reads.push(inWriteLock);
            return rows;
          },
        };
      }
    }

    const store = openDurability({ path: dbPath, runId: 'run-A', Database: LockWatching });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    assert.equal(book.appliedBoundary.connectionId, 'kraken_spot:1', 'the store opened');
    assert.ok(reads.length > 0, 'the columns were read');
    assert.equal(reads.at(-1), true, 'and the set that decided the migration was read with the write lock held');
    store.close();
  });
});

test('a refused declaration still fixes the ceiling, so a later one cannot skip the frame', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('conn-1', { generation: 1, runId: 'run-1' });

    // A frame at 1 declaring origin 3 is refused - and it is still a frame this book has been handed, so the
    // numbering cannot start at 3 whichever way the claim arrives next.
    const refused = book.apply({
      envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: { first_seq: 3 } }),
      changes: [],
    });
    assert.match(refused.reason, /beyond frames that have already arrived/);

    const completed = book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 3 });
    assert.equal(completed.accepted, false, 'the refused declaration is not resurrected as a completion');
    assert.match(completed.reason, /beyond frames that have already arrived/);
    assert.equal(book.appliedBoundary.firstSeq, null);
    assert.equal(
      book.apply({ envelope: envelope(3, 'conn-1', 1, 'run-1', { meta: undefined }), changes: [] }).reason,
      'first sequence unknown',
      'and nothing is applied above the frame that arrived',
    );

    // The frame that arrived is still applicable once an origin that fits it says so.
    assert.equal(book.accept('conn-1', { generation: 1, runId: 'run-1', firstSeq: 1 }).accepted, true);
    assert.equal(
      book.apply({ envelope: envelope(1, 'conn-1', 1, 'run-1', { meta: undefined }), changes: [] }).applied,
      true,
    );
    store.close();
  });
});

test('a board whose owner has not been established takes no frames at all', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');
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
      VALUES ('kraken_spot', 'book', 'legacy', 5, 7, 1);
    `);
    legacy.close();

    const store = openDurability({ path: dbPath, runId: 'run-A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });

    // The row predates the ownership columns, so nobody has said who owns this board yet. A frame that happens to
    // match its connection id and generation would otherwise continue a numbering on behalf of nobody.
    const early = book.apply({
      envelope: envelope(8, 'legacy', 5, null),
      changes: [{ side: 'bid', price: 100, size: 1 }],
    });
    assert.equal(early.applied, false);
    assert.match(early.reason, /has not been established/);
    assert.equal(book.board.size('bid', 100), null, 'nothing reached the board');
    // A-1: this store predates the board-side anchor, so its applied position was rebuilt at open - not
    // copied from the boundary, which would bless a position the board was never checked against. The
    // refused frame must still leave the rebuilt position exactly where the migration left it.
    assert.equal(
      internalsOf(store).db
        .prepare('SELECT up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?')
        .get('kraken_spot', 'book').up_to_receive_seq,
      null,
      'and the rebuilt position in the store did not move',
    );

    // Once the owner is established - the one accept that is allowed to say who it is - the same frame applies.
    assert.equal(book.accept('legacy', { generation: 5, runId: null, firstSeq: 8 }).accepted, true);
    assert.equal(
      book.apply({ envelope: envelope(8, 'legacy', 5, null), changes: [{ side: 'bid', price: 100, size: 1 }] })
        .applied,
      true,
    );
    assert.equal(book.board.size('bid', 100), 1);
    store.close();
  });
});

test('an old connection name is not re-used for another identity, and its frames are refused', async () => {
  await withBook(async (dir) => {
    const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'A' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    assert.equal(book.accept('A:v:m:1', { runId: 'A', generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(
      book.apply({ envelope: envelope(1, 'A:v:m:1', 1, 'A'), changes: [{ side: 'bid', price: 100, size: 1 }] })
        .applied,
      true,
    );

    // A newer generation takes the board over under a name of its own - which is what a reconnect looks like.
    assert.equal(book.accept('A:v:m:2', { runId: 'A', generation: 2, firstSeq: 1 }).accepted, true);

    // And now the old name comes back claiming a third generation. A name is what frames are deduped by, so
    // re-using it for another identity would apply one connection's frames to another's board while the raw
    // still holds the first one's data under that name.
    const reused = book.accept('A:v:m:1', { runId: 'A', generation: 3, firstSeq: 1 });
    assert.equal(reused.accepted, false);
    assert.match(reused.reason, /already been used for another identity/);
    assert.equal(book.appliedBoundary.connectionId, 'A:v:m:2', 'the board still belongs to the current name');

    // A frame carrying the old name is refused as not accepted, with a different raw of its own.
    const stale = book.apply({
      envelope: envelope(1, 'A:v:m:1', 3, 'A', { raw: '{"seq":1,"from":"the old name"}' }),
      changes: [{ side: 'bid', price: 200, size: 9 }],
    });
    assert.equal(stale.applied, false);
    assert.match(stale.reason, /connection not accepted/);
    assert.equal(book.board.size('bid', 200), null, 'and nothing of it reached the board');
    store.close();
  });
});

test('an owner without a run name is replaced for good, and does not come back after a restart', async () => {
  await withBook(async (dir) => {
    const dbPath = join(dir, 'state.sqlite');

    // A board this build recorded with no run at all: an owner, just not a named one.
    const seeding = openDurability({ path: dbPath, runId: 'writer' });
    const board = openBook({ market: 'kraken_spot', stream: 'book', durability: seeding });
    assert.equal(board.accept('null:1', { runId: null, generation: 1, firstSeq: 1 }).accepted, true);
    assert.equal(
      board.apply({ envelope: envelope(1, 'null:1', 1, null), changes: [{ side: 'bid', price: 100, size: 1 }] })
        .applied,
      true,
    );

    // A named run takes it over: the owner it replaced has no name to record, and that is what the empty
    // name in the retirement table stands for.
    assert.equal(
      board.accept('B:v:m:1', { runId: 'B', generation: 1, firstSeq: 1, takeover: true }).accepted,
      true,
    );
    assert.deepEqual(internalsOf(seeding).db.prepare('SELECT run_id FROM retired_run').all().map((r) => r.run_id), ['']);
    seeding.close();

    const store = openDurability({ path: dbPath, runId: 'writer-2' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    assert.deepEqual(reopened.retiredRuns(), [null], 'the retirement survived the restart');

    // The owner without a run name asks for the board back, with a takeover and a bigger number. A replaced
    // owner does not return: the data it already had organized would otherwise be applied a second time.
    const returned = reopened.accept('null:2', { runId: null, generation: 2, firstSeq: 1, takeover: true });
    assert.equal(returned.accepted, false);
    assert.match(returned.reason, /already replaced/);
    assert.equal(reopened.appliedBoundary.runId, 'B', 'the board is still the named run\'s');
    assert.equal(reopened.appliedBoundary.connectionId, 'B:v:m:1');
    assert.equal(
      reopened.apply({ envelope: envelope(1, 'null:2', 2, null), changes: [{ side: 'bid', price: 200, size: 9 }] })
        .applied,
      false,
      'and its frames are not taken',
    );
    assert.equal(reopened.board.size('bid', 200), null, 'nothing of the old owner reached the board');
    assert.equal(reopened.board.size('bid', 100), 1, 'while what the named run inherited is still there');
    store.close();
  });
});

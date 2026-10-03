/**
 * A-1: the board-side anchor.
 *
 * §9.4(1) asks the board to be compared against the record at startup, so that a record that ran ahead
 * of the board - "applied" while nothing was applied - is a contract violation rather than a resumption
 * that silently drops every resend as `already applied`. The anchor is the board's own position, written
 * in the same transaction as the levels and the boundary; opening compares the two and refuses on a
 * disagreement or a missing side.
 *
 * These tests drive the refusal through whatever entry they can reach: `openBook` directly, and the
 * `structure` that wraps it. The tamper is done the way a crash or a bad edit would - on the database
 * itself, after the store was closed - so nothing here is a test-only route in the product code.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { openBook } from '../src/book/state.mjs';
import { openDurability } from '../src/durability.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'anchor-'));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

const envelope = (seq, connectionId = 'run-1:kraken:kraken_spot:1', generation = 1, runId = 'run-1') =>
  makeEnvelope({
    market: 'kraken_spot',
    stream: 'book',
    connectionId,
    runId,
    venue: 'kraken',
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000 + seq,
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

/** Seed a healthy board, then close the store so the file can be tampered with directly. */
function seed(dir) {
  const path = join(dir, 'state.sqlite');
  const store = openDurability({ path, runId: 'run-1' });
  const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
  book.accept('run-1:kraken:kraken_spot:1', { generation: 1, runId: 'run-1', firstSeq: 1 });
  book.apply({ envelope: envelope(1), changes: [{ side: 'bid', price: 100, size: 1 }] });
  store.close();
  return path;
}

function openExpectingRefusal(dir, path, mutate, { onDiagnostic } = {}) {
  const raw = new DatabaseSync(path);
  mutate(raw);
  raw.close();
  const store = openDurability({ path, runId: 'run-2' });
  let thrown = null;
  try {
    openBook({ market: 'kraken_spot', stream: 'book', durability: store, onDiagnostic });
  } catch (error) {
    thrown = error;
  } finally {
    store.close();
  }
  return thrown;
}

test('a record that ran ahead of the board is refused, with a diagnostic', () => {
  withDir((dir) => {
    const path = seed(dir);
    const diagnostics = [];
    const thrown = openExpectingRefusal(
      dir,
      path,
      (raw) => {
        raw.exec('UPDATE applied_boundary SET up_to_receive_seq = 100');
        raw.exec('DELETE FROM book_level');
      },
      { onDiagnostic: (diagnostic) => diagnostics.push(diagnostic) },
    );

    assert.ok(thrown, 'opening a store whose record leads its board refuses');
    assert.equal(thrown.code, 'BOARD_ANCHOR_MISMATCH', 'and refuses for the anchor, not by accident');
    assert.match(thrown.message, /board-side anchor/);
    assert.equal(diagnostics.length, 1, 'the refusal left one diagnostic behind');
    assert.match(diagnostics[0].reason, /refusing to open the board/);
    assert.match(diagnostics[0].reason, /up_to_receive_seq=1/);
    assert.match(diagnostics[0].reason, /100/);
  });
});

test('a board that lost its anchor, or a boundary that lost its record, is refused the same way', () => {
  withDir((dir) => {
    const missingAnchor = seed(dir);
    const thrownAnchor = openExpectingRefusal(dir, missingAnchor, (raw) => {
      raw.exec('DELETE FROM board_anchor');
    });
    assert.ok(thrownAnchor, 'a boundary with no anchor is refused');
    assert.equal(thrownAnchor.code, 'BOARD_ANCHOR_MISMATCH');
    assert.match(thrownAnchor.message, /anchor does not/);
  });

  withDir((dir) => {
    const missingBoundary = seed(dir);
    const thrownBoundary = openExpectingRefusal(dir, missingBoundary, (raw) => {
      raw.exec('DELETE FROM applied_boundary');
    });
    assert.ok(thrownBoundary, 'an anchor with no boundary is refused');
    assert.equal(thrownBoundary.code, 'BOARD_ANCHOR_MISMATCH');
    assert.match(thrownBoundary.message, /boundary record does not/);
  });
});

test('a healthy store reopens with its anchor and position intact', () => {
  withDir((dir) => {
    const path = seed(dir);
    const store = openDurability({ path, runId: 'run-2' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    assert.equal(book.appliedBoundary.upToSeq, 1, 'the position came back');
    assert.equal(book.board.size('bid', 100), 1, 'and so did the board');
    const anchor = internalsOf(store).db
      .prepare('SELECT up_to_receive_seq FROM board_anchor WHERE market = ? AND stream = ?')
      .get('kraken_spot', 'book');
    assert.equal(anchor.up_to_receive_seq, 1, 'and the board-side anchor agrees with the record');
    store.close();
  });
});

test('a legitimately empty board with a matching anchor reopens normally', () => {
  withDir((dir) => {
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    const book = openBook({ market: 'kraken_spot', stream: 'book', durability: store });
    book.accept('run-1:kraken:kraken_spot:1', { generation: 1, runId: 'run-1', firstSeq: 1 });
    book.apply({ envelope: envelope(1), changes: [{ side: 'bid', price: 100, size: 1 }] });
    // A size-0 change empties the board legitimately: the position moved, the board did not. This is
    // exactly the case a "position > 0 but the board is empty" heuristic would misread, which is why the
    // comparison is anchor-against-record and never record-against-board-contents.
    book.apply({ envelope: envelope(2), changes: [{ side: 'bid', price: 100, size: 0 }] });
    assert.equal(book.board.depth, 0, 'the board is empty');
    store.close();

    const second = openDurability({ path, runId: 'run-2' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.equal(reopened.appliedBoundary.upToSeq, 2, 'the position is preserved');
    assert.equal(reopened.board.depth, 0, 'and an empty board is not mistaken for a violation');
    second.close();
  });
});

test('a store that predates the anchor is rebuilt, not copied, and says so', () => {
  withDir((dir) => {
    const path = join(dir, 'state.sqlite');
    // A store written before the board-side anchor existed: an applied position and a board, no anchor.
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE applied_boundary (
        market TEXT NOT NULL,
        stream TEXT NOT NULL,
        connection_id TEXT NOT NULL,
        generation INTEGER,
        up_to_receive_seq INTEGER,
        run_id TEXT,
        first_seq INTEGER,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY (market, stream)
      );
      INSERT INTO applied_boundary
        (market, stream, connection_id, generation, up_to_receive_seq, run_id, first_seq, updated_at_ms)
      VALUES ('kraken_spot', 'book', 'run-1:kraken:kraken_spot:1', 1, 7, 'run-1', 1, 1);
      CREATE TABLE book_level (
        market TEXT NOT NULL, stream TEXT NOT NULL, side TEXT NOT NULL, price REAL NOT NULL,
        size REAL NOT NULL, PRIMARY KEY (market, stream, side, price)
      );
      INSERT INTO book_level (market, stream, side, price, size) VALUES ('kraken_spot', 'book', 'bid', 99, 3);
    `);
    legacy.close();

    const diagnostics = [];
    const store = openDurability({ path, runId: 'run-2' });
    const book = openBook({
      market: 'kraken_spot',
      stream: 'book',
      durability: store,
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    // The position is rebuilt to "nothing applied yet": an unverified position is not blessed by copying
    // it into an anchor. The board's levels are kept (they are facts) and the ownership facts are kept
    // (they are established by the column migration, not by the position).
    assert.equal(book.appliedBoundary.upToSeq, null, 'the unverified position was rebuilt, not copied');
    assert.equal(book.appliedBoundary.firstSeq, 1, 'the origin is kept - it is an ownership fact');
    assert.equal(book.board.size('bid', 99), 3, 'the board is kept');
    assert.equal(diagnostics.some((d) => /board anchor rebuilt/.test(d.reason)), true, 'and the rebuild is reported');
    const anchor = internalsOf(store).db
      .prepare('SELECT up_to_receive_seq FROM board_anchor WHERE market = ? AND stream = ?')
      .get('kraken_spot', 'book');
    assert.equal(anchor.up_to_receive_seq, null, 'the anchor was written to match the rebuilt position');
    store.close();

    // The second open is a normal one: the anchor now exists and agrees with the record.
    const second = openDurability({ path, runId: 'run-3' });
    const reopened = openBook({ market: 'kraken_spot', stream: 'book', durability: second });
    assert.equal(reopened.appliedBoundary.upToSeq, null, 'the rebuilt position is stable across restarts');
    second.close();
  });
});

test('the structure refuses to build on a tampered store, and reports why', () => {
  withDir((dir) => {
    const path = seed(dir);
    const raw = new DatabaseSync(path);
    raw.exec('UPDATE applied_boundary SET up_to_receive_seq = 100');
    raw.exec('DELETE FROM book_level');
    raw.close();

    const diagnostics = [];
    const store = openDurability({ path, runId: 'run-2' });
    let thrown = null;
    try {
      createStructure({
        market: 'kraken_spot',
        stream: 'book',
        adapter: {
          url: 'ws://venue.test/ws',
          stream: 'book',
          parse: () => ({ kind: 'data' }),
          changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
        },
        durability: store,
        runId: 'run-2',
        venue: 'kraken',
        webSocketImpl: () => {
          throw new Error('no socket');
        },
        deferRecovery: true,
        onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
      });
    } catch (error) {
      thrown = error;
    }
    assert.ok(thrown, 'the structure did not build over the tampered store');
    assert.equal(thrown.code, 'BOARD_ANCHOR_MISMATCH');
    assert.equal(diagnostics.length, 1, 'the refusal reached the caller as a diagnostic');
    store.close();
  });
});

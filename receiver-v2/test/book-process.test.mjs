/**
 * Stage 4: the book as its own process, speaking to a fake organize peer over the real IPC transport.
 *
 * These tests drive `src/book/main.mjs` - the book process entrance - against a test-support peer that
 * listens on a unix socket with `src/ipc.mjs`. Nothing here reaches into product code through a
 * test-only route: frames arrive at a fake socket, control messages travel on the stage-1 vocabulary,
 * and the book's store writes to a real file.
 *
 * What each group fixes (the task's ①-⑥):
 *   ① C6/C7: the boundary proof and hole detection keep their meaning across the wire.
 *   ② the applied boundary, the levels and the board-side anchor are written in one transaction.
 *   ③ §9.4(1): a store whose boundary leads its board-side anchor is refused when the book opens.
 *   ④ an invalidation stops serving and is persisted before it is answered; a duplicate is a no-op; a
 *      confirmed loss may not be cancelled; a loss of another connection does not stop the board.
 *   ⑤ the accept verdict is the book's: owner, generation, explicit takeover and old-instance refusal.
 *   ⑥ an applied boundary is announced with `applied_ack`.
 *
 * The ownership group fixes that the book's store carries only the book's tables - neither organize's
 * nor ingest's.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { makeEnvelope } from '../src/envelope.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { CHANGES_FORMAT } from '../src/changes.mjs';
import { createBookProcess, openBookProcess } from '../src/book/main.mjs';
import { startFakeOrganizeForBook } from '../test-support/fake-organize-for-book.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';

const change = (seq) => ({ side: 'bid', price: 100 + seq, size: seq });

// A frame carries its own derived level changes (ruling ③): ingest computes them and writes them into
// the envelope's meta, so the book reads them off the frame rather than asking an adapter.
const envelope = (seq, { connectionId = 'conn-1', runId = 'run-1', generation = 1, venueSeq = seq } = {}) =>
  makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId,
    runId,
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq},"venue_seq":${venueSeq}}`,
    meta: {
      first_seq: 1,
      venue_seq: venueSeq,
      changes_format: CHANGES_FORMAT,
      changes: { replace: false, changes: [change(seq)] },
    },
  });

const sequenceAdapter = (connects) => ({
  boundary: 'sequence',
  connects: connects ?? (({ previous, current, replace }) => replace === true || previous === null || current.meta.venue_seq === previous.meta.venue_seq + 1),
});

async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

/** A channel that records what would have gone on the wire, for tests that drive the process directly. */
function memoryChannel() {
  return {
    sent: [],
    sendControl(message) {
      this.sent.push(message);
      return true;
    },
    sendEnvelope() {
      return true;
    },
    close() {},
  };
}

function acceptMessage({ connectionId = 'conn-1', generation = 1, firstSeq = 1, runId = 'run-1', takeover = false, requestId = 'req-accept' } = {}) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'accept',
    role_instance: 'organize-1',
    request_id: requestId,
    run_id: runId,
    connection_id: connectionId,
    generation,
    payload: { first_seq: firstSeq, takeover },
  });
}

function invalidateMessage({ requestId = 'inv-1', connectionId = 'conn-1', generation = 1, runId = 'run-1', from = 1, to = 2, revision = 1, reason = 'a range must be declared missing' } = {}) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'invalidate',
    role_instance: 'organize-1',
    request_id: requestId,
    run_id: runId,
    connection_id: connectionId,
    generation,
    payload: { from, to, revision, reason },
  });
}

/** A whole world: a fresh directory, a fake organize peer listening, and one book process connected. */
async function setup(options = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'book-proc-'));
  const socketPath = join(dir, 'organize.sock');
  const storePath = options.storePath ?? join(dir, 'book.sqlite');
  const diagnostics = [];
  const organize = await startFakeOrganizeForBook(socketPath, {
    market: MARKET,
    stream: STREAM,
    runId: options.runId ?? 'run-1',
    batchFrames: 1,
  });
  const book = await openBookProcess({
    organizeSocketPath: socketPath,
    market: MARKET,
    stream: STREAM,
    runId: options.runId ?? 'run-1',
    roleInstance: 'book-1',
    adapter: options.adapter ?? null,
    changesFor: options.changesFor ?? ((envelopeIn) => [change(envelopeIn.receive_seq)]),
    storePath,
    channelOptions: { batchFrames: 1 },
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
  });
  const teardown = async () => {
    book.close();
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  };
  return { dir, socketPath, storePath, organize, book, diagnostics, teardown };
}

async function ready(h) {
  await until(() => h.organize.channel !== null);
  h.organize.sendHello();
}

// ---------------------------------------------------------------------------------------------------
// ① C6/C7: the boundary proof and hole detection keep their meaning across the wire
// ---------------------------------------------------------------------------------------------------

test('① C6: a frame the connection rule refuses breaks the proof, and the board stops serving', async () => {
  const h = await setup({ adapter: sequenceAdapter() });
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    assert.equal(h.organize.state.accepted[0].payload.accepted, true, 'the book adopts the connection');

    for (const seq of [1, 2]) h.organize.sendFrame(envelope(seq));
    await until(() => h.organize.state.appliedAcks.length === 2);
    assert.equal(h.book.isRunning, true);
    assert.equal(h.book.proveBoundary().proven, true, 'the rule held over the range');

    // The frame at 3 does not connect to what came before: it must be judged and refused, and write
    // nothing - the proof, the position and the board all stay where they were.
    h.organize.sendFrame(envelope(3, { venueSeq: 9 }));
    await until(() => h.book.phase === 'syncing');
    assert.equal(h.book.appliedBoundary.upToSeq, 2, 'the position did not move over the refused frame');
    assert.equal(h.organize.state.appliedAcks.length, 2, 'no acknowledgement was sent for it');
    assert.equal(h.book.proveBoundary().proven, false, 'a broken proof is not a boundary');
    assert.equal(h.book.board.size('bid', 103), null, 'nor did the level it carried land');

    // The next ordinary frame is still refused while the proof is broken.
    h.organize.sendFrame(envelope(4, { venueSeq: 3 }));
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(h.book.appliedBoundary.upToSeq, 2);
    assert.equal(h.organize.state.appliedAcks.length, 2);
  } finally {
    await h.teardown();
  }
});

test('① C7: a hole seen across the wire takes the board out of service, and filling it restores it', async () => {
  const h = await setup({ adapter: sequenceAdapter() });
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    for (const seq of [1, 2]) h.organize.sendFrame(envelope(seq));
    await until(() => h.organize.state.appliedAcks.length === 2);
    assert.equal(h.book.isRunning, true);

    // Frame 4 arrives before 3: it is held, the hole is recorded, and the board stops serving.
    h.organize.sendFrame(envelope(4));
    await until(() => h.book.phase === 'syncing');
    assert.equal(h.book.openGaps().length, 1, 'the hole is a recorded fact');
    assert.equal(h.organize.state.appliedAcks.length, 2, 'the held frame was not acknowledged');

    // The frame that fills the hole applies, the held one follows it (drained in the same act), and the
    // board serves again. The acknowledgement carries the ceiling the two frames reached together.
    h.organize.sendFrame(envelope(3));
    await until(() => h.organize.state.appliedAcks.length === 3);
    assert.equal(h.organize.state.appliedAcks.at(-1).payload.up_to_seq, 4, 'the position is whole again');
    assert.equal(h.book.appliedBoundary.upToSeq, 4);
    assert.equal(h.book.isRunning, true, 'the board serves again without anyone asking');
    assert.deepEqual(h.book.openGaps(), []);
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ② applied_boundary + levels + board anchor are one transaction
// ---------------------------------------------------------------------------------------------------

test('② the applied boundary, the levels and the board-side anchor are committed together', async () => {
  const h = await setup();
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    h.organize.sendFrame(envelope(1));
    h.organize.sendFrame(envelope(2));
    await until(() => h.organize.state.appliedAcks.length === 2);

    const db = new DatabaseSync(h.storePath);
    const levels = db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n;
    const boundary = db.prepare('SELECT up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?').get(MARKET, STREAM);
    const anchor = db.prepare('SELECT up_to_receive_seq FROM board_anchor WHERE market = ? AND stream = ?').get(MARKET, STREAM);
    db.close();
    assert.equal(levels, 2, 'the levels are committed');
    assert.equal(boundary.up_to_receive_seq, 2, 'so is the applied boundary');
    assert.equal(anchor.up_to_receive_seq, 2, 'and the board-side anchor moved with them');
  } finally {
    await h.teardown();
  }
});

test('② a failure while writing the anchor rolls the levels and the two records back together', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'book-atomic-'));
  const storePath = join(dir, 'book.sqlite');
  try {
    let calls = 0;
    let throwAt = Infinity;
    const nowMs = () => {
      calls += 1;
      if (calls === throwAt) throw new Error('injected clock failure');
      return 1_000_000 + calls;
    };
    const process = createBookProcess({
      market: MARKET,
      stream: STREAM,
      runId: 'run-1',
      storePath,
      nowMs,
      changesFor: (envelopeIn) => [change(envelopeIn.receive_seq)],
    });
    const channel = memoryChannel();
    process.handleControl(acceptMessage(), channel);
    // The accept wrote the boundary and the anchor (two clock calls). The apply writes the boundary and
    // then the anchor: armed to fail on the anchor, so the whole commit must go back.
    throwAt = calls + 2;
    assert.throws(() => process.handleEnvelope(envelope(1), channel), /injected clock failure/);

    const db = new DatabaseSync(storePath);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM book_level').get().n, 0, 'the levels did not survive the rollback');
    // The accept had written a boundary row and an anchor at a null position; the failed apply's writes
    // (levels + boundary + anchor) all went back, so the position is still null on both sides.
    const boundary = db.prepare('SELECT up_to_receive_seq FROM applied_boundary WHERE market = ? AND stream = ?').get(MARKET, STREAM);
    const anchor = db.prepare('SELECT up_to_receive_seq FROM board_anchor WHERE market = ? AND stream = ?').get(MARKET, STREAM);
    db.close();
    assert.equal(boundary.up_to_receive_seq, null, 'the applied boundary did not move');
    assert.equal(anchor.up_to_receive_seq, null, 'nor the board-side anchor');
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ③ §9.4(1): record-first detection on the book side
// ---------------------------------------------------------------------------------------------------

test('③ a store whose boundary leads its board-side anchor is refused when the book opens', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'book-record-first-'));
  const storePath = join(dir, 'book.sqlite');
  try {
    const first = createBookProcess({
      market: MARKET,
      stream: STREAM,
      runId: 'run-1',
      storePath,
      changesFor: (envelopeIn) => [change(envelopeIn.receive_seq)],
    });
    const channel = memoryChannel();
    first.handleControl(acceptMessage(), channel);
    first.handleEnvelope(envelope(1), channel);
    first.close();

    // Tamper: the boundary is made to lead the board and its anchor, exactly the loss the comparison
    // exists to catch. The book must refuse to open rather than adopt a position the board never had.
    const db = new DatabaseSync(storePath);
    db.prepare('UPDATE applied_boundary SET up_to_receive_seq = 999 WHERE market = ? AND stream = ?').run(MARKET, STREAM);
    db.close();

    assert.throws(
      () => createBookProcess({ market: MARKET, stream: STREAM, runId: 'run-1', storePath }),
      (error) => {
        assert.equal(error.code, 'BOARD_ANCHOR_MISMATCH');
        assert.match(String(error.message), /anchor and the applied boundary disagree/);
        return true;
      },
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ④ the invalidation: persist before answering, duplicate no-op, no cancellation, other connections
// ---------------------------------------------------------------------------------------------------

test('④ an invalidation stops serving and is persisted before it is answered, and a duplicate is a no-op', async () => {
  const h = await setup({ adapter: sequenceAdapter() });
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    for (const seq of [1, 2]) h.organize.sendFrame(envelope(seq));
    await until(() => h.organize.state.appliedAcks.length === 2);
    assert.equal(h.book.isRunning, true);

    h.organize.sendInvalidate({ requestId: 'inv-1', connectionId: 'conn-1', from: 1, to: 2, revision: 1, reason: 'a hole must be declared missing' });
    await until(() => h.organize.state.invalidated.length === 1);
    const answer = h.organize.state.invalidated[0];
    assert.equal(answer.request_id, 'inv-1', 'the response answers the request');
    assert.equal(answer.payload.revision, 1);
    assert.equal(answer.payload.noop, false);
    assert.equal(h.book.isRunning, false, 'serving stopped for the connection the board follows');
    assert.equal(h.book.proveBoundary().proven, false, 'the proof was dropped, not only the phase');

    // Both records were committed together: the invalidation with its revision, and the missing record.
    const db = new DatabaseSync(h.storePath);
    const invalidation = db.prepare('SELECT * FROM book_invalidation WHERE request_id = ?').get('inv-1');
    const missing = db.prepare('SELECT COUNT(*) AS n FROM book_missing_record WHERE connection_id = ?').get('conn-1').n;
    db.close();
    assert.equal(invalidation.state, 'invalidated');
    assert.equal(invalidation.revision, 1);
    assert.equal(missing, 1, 'the missing record that stops the proof is written in the same transaction');
    assert.deepEqual(h.book.invalidationRecords().map((r) => r.requestId), ['inv-1']);

    // A duplicate request is a no-op: the same answer, and nothing moves.
    h.organize.sendInvalidate({ requestId: 'inv-1', connectionId: 'conn-1', from: 1, to: 2, revision: 1 });
    await until(() => h.organize.state.invalidated.length === 2);
    assert.equal(h.organize.state.invalidated[1].payload.noop, true, 'the duplicate is reported as a no-op');
    assert.equal(h.book.invalidationRecords().length, 1, 'and nothing was recorded twice');

    // A confirmed loss may not be taken back (the book refuses compensation as well as organize).
    const refused = h.book.cancelInvalidation('inv-1');
    assert.equal(refused.cancelled, false);
    assert.equal(refused.refused, true, 'a confirmed missing may not be cancelled');
    assert.equal(h.book.cancelInvalidation('no-such-request').cancelled, false);
  } finally {
    await h.teardown();
  }
});

test('④ a loss of another connection is history and does not stop the board', async () => {
  const h = await setup();
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    h.organize.sendFrame(envelope(1));
    await until(() => h.organize.state.appliedAcks.length === 1);
    h.book.proveBoundary(); // the venue has no means of proof, but it may still run
    assert.equal(h.book.isRunning, true);

    h.organize.sendInvalidate({ requestId: 'inv-other', connectionId: 'conn-2', from: 1, to: 1, revision: 7 });
    await until(() => h.organize.state.invalidated.length === 1);
    assert.equal(h.book.isRunning, true, 'C11: another connection\u2019s loss does not stop the board');
    assert.equal(h.book.invalidationRecords()[0].revision, 7, 'but the loss is recorded');
  } finally {
    await h.teardown();
  }
});

test('④ a stored invalidation keeps the board unproven across a restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'book-inv-restart-'));
  const storePath = join(dir, 'book.sqlite');
  try {
    const first = createBookProcess({
      market: MARKET,
      stream: STREAM,
      runId: 'run-1',
      storePath,
      changesFor: (envelopeIn) => [change(envelopeIn.receive_seq)],
    });
    const channel = memoryChannel();
    first.handleControl(acceptMessage(), channel);
    first.handleEnvelope(envelope(1), channel);
    first.handleEnvelope(envelope(2), channel);
    assert.equal(first.isRunning, true);

    const outcome = first.handleControl(invalidateMessage({ requestId: 'inv-1', connectionId: 'conn-1' }), channel);
    assert.equal(outcome.invalidated, true);
    first.close();

    const second = createBookProcess({ market: MARKET, stream: STREAM, runId: 'run-1', storePath });
    assert.equal(second.isRunning, false, 'a stored missing record keeps the board unproven');
    const proof = second.proveBoundary();
    assert.equal(proof.proven, false);
    assert.match(proof.reason, /proof is broken/);
    second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑤ the accept verdict is the book's (owner / generation / explicit takeover / old instance)
// ---------------------------------------------------------------------------------------------------

test('⑤ the accept verdict is the book\u2019s: owner, generation, explicit takeover and old-instance refusal', async () => {
  const h = await setup();
  try {
    await ready(h);

    // The first connection takes the board.
    h.organize.sendAccept({ connectionId: 'conn-1', generation: 1 });
    await until(() => h.organize.state.accepted.length === 1);
    assert.equal(h.organize.state.accepted[0].payload.accepted, true);
    assert.match(h.organize.state.accepted[0].payload.reason, /first connection/);

    // The same run and connection again is a reopen: accepted, unconditionally.
    h.organize.sendAccept({ connectionId: 'conn-1', generation: 1 });
    await until(() => h.organize.state.accepted.length === 2);
    assert.equal(h.organize.state.accepted[1].payload.accepted, true);

    // A name that disagrees with the identity that carried it is refused, whoever announces it.
    h.organize.sendAccept({ connectionId: 'conn-1', generation: 5, runId: 'run-1' });
    await until(() => h.organize.state.accepted.length === 3);
    assert.equal(h.organize.state.accepted[2].payload.accepted, false, 'a connection id may not disagree with its identity');

    // A different run without an explicit takeover is refused: a bigger number is not a takeover.
    h.organize.sendAccept({ connectionId: 'conn-2', generation: 9, runId: 'run-2' });
    await until(() => h.organize.state.accepted.length === 4);
    assert.equal(h.organize.state.accepted[3].payload.accepted, false);
    assert.match(h.organize.state.accepted[3].payload.reason, /explicit takeover/);

    // The same run-2 with an explicit takeover is admitted, and run-1 is retired.
    h.organize.sendAccept({ connectionId: 'conn-2', generation: 1, runId: 'run-2', takeover: true });
    await until(() => h.organize.state.accepted.length === 5);
    assert.equal(h.organize.state.accepted[4].payload.accepted, true);
    assert.match(h.organize.state.accepted[4].payload.reason, /took the board over/);
    assert.deepEqual(h.book.retiredRuns(), ['run-1'], 'the replaced run is retired');

    // run-1 may not come back, whatever generation it quotes.
    h.organize.sendAccept({ connectionId: 'conn-1', generation: 9, runId: 'run-1' });
    await until(() => h.organize.state.accepted.length === 6);
    assert.equal(h.organize.state.accepted[5].payload.accepted, false);
    assert.match(h.organize.state.accepted[5].payload.reason, /already replaced/);

    // An old instance of the same run (a generation that is not strictly newer) is refused.
    h.organize.sendAccept({ connectionId: 'conn-3', generation: 0, runId: 'run-2' });
    await until(() => h.organize.state.accepted.length === 7);
    assert.equal(h.organize.state.accepted[6].payload.accepted, false);
    assert.match(h.organize.state.accepted[6].payload.reason, /superseded/);

    // A strictly newer generation of the same run replaces the old connection.
    h.organize.sendAccept({ connectionId: 'conn-3', generation: 2, runId: 'run-2' });
    await until(() => h.organize.state.accepted.length === 8);
    assert.equal(h.organize.state.accepted[7].payload.accepted, true);
    assert.match(h.organize.state.accepted[7].payload.reason, /newer generation/);
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑥ applied_ack
// ---------------------------------------------------------------------------------------------------

test('⑥ an applied boundary is announced with applied_ack', async () => {
  const h = await setup();
  try {
    await ready(h);
    h.organize.sendAccept({ connectionId: 'conn-1' });
    await until(() => h.organize.state.accepted.length === 1);
    h.organize.sendFrame(envelope(1));
    await until(() => h.organize.state.appliedAcks.length === 1);
    const ack = h.organize.state.appliedAcks[0];
    assert.equal(ack.payload.up_to_seq, 1, 'the acknowledgement carries the applied boundary');
    assert.equal(ack.connection_id, 'conn-1');
    assert.equal(ack.generation, 1);
    assert.equal(ack.run_id, 'run-1');

    // A frame that is not applied is not acknowledged.
    h.organize.sendFrame(envelope(3, { venueSeq: 3 }));
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(h.organize.state.appliedAcks.length, 1, 'a held frame produced no acknowledgement');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// Ownership: the book owns its tables, and neither organize's nor ingest's
// ---------------------------------------------------------------------------------------------------

test('③ a frame with no level-changes block is refused rather than applied as empty', () => {
  const dir = mkdtempSync(join(tmpdir(), 'book-no-changes-'));
  try {
    const process = createBookProcess({ market: MARKET, stream: STREAM, runId: 'run-1', storePath: join(dir, 'book.sqlite') });
    const channel = memoryChannel();
    process.handleControl(acceptMessage(), channel);

    const bare = makeEnvelope({
      market: MARKET,
      stream: STREAM,
      connectionId: 'conn-1',
      runId: 'run-1',
      generation: 1,
      receiveSeq: 1,
      recvTsMs: 1_792_000_000_001,
      recvMonoNs: 1_000_000_001,
      raw: '{"seq":1}',
      meta: { first_seq: 1 },
    });
    const result = process.handleEnvelope(bare, channel);
    assert.equal(result.applied, false, 'the frame is refused, not applied');
    assert.match(String(result.reason), /level changes were refused/);
    assert.equal(process.board.size('bid', 101), null, 'nothing landed on the board');
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the book owns its tables: the store has no organize table and no received_tail or spool', () => {
  const dir = mkdtempSync(join(tmpdir(), 'book-own-'));
  try {
    const storePath = join(dir, 'book.sqlite');
    const process = createBookProcess({ market: MARKET, stream: STREAM, runId: 'run-1', storePath });
    process.close();
    const db = new DatabaseSync(storePath);
    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all()
      .map((row) => row.name);
    db.close();

    for (const owned of [
      'applied_boundary',
      'book_level',
      'board_anchor',
      'book_missing_record',
      'book_gap',
      'retired_run',
      'connection_identity',
      'legacy_owner',
      'book_invalidation',
    ]) {
      assert.ok(tables.includes(owned), `the book owns ${owned}`);
    }
    for (const foreign of [
      'run_marker',
      'pending_boundary',
      'suspected_gap',
      'organized_watermark',
      'organize_gap',
      'delivery_ledger',
      'received_tail',
      'invalidation_request',
    ]) {
      assert.equal(tables.includes(foreign), false, `${foreign} belongs to another role, not the book`);
    }
    assert.equal(tables.some((name) => name.includes('spool')), false, 'the spool belongs to ingest');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

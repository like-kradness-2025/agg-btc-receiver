/**
 * Stage 3: organization as its own process, speaking to a fake ingest and a fake book over the real
 * IPC transport.
 *
 * These tests drive `src/organize/main.mjs` - the organize process entrance - against test-support
 * peers that connect to its unix socket with `src/ipc.mjs`. Nothing here reaches into product code
 * through a test-only route: frames arrive from a fake ingest, control messages travel on the stage-1
 * vocabulary, and the store writes to a real file.
 *
 * What each group fixes (the task's ①-⑥):
 *   ① a received frame is durable before it is acknowledged: raw write (fsync) -> store transaction ->
 *      durable_ack, in that order.
 *   ② the watermark, the ledger confirmation and the pending boundary are one transaction.
 *   ③ an invalidation request is persisted, a duplicate is a no-op, the book's invalidated response
 *      confirms it, and the outstanding request is re-derived after a restart.
 *   ④ a confirmed missing may not be cancelled.
 *   ⑤ the run marker records running and invalidated, and only writes complete once every sealed tail
 *      is reached.
 *   ⑥ the all-acknowledged judgement is a truth table over the sealed tails and the hole-less ceiling.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { makeEnvelope } from '../src/envelope.mjs';
import { IPC_VERSION, makeMessage } from '../src/ipc-message.mjs';
import { createOrganizeProcess, judgeAllAcked, openOrganizeProcess } from '../src/organize/main.mjs';
import { openOrganizeStore } from '../src/organize/store.mjs';
import { startFakeIngest } from '../test-support/fake-ingest.mjs';
import { startFakeBook } from '../test-support/fake-book.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const VENUE = 'kraken';
const RUN = 'run-1';
const CID = `${RUN}:${VENUE}:${MARKET}:1`;

const envelope = (seq, { connectionId = CID, runId = RUN, venue = VENUE, generation = 1 } = {}) =>
  makeEnvelope({
    market: MARKET,
    stream: STREAM,
    connectionId,
    runId,
    venue,
    generation,
    receiveSeq: seq,
    recvTsMs: 1_792_000_000_000 + seq,
    recvMonoNs: 1_000_000_000 + seq,
    raw: `{"seq":${seq}}`,
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
    envelopes: [],
    sendControl(message) {
      this.sent.push(message);
      return true;
    },
    sendEnvelope(envelopeOut) {
      this.envelopes.push(envelopeOut);
      return true;
    },
    close() {},
  };
}

/**
 * The book's authorization of a connection, which organize adopts (ruling ②): organize no longer
 * decides an accept, it reflects the answer the supervisor relays from the book.
 */
function acceptedMessage({ connectionId = CID, generation = 1, firstSeq = 1, runId = RUN, requestId = 'req-accept' } = {}) {
  return makeMessage({
    version: IPC_VERSION,
    type: 'accepted',
    role_instance: 'book-1',
    request_id: requestId,
    run_id: runId,
    market: MARKET,
    stream: STREAM,
    connection_id: connectionId,
    generation,
    payload: { accepted: true, reason: '', first_seq: firstSeq, takeover: false },
  });
}

/** A whole world: a fresh directory, an organize process listening, and both fake peers connected. */
async function setup(options = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'organize-proc-'));
  const socketPath = join(dir, 'organize.sock');
  const events = [];
  const ackSnapshots = [];
  const missing = [];
  let org;
  org = await openOrganizeProcess({
    listenPath: socketPath,
    market: MARKET,
    stream: STREAM,
    runId: options.runId ?? RUN,
    roleInstance: 'organize-1',
    storePath: options.storePath ?? join(dir, 'organize.sqlite'),
    rawWriter: options.rawWriter ?? ((frame) => {
      events.push(`raw:${frame.receive_seq}`);
      return true;
    }),
    channelOptions: { batchFrames: 1 },
    onAck: (ack) => {
      events.push(`ack:${ack.upToSeq}`);
      try {
        ackSnapshots.push(org.watermarkRows());
      } catch {
        ackSnapshots.push(null);
      }
    },
    onMissing: (record) => missing.push(record),
  });
  const ingest = await startFakeIngest(socketPath, { market: MARKET, stream: STREAM, runId: options.runId ?? RUN });
  const book = await startFakeBook(socketPath, { market: MARKET, stream: STREAM, runId: options.runId ?? RUN });
  const teardown = async () => {
    org.close();
    ingest.close();
    book.close();
    rmSync(dir, { recursive: true, force: true });
  };
  return { dir, socketPath, org, ingest, book, events, ackSnapshots, missing, teardown };
}

// ---------------------------------------------------------------------------------------------------
// Startup recovery: an accepted startup must not cross the old boundary before its applied ACK clears it.
// ---------------------------------------------------------------------------------------------------

test('recovery status remains blocked until the book applied ACK releases the owed ledger row', async () => {
  const h = await setup();
  try {
    h.book.sendAccepted({ requestId: `ingest-1:accept:${CID}:1`, connectionId: CID, generation: 1, firstSeq: 1 });
    await until(() => h.org.acceptedConnectionId === CID);

    h.ingest.sendFrame(envelope(1));
    await until(() => h.book.state.envelopes.length === 1);

    const pending = h.org.recoveryStatus();
    assert.equal(pending.resolved, false, 'a sent frame is still owed until the book confirms application');
    assert.equal(pending.owedCount, 1);

    h.book.sendAppliedAck({ connectionId: CID, generation: 1, upToSeq: 1 });
    await until(() => h.org.recoveryStatus().resolved === true);
    assert.deepEqual(h.org.recoveryStatus(), { resolved: true, owedCount: 0 });
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ① durability before acknowledgement, and the order fsync -> store -> ACK
// ---------------------------------------------------------------------------------------------------

test('① a frame is durable (raw, then the store) before its durable_ack is sent', async () => {
  const h = await setup();
  try {
    // The book authorizes the connection (as the supervisor relays it); organize adopts and confirms.
    h.book.sendAccepted({ requestId: `${'ingest-1'}:accept:${CID}:1`, connectionId: CID, generation: 1, firstSeq: 1 });
    await until(() => h.ingest.state.accepted.length === 1);
    assert.equal(h.ingest.state.accepted[0].payload.accepted, true, 'organize adopts what the book authorized');

    h.ingest.sendFrame(envelope(1));
    await until(() => h.ingest.state.durableAcks.length === 1);
    const ack = h.ingest.state.durableAcks[0];
    assert.equal(ack.payload.up_to_seq, 1, 'the ceiling carries the durable position');
    assert.equal(ack.connection_id, CID);
    assert.equal(ack.generation, 1);

    // The order: the raw write happens before the acknowledgement, and by the time the acknowledgement
    // is sent the store has already committed the watermark - so the ACK never outran the record.
    assert.deepEqual(h.events, ['raw:1', 'ack:1'], 'raw (fsync) before ACK');
    assert.equal(h.ackSnapshots[0][0].upToSeq, 1, 'the watermark was committed before the ACK was sent');

    // The frame was written down as owed, and the pending boundary exists.
    const entries = h.org.ledgerEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].state, 'owed');
    assert.equal(h.org.ledgerEntries({ state: 'owed' }).length, 1, 'the durable frame is still owed');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ② watermark + ledger confirmation are one transaction
// ---------------------------------------------------------------------------------------------------

test('② a commit failure rolls the watermark and ledger confirmation back together', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-atomic-'));
  const failure = { armed: false };
  class FailOneCommit extends DatabaseSync {
    exec(sql) {
      if (failure.armed && sql.trim().toUpperCase() === 'COMMIT') {
        failure.armed = false;
        throw new Error('injected organizer commit failure');
      }
      return super.exec(sql);
    }
  }
  let store;
  let process;
  try {
    store = openOrganizeStore({
      path: join(dir, 'organize.sqlite'),
      runId: RUN,
      Database: FailOneCommit,
      nowMs: () => 1_000_000,
    });
    process = createOrganizeProcess({
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      store,
      rawWriter: () => true,
      markRunning: false,
    });
    const channel = memoryChannel();
    process.handleControl(acceptedMessage(), channel);
    failure.armed = true;

    assert.throws(() => process.handleEnvelope(envelope(1), channel), /injected organizer commit failure/);

    // The external raw write's pre-record survives, but the transaction's watermark and confirmation do not.
    assert.deepEqual(process.watermarkRows(), [], 'the watermark did not survive the rollback');
    const entries = process.ledgerEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].state, 'intent', 'only the pre-write intent remains for a retry');
    assert.equal(process.recoveryStatus().owedCount, 0, 'an uncommitted intent is not a pending book delivery');
  } finally {
    process?.close();
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ③ the invalidation request: persist, no-op on duplicate, confirm on response, re-derive on restart
// ---------------------------------------------------------------------------------------------------

test('③ an invalidation is a persisted request, a duplicate is a no-op, and it is confirmed by the book', async () => {
  const h = await setup();
  try {
    const first = h.org.requestInvalidation({ connectionId: CID, from: 5, to: 7, reason: 'a hole must be declared missing' });
    assert.equal(first.created, true);
    assert.equal(first.revision, 1, 'the first request carries revision 1');
    await until(() => h.book.state.invalidations.length === 1);
    assert.equal(h.book.state.invalidations[0].request_id, first.requestId, 'the book was asked');
    assert.equal(h.book.state.invalidations[0].payload.revision, 1);

    const duplicate = h.org.requestInvalidation({ connectionId: CID, from: 5, to: 7 });
    assert.equal(duplicate.created, false, 'a duplicate request is a no-op');
    assert.equal(duplicate.noop, true);
    assert.equal(duplicate.revision, 1, 'and the revision did not move');
    assert.equal(h.book.state.invalidations.length, 1, 'the book was not asked twice');

    h.book.sendInvalidated({ requestId: first.requestId, connectionId: CID });
    await until(() => h.org.invalidationRequests({ state: 'confirmed' }).length === 1);
    assert.equal(h.org.invalidationRequests()[0].confirmedBy, 'book');
    assert.equal(h.missing.length, 1, 'the confirmed loss is announced');
    assert.equal(h.missing[0].requestId, first.requestId);

    // The revision is monotonic per board: a new range gets the next number.
    const second = h.org.requestInvalidation({ connectionId: CID, from: 8, to: 9 });
    assert.equal(second.revision, 2, 'the revision is monotonic in the store');
  } finally {
    await h.teardown();
  }
});

test('③ the outstanding request is re-derived after a restart, and the confirmed one is not re-sent', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-reopen-'));
  const storePath = join(dir, 'organize.sqlite');
  try {
    const first = await openOrganizeProcess({
      listenPath: join(dir, 'organize-a.sock'),
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
    });
    const bookA = await startFakeBook(join(dir, 'organize-a.sock'), { market: MARKET, stream: STREAM, runId: RUN });
    const confirmed = first.requestInvalidation({ connectionId: CID, from: 5, to: 7 });
    await until(() => bookA.state.invalidations.length === 1);
    bookA.sendInvalidated({ requestId: confirmed.requestId, connectionId: CID });
    await until(() => first.invalidationRequests({ state: 'confirmed' }).length === 1);
    const outstanding = first.requestInvalidation({ connectionId: CID, from: 8, to: 9 });
    await until(() => bookA.state.invalidations.length === 2);
    first.close();
    bookA.close();

    // A restart re-derives what the book never answered - and only that. The confirmed loss is retained
    // and is not asked again; the outstanding request travels once more.
    const second = await openOrganizeProcess({
      listenPath: join(dir, 'organize-b.sock'),
      market: MARKET,
      stream: STREAM,
      runId: RUN,
      roleInstance: 'organize-1',
      storePath,
    });
    const bookB = await startFakeBook(join(dir, 'organize-b.sock'), { market: MARKET, stream: STREAM, runId: RUN });
    await until(() => bookB.state.invalidations.length >= 1);
    assert.equal(bookB.state.invalidations.length, 1, 'only the outstanding request was re-sent');
    assert.equal(bookB.state.invalidations[0].request_id, outstanding.requestId);
    const confirmedAfter = second.invalidationRequests().find((r) => r.requestId === confirmed.requestId);
    assert.equal(confirmedAfter.state, 'confirmed', 'the confirmed loss was retained across the restart');
    second.close();
    bookB.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ④ a confirmed missing may not be cancelled
// ---------------------------------------------------------------------------------------------------

test('④ a confirmed missing cannot be cancelled, by a book response or by compensation', async () => {
  const h = await setup();
  try {
    const request = h.org.requestInvalidation({ connectionId: CID, from: 1, to: 2 });
    await until(() => h.book.state.invalidations.length === 1);
    h.book.sendInvalidated({ requestId: request.requestId, connectionId: CID });
    await until(() => h.org.invalidationRequests({ state: 'confirmed' }).length === 1);

    const cancel = h.org.cancelInvalidation(request.requestId);
    assert.equal(cancel.cancelled, false);
    assert.equal(cancel.refused, true, 'a compensation over a confirmed loss is refused');
    assert.equal(h.org.invalidationRequests({ state: 'confirmed' }).length, 1, 'and the loss stays confirmed');

    // A request that was never confirmed may still be withdrawn.
    const outstanding = h.org.requestInvalidation({ connectionId: CID, from: 3, to: 4 });
    const withdrawal = h.org.cancelInvalidation(outstanding.requestId);
    assert.equal(withdrawal.cancelled, true);
    assert.equal(h.org.invalidationRequests().some((r) => r.requestId === outstanding.requestId), false);
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑤ the run marker: running, invalidated, and complete only when every tail is reached
// ---------------------------------------------------------------------------------------------------

test('⑤ the run marker records running and invalidated, and complete only after every tail is reached', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-run-'));
  const storePath = join(dir, 'organize.sqlite');
  const storePath2 = join(dir, 'organize-2.sqlite');
  try {
    const oldRun = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: 'run-old', storePath });
    assert.equal(oldRun.runMarkerState('run-old'), 'running', 'opening writes the running marker');
    oldRun.close();

    const process = createOrganizeProcess({
      market: MARKET,
      stream: STREAM,
      runId: 'run-new',
      storePath,
      rawWriter: () => true,
    });
    assert.equal(process.runMarkerState('run-old'), 'invalidated', 'the previous running run was invalidated');
    assert.equal(process.runMarkerState('run-new'), 'running');

    // Drive one frame and seal the tail it reaches: every tail is reached, so a clean end may be written.
    const channel = memoryChannel();
    const newCid = 'run-new:kraken:kraken_spot:1';
    process.handleControl(acceptedMessage({ connectionId: newCid, runId: 'run-new' }), channel);
    process.handleEnvelope(envelope(1, { connectionId: newCid, runId: 'run-new' }), channel);
    const verdict = process.handleControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'tail_sealed',
        role_instance: 'ingest-1',
        run_id: 'run-new',
        payload: { tails: [{ connectionId: newCid, lastReceivedSeq: 1 }], spool_empty: true },
      }),
      channel,
    );
    assert.equal(verdict.allAcked, true, 'the sealed tail was reached by the durable ceiling');
    assert.ok(process.allAcked);
    const stopped = process.requestStop('the organizer stopped before finalization');
    assert.equal(stopped.stopped, true, 'processing stops before the supervisor prepares completion');
    const request = {
      finalizeRequestId: 'run-new:finalize:barrier-1',
      barrierId: 'barrier-1',
      tails: [{ connectionId: newCid, lastReceivedSeq: 1 }],
      bookStop: { requestId: 'book-stop-1', roleInstance: 'book-1', stopped: true },
    };
    const prepared = process.prepareFinalize(request);
    assert.equal(prepared.prepared, true, 'the fixed supervisor request is persisted before completion');
    const completion = process.finalize(request);
    assert.equal(completion.completed, true, 'a clean end is written only when every tail is reached');
    assert.equal(process.runMarkerState('run-new'), 'complete');
    process.close();

    // A stop with no sealed tail is not a normal completion: the tail is unknown, so nothing is written.
    const unknownTail = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: 'run-unknown', storePath: storePath2 });
    const earlyStop = unknownTail.stop();
    assert.equal(earlyStop.stopped, true);
    assert.equal('completed' in earlyStop, false, 'ordinary stop makes no completeness claim');
    assert.equal(unknownTail.runMarkerState('run-unknown'), 'running', 'no complete marker was written');
    unknownTail.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑥ the all-acknowledged judgement is a truth table (rulings ⑨⑩)
// ---------------------------------------------------------------------------------------------------

test('⑥ all-acknowledged: every sealed tail reached, no spool, no hole - otherwise withheld', () => {
  const ceilings = (entries) => new Map(entries);
  const tails = (entries) => entries;

  // An unknown tail is never a normal completion.
  assert.equal(judgeAllAcked({ tails: [], ceilings: ceilings([]), spoolEmpty: true }).allAcked, false);
  assert.equal(judgeAllAcked({ tails: null, ceilings: ceilings([]), spoolEmpty: true }).allAcked, false);

  // The ceiling reached the tail: acknowledged.
  assert.equal(
    judgeAllAcked({
      tails: tails([{ connectionId: 'c1', lastReceivedSeq: 5 }]),
      ceilings: ceilings([['c1', { upToSeq: 5 }]]),
      spoolEmpty: true,
    }).allAcked,
    true,
  );

  // The ceiling is short of the tail, or absent: withheld.
  assert.equal(
    judgeAllAcked({
      tails: tails([{ connectionId: 'c1', lastReceivedSeq: 5 }]),
      ceilings: ceilings([['c1', { upToSeq: 4 }]]),
      spoolEmpty: true,
    }).allAcked,
    false,
  );
  assert.equal(
    judgeAllAcked({
      tails: tails([{ connectionId: 'c1', lastReceivedSeq: 5 }]),
      ceilings: ceilings([['c1', { upToSeq: null }]]),
      spoolEmpty: true,
    }).allAcked,
    false,
  );

  // An unprocessed spool withholds it even when the ceiling reached the tail.
  assert.equal(
    judgeAllAcked({
      tails: tails([{ connectionId: 'c1', lastReceivedSeq: 5 }]),
      ceilings: ceilings([['c1', { upToSeq: 5 }]]),
      spoolEmpty: false,
    }).allAcked,
    false,
  );

  // A raw hole withholds it.
  assert.equal(
    judgeAllAcked({
      tails: tails([{ connectionId: 'c1', lastReceivedSeq: 5 }]),
      ceilings: ceilings([['c1', { upToSeq: 5 }]]),
      spoolEmpty: true,
      holes: [{ connectionId: 'c1', from: 3, to: 4 }],
    }).allAcked,
    false,
  );

  // Every connection, old generations included, must be reached: one short tail withholds it.
  assert.equal(
    judgeAllAcked({
      tails: tails([
        { connectionId: 'run-old:kraken:kraken_spot:1', lastReceivedSeq: 9 },
        { connectionId: 'run-new:kraken:kraken_spot:1', lastReceivedSeq: 4 },
      ]),
      ceilings: ceilings([
        ['run-old:kraken:kraken_spot:1', { upToSeq: 9 }],
        ['run-new:kraken:kraken_spot:1', { upToSeq: 3 }],
      ]),
      spoolEmpty: true,
    }).allAcked,
    false,
  );

  // All of them reached: acknowledged.
  assert.equal(
    judgeAllAcked({
      tails: tails([
        { connectionId: 'run-old:kraken:kraken_spot:1', lastReceivedSeq: 9 },
        { connectionId: 'run-new:kraken:kraken_spot:1', lastReceivedSeq: 4 },
      ]),
      ceilings: ceilings([
        ['run-old:kraken:kraken_spot:1', { upToSeq: 9 }],
        ['run-new:kraken:kraken_spot:1', { upToSeq: 4 }],
      ]),
      spoolEmpty: true,
    }).allAcked,
    true,
  );
});

// ---------------------------------------------------------------------------------------------------
// Ownership: organize's store carries its own tables and neither the receive tail nor a spool
// ---------------------------------------------------------------------------------------------------

test('② organize authorizes nothing: a raw accept is refused and no connection is adopted', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-no-accept-'));
  try {
    const process = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: RUN, storePath: join(dir, 'organize.sqlite') });
    const channel = memoryChannel();
    const outcome = process.handleControl(
      makeMessage({
        version: IPC_VERSION,
        type: 'accept',
        role_instance: 'ingest-1',
        request_id: 'req-x',
        run_id: RUN,
        connection_id: CID,
        generation: 1,
        payload: { first_seq: 1 },
      }),
      channel,
    );
    assert.equal(outcome.accepted, false, 'organize does not grant an accept');
    assert.equal(
      channel.sent.some((m) => m.type === 'accepted'),
      false,
      'the unconditional accepted:true is gone',
    );
    assert.equal(process.acceptedConnectionId, null, 'nothing was adopted');
    // And a frame from that connection is refused for want of an adoption.
    const refused = process.handleEnvelope(envelope(1), channel);
    assert.equal(refused.accepted, false);
    assert.match(String(refused.reason), /no connection has been accepted/);
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the owed set is offered once: a frame above an open hole does not re-send the frames below it', async () => {
  const h = await setup();
  try {
    await until(() => h.org.channels().book !== null && h.org.channels().ingest !== null);
    h.book.sendAccepted({ requestId: 'ingest-1:accept:1', connectionId: CID, generation: 1, firstSeq: 1 });
    await until(() => h.org.acceptedConnectionId === CID);

    h.ingest.sendFrame(envelope(1));
    h.ingest.sendFrame(envelope(2));
    await until(() => h.book.state.envelopes.length >= 2);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(
      h.book.state.envelopes.map((e) => e.receive_seq),
      [1, 2],
      'both durable frames were offered to the book once each',
    );

    // A hole opens at 3 and stays open while 4, 5 and 6 arrive. Each arrival must add exactly one
    // offer - its own - not one per owed frame below it: the owed set is not re-sent per frame.
    h.ingest.sendFrame(envelope(4));
    h.ingest.sendFrame(envelope(5));
    h.ingest.sendFrame(envelope(6));
    await until(() => h.book.state.envelopes.length >= 5);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(
      h.book.state.envelopes.map((e) => e.receive_seq),
      [1, 2, 4, 5, 6],
      'each new frame is offered once; nothing below it is offered again',
    );

    // The frame that fills the hole arrives late, below everything already offered: it must still
    // reach the book - a single high-water mark would have called it "offered" and skipped it for
    // ever, leaving the book's hole open even though organize's own hole is filled.
    h.ingest.sendFrame(envelope(3));
    await until(() => h.book.state.envelopes.length >= 6);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(
      h.book.state.envelopes.map((e) => e.receive_seq),
      [1, 2, 4, 5, 6, 3],
      'the late frame that fills the hole is offered too',
    );

    // An explicit resend re-offers the whole owed set on a fresh memory: the book hears it again.
    h.book.sendResend({ connectionId: CID });
    await until(() => h.book.state.envelopes.length >= 12);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(
      h.book.state.envelopes.slice(6).map((e) => e.receive_seq),
      [1, 2, 4, 5, 6, 3],
      'the resend offers the owed set again, in arrival order',
    );
  } finally {
    await h.teardown();
  }
});

test('a restart offers the owed set again on the new channel', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-restart-'));
  try {
    const storePath = join(dir, 'organize.sqlite');
    // First life: durable frames with a hole at 3, never acknowledged - they stay owed.
    const first = await setup({ storePath });
    try {
      await until(() => first.org.channels().book !== null);
      first.book.sendAccepted({ requestId: 'ingest-1:accept:1', connectionId: CID, generation: 1, firstSeq: 1 });
      await until(() => first.org.acceptedConnectionId === CID);
      first.ingest.sendFrame(envelope(1));
      first.ingest.sendFrame(envelope(2));
      first.ingest.sendFrame(envelope(4));
      first.ingest.sendFrame(envelope(5));
      await until(() => first.book.state.envelopes.length >= 4);
    } finally {
      await first.teardown();
    }

    // Second life, same store: the owed ledger survives - hole and all - and the new channel hears
    // the set again.
    const second = await setup({ storePath });
    try {
      await until(() => second.org.channels().book !== null);
      second.book.sendAccepted({ requestId: 'ingest-1:accept:1', connectionId: CID, generation: 1, firstSeq: 1 });
      await until(() => second.org.acceptedConnectionId === CID);
      await until(() => second.book.state.envelopes.length >= 4);
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.deepEqual(
        second.book.state.envelopes.map((e) => e.receive_seq),
        [1, 2, 4, 5],
        'the owed set from the earlier life is offered on the new channel, hole and all',
      );

      // A replay of the durable-above frames, as the startup path must eventually perform: the watermark
      // row restores the ceiling, but not the set of frames already waiting above it, so without this
      // replay the ceiling stalls at the hole-filler. This pins the watermark mechanics given the replay;
      // OPEN ITEM: the real startup order cannot yet deliver one - adoption happens after the spool
      // drain (run.mjs (c)->(e)) and normally-sent frames are not in the spool - see the audit notes.
      second.ingest.sendFrame(envelope(4));
      second.ingest.sendFrame(envelope(5));

      // The hole fills: the missing frame arrives on the replayed run, and the book applies the lot.
      second.ingest.sendFrame(envelope(3));
      await until(() => second.book.state.envelopes.length >= 5);
      second.book.sendAppliedAck({ connectionId: CID, upToSeq: 5 });
      await until(() => second.org.ledgerSize() === 0);

      const db = new DatabaseSync(storePath);
      try {
        const watermark = db
          .prepare('SELECT up_to_receive_seq FROM organized_watermark WHERE connection_id = ? AND market = ? AND stream = ?')
          .get(CID, MARKET, STREAM);
        assert.equal(watermark.up_to_receive_seq, 5, 'the watermark advanced through the replayed run');
        const open = db
          .prepare('SELECT COUNT(*) AS n FROM organize_gap WHERE connection_id = ? AND filled_at_ms IS NULL')
          .get(CID);
        assert.equal(open.n, 0, 'and the hole it crossed is closed');
      } finally {
        db.close();
      }
    } finally {
      await second.teardown();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a live channel change re-offers the owed set on the new channel', async () => {
  const h = await setup();
  try {
    await until(() => h.org.channels().book !== null && h.org.channels().ingest !== null);
    h.book.sendAccepted({ requestId: 'ingest-1:accept:1', connectionId: CID, generation: 1, firstSeq: 1 });
    await until(() => h.org.acceptedConnectionId === CID);
    h.ingest.sendFrame(envelope(1));
    h.ingest.sendFrame(envelope(2));
    await until(() => h.book.state.envelopes.length >= 2);

    // The book's channel is replaced mid-run: the new audience has seen nothing.
    const before = h.org.channels().book;
    h.book.close();
    const book2 = await startFakeBook(h.socketPath, { market: MARKET, stream: STREAM, runId: RUN });
    try {
      await until(() => h.org.channels().book !== null && h.org.channels().book !== before);
      // One more frame runs the sweep the channel change asked for.
      h.ingest.sendFrame(envelope(3));
      await until(() => book2.state.envelopes.length >= 3);
      await new Promise((resolve) => setTimeout(resolve, 50));
      assert.deepEqual(
        book2.state.envelopes.map((e) => e.receive_seq),
        [1, 2, 3],
        'the new channel hears the whole owed set plus the frame that triggered the sweep',
      );
    } finally {
      book2.close();
    }
  } finally {
    await h.teardown();
  }
});

test('an offer the link refused is swept again on the next frame', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-blocked-'));
  try {
    const process = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: RUN, storePath: join(dir, 'organize.sqlite') });
    const book = memoryChannel();
    process.handleControl(
      makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: 'book-1', run_id: RUN, payload: { role: 'book' } }),
      book,
    );
    process.handleControl(acceptedMessage({ connectionId: CID }), book);
    const ingest = memoryChannel();

    let block = false;
    const inner = book.sendEnvelope.bind(book);
    book.sendEnvelope = (envelopeOut) => (block ? false : inner(envelopeOut));

    block = true;
    process.handleEnvelope(envelope(1), ingest);
    assert.equal(book.envelopes.length, 0, 'the blocked offer delivered nothing');

    block = false;
    process.handleEnvelope(envelope(2), ingest);
    assert.deepEqual(
      book.envelopes.map((e) => e.receive_seq),
      [1, 2],
      'the sweep re-offers the blocked frame and the new one, each once',
    );
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a refused offer on the ordinary path arms a sweep for the next frame', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-blocked2-'));
  try {
    const process = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: RUN, storePath: join(dir, 'organize.sqlite') });
    const book = memoryChannel();
    process.handleControl(
      makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: 'book-1', run_id: RUN, payload: { role: 'book' } }),
      book,
    );
    process.handleControl(acceptedMessage({ connectionId: CID }), book);
    const ingest = memoryChannel();

    let block = false;
    const inner = book.sendEnvelope.bind(book);
    book.sendEnvelope = (envelopeOut) => (block ? false : inner(envelopeOut));

    // The first frame completes the initial sweep: from here the ordinary path is in charge.
    process.handleEnvelope(envelope(1), ingest);
    assert.deepEqual(book.envelopes.map((e) => e.receive_seq), [1], 'the initial sweep delivered the first frame');

    // The next frame is refused on the ordinary path: it must arm a sweep, not vanish.
    block = true;
    process.handleEnvelope(envelope(2), ingest);
    assert.deepEqual(book.envelopes.map((e) => e.receive_seq), [1], 'the refused frame delivered nothing');

    block = false;
    process.handleEnvelope(envelope(3), ingest);
    assert.deepEqual(
      book.envelopes.map((e) => e.receive_seq),
      [1, 2, 3],
      'the armed sweep re-offers the refused frame and the new one, each once',
    );
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a router error without a channel clears the offer memory, and the next frame re-offers', () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-router-err-'));
  try {
    const process = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: RUN, storePath: join(dir, 'organize.sqlite') });
    const book = memoryChannel();
    process.handleControl(
      makeMessage({ version: IPC_VERSION, type: 'hello', role_instance: 'book-1', run_id: RUN, payload: { role: 'book' } }),
      book,
    );
    process.handleControl(acceptedMessage({ connectionId: CID }), book);
    const ingest = memoryChannel();

    process.handleEnvelope(envelope(1), ingest);
    assert.deepEqual(book.envelopes.map((e) => e.receive_seq), [1]);

    // The router reports a failure without naming a channel: the frames it was offered are not its
    // to keep, so the next frame re-offers the set.
    process.handleError(new Error('the router failed'), undefined);
    process.handleEnvelope(envelope(2), ingest);
    assert.deepEqual(
      book.envelopes.map((e) => e.receive_seq),
      [1, 1, 2],
      'the reset re-offers the whole owed set alongside the new frame',
    );
    process.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('an acknowledged range is not re-offered, and a resend sends only what is still owed', async () => {
  const h = await setup();
  try {
    await until(() => h.org.channels().book !== null && h.org.channels().ingest !== null);
    h.book.sendAccepted({ requestId: 'ingest-1:accept:1', connectionId: CID, generation: 1, firstSeq: 1 });
    await until(() => h.org.acceptedConnectionId === CID);
    for (const seq of [1, 2, 3]) h.ingest.sendFrame(envelope(seq));
    await until(() => h.book.state.envelopes.length >= 3);

    h.book.sendAppliedAck({ connectionId: CID, upToSeq: 2 });
    await until(() => h.org.ledgerSize() === 1);

    h.book.sendResend({ connectionId: CID });
    await until(() => h.book.state.envelopes.length >= 4);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(
      h.book.state.envelopes.slice(3).map((e) => e.receive_seq),
      [3],
      'only the unacknowledged frame is re-offered',
    );
  } finally {
    await h.teardown();
  }
});

test('organize owns its tables and recovers unapplied work from the watermark plus ledger', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'organize-own-'));
  try {
    const storePath = join(dir, 'organize.sqlite');
    const process = createOrganizeProcess({ market: MARKET, stream: STREAM, runId: RUN, storePath });
    process.close();
    const db = new DatabaseSync(storePath);
    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all()
      .map((row) => row.name);
    db.close();
    assert.ok(tables.includes('run_marker'), 'organize owns the run marker');
    assert.equal(tables.includes('pending_boundary'), false, 'the durable ceiling and owed ledger replace the old boundary table');
    assert.ok(tables.includes('organized_watermark'), 'organize owns the watermark');
    assert.ok(tables.includes('delivery_ledger'), 'organize owns the delivery ledger');
    assert.ok(tables.includes('invalidation_request'), 'organize owns the invalidation request');
    assert.equal(tables.includes('received_tail'), false, 'the receive tail belongs to ingest, not organize');
    assert.equal(tables.some((name) => name.includes('spool')), false, 'the spool belongs to ingest, not organize');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

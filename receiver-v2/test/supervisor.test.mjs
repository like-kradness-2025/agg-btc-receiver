/**
 * Set 7: the supervisor's startup sequence, its notifications, and the operational entrance.
 *
 * The startup order (a) begin -> (b) restore -> (c) drain -> (d)+(e) start is not a matter of taste: a
 * restart whose ledger is empty but whose spool still holds frames has to organize them on the connection
 * the board recorded, and the organizer learns that connection nowhere else. The tests below pin the order
 * by its observable effects, pin that a refused connection never opens a socket and is reported, and pin
 * that a notification's request is carried out after the operation it arrived in has ended.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { makeEnvelope, encodeEnvelope, frame } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { createSupervisor } from '../src/supervisor/supervisor.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';
import { internalsOf } from '../src/internal/wiring.mjs';

const CONNECTION = 'run-1:kraken:kraken_spot:1';

const envelope = (seq, connectionId = CONNECTION) =>
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
    raw: `{"seq":${seq}}`,
    meta: { first_seq: 1 },
  });

/**
 * A database whose writes against a pattern fail. It is a production option (the supervisor hands a
 * `Database` to its store) rather than a hole opened for the test: the failure is a real write that
 * really throws, which is exactly how a save failure reaches the supervisor.
 */
function failingDatabase(pattern) {
  return class extends DatabaseSync {
    prepare(sql, ...rest) {
      const statement = super.prepare(sql, ...rest);
      if (!pattern.test(sql)) return statement;
      return {
        run: () => {
          throw new Error('injected write failure');
        },
        get: (...args) => statement.get(...args),
        all: (...args) => statement.all(...args),
      };
    }
  };
}

/** Write a raw segment file for the spool, so a restart meets exactly the bytes a test wants. */
function writeSpoolSegment(spoolDir, bytes) {
  mkdirSync(spoolDir, { recursive: true });
  writeFileSync(join(spoolDir, 'segment-0000000001.spool'), bytes);
}

/** A structure whose adapter carries a sequence proof, used where a frame must be able to break it. */
function proofAdapter(overrides = {}) {
  return {
    url: 'ws://venue.test/ws',
    stream: 'trades',
    parse: () => ({ kind: 'data' }),
    boundary: 'sequence',
    venueSeqOf: (raw) => JSON.parse(String(raw)).venue_seq,
    connects: ({ previous, current, replace }) =>
      replace === true ||
      previous === null ||
      current.meta.venue_seq === previous.meta.venue_seq + 1,
    changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
    ...overrides,
  };
}

/** A store holding a board boundary for CONNECTION plus one owed (durable, unapplied) frame at seq. */
function seedBoundaryWithOwed(path, seq = 1) {
  const store = openDurability({ path, runId: 'run-1' });
  const seeder = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter,
    durability: store,
    runId: 'run-1',
    venue: 'kraken',
    webSocketImpl: function unused() {
      throw new Error('this fixture feeds frames directly');
    },
    rawWriter: () => true,
    spoolDir: null,
    deferRecovery: true,
    onAck: () => {},
    onGap: () => {},
    onStop: () => {},
  });
  seeder.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
  // An owed entry, written directly: a frame the raw holds and the board has not applied. It is the
  // exact state a restart's (b)/(d) steps act on.
  const parts = internalsOf(seeder);
  parts.ledger.record(envelope(seq), 'durable in the raw and not applied to the board yet');
  seeder.stop();
  store.close();
}

/** A store holding a board boundary for CONNECTION, with frames spilled because the raw refused them. */
function seedBoundaryWithSpool(path, spoolDir, seqs = [1]) {
  const store = openDurability({ path, runId: 'run-1' });
  const seeder = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter,
    durability: store,
    runId: 'run-1',
    venue: 'kraken',
    webSocketImpl: function unused() {
      throw new Error('this fixture feeds frames directly');
    },
    rawWriter: () => false,
    spoolDir,
    deferRecovery: true,
    onAck: () => {},
    onGap: () => {},
    onStop: () => {},
  });
  seeder.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
  for (const seq of seqs) seeder.feed(envelope(seq));
  seeder.stop();
  store.close();
}

/** A store whose board follows CONNECTION, and whose ledger also owes a frame for a connection now gone. */
function seedBoundaryWithGoneOwed(path) {
  const store = openDurability({ path, runId: 'run-1' });
  const seeder = createStructure({
    market: 'kraken_spot',
    stream: 'trades',
    adapter,
    durability: store,
    runId: 'run-1',
    venue: 'kraken',
    webSocketImpl: function unused() {
      throw new Error('this fixture feeds frames directly');
    },
    rawWriter: () => true,
    spoolDir: null,
    deferRecovery: true,
    onAck: () => {},
    onGap: () => {},
    onStop: () => {},
  });
  seeder.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
  // An owed frame the board does not follow: the connection it arrived on is gone, so the (d) redelivery
  // pass writes it down as a permanent loss and reports it. That gap notification is the news a restart
  // reacts to - the request a test raises from inside the sequence's own step.
  const parts = internalsOf(seeder);
  parts.ledger.record(
    envelope(1, 'run-0:kraken:kraken_spot:1'),
    'durable in the raw and not applied to the board yet',
  );
  seeder.stop();
  store.close();
}

const adapter = {
  url: 'ws://venue.test/ws',
  stream: 'trades',
  parse: () => ({ kind: 'data' }),
  changesFor: (frame) => [{ side: 'bid', price: 100 + frame.receive_seq, size: 1 }],
};

/** A websocket implementation a test can count: every socket the structure opens shows up here. */
function fakeSockets() {
  const sockets = [];
  const impl = function fakeSocket(url) {
    const socket = {
      url,
      sent: [],
      closed: false,
      onopen: null,
      onmessage: null,
      onclose: null,
      onerror: null,
      send(message) {
        socket.sent.push(message);
      },
      close() {
        socket.closed = true;
      },
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

async function withDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), 'supervisor-'));
  try {
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test('the operational entrance is a path: a store object is refused', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const store = openDurability({ path, runId: 'run-1' });
    try {
      assert.throws(
        () =>
          createSupervisor({
            market: 'kraken_spot',
            adapter,
            path,
            venue: 'kraken',
            runId: 'run-1',
            durability: store,
            webSocketImpl: fakeSockets().impl,
            rawWriter: () => true,
          }),
        /does not accept one/,
        'the supervisor opens its own store; nothing operational hands it one',
      );
    } finally {
      store.close();
    }
  });
});

test('the startup sequence runs begin, restore, drain and start, and only then opens the socket', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const steps = [];
    const exits = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-new',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      exit: (code) => exits.push(code),
      onStep: (s) => steps.push(s.step),
      onStop: () => {},
      onRefetch: () => {},
    });

    const started = supervisor.start();
    assert.equal(started.started, true, 'the connection was admitted and the socket opened');
    assert.deepEqual(
      steps,
      ['beginRun', 'restore', 'drain', 'redeliver', 'start'],
      'the sequence is carried out in order, redelivery before the connection is announced',
    );
    assert.equal(sockets.length, 1, 'and the socket opens only after the sequence is done');
    // Readiness is not "a socket opened": the board is serving only once the current connection's
    // snapshot has actually been applied (C6/C7). A fresh board has proven nothing yet.
    assert.equal(supervisor.ready, false, 'an open socket with an unproven board is not ready');
    // The snapshot arrives: the frame is applied and the board is proved, and only now is the run ready.
    supervisor.feed(
      makeEnvelope({
        market: 'kraken_spot',
        stream: 'trades',
        connectionId: supervisor.connection.connectionId,
        runId: 'run-new',
        venue: 'kraken',
        generation: 1,
        receiveSeq: 1,
        recvTsMs: 1_792_000_000_001,
        recvMonoNs: 1_000_001,
        raw: '{"seq":1}',
        meta: { first_seq: 1 },
      }),
    );
    assert.equal(supervisor.ready, true, 'the board is serving on the current connection');
    assert.deepEqual(exits, [], 'no failure, no exit code');
    supervisor.stop();
    supervisor.close();
  });
});

test('a connection the book refuses never opens a socket, and a throwing diagnostic does not hide the stop', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // Seed a board owned by this run at generation 9. The connection this process will announce is
    // generation 1 of the same run, which cannot be a step forward - so the book refuses it.
    const store = openDurability({ path, runId: 'run-1' });
    const board = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: store,
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });
    board.accept('run-1:kraken:kraken_spot:9', { runId: 'run-1', generation: 9, firstSeq: 1 });
    board.stop();
    store.close();

    const { sockets, impl } = fakeSockets();
    const exits = [];
    const stops = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      exit: (code) => exits.push(code),
      onStop: (stop) => stops.push(stop),
      onRefetch: () => {},
      onDiagnostic: () => {
        throw new Error('the diagnostic itself failed');
      },
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true, 'a refused connection is not a start');
    assert.equal(sockets.length, 0, 'and no socket was opened for it');
    assert.equal(stops.length >= 1, true, 'the refusal reached onStop despite the diagnostic throwing');
    assert.deepEqual(exits, [1], 'and the run ended with a non-zero code');
    assert.equal(supervisor.abnormal, true);
    supervisor.close();
  });
});

test('a stop asked for from inside a notification is carried out after the operation, exactly once', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const exits = [];
    const stops = [];
    let supervisor;
    supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      // The raw refuses and there is no spool: the frame cannot be held, so reception stops and onGap fires.
      rawWriter: () => false,
      spoolDir: null,
      exit: (code) => exits.push(code),
      onStop: (stop) => stops.push(stop),
      onRefetch: () => {},
      onGap: () => {
        // Asking from inside the frame is refused as re-entrant there, but must not be lost; and asking
        // twice is one need, not two stops.
        supervisor.requestStop('asked from the notification');
        supervisor.requestStop('asked again');
      },
    });

    supervisor.start();
    assert.equal(sockets.length, 1, 'the first connection was admitted and opened');
    const result = supervisor.feed(envelope(1));
    assert.equal(result.stopped, true, 'nothing could hold the frame, so reception stopped');

    assert.deepEqual(exits, [1], 'the request raised inside the frame was carried out once, after it');
    assert.equal(supervisor.stopped, true);
    assert.equal(supervisor.pending.stop, null, 'and it is no longer pending');
    supervisor.close();
  });
});

test('a clean end writes a completion and an abnormal end does not', async () => {
  await withDir(async (dir) => {
    const cleanPath = join(dir, 'clean.sqlite');
    const clean = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path: cleanPath,
      venue: 'kraken',
      runId: 'run-clean',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });
    clean.start();
    clean.stop();
    clean.close();

    let reader = openDurability({ path: cleanPath, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun()?.runId, 'run-clean', 'a clean stop leaves a completion');
    reader.close();

    // A board owned by this run at generation 9; the connection this process announces is generation 1 of
    // the same run, which the book refuses - an abnormal end on a store of its own.
    const abnPath = join(dir, 'abnormal.sqlite');
    const seeder = openDurability({ path: abnPath, runId: 'run-1' });
    const board = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: seeder,
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });
    board.accept('run-1:kraken:kraken_spot:9', { runId: 'run-1', generation: 9, firstSeq: 1 });
    board.stop();
    seeder.close();

    const abnormal = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path: abnPath,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });
    abnormal.start();
    assert.equal(abnormal.abnormal, true, 'the refused connection ended the run abnormally');
    abnormal.close();

    reader = openDurability({ path: abnPath, runId: 'reader-2' });
    assert.equal(reader.lastCompleteRun(), null, 'an abnormal end writes no completion');
    reader.close();
  });
});

test('a restart with an empty ledger drains its old spool only after the boundary is restored', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');

    // Seed: the raw refuses every frame, so they spill. Nothing is owed to the board, because the raw never
    // took anything - the ledger is empty and the spool is the only record.
    const store = openDurability({ path, runId: 'run-1' });
    const seeder = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: store,
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => false,
      spoolDir,
      deferRecovery: true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });
    seeder.accept(CONNECTION, { runId: 'run-1', generation: 1, firstSeq: 1 });
    for (const seq of [1, 2, 3]) seeder.feed(envelope(seq));
    assert.equal(seeder.ledger.size(), 0, 'nothing is owed: the raw refused every frame');
    assert.ok(seeder.spool.bytes > 0, 'and they wait in the spool');
    seeder.stop();
    store.close();

    // Restart, raw healthy again. Recovery is deferred so the test drives the (b) step itself.
    const store2 = openDurability({ path, runId: 'run-2' });
    const rawWritten = [];
    const restarted = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: store2,
      runId: 'run-2',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: (frame) => {
        rawWritten.push(frame.receive_seq);
        return true;
      },
      spoolDir,
      deferRecovery: true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
    });

    // (b) skipped: the organizer knows no connection, so the walk is refused frame by frame.
    const skipped = restarted.drainSpool();
    assert.equal(skipped.consumed, 0, 'nothing is drained without the board boundary being restored');
    assert.ok(skipped.stopped, 'the walk stopped at the first record instead of pretending');
    assert.deepEqual(rawWritten, [], 'and nothing was written');

    // (b) done: the organizer follows the board's recorded connection, and the walk proceeds.
    restarted.restore();
    const drained = restarted.drainSpool();
    assert.equal(drained.consumed, 3, 'every spilled frame is walked back out');
    assert.deepEqual(rawWritten, [1, 2, 3]);
    assert.equal(restarted.book.appliedBoundary.upToSeq, 3, 'and the board has them');
    restarted.stop();
    store2.close();
  });
});

test('a re-anchor request replaces the socket and readiness returns only after the new one is admitted', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    // A re-anchor is a reconnect, so the new socket is scheduled with the reconnect backoff rather than
    // opened on the spot; the test holds the timer and fires it, which is the moment the socket opens.
    const timers = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });

    supervisor.start();
    // A freshly started run with no frames yet is not serving, so it is not yet ready: the socket is
    // open but the board has proved nothing.
    assert.equal(supervisor.ready, false, 'an open socket on an unproven board is not ready');
    assert.equal(sockets.length, 1);
    const firstId = supervisor.appliedBoundary.connectionId;

    // A caller (or a notification) asks for a re-anchor. The old socket is abandoned; a new generation is
    // issued and admitted, and its socket opens when the reconnect timer fires.
    supervisor.requestRefetch({ connectionId: firstId });
    assert.equal(sockets[0].closed, true, 'the old socket was abandoned');
    assert.notEqual(supervisor.appliedBoundary.connectionId, firstId, 'a new connection now owns the board');
    const reconnect = timers.filter((timer) => !timer.cleared).at(-1);
    reconnect.fn();
    assert.equal(sockets.length, 2, 'the new socket was opened for the re-anchor');
    // The new connection is current, but its snapshot has not arrived: readiness must not return yet.
    assert.equal(supervisor.ready, false, 'the new socket is open, but its board is not serving yet');
    // The current connection's snapshot is actually applied: only now is the run ready again, and it is
    // ready on the connection that is current.
    sockets[1].onmessage({ data: '{"seq":1}' });
    assert.equal(supervisor.ready, true, 'readiness returned on the connection that is now current');
    supervisor.stop();
    supervisor.close();
  });
});

test('two connections of one run do not share a generation, so their names cannot collide', () => {
  let issued = 0;
  const issueGeneration = () => (issued += 1);
  const onGeneration = (info) => {
    info.settle?.(true);
    return true;
  };
  const impl = fakeSockets().impl;
  const build = () =>
    createReceiveConnection({
      adapter: { url: 'ws://venue.test/ws', stream: 'trades', parse: () => ({ kind: 'data' }) },
      market: 'kraken_spot',
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: impl,
      issueGeneration,
      onGeneration,
    });

  const first = build();
  first.start();
  const second = build();
  second.start();

  assert.equal(first.connectionId, 'run-1:kraken:kraken_spot:1');
  assert.equal(second.connectionId, 'run-1:kraken:kraken_spot:2', 'the shared issuer moves the second on');
  assert.notEqual(second.connectionId, first.connectionId);
});

// ---------------------------------------------------------------------------
// (c): the drain's stop is classified by a machine-readable code, not by its wording. Only a raw that
// refused a record is transient; a corrupt spool, a save that failed and a frame no connection has been
// accepted for are not - those stop the run at once.
// ---------------------------------------------------------------------------

test('a raw that keeps refusing the old spool is retried a bounded number of times, then the run ends non-zero', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    seedBoundaryWithSpool(path, spoolDir, [1, 2, 3]);

    const exits = [];
    let rawCalls = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      // The raw never takes a record: the only transient stop there is.
      rawWriter: () => {
        rawCalls += 1;
        return false;
      },
      spoolDir,
      maxRecoveryAttempts: 2,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true, 'a spool that will not drain is not a start');
    assert.deepEqual(exits, [1], 'the run ended non-zero after the bounded retries');
    assert.equal(rawCalls, 3, 'maxRecoveryAttempts + 1 attempts, and no more');
    assert.equal(supervisor.abnormal, true);
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null, 'an abnormal end writes no completion');
    reader.close();
  });
});

test('a spool that cannot hand back a record is not retried: the run stops at once', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    // A frame whose decode error text happens to contain the transient wording. The inner header length
    // is well-formed so the walk reaches the JSON parse (which is what reports the text); the payload is
    // not an envelope. If the supervisor classified by the report's words it would retry this; by the
    // code, it stops at once.
    const wording = Buffer.from('raw still refused');
    const innerLength = Buffer.alloc(4);
    innerLength.writeUInt32BE(wording.length, 0);
    writeSpoolSegment(spoolDir, frame(Buffer.concat([innerLength, wording])));

    const exits = [];
    let rawCalls = 0;
    const diagnostics = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => {
        rawCalls += 1;
        return true;
      },
      spoolDir,
      maxRecoveryAttempts: 3,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1]);
    assert.equal(rawCalls, 0, 'the walk stopped at the broken record before any write');
    assert.equal(
      diagnostics.filter((diagnostic) => /spool could not hand back/.test(String(diagnostic.reason))).length,
      1,
      'a stop that is not a raw refusal is not retried, even when its wording matches one',
    );
    supervisor.close();
  });
});

test('the drain stop carries a machine-readable code alongside its report', async () => {
  await withDir(async (dir) => {
    // raw-refused: a valid record the raw keeps refusing.
    const rawPath = join(dir, 'raw.sqlite');
    const rawSpool = join(dir, 'raw-spool');
    seedBoundaryWithSpool(rawPath, rawSpool, [1]);
    const store = openDurability({ path: rawPath, runId: 'run-1' });
    const structure = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: store,
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => false,
      spoolDir: rawSpool,
      deferRecovery: true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });
    structure.restore();
    const refused = structure.drainSpool();
    assert.equal(refused.stoppedCode, 'raw-refused', 'the transient stop has its own code');
    assert.match(String(refused.stopped), /still refused/, 'and the report is kept for people');
    structure.stop();
    store.close();

    // spool-unreadable: a record whose bytes cannot be decoded.
    const badPath = join(dir, 'bad.sqlite');
    const badSpool = join(dir, 'bad-spool');
    writeSpoolSegment(badSpool, frame(Buffer.from('not an envelope')));
    const store2 = openDurability({ path: badPath, runId: 'run-2' });
    const structure2 = createStructure({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      durability: store2,
      runId: 'run-2',
      venue: 'kraken',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: badSpool,
      deferRecovery: true,
      onAck: () => {},
      onGap: () => {},
      onStop: () => {},
      onDiagnostic: () => {},
    });
    const broken = structure2.drainSpool();
    assert.equal(broken.stoppedCode, 'spool-unreadable', 'a corrupt spool has a different code');
    structure2.stop();
    store2.close();
  });
});

test('a spilled frame no connection has been accepted for is not retried: the run stops at once', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    // A valid record, but this store's board has never accepted a connection: the organizer refuses it
    // as fail-closed, and that refusal is not something a retry improves.
    writeSpoolSegment(spoolDir, frame(encodeEnvelope(envelope(1))));

    const exits = [];
    let rawCalls = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => {
        rawCalls += 1;
        return true;
      },
      spoolDir,
      maxRecoveryAttempts: 3,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1]);
    assert.equal(rawCalls, 0, 'the refusal came before any raw write, and is not retried');
    supervisor.close();
  });
});

test('a write that fails while draining the spool is not retried: the run stops at once', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    seedBoundaryWithSpool(path, spoolDir, [1]);

    const exits = [];
    let rawCalls = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => {
        rawCalls += 1;
        return true;
      },
      spoolDir,
      // The board's own save fails: a frame the store could not record is a stop, not a retry.
      Database: failingDatabase(/INSERT OR REPLACE INTO applied_boundary/),
      maxRecoveryAttempts: 3,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1]);
    assert.equal(rawCalls, 1, 'the save failure stopped the walk; it was not attempted again');
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// (a)(b)(d): every step of the startup sequence that fails ends the run non-zero and writes no
// completion. The failure is injected through the store's own Database option, so no test-only route
// is opened in the product code.
// ---------------------------------------------------------------------------

test('(a) a run that cannot be marked live ends non-zero and writes no completion', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const exits = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      // The run marker's own write fails: (a) cannot be carried out.
      Database: failingDatabase(/INSERT OR REPLACE INTO run_marker/),
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1], 'a failed beginRun still ends the run non-zero');
    assert.equal(supervisor.abnormal, true);
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null, 'no completion is written for a run that never began');
    reader.close();
  });
});

test('(b) a boundary that cannot be restored ends non-zero and writes no completion', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    seedBoundaryWithOwed(path, 1);

    const exits = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir: null,
      // Restoring the boundary tells the organizer what the raw already holds, and that write fails.
      Database: failingDatabase(/INSERT OR REPLACE INTO organized_watermark/),
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1], 'a failed restore still ends the run non-zero');
    assert.equal(supervisor.abnormal, true);
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null);
    reader.close();
  });
});

test('(d) a redelivery that fails ends non-zero and writes no completion', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    seedBoundaryWithOwed(path, 1);

    const exits = [];
    let rawCalls = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => {
        rawCalls += 1;
        return true;
      },
      spoolDir: null,
      // The board's own save fails while the owed frame is being redelivered.
      Database: failingDatabase(/INSERT OR REPLACE INTO applied_boundary/),
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true);
    assert.deepEqual(exits, [1], 'a failed redelivery still ends the run non-zero');
    assert.equal(supervisor.abnormal, true);
    assert.equal(rawCalls, 0, 'the owed frame came from the raw, so no write was attempted');
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null);
    reader.close();
  });
});

// ---------------------------------------------------------------------------
// A request raised from inside a notification runs after the operation that raised it, and is not
// recorded as satisfied until it has actually been carried out.
// ---------------------------------------------------------------------------

test('a re-anchor asked for from inside a notification is carried out after the operation, not inside it', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const exits = [];
    let sawSocketsInHook = null;
    let sawReadyInHook = null;
    let sawPendingInHook = null;
    let supervisor;
    supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: proofAdapter(),
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {
        sawSocketsInHook = sockets.length;
        sawReadyInHook = supervisor.ready;
        // Ask again from inside the notification: it must not be carried out mid-operation, and it must
        // not be lost.
        supervisor.requestRefetch({ connectionId: 'run-1:kraken:kraken_spot:1' });
        sawPendingInHook = supervisor.pending.refetch !== null;
      },
    });

    supervisor.start();
    assert.equal(sockets.length, 1);
    // The frames are handed in through the supervisor, so an operation of its own wraps each one and its
    // exit is the moment a request raised inside can be carried out.
    const connectionId = supervisor.connection.connectionId;
    const frame = (seq, venueSeq) =>
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
        raw: JSON.stringify({ seq, venue_seq: venueSeq }),
        meta: { first_seq: 1, venue_seq: venueSeq },
      });

    supervisor.feed(frame(1, 1));
    assert.equal(supervisor.ready, true, 'the first frame anchors and serves the board');
    // A jump in the venue sequence breaks the proof; the structure asks for a re-anchor.
    supervisor.feed(frame(2, 50));

    assert.equal(sawSocketsInHook, 1, 'nothing was carried out while the notification was running');
    assert.equal(sawReadyInHook, false, 'the board had already stopped serving');
    assert.equal(sawPendingInHook, true, 'a request raised inside the operation is still pending, not "done"');
    assert.equal(sockets[0].closed, true, 'once the operation is over, the old socket is abandoned');
    assert.equal(supervisor.pending.refetch, null, 'and the request is cleared only once carried out');

    const reconnect = timers.filter((timer) => !timer.cleared).at(-1);
    reconnect.fn();
    assert.equal(sockets.length, 2, 'the replacement socket opens on the reconnect timer');
    assert.deepEqual(exits, [], 'the recovery is not a failure');
    supervisor.stop();
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// After a stop: the old connection's timers do nothing, a frame from a replaced connection is not
// applied, and readiness is the current connection's snapshot actually being applied.
// ---------------------------------------------------------------------------

test("after a stop the old connection's timers do nothing", async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });

    supervisor.start();
    assert.equal(sockets.length, 1);
    // The socket opens: its handlers arm the silence and stability timers.
    sockets[0].onopen();
    assert.ok(timers.length >= 1, 'the connection armed its silence and stability timers');
    const receiveSeqBefore = supervisor.stats.receiveSeq;

    supervisor.stop();
    // Fire every timer ever armed - cleared or not - as a stopped connection must ignore them all.
    for (const timer of timers) timer.fn();

    assert.equal(sockets.length, 1, 'no old timer opened a socket');
    assert.equal(supervisor.stats.receiveSeq, receiveSeqBefore, 'and none received anything');
    assert.equal(supervisor.ready, false, 'a stopped run is not ready');
    supervisor.close();
  });
});

test('a frame from a connection that has been replaced is not applied to the board', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: proofAdapter(),
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
    });

    supervisor.start();
    const oldSocket = sockets[0];
    const oldHandler = oldSocket.onmessage;
    oldHandler({ data: JSON.stringify({ seq: 1, venue_seq: 1 }) });
    assert.equal(supervisor.ready, true, 'the board is serving on the current connection');
    const depthBefore = supervisor.book.board.depth;

    // A re-anchor abandons the old socket and makes a new connection current.
    supervisor.requestRefetch({ connectionId: supervisor.appliedBoundary.connectionId });
    assert.equal(oldSocket.onmessage, null, 'the replaced socket has no handler left');
    const receiveSeqBefore = supervisor.stats.receiveSeq;

    // A late frame arrives through the handler saved from the replaced connection: it must not even be
    // stamped, let alone applied.
    oldHandler({ data: JSON.stringify({ seq: 2, venue_seq: 2 }) });
    assert.equal(supervisor.stats.receiveSeq, receiveSeqBefore, 'the late frame was not stamped');
    assert.equal(supervisor.book.board.depth, depthBefore, 'and the board is unchanged');
    supervisor.stop();
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// The socket arrival is the main road in, and it does not pass through a supervisor API call. A
// notification raised by a frame the socket delivered must still be carried out, and it must be carried
// out once that arrival's operation has ended - never from inside the frame.
// ---------------------------------------------------------------------------

test('a re-anchor raised by a socket arrival is carried out after that arrival, with no supervisor call', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const exits = [];
    const steps = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter: proofAdapter(),
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: (code) => exits.push(code),
      onStep: (s) => steps.push(s.step),
      onStop: () => {},
      onRefetch: () => {},
    });

    supervisor.start();
    assert.equal(sockets.length, 1, 'the run admitted and opened the first socket');

    // The first frame anchors and proves the board. The second jumps the venue sequence, which breaks the
    // proof and makes the structure ask for a re-anchor. Both arrive through the socket's own handler - no
    // supervisor call is made between them and the assertions below.
    sockets[0].onmessage({ data: JSON.stringify({ seq: 1, venue_seq: 1 }) });
    assert.equal(supervisor.ready, true, 'the first socket frame anchored and served the board');
    sockets[0].onmessage({ data: JSON.stringify({ seq: 2, venue_seq: 50 }) });

    // The request was raised inside the arrival and carried out the moment that arrival's operation ended:
    // a new generation owns the board, the old socket is abandoned, and the recovery step heard it.
    assert.ok(steps.includes('recover'), 'the re-anchor was carried out after the arrival, not inside it');
    assert.equal(sockets[0].closed, true, 'the old socket was abandoned');
    assert.notEqual(
      supervisor.appliedBoundary.connectionId,
      'run-1:kraken:kraken_spot:1',
      'a new connection now owns the board',
    );
    assert.equal(supervisor.pending.refetch, null, 'and the request is cleared once carried out');

    const reconnect = timers.filter((timer) => !timer.cleared).at(-1);
    reconnect.fn();
    assert.equal(sockets.length, 2, 'the replacement socket opens on the reconnect timer');
    assert.deepEqual(exits, [], 'the recovery is not a failure');
    supervisor.stop();
    supervisor.close();
  });
});

test('a stop raised by a socket arrival is carried out after that arrival, ending the run non-zero', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const exits = [];
    const stops = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      // The raw refuses and there is no spool: nothing can hold the frame, so reception stops and onStop
      // fires from inside the arrival's own handling.
      rawWriter: () => false,
      spoolDir: null,
      exit: (code) => exits.push(code),
      onStop: (stop) => stops.push(stop),
      onRefetch: () => {},
      onGap: () => {},
    });

    supervisor.start();
    assert.equal(sockets.length, 1, 'the socket opened with the run');
    assert.deepEqual(exits, [], 'nothing has ended yet');

    // The frame arrives through the socket and raises the stop inside the frame. No supervisor call is
    // made after this line: the exit below must be the arrival's own doing.
    sockets[0].onmessage({ data: '{"seq":1}' });

    assert.ok(stops.length >= 1, 'the stop reached onStop');
    assert.deepEqual(exits, [1], 'and it was carried out once the arrival ended');
    assert.equal(supervisor.stopped, true);
    assert.equal(supervisor.pending.stop, null, 'the request is no longer pending');
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// A request raised inside the startup sequence waits for the whole sequence: §5.7 wants it carried out
// after "the current operation", and the operation the supervisor is running is the startup sequence
// itself - not the (d) step it happened to be inside when the notification arrived.
// ---------------------------------------------------------------------------

test('a re-anchor raised during the startup sequence is carried out only after start() returns', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    // The board follows CONNECTION, and an owed frame for a connection that is gone makes (d) redelivery
    // report a gap - the notification a restart reacts to.
    seedBoundaryWithGoneOwed(path);
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const exits = [];
    const steps = [];
    let recoverSeenBeforeStart = null;
    let supervisor;
    supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: (code) => exits.push(code),
      onStep: (s) => {
        // Was a re-anchor already carried out by the time the sequence reached its own (e)?
        if (s.step === 'start') recoverSeenBeforeStart = steps.includes('recover');
        steps.push(s.step);
      },
      onStop: () => {},
      onRefetch: () => {},
      // (d) redelivery meets the owed frame of the gone connection and reports it; asking for a re-anchor
      // here is a request that arrives from inside the sequence's own step.
      onGap: () => {
        supervisor.requestRefetch({ connectionId: CONNECTION });
      },
    });

    const started = supervisor.start();

    assert.equal(started.started, true, 'the sequence completed and its own connection was admitted');
    assert.equal(
      recoverSeenBeforeStart,
      false,
      'no re-anchor was carried out while the sequence was still running',
    );
    assert.deepEqual(
      steps,
      ['beginRun', 'restore', 'drain', 'redeliver', 'start', 'recover'],
      'the re-anchor is carried out after the sequence, not in the middle of (d)',
    );
    // The sequence issued exactly one connection (generation 1), and the re-anchor - carried out only once
    // start() had returned - replaced it exactly once (generation 2). A re-anchor run from inside (d) would
    // have advanced the connection before (e) and left (e) to issue over it.
    assert.equal(started.connectionId, 'run-1:kraken:kraken_spot:1', 'the sequence opened its own connection');
    assert.equal(
      supervisor.connection.connectionId,
      'run-1:kraken:kraken_spot:2',
      'exactly one re-anchor followed, so no connection was issued over the sequence',
    );
    assert.equal(sockets.length, 1, 'no second socket was opened while start() was still in flight');
    assert.equal(supervisor.pending.refetch, null, 'the request was carried out, not left pending');
    assert.deepEqual(exits, [], 'the re-anchor is not a failure');
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// (c) in full: one bounded walk is not a finished drain, and a spool that cannot be read or saved is a
// stop rather than something the sequence carries on past. The old connection retires the moment the new
// one is admitted, so anything left in the spool here would never be read again.
// ---------------------------------------------------------------------------

test('a restart drains the old spool to empty before handing the board to the new run', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    seedBoundaryWithSpool(path, spoolDir, []);
    // One record more than a single bounded walk consumes (the walk's own default limit is 512).
    writeSpoolSegment(
      spoolDir,
      Buffer.concat(Array.from({ length: 513 }, (_, i) => frame(encodeEnvelope(envelope(i + 1))))),
    );

    const exits = [];
    let writes = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-2',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => {
        writes += 1;
        return true;
      },
      spoolDir,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.equal(started.started, true, 'the old spool is drained and the new run starts');
    assert.equal(writes, 513, 'every spilled frame is walked out, not only the first batch');
    assert.equal(supervisor.spool.bytes, 0, 'nothing is left for a retired connection to have carried');
    assert.deepEqual(exits, [], 'a drained spool is not a failure');
    supervisor.close();
  });
});

test('a save that throws while draining the old spool ends the run non-zero and writes no completion', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    seedBoundaryWithSpool(path, spoolDir, []);
    writeSpoolSegment(spoolDir, frame(encodeEnvelope(envelope(1))));
    // A directory where the cursor file belongs makes the real cursor save fail with EISDIR, which is a
    // genuine save failure on the drain's own path - not a route opened for the test.
    mkdirSync(join(spoolDir, 'cursor'));

    const exits = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-2',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true, 'a drain that could not be saved is not a start');
    assert.deepEqual(exits, [1], 'the failure reached the stop path, not the caller');
    assert.equal(supervisor.abnormal, true);
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null, 'the failed run is not recorded as complete');
    reader.close();
  });
});

test('a corrupt length in the old spool ends the run non-zero instead of being read as a finished drain', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const spoolDir = join(dir, 'spool');
    seedBoundaryWithSpool(path, spoolDir, []);
    // A length the framing can never have written: the segment desynchronised, and nothing after it can
    // be trusted. The walk must stop on it, not report it as the end of the spool.
    writeSpoolSegment(spoolDir, Buffer.from([255, 255, 255, 255, 0, 0, 0, 0]));

    const exits = [];
    const diagnostics = [];
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-2',
      webSocketImpl: fakeSockets().impl,
      rawWriter: () => true,
      spoolDir,
      exit: (code) => exits.push(code),
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    });

    const started = supervisor.start();
    assert.notEqual(started.started, true, 'a corrupt segment is not a finished drain');
    assert.deepEqual(exits, [1]);
    assert.equal(supervisor.abnormal, true);
    assert.ok(supervisor.spool.bytes > 0, 'the unreadable bytes are left where they are');
    assert.equal(
      diagnostics.filter((diagnostic) => /spool could not hand back/.test(String(diagnostic.reason))).length,
      1,
      'the corruption is reported once',
    );
    supervisor.close();

    const reader = openDurability({ path, runId: 'reader-1' });
    assert.equal(reader.lastCompleteRun(), null, 'the failed run is not recorded as complete');
    reader.close();
  });
});

// ---------------------------------------------------------------------------
// (e): reception that is never reached has a deadline. The reconnect backoff alone would run for ever,
// which is the quiet liveness this set exists to remove.
// ---------------------------------------------------------------------------

test('a connection that never opens ends the run non-zero at the startup deadline', async () => {
  await withDir(async (dir) => {
    const exits = [];
    const timers = [];
    let time = 0;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path: join(dir, 'state.sqlite'),
      venue: 'kraken',
      runId: 'run-2',
      // Every socket attempt fails: the venue is unavailable and stays that way.
      webSocketImpl: function unavailable() {
        throw new Error('venue unavailable');
      },
      rawWriter: () => true,
      exit: (code) => exits.push(code),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: () => {},
      wallClockMs: () => time,
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    const started = supervisor.start();
    assert.equal(started.started, true, 'the sequence itself completed');
    // Drive the clock and every timer for far longer than the deadline. The reconnect backoff keeps
    // scheduling attempts; the deadline must end the run rather than let that continue for ever.
    for (let i = 0; i < 40; i += 1) {
      time += 3600_000;
      timers.shift()?.fn();
    }
    assert.equal(supervisor.ended, true, 'reception that is never reached ends the run');
    assert.equal(supervisor.abnormal, true);
    assert.deepEqual(exits, [1], 'and it ends non-zero');
    supervisor.close();
  });
});

test('a run that reached reception is not ended by its startup deadline', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    const exits = [];
    let time = 1_792_000_000_000;
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      exit: (code) => exits.push(code),
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      wallClockMs: () => time,
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    supervisor.start();
    // A frame anchors and serves the board: the run reached reception.
    sockets[0].onmessage({ data: '{"seq":1}' });
    assert.equal(supervisor.ready, true);

    // Now let the deadline fire long after it would have. A run that already received is not a startup
    // that never completed, so the deadline is spent and must do nothing.
    const deadline = timers[0];
    time += 3600_000;
    deadline.fn();
    assert.deepEqual(exits, [], 'a run that already served is not ended by the startup deadline');
    assert.equal(supervisor.ended, false);
    supervisor.stop();
    supervisor.close();
  });
});

// ---------------------------------------------------------------------------
// The generation issuer is the supervisor's, not the connection's: the run's connections must number from
// one shared source or two of them collide on the name run:venue:market:generation (C2). The test hands
// the issuer to the supervisor only - never to a connection - and pins the wiring that carries it there.
// ---------------------------------------------------------------------------

test('the supervisor numbers the connections of a run from one shared issuer, not from each connection', async () => {
  await withDir(async (dir) => {
    const path = join(dir, 'state.sqlite');
    const { sockets, impl } = fakeSockets();
    const timers = [];
    // The run's shared issuer. It is given to the supervisor; the supervisor must pass it to the
    // connection it builds. Without that wiring the connection would number itself, and a second
    // connection of the same run - built here with the same issuer - would collide with it.
    let issued = 0;
    const issueGeneration = () => (issued += 1);
    const supervisor = createSupervisor({
      market: 'kraken_spot',
      stream: 'trades',
      adapter,
      path,
      venue: 'kraken',
      runId: 'run-1',
      webSocketImpl: impl,
      rawWriter: () => true,
      spoolDir: join(dir, 'spool'),
      issueGeneration,
      setTimer: (fn, ms) => {
        const timer = { fn, ms, cleared: false, unref() {} };
        timers.push(timer);
        return timer;
      },
      clearTimer: (timer) => {
        timer.cleared = true;
      },
      exit: () => {},
      onStop: () => {},
      onRefetch: () => {},
      onDiagnostic: () => {},
    });

    supervisor.start();
    assert.equal(issued, 1, 'the supervisor used the run issuer for the connection it built');
    const firstId = supervisor.connection.connectionId;

    // A second connection of the same run, sharing the run issuer exactly as the supervisor's did. If the
    // supervisor had not wired its issuer through, its connection would have numbered itself and this one
    // would reuse the same name.
    const second = createReceiveConnection({
      adapter,
      market: 'kraken_spot',
      runId: 'run-1',
      venue: 'kraken',
      webSocketImpl: impl,
      issueGeneration,
      onGeneration: (info) => {
        info.settle?.(true);
        return true;
      },
    });
    second.start();
    assert.notEqual(second.connectionId, firstId, 'the two connections of the run do not share a name');
    assert.equal(issued, 2, 'and they numbered from the same run issuer');

    supervisor.stop();
    supervisor.close();
  });
});

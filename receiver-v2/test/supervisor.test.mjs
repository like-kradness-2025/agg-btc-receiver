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
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';
import { createSupervisor } from '../src/supervisor/supervisor.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';

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
    assert.deepEqual(steps, ['beginRun', 'restore', 'drain', 'start'], 'the sequence is carried out in order');
    assert.equal(sockets.length, 1, 'and the socket opens only after the sequence is done');
    assert.equal(supervisor.ready, true);
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
    assert.equal(supervisor.ready, true);
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

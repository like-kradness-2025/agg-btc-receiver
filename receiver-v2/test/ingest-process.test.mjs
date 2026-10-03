/**
 * Stage 2: reception as its own process, speaking to a fake organize peer over the real IPC transport.
 *
 * These tests drive `src/ingest/main.mjs` - the ingest process entrance - against a test-support peer
 * that listens on a unix socket with `src/ipc.mjs`. Nothing here reaches into product code through a
 * test-only route: the frames arrive at a fake socket, the control messages travel on the stage-1
 * vocabulary, and the spool writes to a real directory.
 *
 * What each group fixes (the task's ①-⑥):
 *   ① C2: the accept is issued inside the real generation, an explicit takeover is carried, an old
 *      instance's answer is ignored, and only an acceptance opens the socket.
 *   ② C3: a refused subscription and an unanswered ack deadline reach organize over IPC.
 *   ③ C4: the three keep-alive forms keep their meaning through the ingest process.
 *   ④ the receive tail is written to the ingest's own store.
 *   ⑤ the spool is owned by ingest: append, oldest-first resend, and cursor advance plus segment
 *      deletion after organize acknowledges.
 *   ⑥ the child restart id and the receive run / connection generation are separate.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { openIngestProcess } from '../src/ingest/main.mjs';
import { DEFAULT_ACK_DEADLINE_MS } from '../src/ingest/connection.mjs';
import { startFakeOrganize } from '../test-support/fake-organize.mjs';

const MARKET = 'kraken_spot';
const STREAM = 'trades';
const VENUE = 'kraken';

const dataAdapter = {
  url: 'ws://venue.test/ws',
  stream: STREAM,
  parse: () => ({ kind: 'data' }),
  // Level changes are derived on reception (ruling ③) and ride on the envelope. An explicit empty
  // diff is a valid derivation; an absent one would be refused.
  changesFor: () => ({ replace: false, changes: [] }),
  subscribeMessages: () => ['{"subscribe":"trades"}'],
  heartbeatMessage: () => null,
};

/** Wait for a condition, failing rather than hanging forever. */
async function until(predicate, { timeoutMs = 4000, stepMs = 5 } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error('timed out waiting for the condition');
}

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
      deliver(data) {
        socket.onmessage?.({ data });
      },
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

/** Build one ingest process against an existing fake organize peer and directory. */
async function buildIngest({ dir, organize, label = 'a', ...options }) {
  const { sockets, impl } = fakeSockets();
  const timers = [];
  const stops = [];
  const diagnostics = [];
  const gaps = [];
  const process = await openIngestProcess({
    market: MARKET,
    stream: STREAM,
    adapter: options.adapter ?? dataAdapter,
    venue: VENUE,
    runId: options.runId ?? 'run-1',
    roleInstance: options.roleInstance ?? `ingest-${options.runId ?? 'run-1'}-${label}`,
    takeoverFor: options.takeoverFor ?? (() => false),
    webSocketImpl: impl,
    organizeSocketPath: organize.server.path,
    ingestStorePath: join(dir, `ingest-${label}.sqlite`),
    spoolDir: options.spoolDir ?? join(dir, `spool-${label}`),
    spoolOptions: options.spoolOptions ?? {},
    channelOptions: { batchFrames: 1 },
    setTimer: (fn, ms) => {
      const timer = { fn, ms, cleared: false, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => {
      timer.cleared = true;
    },
    onStop: (stop) => stops.push(stop),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    onGap: (gap) => gaps.push(gap),
  });
  const fireByDelay = (ms) => {
    const timer = timers.find((entry) => entry.ms === ms && !entry.cleared);
    assert.ok(timer, `a timer of ${ms} ms was armed`);
    timer.cleared = true;
    timer.fn();
    return timer;
  };
  const fireAll = () => {
    for (const timer of timers.filter((entry) => !entry.cleared).sort((a, b) => a.ms - b.ms)) {
      timer.cleared = true;
      timer.fn();
    }
  };
  return { process, sockets, timers, stops, diagnostics, gaps, fireByDelay, fireAll };
}

/** A whole world: a fresh directory, a fake peer, and one ingest process. */
async function setup(options = {}) {
  const dir = options.dir ?? mkdtempSync(join(tmpdir(), 'ingest-proc-'));
  const organize = options.organize ?? (await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 }));
  const ingest = await buildIngest({ dir, organize, label: options.label ?? 'a', ...options });
  const teardown = async () => {
    ingest.process.close();
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  };
  return { dir, organize, ...ingest, teardown };
}

/** Adopt an existing world (shared peer), for the multi-process takeover test. */
async function withWorld(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'ingest-world-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  try {
    return await fn({ dir, organize });
  } finally {
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------------------------------
// ① C2: generation, accept, explicit takeover, old-instance rejection
// ---------------------------------------------------------------------------------------------------

test('C2: the accept is issued inside the real generation, and only an acceptance opens the socket', async () => {
  const h = await setup();
  try {
    h.process.start();
    assert.equal(h.sockets.length, 0, 'no socket is opened before the acceptance is heard');

    await until(() => h.sockets.length === 1);
    const accept = h.organize.state.accepts[0];
    assert.equal(accept.type, 'accept', 'the generation announcement produced an accept');
    assert.equal(accept.generation, 1, 'the first generation was announced');
    assert.equal(accept.connection_id, 'run-1:kraken:kraken_spot:1', 'named run:venue:market:generation (C2)');
    assert.equal(accept.role_instance, h.process.roleInstance, 'carrying the ingest instance id');
    assert.equal(accept.payload.takeover, false, 'a first run asserts no takeover');

    h.sockets[0].onopen();
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'the socket opened only after acceptance');
    assert.equal(h.process.connectionId, 'run-1:kraken:kraken_spot:1');
    assert.equal(h.process.generation, 1);
  } finally {
    await h.teardown();
  }
});

test('C2: a different run needs an explicit takeover, and a stale instance is refused', async () => {
  await withWorld(async ({ dir, organize }) => {
    // run-1 takes the board.
    const first = await buildIngest({ dir, organize, label: 'r1', runId: 'run-1' });
    first.process.start();
    await until(() => first.sockets.length === 1);
    first.process.close();

    // run-2 without an explicit takeover is refused: nothing opens, and the refusal is reported.
    const noTakeover = await buildIngest({ dir, organize, label: 'no', runId: 'run-2', takeoverFor: () => false });
    noTakeover.process.start();
    await until(() => noTakeover.stops.length === 1);
    assert.equal(noTakeover.sockets.length, 0, 'a run with no takeover opens no socket');
    assert.match(String(noTakeover.stops[0].reason), /not admitted/i, 'and the refusal is not silent');
    noTakeover.process.close();

    // The same run-2 with an explicit takeover is admitted.
    const takeover = await buildIngest({ dir, organize, label: 'take', runId: 'run-2', takeoverFor: () => true });
    takeover.process.start();
    await until(() => takeover.sockets.length === 1);
    const accept = organize.state.accepts.at(-1);
    assert.equal(accept.payload.takeover, true, 'the takeover was explicit on the wire');
    assert.equal(takeover.process.connectionId, 'run-2:kraken:kraken_spot:1');
    takeover.process.close();

    // A second instance of that same run, at the same generation, is an old instance: refused.
    const stale = await buildIngest({ dir, organize, label: 'stale', runId: 'run-2', takeoverFor: () => true });
    stale.process.start();
    await until(() => stale.stops.length === 1);
    assert.equal(stale.sockets.length, 0, 'an old instance of an owned board opens no socket');
    assert.match(String(stale.stops[0].reason), /old instance/i);
    stale.process.close();
  });
});

test('C2: an acceptance for a generation this process is not waiting on is ignored', async () => {
  const h = await setup();
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    const connectionId = h.process.connectionId;

    // A stale answer for the generation that already settled: nothing may act on it.
    h.organize.reply({
      version: 1,
      type: 'accepted',
      role_instance: 'organize-1',
      request_id: `${h.process.roleInstance}:accept:${connectionId}:1`,
      connection_id: connectionId,
      generation: 1,
      payload: { accepted: true },
    });
    await until(() => h.diagnostics.some((d) => /ignored an acceptance/.test(d.reason)));
    assert.equal(h.sockets.length, 1, 'the stale answer opened nothing');

    // A reconnect issues a new generation; the peer admits it, and the connection name moves. The
    // replacement is scheduled behind a backoff, and that timer is armed only once the acceptance for
    // the new generation is heard, so the test keeps firing until the socket is there.
    h.sockets[0].onclose();
    for (let i = 0; i < 200 && h.sockets.length < 2; i += 1) {
      h.fireAll();
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(h.sockets.length, 2, 'the reconnected generation opened a new socket');
    assert.equal(h.process.generation, 2);
    assert.equal(h.process.connectionId, 'run-1:kraken:kraken_spot:2');

    // An answer for the old generation, arriving after the new one is current, is still ignored.
    h.organize.reply({
      version: 1,
      type: 'accepted',
      role_instance: 'organize-1',
      request_id: `${h.process.roleInstance}:accept:${connectionId}:1`,
      connection_id: connectionId,
      generation: 1,
      payload: { accepted: true },
    });
    await until(() => h.diagnostics.filter((d) => /ignored an acceptance/.test(d.reason)).length >= 2);
    assert.equal(h.process.generation, 2, 'the live generation did not move backwards');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ② C3: a failed subscription reaches organize over IPC
// ---------------------------------------------------------------------------------------------------

test('C3: a refused subscription travels to organize as readiness (failed) and an error', async () => {
  const adapter = {
    ...dataAdapter,
    parse: (raw) => {
      const parsed = JSON.parse(String(raw));
      if (parsed.rejected !== undefined) {
        return { kind: 'subscription', key: parsed.rejected, ok: false, detail: 'denied' };
      }
      return { kind: 'data' };
    },
  };
  const h = await setup({ adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"rejected":"trades"}');

    await until(() =>
      h.organize.state.controls.some((m) => m.type === 'readiness' && m.payload?.state === 'failed'),
    );
    const readiness = h.organize.state.controls.find((m) => m.type === 'readiness' && m.payload?.state === 'failed');
    assert.equal(readiness.payload.ready, false, 'the failure is not readiness');
    assert.equal(readiness.payload.reason, 'denied', 'and it carries the venue reason');
    assert.match(String(readiness.payload.connection_id), /run-1:kraken:kraken_spot:1/);

    const error = h.organize.state.controls.find((m) => m.type === 'error');
    assert.ok(error, 'the reason also travels on the error path');
    assert.equal(error.payload.reason, 'denied');
    assert.equal(h.process.subscriptionState, 'failed');
  } finally {
    await h.teardown();
  }
});

test('C3: an unanswered subscription that passes the ack deadline reports itself over IPC', async () => {
  const adapter = { ...dataAdapter, expectedSubscriptions: () => ['trades'] };
  const h = await setup({ adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.fireByDelay(DEFAULT_ACK_DEADLINE_MS);

    await until(() =>
      h.organize.state.controls.some((m) => m.type === 'readiness' && m.payload?.state === 'failed'),
    );
    const readiness = h.organize.state.controls.find((m) => m.type === 'readiness' && m.payload?.state === 'failed');
    assert.match(String(readiness.payload.reason), /ack deadline/i);
    assert.equal(h.process.subscriptionState, 'failed');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ③ C4: the three keep-alive forms keep their meaning through the ingest process
// ---------------------------------------------------------------------------------------------------

test('C4: the interval form sends on its rhythm, not on open', async () => {
  const adapter = { ...dataAdapter, keepAlive: () => ({ intervalMs: 20_000, payload: () => '{"ping":1}' }) };
  const h = await setup({ adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'opening sends no ping');
    h.fireByDelay(20_000);
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}'], 'the ping is one interval in');
  } finally {
    await h.teardown();
  }
});

test('C4: the no-activity form sends only after real silence', async () => {
  const adapter = { ...dataAdapter, keepAlive: () => ({ noActivityMs: 30_000, payload: () => '{"ping":1}' }) };
  const h = await setup({ adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'nothing is sent while the link is active');
    h.fireByDelay(30_000);
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}'], 'silence sends one ping');
  } finally {
    await h.teardown();
  }
});

test('C4: the null form sends nothing at all - never an empty frame', async () => {
  const adapter = { ...dataAdapter, heartbeatMessage: () => null };
  const h = await setup({ adapter });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.fireAll();
    assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'a null keep-alive sends nothing');
    assert.equal(h.sockets[0].sent.includes(''), false, 'the old empty frame is gone');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ④ received_tail is written to the ingest's own store
// ---------------------------------------------------------------------------------------------------

test('④ the receive tail is written to the ingest store, and survives as a row on disk', async () => {
  const h = await setup({ label: 'tail' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    for (let seq = 1; seq <= 3; seq += 1) h.sockets[0].deliver(`{"seq":${seq}}`);

    await until(() => h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM)?.lastReceivedSeq === 3);
    const tail = h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM);
    assert.equal(tail.market, MARKET);
    assert.equal(tail.stream, STREAM, 'the stream is part of the key');
    assert.ok(tail.lastRecvMonoNs > 0);

    // Read the store's own file: the assertion is about what was persisted, not what an API reports.
    h.process.stop();
    const db = new DatabaseSync(join(h.dir, 'ingest-tail.sqlite'));
    const rows = db.prepare('SELECT * FROM received_tail').all();
    db.close();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].connection_id, h.process.connectionId);
    assert.equal(rows[0].last_received_seq, 3);
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑤ spool ownership: append, oldest-first resend, cursor advance and deletion after ACK
// ---------------------------------------------------------------------------------------------------

test('⑤ the spool is ingest-owned: append, oldest-first resend, and cursor advance with deletion', async () => {
  const h = await setup({ label: 'spool', spoolOptions: { segmentBytes: 700 } });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    const connectionId = h.process.connectionId;

    // Organize reports it has no room: from here frames cannot be handed on and go to the spool.
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.organizeCapacity === 'full');

    for (let seq = 1; seq <= 8; seq += 1) h.sockets[0].deliver(`{"seq":${seq}}`);
    await until(() => h.process.stats.spooledFrames === 8);
    assert.ok(h.process.spool.bytes > 0, 'the frames are held in the spool');
    const segmentCount = h.process.spool.segments.length;
    assert.ok(segmentCount > 1, 'the small segments forced a rotation');
    assert.equal(h.organize.state.envelopes.length, 0, 'nothing was handed on while there was no room');

    // Capacity is back: the spool is offered again, oldest first.
    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length === 8);
    assert.deepEqual(
      h.organize.state.envelopes.map((e) => e.receive_seq),
      [1, 2, 3, 4, 5, 6, 7, 8],
      'the resend is oldest segment first, in write order',
    );

    // Organize acknowledges the contiguous ceiling: the cursor moves and the segments are deleted.
    h.organize.sendDurableAck({ connectionId, generation: 1, upToSeq: 8 });
    await until(() => h.process.spool.bytes === 0);
    assert.deepEqual(h.process.spool.segments, [], 'every fully-consumed segment was deleted');
    assert.equal(h.process.spool.cursor.segment, segmentCount + 1, 'the cursor sits past the last consumed segment');
  } finally {
    await h.teardown();
  }
});

// ---------------------------------------------------------------------------------------------------
// ⑥ child restart id is separate from the receive run / generation
// ---------------------------------------------------------------------------------------------------

test('⑥ a peer restart changes neither the receive generation nor the run', async () => {
  const h = await setup({ runId: 'run-1', roleInstance: 'ingest-a' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    const generation = h.process.generation;
    const connectionId = h.process.connectionId;

    // A book restart, announced as a new instance of that role, is a fact about the book - not
    // about reception.
    h.organize.sendHello({ role: 'book', instance: 'book-2' });
    await until(() => h.process.peerInstances.get('book') === 'book-2');
    h.organize.sendHello({ role: 'book', instance: 'book-3' });
    await until(() => h.process.peerInstances.get('book') === 'book-3');

    assert.equal(h.process.generation, generation, 'the receive generation did not move');
    assert.equal(h.process.connectionId, connectionId, 'nor did the connection name');
    assert.equal(h.process.runId, 'run-1', 'nor the receive run');
    assert.equal(h.process.roleInstance, 'ingest-a', 'nor the ingest instance id');
    assert.equal(h.process.peerInstances.get('book'), 'book-3', 'only the peer instance changed');
  } finally {
    await h.teardown();
  }
});

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
import fs from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { createIngestProcess, openIngestProcess } from '../src/ingest/main.mjs';
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
    ackStallMs: options.ackStallMs,
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
    onDiagnostic: options.onDiagnostic ?? ((diagnostic) => diagnostics.push(diagnostic)),
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

test('the open ingest factory rejects a deferred executor before connecting its IPC channel', async () => {
  await withWorld(async ({ dir, organize }) => {
    await assert.rejects(
      openIngestProcess({
        market: MARKET,
        stream: STREAM,
        adapter: dataAdapter,
        venue: VENUE,
        runId: 'run-1',
        webSocketImpl: fakeSockets().impl,
        organizeSocketPath: organize.server.path,
        ingestStorePath: join(dir, 'ingest.sqlite'),
        spoolDir: join(dir, 'spool'),
        onEvent: (_label, work) => queueMicrotask(work),
      }),
      /deferred receive-event executor/,
    );
    assert.equal(organize.server.channels.size, 0, 'refusal leaves no unowned IPC channel behind');
  });
});

test('the ingest factory rejects deferred receive executors without a drain contract', () => {
  const dir = mkdtempSync(join(tmpdir(), 'ingest-event-executor-'));
  let process = null;
  try {
    assert.throws(
      () => {
        process = createIngestProcess({
          market: MARKET,
          stream: STREAM,
          adapter: dataAdapter,
          venue: VENUE,
          runId: 'run-1',
          webSocketImpl: fakeSockets().impl,
          ingestStorePath: join(dir, 'ingest.sqlite'),
          spoolDir: join(dir, 'spool'),
          onEvent: (_label, work) => queueMicrotask(work),
        });
      },
      /deferred receive-event executor/,
      'the ingest owner cannot fence an executor whose accepted work it cannot drain',
    );
  } finally {
    process?.close();
    rmSync(dir, { recursive: true, force: true });
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

test('a quiesce begun from an accepted receive callback keeps its tail before the socket fence', async () => {
  const h = await setup({ label: 'quiesce-race' });
  let quiescePromise = null;
  try {
    const updateReceivedTail = h.process.ingestStore.updateReceivedTail;
    h.process.ingestStore.updateReceivedTail = (tail) => {
      // This is the strongest callback/stop interleaving available on the synchronous fork path: the
      // receive callback has entered, and quiescence begins before its durable tail write completes.
      quiescePromise = h.process.quiesce();
      return updateReceivedTail(tail);
    };

    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');

    const received = h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM);
    assert.equal(received?.lastReceivedSeq, 1, 'the accepted callback finishes its tail after the fence begins');
    assert.ok(quiescePromise instanceof Promise, 'quiescence settles after synchronous receive work returns');
    await quiescePromise;
    await until(() => h.organize.state.envelopes.length === 1, 'the already-accepted frame reaches organize');
    assert.equal(h.sockets[0].closed, true, 'the active socket was fenced');
    assert.equal(h.organize.state.envelopes.length, 1, 'the already-accepted frame is retained by organize');
    assert.equal(h.organize.state.envelopes[0].receive_seq, 1, 'the retained frame matches the tail');
    assert.deepEqual(h.process.start(), { started: false, reason: 'this ingest process has stopped' });
  } finally {
    await h.teardown();
  }
});

test('a throwing tail diagnostic cannot discard a frame the organize link can take', async () => {
  const h = await setup({
    label: 'tail-diagnostic-failure',
    onDiagnostic: () => {
      throw new Error('injected diagnostic failure');
    },
  });
  try {
    h.process.ingestStore.updateReceivedTail = () => {
      throw new Error('injected tail write failure');
    };

    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    assert.doesNotThrow(() => h.sockets[0].deliver('{"seq":1}'), 'diagnostic failure does not escape receive handling');
    await until(() => h.organize.state.envelopes.length === 1);
    assert.equal(h.organize.state.envelopes[0].receive_seq, 1, 'the accepted frame still reaches organize');
    assert.equal(await h.process.sealTails(), null, 'the failed durable tail still forbids a candidate');
  } finally {
    await h.teardown();
  }
});

test('a durable ceiling that stops moving while frames are sent is reported, and an advance clears it', async () => {
  const h = await setup({ label: 'ack-stall', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);
    h.sockets[0].deliver('{"seq":2}');
    await until(() => h.organize.state.envelopes.length === 2);

    // The progress deadline is the timer that turns "nothing was acknowledged" into a report and a
    // resend. Nothing acknowledges these frames, so firing it is the expiry.
    h.fireByDelay(60);
    const report = h.diagnostics.find((d) => /durable ceiling has stalled/.test(String(d.reason)));
    assert.ok(report, 'the expired deadline reports the stall');
    assert.equal(report.framesSinceProgress, 2, 'both sent frames are counted as sent since the last advance');
    await until(() => h.organize.state.envelopes.length === 4, { timeoutMs: 2000 });
    assert.equal(h.organize.state.envelopes.length, 4, 'the expired deadline asks for a resend of what is unacknowledged');

    // An acknowledgement of everything so far clears the clock and the pending deadline; the clean
    // run then re-arms it only for frames sent after it.
    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 2 });
    await until(() => h.process.stats.lastAckUpToSeq === 2);
    h.sockets[0].deliver('{"seq":3}');
    await until(() => h.process.stats.sentFrames === 3);
    assert.equal(h.process.stats.framesSinceProgress, 1, 'the clock restarts with the frame sent after the advance');
    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 3 });
    await until(() => h.process.stats.lastAckUpToSeq === 3);
    assert.equal(h.process.stats.framesSinceProgress, 0, 'the advance reset the stall clock');
    assert.throws(() => h.fireByDelay(60), /a timer of 60 ms was armed/, 'the pending deadline was cleared');
  } finally {
    await h.teardown();
  }
});

test('a partial acknowledgement leaves the rest stalled and reported', async () => {
  const h = await setup({ label: 'stall-partial', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);
    h.sockets[0].deliver('{"seq":2}');
    await until(() => h.organize.state.envelopes.length === 2);

    // Only the first frame is acknowledged: the second is still retained, so the deadline fires
    // again for it instead of the clock dying with the partial advance.
    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.stats.lastAckUpToSeq === 1);
    h.fireByDelay(60);
    const report = h.diagnostics.find((d) => /durable ceiling has stalled/.test(String(d.reason)));
    assert.ok(report, 'the deadline still fires while a frame stays unacknowledged');
    assert.equal(report.unreleasedFrames, 1, 'the report names what is still retained');
  } finally {
    await h.teardown();
  }
});

test('a stall that survives the retry budget stops reception instead of resending for ever', async () => {
  const h = await setup({ label: 'stall-limit', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);

    for (let attempt = 0; attempt < 8 && !h.process.stats.stopped; attempt += 1) {
      try {
        h.fireByDelay(60);
      } catch {
        break; // no deadline armed any more
      }
    }
    assert.equal(h.process.stats.stopped, true, 'the run stops rather than resending for ever');
    assert.ok(
      h.diagnostics.filter((d) => /durable ceiling has stalled/.test(String(d.reason))).length >= 6,
      'every expiry was reported before the stop',
    );
  } finally {
    await h.teardown();
  }
});

test('sealing does not stop the drain: a stalled acknowledgement is still detected while sealed', async () => {
  const h = await setup({ label: 'sealed-stall', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);
    assert.equal(await h.process.sealTails(), null, 'the retained frame refuses a candidate');

    // The deadline keeps its life through quiescence: the sealed process still owes its drain.
    h.fireByDelay(60);
    assert.ok(
      h.diagnostics.some((d) => /durable ceiling has stalled/.test(String(d.reason))),
      'the sealed drain is still watched',
    );
    await until(() => h.organize.state.envelopes.length === 2, { timeoutMs: 2000 });
  } finally {
    await h.teardown();
  }
});

test('a resend window continues past its bound instead of re-sending the head', async () => {
  const h = await setup({ label: 'resend-window' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    await until(() => h.organize.channel !== null);
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    for (let seq = 1; seq <= 300; seq += 1) h.sockets[0].deliver(`{"seq":${seq}}`);
    await until(() => h.process.stats.spooledFrames === 300);

    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length === 256, { timeoutMs: 4000 });
    // The window was full: the continuation timer carries on from where it stopped, so the frames
    // past the 256th arrive next - and the head is never sent twice.
    h.fireByDelay(0);
    await until(() => h.organize.state.envelopes.length === 300, { timeoutMs: 4000 });
    assert.deepEqual(
      h.organize.state.envelopes.map((e) => e.receive_seq).slice(256),
      Array.from({ length: 44 }, (_, i) => 257 + i),
      'the second window resumes at the 257th frame',
    );
  } finally {
    await h.teardown();
  }
});

test('a fresh frame never overtakes one that is still held', async () => {
  const h = await setup({ label: 'no-overtaking' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    await until(() => h.organize.channel !== null);
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    for (let seq = 1; seq <= 300; seq += 1) h.sockets[0].deliver(`{"seq":${seq}}`);
    await until(() => h.process.stats.spooledFrames === 300);

    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length === 256, { timeoutMs: 4000 });
    // While the walk is parked between windows, a fresh frame arrives: it must wait for the frames
    // held in front of it, not jump the queue (the overtaking that opened a hole under the soak).
    h.sockets[0].deliver('{"seq":301}');
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(
      h.organize.state.envelopes.length,
      256,
      'the fresh frame did not overtake the 44 still held in front of it',
    );

    h.fireByDelay(0); // the continuation window carries on
    await until(() => h.organize.state.envelopes.length === 301, { timeoutMs: 4000 });
    assert.deepEqual(
      h.organize.state.envelopes.map((e) => e.receive_seq),
      Array.from({ length: 301 }, (_, i) => i + 1),
      'everything arrived in order, exactly once',
    );
  } finally {
    await h.teardown();
  }
});

test('a capacity return drains only what has never been sent', async () => {
  const h = await setup({ label: 'drain-no-dup' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    h.sockets[0].deliver('{"seq":2}');
    await until(() => h.organize.state.envelopes.length === 2);

    // Both frames are in flight (sent, not yet acknowledged). The link reports full and then ok:
    // the drain continues from where sending stopped - it does not re-send what is already out.
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    h.organize.sendReadiness({ capacity: 'ok' });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(h.organize.state.envelopes.length, 2, 'nothing already sent was sent twice');
  } finally {
    await h.teardown();
  }
});

test('a stalled ceiling is reported while the link is full, and recovers from the head when it opens', async () => {
  const h = await setup({ label: 'recover-full', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);

    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    h.fireByDelay(60);
    const stalled = h.diagnostics.find((d) => /durable ceiling has stalled/.test(String(d.reason)));
    assert.ok(stalled, 'the stall is reported even while the link is full');
    assert.equal(stalled.capacity, 'full', 'and the report names the link state');

    // The link opens: the recovery the stall wanted runs from the first unreleased record, so the
    // frame that never got its acknowledgement is offered again.
    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length >= 2, { timeoutMs: 2000 });
    assert.deepEqual(
      h.organize.state.envelopes.map((e) => e.receive_seq),
      [1, 1],
      'the recovery re-offers the unacknowledged frame from the head',
    );
  } finally {
    await h.teardown();
  }
});

test('retention that began while the link was full is still watched and recovers', async () => {
  const h = await setup({ label: 'full-first', ackStallMs: 60 });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    await until(() => h.organize.channel !== null);
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.process.stats.spooledFrames === 1);

    // The retention began while the link could take nothing: the deadline still has to arm, report,
    // and remember that a recovery is wanted.
    h.fireByDelay(60);
    const stalled = h.diagnostics.find((d) => /durable ceiling has stalled/.test(String(d.reason)));
    assert.ok(stalled, 'the deadline armed while the link was full and reported');
    assert.equal(stalled.capacity, 'full');

    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length >= 1, { timeoutMs: 2000 });
    assert.deepEqual(
      h.organize.state.envelopes.map((e) => e.receive_seq),
      [1],
      'the frame the link never took is offered when it opens',
    );
  } finally {
    await h.teardown();
  }
});

test('frames the link cannot take are reported once per episode, and a delivered frame closes it', async () => {
  const h = await setup({ label: 'spool-report' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    await until(() => h.organize.channel !== null);
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.process.stats.spooledFrames === 1);
    h.sockets[0].deliver('{"seq":2}');
    await until(() => h.process.stats.spooledFrames === 2);
    assert.equal(
      h.diagnostics.filter((d) => /held in the spool/.test(String(d.reason))).length,
      1,
      'the episode is reported once, not per frame',
    );

    // The link takes frames again: the resend delivers them, which closes the episode...
    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length === 2);

    // ...so the next frame the link refuses opens a new one.
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.stats.capacity === 'full');
    h.sockets[0].deliver('{"seq":3}');
    await until(() => h.process.stats.spooledFrames === 3);
    assert.equal(
      h.diagnostics.filter((d) => /held in the spool/.test(String(d.reason))).length,
      2,
      'a new episode is reported again',
    );
  } finally {
    await h.teardown();
  }
});

// Exercise the inline receive callback used by the fork, including a seal begun inside diagnostics.
function channelDiagnosticFailure(spoolMode) {
  const dir = mkdtempSync(join(tmpdir(), 'ingest-channel-diagnostic-'));
  const { sockets, impl } = fakeSockets();
  const controls = [];
  const diagnostics = [];
  const envelopes = [];
  let unavailable = spoolMode === 'unavailable';
  let refusing = spoolMode === 'refusing';
  let sealPromise = null;
  const channel = {
    sendControl(message) {
      controls.push(message);
      return true;
    },
    sendEnvelope() {
      throw new Error('injected channel send failure');
    },
  };
  const process = createIngestProcess({
    market: MARKET,
    stream: STREAM,
    adapter: dataAdapter,
    venue: VENUE,
    runId: 'run-1',
    webSocketImpl: impl,
    organizeChannel: channel,
    ingestStorePath: join(dir, 'ingest.sqlite'),
    spoolDir: spoolMode === 'absent' ? null : join(dir, 'spool'),
    spoolOptions: {
      freeSpace: () => refusing ? 0 : Infinity,
      fsModule: {
        ...fs,
        openSync: (...args) => {
          if (unavailable) throw new Error('injected spool unavailable');
          return fs.openSync(...args);
        },
      },
    },
    onDiagnostic(diagnostic) {
      diagnostics.push(diagnostic);
      if (diagnostic.reason.startsWith('the organize link refused a frame:')) {
        sealPromise = process.sealTails();
      }
      throw new Error('injected diagnostic failure');
    },
  });
  process.start();
  const accept = controls.find((message) => message.type === 'accept');
  process.handleControl({ ...accept, type: 'accepted', payload: { accepted: true } });
  assert.equal(sockets.length, 1);
  sockets[0].onopen();
  return {
    process, socket: sockets[0], diagnostics, envelopes,
    get sealPromise() { return sealPromise; },
    recover() {
      unavailable = false;
      refusing = false;
      channel.sendEnvelope = (envelope) => { envelopes.push(envelope); return true; };
    },
    close() {
      process.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

test('a throwing channel diagnostic still spools the frame and permits a candidate after durable ACK', async () => {
  const h = channelDiagnosticFailure('healthy');
  try {
    let deliveryError = null;
    try { h.socket.deliver('{"seq":1}'); } catch (error) { deliveryError = error; }
    assert.ok(h.sealPromise instanceof Promise, 'the diagnostic began sealing reentrantly');
    const candidate = await h.sealPromise;
    assert.equal(h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM)?.lastReceivedSeq, 1);
    assert.equal(h.process.stats.spooledFrames, 1, 'the accepted frame is retained despite both exceptions');
    const retained = [...h.process.spool.drain()];
    assert.equal(retained.length, 1);
    assert.equal(retained[0].receive_seq, 1);
    assert.equal(retained[0].connection_id, h.process.connectionId);
    assert.equal(deliveryError, null, 'the diagnostic exception is isolated');
    assert.equal(candidate, null, 'reentrant sealing sees the outstanding spool obligation');
    assert.deepEqual(h.diagnostics[0], { market: MARKET, reason: 'the organize link refused a frame: injected channel send failure' });

    h.recover();
    assert.deepEqual(h.process.drainSpool(), { resent: 1 });
    assert.equal(h.envelopes[0].receive_seq, 1);
    assert.equal(await h.process.sealTails(), null, 'resend alone does not release retention');
    h.process.handleControl({
      type: 'durable_ack',
      market: MARKET,
      stream: STREAM,
      run_id: 'run-1',
      connection_id: h.process.connectionId,
      generation: 1,
      payload: { up_to_seq: 1 },
    });
    assert.equal(h.process.spool.bytes, 0);
    const afterAck = await h.process.sealTails();
    assert.equal(afterAck?.spoolEmpty, true, 'successful fallback was not marked as lost');
    assert.equal(afterAck?.tails[0]?.lastReceivedSeq, 1);
  } finally {
    h.close();
  }
});

test('a throwing channel diagnostic with an absent spool keeps reentrant sealing failure sticky', async () => {
  const h = channelDiagnosticFailure('absent');
  try {
    let deliveryError = null;
    try { h.socket.deliver('{"seq":1}'); } catch (error) { deliveryError = error; }
    assert.ok(h.sealPromise instanceof Promise, 'the diagnostic began sealing reentrantly');
    const candidate = await h.sealPromise;
    assert.equal(h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM)?.lastReceivedSeq, 1);
    assert.equal(h.process.stats.spooledFrames, 0);
    assert.equal(h.process.spool?.bytes ?? 0, 0);
    assert.equal(candidate, null, 'a received but unretained frame must never yield a candidate');
    assert.deepEqual(h.diagnostics[0], { market: MARKET, reason: 'the organize link refused a frame: injected channel send failure' });
    assert.equal(deliveryError, null, 'the diagnostic exception is isolated');
    h.recover();
    assert.equal(await h.process.sealTails(), null, 'recovered channel and spool availability cannot erase an unretained frame');
  } finally {
    h.close();
  }
});

for (const spoolMode of ['unavailable', 'refusing']) {
  test(`a frame a ${spoolMode} spool cannot retain is never handed on, and sealing stays refused`, async () => {
    const h = channelDiagnosticFailure(spoolMode);
    try {
      let deliveryError = null;
      try { h.socket.deliver('{"seq":1}'); } catch (error) { deliveryError = error; }
      // Retention comes first: the link was never asked, so nothing reported it and nothing began
      // sealing reentrantly.
      assert.equal(h.diagnostics.filter((d) => /refused a frame/.test(String(d.reason))).length, 0);
      assert.equal(h.process.stats.spooledFrames, 0);
      assert.equal(h.process.spool?.bytes ?? 0, 0);
      assert.equal(h.process.stats.stopped, true, 'the ladder stopped reception');
      if (spoolMode === 'unavailable') {
        assert.match(deliveryError?.message ?? '', /injected spool unavailable/, 'retention reached the unavailable spool');
        assert.equal(h.process.spool.failed, null, 'candidate refusal requires sticky ingest failure even without spool.failed');
      } else {
        assert.equal(deliveryError, null, 'a refused record is the ladder, not an exception');
      }
      h.recover();
      assert.equal(await h.process.sealTails(), null, 'an unretained frame keeps sealing refused');
    } finally {
      h.close();
    }
  });
}

test('a receive-tail write failure stays sticky without dropping later handable frames', async () => {
  const h = await setup({ label: 'tail-failure' });
  try {
    const updateReceivedTail = h.process.ingestStore.updateReceivedTail;
    let failOnce = true;
    h.process.ingestStore.updateReceivedTail = (tail) => {
      if (failOnce) {
        failOnce = false;
        throw new Error('injected tail write failure');
      }
      return updateReceivedTail(tail);
    };

    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    h.sockets[0].deliver('{"seq":2}');

    await until(() => h.organize.state.envelopes.length === 2);
    assert.deepEqual(
      h.organize.state.envelopes.map((envelope) => envelope.receive_seq),
      [1, 2],
      'a failed tail write does not prevent either frame from reaching organize',
    );
    assert.equal(
      h.process.ingestStore.readReceivedTail(h.process.connectionId, STREAM)?.lastReceivedSeq,
      2,
      'a later successful tail write does not erase the earlier failure',
    );

    const candidate = await h.process.sealTails();
    assert.equal(candidate, null, 'a run with any tail-write failure has no final candidate');
  } finally {
    await h.teardown();
  }
});

test('a returned final-tail candidate and all of its tail entries are immutable', async () => {
  const h = await setup({ label: 'immutable-tail-candidate' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);

    // The retained frame is released by its acknowledgement: a nonempty spool refuses a candidate.
    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.spool.bytes === 0);

    const candidate = await h.process.sealTails();
    assert.equal(Object.isFrozen(candidate), true, 'the candidate object is immutable');
    assert.equal(Object.isFrozen(candidate.tails), true, 'the tail list is immutable');
    assert.equal(Object.isFrozen(candidate.tails[0]), true, 'each tail identity and position is immutable');
  } finally {
    await h.teardown();
  }
});

test('a frame with neither organizer nor spool retention cannot yield a final-tail candidate', async () => {
  const h = await setup({ label: 'unretained-frame', spoolOptions: { maxBytes: 0 } });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.organizeCapacity === 'full');
    h.sockets[0].deliver('{"seq":1}');

    await until(() => h.stops.length === 1);
    assert.equal(h.process.spool.bytes, 0, 'the refused frame left no local spool record');
    assert.equal(await h.process.sealTails(), null, 'a received-but-unretained frame forbids a final candidate');
  } finally {
    await h.teardown();
  }
});

test('a frame whose changes cannot be derived cannot yield a final-tail candidate', async () => {
  const h = await setup({
    label: 'unrepresentable-frame',
    adapter: { ...dataAdapter, changesFor: () => null },
  });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');

    await until(() => h.gaps.length === 1);
    assert.equal(h.organize.state.envelopes.length, 0, 'an un-derived frame was not handed to organize');
    assert.equal(h.process.spool.bytes, 0, 'an un-derived frame was not held by the spool');
    assert.equal(await h.process.sealTails(), null, 'an unretained frame forbids a final-tail candidate');
  } finally {
    await h.teardown();
  }
});

test('a failed spool write with no counted bytes cannot produce a final-tail candidate', async () => {
  const fsModule = { ...fs, writeSync: () => { throw new Error('injected spool write failure'); } };
  const h = await setup({ label: 'failed-spool-candidate', spoolOptions: { fsModule } });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.organizeCapacity === 'full');

    assert.throws(() => h.sockets[0].deliver('{"seq":1}'), /injected spool write failure/);
    assert.equal(h.process.spool.bytes, 0, 'the failed write counted no bytes');
    assert.notEqual(h.process.spool.failed, null, 'the spool records that it cannot safely accept data');
    assert.equal(await h.process.sealTails(), null, 'a failed, empty-looking spool is not an ACK-cleared spool');
  } finally {
    await h.teardown();
  }
});

test('a tail candidate waits for a durable ACK to release the local spool obligation', async () => {
  const h = await setup({ label: 'tail-spool-gate' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    const connectionId = h.process.connectionId;

    h.organize.sendReadiness({ capacity: 'full' });
    await until(() => h.process.organizeCapacity === 'full');
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.process.stats.spooledFrames === 1);
    assert.ok(h.process.spool.bytes > 0, 'the frame remains a local obligation');

    const whileSpooled = await h.process.sealTails();
    assert.equal(whileSpooled, null, 'a nonempty spool cannot produce a final tail candidate');
    assert.ok(h.process.spool.bytes > 0, 'quiescence keeps the spool available for ACK-driven draining');

    h.organize.sendReadiness({ capacity: 'ok' });
    await until(() => h.organize.state.envelopes.length === 1);
    assert.ok(h.process.spool.bytes > 0, 'successful resend does not release the record');
    assert.equal(await h.process.sealTails(), null, 'send success is not durable receipt');

    h.organize.sendDurableAck({ connectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.spool.bytes === 0);
    const afterAck = await h.process.sealTails();
    assert.equal(afterAck?.spoolEmpty, true, 'the ACK-released, empty spool permits a candidate');
    assert.equal(afterAck?.tails[0]?.lastReceivedSeq, 1);
  } finally {
    await h.teardown();
  }
});

test('a sent frame is retained until its acknowledgement, and an acknowledgement releases it', async () => {
  const h = await setup({ label: 'write-ahead' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);
    assert.ok(h.process.spool.bytes > 0, 'the frame the link took is still retained until acknowledged');
    assert.equal(h.process.stats.unreleasedFrames, 1);

    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.spool.bytes === 0);
    assert.equal(h.process.stats.unreleasedFrames, 0, 'the acknowledgement released the retention');
  } finally {
    await h.teardown();
  }
});

test('an acknowledgement releases without asking for a resend', async () => {
  const h = await setup({ label: 'ack-no-resend' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);

    h.organize.sendDurableAck({ connectionId: h.process.connectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.spool.bytes === 0);
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(h.organize.state.envelopes.length, 1, 'the acknowledgement itself never triggers a resend');
  } finally {
    await h.teardown();
  }
});

test('a hand-over waits for the previous connection to be acknowledged before it is announced', async () => {
  const h = await setup({ label: 'handover-gate' });
  try {
    h.process.start();
    await until(() => h.sockets.length === 1);
    h.sockets[0].onopen();
    h.sockets[0].deliver('{"seq":1}');
    await until(() => h.organize.state.envelopes.length === 1);
    const oldConnectionId = h.process.connectionId;

    // The venue connection dies: the next generation must not be announced while the old one's
    // frames are still unacknowledged.
    h.sockets[0].onclose();
    await until(() => h.process.generation === 2);
    await until(() => h.organize.state.envelopes.length >= 2, { timeoutMs: 2000 });
    assert.equal(
      h.organize.state.accepts.filter((message) => message.generation === 2).length,
      0,
      'the deferred generation is not announced while the old one is unacknowledged',
    );

    // The acknowledgement drains the previous connection's retention and opens the gate.
    h.organize.sendDurableAck({ connectionId: oldConnectionId, generation: 1, upToSeq: 1 });
    await until(() => h.process.spool.bytes === 0);
    await until(() => h.organize.state.accepts.some((message) => message.generation === 2), { timeoutMs: 2000 });
  } finally {
    await h.teardown();
  }
});

test('a restart rebuilds the release order from the spool and acknowledges what the old life retained', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'ingest-restart-'));
  const organize = await startFakeOrganize(join(dir, 'organize.sock'), { batchFrames: 1 });
  try {
    const first = await buildIngest({ dir, organize, label: 'first' });
    first.process.start();
    await until(() => first.sockets.length === 1);
    first.sockets[0].onopen();
    first.sockets[0].deliver('{"seq":1}');
    await until(() => organize.state.envelopes.length === 1);
    assert.ok(first.process.spool.bytes > 0, 'the frame is retained by the first life');
    first.process.close();

    // A new life over the same spool: the walk rebuilds the release order and the record can be
    // offered and acknowledged again.
    const second = await buildIngest({ dir, organize, label: 'second', spoolDir: join(dir, 'spool-first'), ackStallMs: 60 });
    assert.ok(second.process.spool.bytes > 0, 'the new life found the retained record');
    assert.equal(second.process.stats.unreleasedFrames, 1, 'and rebuilt the release order from the walk');

    assert.equal(second.process.drainSpool().resent, 1, 'the startup drain offers it again');
    await until(() => organize.state.envelopes.length === 2);
    // The resend restarts the stall clock: with nothing acknowledging, the deadline still fires.
    second.fireByDelay(60);
    assert.ok(
      second.diagnostics.some((d) => /durable ceiling has stalled/.test(String(d.reason))),
      'a restart alone does not lose the stall detection',
    );
    organize.sendDurableAck({ connectionId: 'run-1:kraken:kraken_spot:1', generation: 1, upToSeq: 1 });
    await until(() => second.process.spool.bytes === 0);
    assert.deepEqual(second.process.spool.segments, [], 'the acknowledged segment is released');
    second.process.close();
  } finally {
    await organize.close();
    rmSync(dir, { recursive: true, force: true });
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

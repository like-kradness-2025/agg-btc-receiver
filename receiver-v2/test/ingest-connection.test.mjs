import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createReceiveConnection } from '../src/ingest/connection.mjs';

const SILENCE_MS = 1_000;
const STABILITY_MS = 5_000;

function harness(adapterOverrides = {}, options = {}) {
  const sockets = [];
  const timers = [];
  const envelopes = [];
  const generations = [];
  const states = [];
  const subscriptions = [];
  const diagnostics = [];
  const failures = [];

  const webSocketImpl = function fakeSocket(url) {
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
      /** Deliver a message as the venue would, through the socket that is still current. */
      deliver(data) {
        socket.onmessage?.({ data });
      },
    };
    sockets.push(socket);
    return socket;
  };

  const adapter = {
    url: 'ws://venue.test/ws',
    stream: 'trades',
    parse: (raw) => {
      const parsed = JSON.parse(raw);
      if (parsed.ping) return { kind: 'heartbeat', answered: true };
      return { kind: 'data' };
    },
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    heartbeatMessage: () => '{"ping":1}',
    ...adapterOverrides,
  };

  const connection = createReceiveConnection({
    adapter,
    market: 'kraken_spot',
    // A connection is named from its run and its venue: the harness supplies both, and the one test that is
    // about a reception without them overrides them with undefined.
    runId: 'run-1',
    venue: 'kraken',
    webSocketImpl,
    setTimer: (fn, ms) => {
      const timer = { fn, ms, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => {
      timer.cleared = true;
    },
    silenceDeadlineMs: SILENCE_MS,
    stabilityMs: STABILITY_MS,
    onEnvelope: (envelope) => envelopes.push(envelope),
    // The wiring's contract: the announcement is settled by calling `settle(true)`, which is what opens the
    // socket; `settle(false)` - or returning `false` - refuses it and nothing is opened.
    onGeneration: (info) => {
      generations.push(info);
      info.settle?.(true);
      return true;
    },
    onState: (info) => states.push(info.state),
    onSubscriptions: (info) => subscriptions.push(info),
    onDiagnostic: (info) => diagnostics.push(info),
    onFailure: (info) => failures.push(info),
    ...options,
  });

  const fireAll = () => {
    const pending = timers.filter((timer) => !timer.cleared).sort((a, b) => a.ms - b.ms);
    for (const timer of pending) {
      timer.cleared = true;
      timer.fn();
    }
  };
  const fireByDelay = (ms) => {
    const timer = timers.find((entry) => entry.ms === ms && !entry.cleared);
    if (timer) {
      timer.cleared = true;
      timer.fn();
    }
    return Boolean(timer);
  };

  return { connection, sockets, timers, envelopes, generations, states, subscriptions, diagnostics, failures, fireAll, fireByDelay };
}

test('starting opens a socket, subscribes, and issues the first generation', () => {
  const h = harness();
  h.connection.start();
  assert.equal(h.sockets.length, 1);
  assert.equal(h.connection.generation, 1);
  assert.equal(h.connection.connectionId, 'run-1:kraken:kraken_spot:1');
  h.sockets[0].onopen();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}']);
});

test('data is stamped with the receive metadata the rest of the structure depends on', () => {
  const h = harness();
  h.connection.start();
  h.sockets[0].onopen();
  h.sockets[0].deliver('{"price":1}');
  h.sockets[0].deliver('{"price":2}');
  // The names here are the contract's names, not a private spelling: the same envelope goes on the
  // wire and into the book, so a test that reads camelCase would pin a shape nothing else uses.
  assert.deepEqual(h.envelopes.map((e) => e.receive_seq), [1, 2]);
  assert.equal(h.envelopes[0].connection_id, 'run-1:kraken:kraken_spot:1');
  assert.ok(h.envelopes[0].recv_ts_ms > 0, 'the wall clock is stamped at the boundary');
  assert.ok(h.envelopes[1].recv_mono_ns > h.envelopes[0].recv_mono_ns, 'and the monotonic clock advances');
  assert.equal(h.envelopes[0].meta.first_seq, 1, 'so a book can anchor where this stream begins');
  assert.equal(h.envelopes[0].generation, 1);
});

test('a replaced socket issues a new generation, and the old one is no longer heard', () => {
  const h = harness();
  h.connection.start();
  const first = h.sockets[0];
  first.onopen();
  first.deliver('{"price":1}');
  first.onclose();
  h.fireAll(); // the reconnect is scheduled behind a backoff, not opened immediately

  assert.equal(h.connection.generation, 2, 'the generation moves with the socket');
  assert.equal(h.sockets.length, 2);
  const second = h.sockets[1];
  second.onopen();

  const before = h.envelopes.length;
  first.deliver('{"price":9}'); // a frame from the abandoned socket
  assert.equal(h.envelopes.length, before, 'nothing from an old socket is used');

  second.deliver('{"price":2}');
  assert.equal(h.envelopes.at(-1).connection_id, 'run-1:kraken:kraken_spot:2');
  assert.equal(h.envelopes.at(-1).receive_seq, 1, 'and the sequence starts again with the connection');
});

test('silence is treated as a dead link, not as a quiet market', () => {
  const h = harness();
  h.connection.start();
  h.sockets[0].onopen();
  assert.equal(h.connection.generation, 1);

  h.fireByDelay(SILENCE_MS);
  assert.equal(h.connection.generation, 2, 'no data and no heartbeat for the deadline means replace it');
  assert.ok(h.diagnostics.some((d) => /silence deadline/.test(d.reason)));
});

test('the link is only usable once the subscription is acknowledged', () => {
  const h = harness({
    parse: (raw) => {
      const parsed = JSON.parse(raw);
      if (parsed.subscribed) return { kind: 'subscription', key: parsed.subscribed, ok: true };
      if (parsed.rejected) return { kind: 'subscription', key: parsed.rejected, ok: false, detail: 'limit' };
      return { kind: 'data' };
    },
  });
  h.connection.start();
  h.sockets[0].onopen();
  assert.equal(h.connection.subscriptionState, 'unsubscribed', 'asking is not agreeing');

  h.sockets[0].deliver('{"subscribed":"trades"}');
  assert.equal(h.connection.subscriptionState, 'acknowledged');
  assert.equal(h.connection.state, 'subscribed');

  h.sockets[0].deliver('{"rejected":"depth"}');
  assert.equal(h.connection.subscriptionState, 'failed', 'a refused subscription is not a usable link');
  assert.equal(h.connection.state, 'awaiting-subscription');
});

test('a shutdown announcement replaces the socket instead of waiting for the silence deadline', () => {
  const h = harness({
    parse: (raw) => {
      const parsed = JSON.parse(raw);
      if (parsed.shutdown) return { kind: 'shutdown', detail: parsed.shutdown };
      return { kind: 'data' };
    },
  });
  h.connection.start();
  h.sockets[0].onopen();
  h.sockets[0].deliver('{"shutdown":"maintenance"}');
  assert.equal(h.connection.generation, 2);
  assert.ok(h.diagnostics.some((d) => /shutdown/.test(d.reason)));
});

test('attempts only reset once the link has actually held', () => {
  const h = harness();
  h.connection.start();
  h.sockets[0].onopen();
  h.sockets[0].onclose();
  assert.equal(h.connection.attempts, 2, 'a close counts as an attempt that did not hold');

  h.fireAll();
  const current = h.sockets.at(-1);
  current.onopen();
  h.fireByDelay(STABILITY_MS); // the link held for the stability window
  assert.equal(h.connection.attempts, 0, 'and only then do attempts reset');
});

test('the announcement names the run and the venue, because a takeover is a question about runs', () => {
  const h = harness({}, { runId: 'run-1', venue: 'kraken' });
  h.connection.start();
  assert.equal(h.connection.connectionId, 'run-1:kraken:kraken_spot:1', 'the run is part of the name');
  const announced = h.generations.at(-1);
  assert.equal(announced.runId, 'run-1', 'and it travels with the announcement');
  assert.equal(announced.venue, 'kraken');
  assert.equal(announced.firstSeq, 1);
});

test('a connection the wiring refuses never opens a socket', () => {
  const h = harness({}, { onGeneration: () => false });
  h.connection.start();
  assert.equal(h.sockets.length, 0, 'nothing is opened for a connection that was not admitted');
  assert.equal(h.connection.state, 'refused');
  assert.ok(
    h.diagnostics.some((d) => /not admitted downstream/.test(d.reason)),
    'and the refusal is reported rather than silent',
  );
});

test('reception will not name a connection without a run and a venue', () => {
  // Both are part of the name (C2), and a name two runs can share is how a board and a watermark end up
  // describing different data under one connection id. This configuration cannot receive, so it never opens.
  const h = harness({}, { runId: undefined, venue: undefined });
  assert.throws(() => h.connection.start(), /run and a venue/);
  assert.equal(h.sockets.length, 0, 'and nothing was opened');
});

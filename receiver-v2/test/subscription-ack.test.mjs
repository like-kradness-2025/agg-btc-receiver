/**
 * C3: subscription establishment is a fact the band carries, and a failed one blocks serving.
 *
 * The contract wants three things the connection and the supervisor did not do before:
 *  - the expected set is registered when the subscriptions are sent, and one unanswered key is not
 *    "established";
 *  - a refusal, or silence past the ack deadline, is recorded as `failed` and blocks `ready` / serving;
 *  - the entrance's `receiver: started` cannot be the whole story when the subscription failed.
 *
 * These tests drive a fake socket so a rejection and an unanswered request can both be injected at
 * their real entry point - the socket - rather than through a test-only route in the product code.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createSupervisor } from '../src/supervisor/supervisor.mjs';
import { DEFAULT_ACK_DEADLINE_MS } from '../src/ingest/connection.mjs';

function withDir(fn) {
  const dir = mkdtempSync(join(tmpdir(), 'ack-'));
  try {
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
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
    };
    sockets.push(socket);
    return socket;
  };
  return { sockets, impl };
}

/** An adapter whose frames are a rejection, an acknowledgement, or plain data. */
function ackAdapter({ ackMode, expectedSubscriptions, subscribeMessages } = {}) {
  return {
    url: 'ws://venue.test/ws',
    stream: 'trades',
    ackMode,
    expectedSubscriptions,
    subscribeMessages: subscribeMessages ?? (() => ['{"subscribe":"trades"}']),
    heartbeatMessage: () => null,
    parse: (raw) => {
      const parsed = JSON.parse(String(raw));
      if (parsed.rejected !== undefined) {
        return { kind: 'subscription', key: parsed.rejected, ok: false, detail: 'denied' };
      }
      if (parsed.subscribed !== undefined) {
        return { kind: 'subscription', key: parsed.subscribed, ok: true };
      }
      return { kind: 'data' };
    },
    changesFor: () => [{ side: 'bid', price: 100, size: 1 }],
  };
}

function build(dir, { adapter }) {
  const { sockets, impl } = fakeSockets();
  const timers = [];
  const exits = [];
  const stops = [];
  const states = [];
  const subscriptionEvents = [];
  const supervisor = createSupervisor({
    market: 'kraken_spot',
    stream: 'trades',
    adapter,
    path: join(dir, 'state.sqlite'),
    venue: 'kraken',
    runId: 'run-1',
    webSocketImpl: impl,
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
    onStop: (stop) => stops.push(stop),
    onState: (info) => states.push(info.state),
    onSubscriptions: (info) => subscriptionEvents.push(info),
    onDiagnostic: () => {},
  });
  return { supervisor, sockets, timers, exits, stops, states, subscriptionEvents };
}

/** Fire the one timer armed for exactly this delay, as a test's control of the clock. */
function fireTimer(timers, ms) {
  const timer = timers.find((entry) => entry.ms === ms && !entry.cleared);
  assert.ok(timer, `a timer of ${ms} ms was armed`);
  timer.cleared = true;
  timer.fn();
  return timer;
}

test('a refused subscription is recorded as failed, is never reported as serving, and ends the run', () => {
  withDir((dir) => {
    const h = build(dir, { adapter: ackAdapter() });
    const started = h.supervisor.start();
    assert.equal(started.started, true, 'the sequence completed and a socket opened');

    h.sockets[0].onopen();
    assert.ok(h.states.includes('subscribing'), 'the connection asked before it was answered');
    h.sockets[0].onmessage({ data: '{"rejected":"trades"}' });

    // The refusal reaches the connection's own state first, then the run's end; it is never collapsed
    // into "awaiting-subscription".
    assert.ok(h.states.includes('subscription-failed'), 'the band state names the failure');
    assert.equal(h.supervisor.stats.subscriptionState, 'failed', 'the failed subscription is the band state');
    assert.equal(h.supervisor.connection.subscriptionFailure, 'denied', 'and the venue reason is kept');
    assert.equal(h.subscriptionEvents.at(-1).state, 'failed');
    assert.equal(h.supervisor.ready, false, 'a failed subscription is never reported as serving');
    // The entrance printed `receiver: started` for an admitted socket; the failure is not hidden behind
    // it - the run ends non-zero instead of sitting as if it could serve.
    assert.equal(h.supervisor.ended, true);
    assert.equal(h.supervisor.abnormal, true);
    assert.deepEqual(h.exits, [1], 'the failed subscription ends the run non-zero');
    assert.equal(h.stops.length, 1, 'and is reported once through onStop');
    assert.match(String(h.stops[0].reason), /subscription failed/i);
    h.supervisor.close();
  });
});

test('an unanswered subscription is failed when the ack deadline passes', () => {
  withDir((dir) => {
    const adapter = ackAdapter({ expectedSubscriptions: () => ['trades'] });
    const h = build(dir, { adapter });
    h.supervisor.start();
    h.sockets[0].onopen();
    assert.equal(h.supervisor.stats.subscriptionState, 'unsubscribed', 'asked, not yet answered');

    fireTimer(h.timers, DEFAULT_ACK_DEADLINE_MS);

    assert.equal(h.supervisor.stats.subscriptionState, 'failed', 'silence past the deadline is failure');
    assert.equal(
      h.supervisor.connection.subscriptionFailure,
      'the subscription ack deadline passed',
      'and the reason is the deadline, not a refusal',
    );
    assert.equal(h.supervisor.ready, false);
    assert.deepEqual(h.exits, [1], 'a subscription that is never established ends the run');
    h.supervisor.close();
  });
});

test('an acknowledged subscription serves exactly as before', () => {
  withDir((dir) => {
    const adapter = ackAdapter({ expectedSubscriptions: () => ['trades'] });
    const h = build(dir, { adapter });
    h.supervisor.start();
    h.sockets[0].onopen();
    h.sockets[0].onmessage({ data: '{"subscribed":"trades"}' });
    assert.equal(h.supervisor.stats.subscriptionState, 'acknowledged');
    assert.equal(h.states.at(-1), 'subscribed');
    assert.equal(h.supervisor.ready, false, 'the board has not served yet');

    h.sockets[0].onmessage({ data: '{"price":1}' });
    assert.equal(h.supervisor.ready, true, 'an acknowledged subscription and a served board are ready');
    assert.deepEqual(h.exits, [], 'no failure, no exit code');
    h.supervisor.stop();
    h.supervisor.close();
  });
});

test('one unanswered expected subscription is not establishment', () => {
  withDir((dir) => {
    const adapter = ackAdapter({ expectedSubscriptions: () => ['a', 'b'] });
    const h = build(dir, { adapter });
    h.supervisor.start();
    h.sockets[0].onopen();

    h.sockets[0].onmessage({ data: '{"subscribed":"a"}' });
    assert.equal(h.supervisor.stats.subscriptionState, 'pending', 'a alone is not the whole expected set');
    assert.equal(h.supervisor.ready, false);

    h.sockets[0].onmessage({ data: '{"subscribed":"b"}' });
    assert.equal(h.supervisor.stats.subscriptionState, 'acknowledged', 'every expected key answered is the set');
    h.supervisor.stop();
    h.supervisor.close();
  });
});

test('a first-data venue is established by its first data frame, and by nothing else', () => {
  withDir((dir) => {
    const adapter = ackAdapter({ ackMode: 'first-data' });
    const h = build(dir, { adapter });
    h.supervisor.start();
    h.sockets[0].onopen();
    assert.equal(h.supervisor.stats.subscriptionState, 'unsubscribed', 'asking is not establishing');

    h.sockets[0].onmessage({ data: '{"price":1}' });
    assert.equal(h.supervisor.stats.subscriptionState, 'acknowledged', 'the first data frame establishes it');
    assert.equal(h.states.at(-1), 'subscribed');
    assert.equal(h.supervisor.ready, true);
    h.supervisor.stop();
    h.supervisor.close();
  });
});

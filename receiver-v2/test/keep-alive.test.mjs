/**
 * C4: the three keep-alive forms, and the empty frame that must never be sent.
 *
 * The contract names three shapes an adapter may declare - periodic (`{ intervalMs, payload() }`), silence
 * triggered (`{ noActivityMs, payload() }`) and none (`null`) - and keeps the silence deadline as a
 * separate question. These tests drive a fake socket and a fake clock so each form's send condition, its
 * timer's clearing, and its behaviour across a reconnect can be observed at the socket itself.
 *
 * The regression the forms expose: the old code sent `heartbeatMessage() ?? ''`, so an adapter that
 * declares `null` (Kraken) emitted an empty frame on every open. Nothing here sends `''`.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createReceiveConnection } from '../src/ingest/connection.mjs';

const SILENCE_MS = 60_000; // large, so the silence deadline is not what a test fires by accident
const STABILITY_MS = 120_000;

function harness(adapterOverrides = {}) {
  const sockets = [];
  const timers = [];
  const diagnostics = [];

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
    parse: (raw) => (JSON.parse(raw).ping ? { kind: 'heartbeat', answered: true } : { kind: 'data' }),
    subscribeMessages: () => ['{"subscribe":"trades"}'],
    ...adapterOverrides,
  };

  const connection = createReceiveConnection({
    adapter,
    market: 'kraken_spot',
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
    onGeneration: (info) => {
      info.settle?.(true);
      return true;
    },
    onDiagnostic: (info) => diagnostics.push(info),
  });

  const fireByDelay = (ms) => {
    const timer = timers.find((entry) => entry.ms === ms && !entry.cleared);
    assert.ok(timer, `a timer of ${ms} ms was armed`);
    timer.cleared = true;
    timer.fn();
    return timer;
  };
  const fireAll = () => {
    const pending = timers.filter((timer) => !timer.cleared).sort((a, b) => a.ms - b.ms);
    for (const timer of pending) {
      timer.cleared = true;
      timer.fn();
    }
  };

  return { connection, sockets, timers, diagnostics, fireByDelay, fireAll };
}

test('the interval form sends on its rhythm, not on open', () => {
  const h = harness({ keepAlive: () => ({ intervalMs: 20_000, payload: () => '{"ping":1}' }) });
  h.connection.start();
  h.sockets[0].onopen();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'opening sends the subscription, not a ping');

  h.fireByDelay(20_000);
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}'], 'the first ping is one interval in');
  h.fireByDelay(20_000);
  assert.deepEqual(
    h.sockets[0].sent,
    ['{"subscribe":"trades"}', '{"ping":1}', '{"ping":1}'],
    'and the timer re-arms itself',
  );
});

test('the no-activity form sends only after real silence, and a frame resets the window', () => {
  const h = harness({ keepAlive: () => ({ noActivityMs: 30_000, payload: () => '{"ping":1}' }) });
  h.connection.start();
  h.sockets[0].onopen();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'nothing is sent while the link is active');

  // A frame clears the armed window and arms a fresh one: the ping is measured from the last thing heard,
  // so a venue that keeps talking is never pinged.
  const armed = h.timers.find((timer) => timer.ms === 30_000 && !timer.cleared);
  assert.ok(armed, 'a silent window was armed when the socket opened');
  h.sockets[0].deliver('{"price":1}');
  assert.equal(armed.cleared, true, 'the frame reset the window');
  assert.ok(
    h.timers.find((timer) => timer.ms === 30_000 && !timer.cleared),
    'and armed a new one',
  );

  h.fireByDelay(30_000);
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}'], 'a silent window sends one ping');
  // The window restarts on its own: silence that continues keeps pinging rather than falling quiet after one.
  h.fireByDelay(30_000);
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}', '{"ping":1}', '{"ping":1}']);
});

test('the null form sends nothing at all - never an empty frame', () => {
  const h = harness({ heartbeatMessage: () => null });
  h.connection.start();
  h.sockets[0].onopen();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'a null keep-alive is not an empty send');

  h.fireAll();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'and no timer decides to send one either');
  assert.equal(
    h.sockets[0].sent.includes(''),
    false,
    'the empty frame the old `?? \'\'` fallback produced is gone',
  );
});

test('a payload that resolves to nothing is skipped rather than sent empty', () => {
  const h = harness({ keepAlive: () => ({ intervalMs: 20_000, payload: () => null }) });
  h.connection.start();
  h.sockets[0].onopen();
  h.fireByDelay(20_000);
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'a null payload tick sends nothing');
});

test('the keep-alive is cleared on teardown and re-armed for the replacement socket', () => {
  const h = harness({ keepAlive: () => ({ intervalMs: 20_000, payload: () => '{"ping":1}' }) });
  h.connection.start();
  const first = h.sockets[0];
  first.onopen();

  first.onclose();
  h.fireAll(); // the reconnect backoff timer opens the replacement
  assert.equal(h.sockets.length, 2, 'a replacement socket was opened');
  const second = h.sockets[1];
  second.onopen();

  // The old socket's keep-alive is gone: firing the interval only pings the current one.
  h.fireByDelay(20_000);
  assert.deepEqual(first.sent, ['{"subscribe":"trades"}'], 'the abandoned socket is never pinged again');
  assert.deepEqual(second.sent, ['{"subscribe":"trades"}', '{"ping":1}'], 'the replacement has its own rhythm');
});

test('stopping clears the keep-alive timer for good', () => {
  const h = harness({ keepAlive: () => ({ intervalMs: 20_000, payload: () => '{"ping":1}' }) });
  h.connection.start();
  h.sockets[0].onopen();
  h.connection.stop();

  const before = h.sockets[0].sent.length;
  h.fireAll();
  assert.equal(h.sockets[0].sent.length, before, 'no keep-alive is sent after the connection stopped');
});

test('an unknown keep-alive shape is refused as a diagnostic, not sent as garbage', () => {
  const h = harness({ keepAlive: () => ({ intervalMs: -1, payload: () => '{"ping":1}' }) });
  h.connection.start();
  h.sockets[0].onopen();
  assert.deepEqual(h.sockets[0].sent, ['{"subscribe":"trades"}'], 'nothing malformed reaches the socket');
  assert.equal(
    h.diagnostics.some((d) => /keep-alive declaration refused/.test(d.reason)),
    true,
    'and the refusal is reported',
  );
});

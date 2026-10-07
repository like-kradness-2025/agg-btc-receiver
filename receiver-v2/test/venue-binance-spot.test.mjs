import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
  createBinanceSpotAdapter,
  createBinanceDepthSynchronizer,
} from '../src/ingest/venues/binance-spot.mjs';
import { loadConfig, adapterFor, knownVenues } from '../src/entry/config.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';

const trade = (overrides = {}) => ({
  stream: 'btcusdt@trade',
  data: { e: 'trade', E: 1700000000000, s: 'BTCUSDT', t: 7, p: '50000.00', q: '0.010', b: 1, a: 2, T: 1700000000000, m: true, M: true, ...overrides },
});
const depth = (overrides = {}) => ({
  stream: 'btcusdt@depth@100ms',
  data: { e: 'depthUpdate', E: 1700000000100, s: 'BTCUSDT', U: 101, u: 102, b: [['50000.00', '1.25']], a: [['50001.00', '2.50']], ...overrides },
});
const raw = (value) => Buffer.from(JSON.stringify(value));

function adapter(options = {}) {
  return createBinanceSpotAdapter({
    market: 'binance_spot',
    symbol: 'BTCUSDT',
    url: 'ws://127.0.0.1:1/stream?streams=btcusdt@trade/btcusdt@depth@100ms',
    restUrl: 'http://127.0.0.1:1/api/v3/depth?symbol=BTCUSDT&limit=5000',
    ...options,
  });
}

test('combined wrapper parsing distinguishes trade and depth data', () => {
  const subject = adapter();
  assert.deepEqual(subject.parse(JSON.stringify(trade())), { kind: 'data', trade: true });
  assert.deepEqual(subject.parse(JSON.stringify(depth())), { kind: 'data', depth: true });
  assert.equal(subject.parse(JSON.stringify({ data: depth().data })), null, 'unwrapped data is refused');
});

test('subscription uses only the official combined stream URL and no guessed payload', () => {
  const subject = adapter();
  assert.equal(subject.url, 'ws://127.0.0.1:1/stream?streams=btcusdt@trade/btcusdt@depth@100ms');
  assert.deepEqual(subject.subscribeMessages(), []);
  assert.equal(subject.heartbeatMessage(), null, 'the server ping is answered by the websocket implementation');
});

test('trade handling produces no book changes while preserving valid data classification', () => {
  const subject = adapter();
  assert.deepEqual(subject.changesFor({ raw: raw(trade()) }), { replace: false, changes: [] });
});

test('snapshot boundary accepts U <= lastUpdateId + 1 <= u and drops earlier events', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [['49999.00', '1.0']], asks: [['50001.00', '2.0']] });
  assert.equal(subject.acceptDepthEvent({ ...depth().data, U: 99, u: 100 }).status, 'discarded');
  assert.equal(subject.acceptDepthEvent({ ...depth().data, U: 100, u: 101 }).status, 'applied');
  assert.deepEqual(subject.changesFor({ raw: raw(depth({ U: 100, u: 101 })) }), {
    replace: true,
    levels: [
      { side: 'bid', price: 49999, size: 1 },
      { side: 'ask', price: 50001, size: 2.5 },
      { side: 'bid', price: 50000, size: 1.25 },
    ],
  });
});

test('snapshot refetch is required when lastUpdateId is below first buffered U', async () => {
  let calls = 0;
  const sync = createBinanceDepthSynchronizer({
    fetchSnapshot: async () => {
      calls += 1;
      return { lastUpdateId: calls === 1 ? 10 : 101, bids: [], asks: [] };
    },
  });
  const result = await sync.sync([depth({ U: 101, u: 102 })]);
  assert.equal(result.status, 'synced');
  assert.equal(calls, 2);
});

test('update continuity rejects a gap and requires resync', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  assert.equal(subject.acceptDepthEvent(depth({ U: 101, u: 102 }).data).status, 'applied');
  const gap = subject.acceptDepthEvent(depth({ U: 104, u: 105 }).data);
  assert.deepEqual(gap, { status: 'resync', reason: 'depth update gap', expected: 103, first: 104, final: 105 });
  assert.equal(subject.needsResync, true);
});

test('absolute quantities replace levels and zero deletes a level', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [['50000.00', '1.0']], asks: [] });
  const event = depth({ U: 101, u: 101, b: [['50000.00', '2.0'], ['49999.00', '0']], a: [] });
  assert.equal(subject.acceptDepthEvent(event.data).status, 'applied');
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: true,
    levels: [{ side: 'bid', price: 50000, size: 2 }],
  });
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: false,
    changes: [
      { side: 'bid', price: 50000, size: 2 },
      { side: 'bid', price: 49999, size: 0 },
    ],
  });
});

test('malformed messages are refused fail-closed', () => {
  const subject = adapter();
  for (const value of ['not json', '{}', JSON.stringify({ stream: 'btcusdt@depth@100ms', data: { e: 'depthUpdate' } })]) {
    assert.equal(subject.parse(value), null);
  }
  assert.deepEqual(subject.changesFor({ raw: raw({ stream: 'btcusdt@depth@100ms', data: { e: 'depthUpdate' } }) }), {
    replace: false,
    changes: [],
  });
});

test('strict Binance validation rejects null quantities and identifiers', () => {
  const subject = adapter();
  assert.equal(subject.parse(JSON.stringify(depth({ U: null }))), null);
  assert.equal(subject.parse(JSON.stringify(depth({ b: [['50000.00', null]] }))), null);
  assert.throws(
    () => subject.syncSnapshot({ lastUpdateId: 100, bids: [['50000.00', null]], asks: [] }),
    /valid bids and asks/,
  );
});

test('snapshot levels are emitted as one replacement before buffered depth changes', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [['49999.00', '1.0']], asks: [['50001.00', '2.0']] });
  const event = depth({ U: 101, u: 101 });
  assert.equal(subject.acceptDepthEvent(event.data).status, 'applied');
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: true,
    levels: [
      { side: 'bid', price: 49999, size: 1 },
      { side: 'ask', price: 50001, size: 2.5 },
      { side: 'bid', price: 50000, size: 1.25 },
    ],
  });
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: false,
    changes: [
      { side: 'bid', price: 50000, size: 1.25 },
      { side: 'ask', price: 50001, size: 2.5 },
    ],
  });
});

test('the first accepted depth event is included in the snapshot replacement exactly once', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [['49999.00', '1.0']], asks: [['50001.00', '2.0']] });
  const event = depth({ U: 101, u: 101, b: [['49999.00', '3.0']] });
  assert.equal(subject.acceptDepthEvent(event.data).status, 'applied');
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: true,
    levels: [
      { side: 'bid', price: 49999, size: 3 },
      { side: 'ask', price: 50001, size: 2.5 },
    ],
  });
  assert.deepEqual(subject.changesFor({ raw: raw(event) }), {
    replace: false,
    changes: [
      { side: 'bid', price: 49999, size: 3 },
      { side: 'ask', price: 50001, size: 2.5 },
    ],
  });
});

test('depth validation rejects negative quantities, array coercion, and unknown buffered streams', () => {
  const subject = adapter();
  assert.equal(subject.parse(JSON.stringify(depth({ b: [['50000.00', '-1']] }))), null);
  assert.equal(subject.parse(JSON.stringify(depth({ b: [[[], '1']] }))), null);
  assert.equal(subject.parse(JSON.stringify({ stream: 'other@depth', data: depth().data })), null);
  assert.doesNotThrow(() => subject.bufferDuringPreparation(JSON.stringify({ stream: 'other@depth', data: depth().data })));
  subject.syncSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  assert.equal(subject.acceptDepthEvent(JSON.stringify({ stream: 'other@depth', data: depth().data })).status, 'malformed');
});

test('receive path emits only accepted depth events and never forwards a gap', async () => {
  const subject = adapter({ fetchImpl: async () => ({ ok: true, json: async () => ({ lastUpdateId: 100, bids: [], asks: [] }) }) });
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  const envelopes = [];
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'binance_spot',
    venue: 'binance_spot',
    runId: 'run-filter',
    webSocketImpl: FakeWebSocket,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  for (let i = 0; i < 100 && subject.needsResync; i += 1) await new Promise((resolve) => setImmediate(resolve));
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 99, u: 100 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 101, u: 101 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 103, u: 103 })) });
  for (let i = 0; i < 100 && sockets.length < 2; i += 1) await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(envelopes.length, 1);
  assert.equal(subject.needsResync, true);
  connection.stop();
});

test('production receive path ignores a valid Binance trade before accepting depth', async () => {
  const subject = adapter({ fetchImpl: async () => ({ ok: true, json: async () => ({ lastUpdateId: 100, bids: [], asks: [] }) }) });
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  const envelopes = [];
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'binance_spot',
    venue: 'binance_spot',
    runId: 'run-trade-filter',
    webSocketImpl: FakeWebSocket,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  for (let i = 0; i < 100 && typeof sockets[0]?.onopen !== 'function'; i += 1) await new Promise((resolve) => setImmediate(resolve));
  sockets[0].onopen();
  for (let i = 0; i < 100 && typeof sockets[0]?.onmessage !== 'function'; i += 1) await new Promise((resolve) => setImmediate(resolve));
  for (let i = 0; i < 100 && subject.needsResync; i += 1) await new Promise((resolve) => setImmediate(resolve));
  sockets[0].onmessage({ data: JSON.stringify(trade()) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 101, u: 101 })) });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(envelopes.length, 1, 'the valid trade does not tear down the socket or enter the depth synchronizer');
  assert.equal(envelopes[0].raw.toString(), JSON.stringify(depth({ U: 101, u: 101 })));
  connection.stop();
});

test('the real receive path buffers depth during REST and resyncs on a received gap', async () => {
  let releaseSnapshot;
  const snapshot = new Promise((resolve) => { releaseSnapshot = resolve; });
  const subject = adapter({ fetchImpl: async () => ({ ok: true, json: async () => snapshot }) });
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  const envelopes = [];
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'binance_spot',
    venue: 'binance_spot',
    runId: 'run-race',
    webSocketImpl: FakeWebSocket,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 101, u: 101 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 102, u: 102 })) });
  releaseSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  for (let i = 0; i < 100 && (subject.needsResync || envelopes.length < 2); i += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(envelopes.length, 2, 'buffered frames are processed after snapshot preparation');
  assert.equal(subject.needsResync, false);
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 104, u: 104 })) });
  for (let i = 0; i < 500 && sockets.length < 2; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.ok(sockets.length >= 2, 'a gap received from the websocket replaces the socket');
  connection.stop();
});

test('a gap during buffered replay abandons the rest of the old generation exactly once', async () => {
  let releaseSnapshot;
  const snapshot = new Promise((resolve) => { releaseSnapshot = resolve; });
  const subject = adapter({ fetchImpl: async () => ({ ok: true, json: async () => snapshot }) });
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  const envelopes = [];
  const generations = [];
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'binance_spot',
    venue: 'binance_spot',
    runId: 'run-replay-generation',
    webSocketImpl: FakeWebSocket,
    onGeneration: (info) => {
      generations.push(info.generation);
      info.settle(true);
    },
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
    maxBackoffMs: 1,
  });
  connection.start();
  sockets[0].onopen();
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 101, u: 101 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 103, u: 103 })) });
  sockets[0].onmessage({ data: JSON.stringify(trade({ t: 8 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 104, u: 104 })) });
  sockets[0].onmessage({ data: JSON.stringify(depth({ U: 105, u: 105 })) });

  releaseSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  for (let i = 0; i < 100 && generations.length < 2; i += 1) await new Promise((resolve) => setImmediate(resolve));

  assert.deepEqual(generations, [1, 2], 'the replay gap causes one replacement, not a chain of old-buffer replacements');
  assert.deepEqual(envelopes.map((envelope) => envelope.raw.toString()), [JSON.stringify(depth({ U: 101, u: 101 }))]);
  assert.equal(envelopes.some((envelope) => envelope.raw.toString() === JSON.stringify(trade({ t: 8 }))), false, 'old trade is not leaked into generation 2');
  connection.stop();
});

test('a message received while REST preparation waits extends the silence deadline without forwarding it', async () => {
  let releaseSnapshot;
  const snapshot = new Promise((resolve) => { releaseSnapshot = resolve; });
  const subject = adapter({ fetchImpl: async () => ({ ok: true, json: async () => snapshot }) });
  const sockets = [];
  const timers = [];
  const envelopes = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  const connection = createReceiveConnection({
    adapter: subject,
    market: 'binance_spot',
    venue: 'binance_spot',
    runId: 'run-rest-silence',
    webSocketImpl: FakeWebSocket,
    setTimer: (fn, ms) => {
      const timer = { fn, ms, cleared: false, unref() {} };
      timers.push(timer);
      return timer;
    },
    clearTimer: (timer) => { timer.cleared = true; },
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 1_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  const initialSilence = timers.find((timer) => timer.ms === 1_000);
  sockets[0].onmessage({ data: JSON.stringify(trade({ t: 9 })) });

  assert.equal(initialSilence.cleared, true, 'the REST-waiting arrival resets the old silence timer');
  const silenceAfterArrival = timers.filter((timer) => timer.ms === 1_000 && !timer.cleared).at(-1);
  assert.ok(silenceAfterArrival, 'a new silence deadline is armed for the same socket');
  if (!initialSilence.cleared) initialSilence.fn();
  assert.equal(connection.generation, 1, 'the stale silence callback cannot replace a live socket');
  assert.equal(envelopes.length, 0, 'the REST-waiting trade is not forwarded before synchronization');
  releaseSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  connection.stop();
});

test('a REST response from an old connection generation cannot synchronize the replacement', async () => {
  const pending = [];
  const subject = adapter({
    fetchImpl: () => new Promise((resolve) => pending.push({ resolve })),
  });
  const first = subject.onConnectionOpen({ connectionId: 'run:binance_spot:binance_spot:1', generation: 1 });
  const second = subject.onConnectionOpen({ connectionId: 'run:binance_spot:binance_spot:2', generation: 2 });
  pending[1].resolve({ ok: true, json: async () => ({ lastUpdateId: 200, bids: [], asks: [] }) });
  await second;
  pending[0].resolve({ ok: true, json: async () => ({ lastUpdateId: 100, bids: [], asks: [] }) });
  await first;
  assert.equal(subject.connectionId, 'run:binance_spot:binance_spot:2');
  assert.equal(subject.lastUpdateId, 200);
});

test('a new connection resets depth synchronization and reconnects fail-closed', () => {
  const subject = adapter();
  subject.syncSnapshot({ lastUpdateId: 100, bids: [], asks: [] });
  subject.acceptDepthEvent(depth({ data: { ...depth().data, U: 101, u: 101 } }));
  subject.resetConnection('connection-2');
  assert.equal(subject.needsResync, true);
  assert.equal(subject.acceptDepthEvent(depth({ U: 102, u: 102 }).data).status, 'resync');
});

test('REST snapshot sync uses a deterministic local HTTP server and official request shape', async () => {
  const requests = [];
  const server = createServer((request, response) => {
    requests.push(request.url);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ lastUpdateId: 100, bids: [['50000.00', '1.0']], asks: [] }));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    const subject = adapter({ restUrl: `http://127.0.0.1:${port}/api/v3/depth?symbol=BTCUSDT&limit=5000` });
    await subject.syncDepth();
    assert.deepEqual(requests, ['/api/v3/depth?symbol=BTCUSDT&limit=5000']);
    assert.equal(subject.needsResync, false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('the connection seam performs REST sync before a fake Binance websocket is usable', async () => {
  const server = createServer((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ lastUpdateId: 100, bids: [], asks: [] }));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  const sockets = [];
  function FakeWebSocket(url) {
    const socket = { url, onopen: null, onmessage: null, onclose: null, onerror: null, send() {}, close() {} };
    sockets.push(socket);
    return socket;
  }
  try {
    const subject = adapter({ restUrl: `http://127.0.0.1:${port}/api/v3/depth?symbol=BTCUSDT&limit=5000` });
    const connection = createReceiveConnection({
      adapter: subject,
      market: 'binance_spot',
      venue: 'binance_spot',
      runId: 'run-1',
      webSocketImpl: FakeWebSocket,
      onGeneration: ({ settle }) => settle(true),
      silenceDeadlineMs: 60_000,
      ackDeadlineMs: 60_000,
    });
    connection.start();
    sockets[0].onopen();
    for (let attempt = 0; attempt < 20 && subject.needsResync; attempt += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    assert.equal(subject.needsResync, false, 'REST snapshot completes before stream use');
    assert.equal(sockets.length, 1, 'the fake websocket was opened deterministically');
    connection.stop();
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('config enables only binance_spot while preserving disabled bitfinex', () => {
  assert.ok(knownVenues().includes('binance_spot'));
  assert.throws(() => adapterFor({ venue: 'bitfinex', market: 'bitfinex_spot', stream: 'trades' }), /not supported/);
});
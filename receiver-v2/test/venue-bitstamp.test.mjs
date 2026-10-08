/**
 * Bitstamp WebSocket API v2: the spot market this receiver admits - bitstamp_spot (BTC/USD).
 *
 * The wire contract here is the official one, checked against the live public stream on
 * `wss://ws.bitstamp.net` and the public REST order book:
 *
 *  - Two channels are asked for, each in its own `bts:subscribe` frame: `live_trades_btcusd` (trades)
 *    and `diff_order_book_btcusd` (the book diffs). The acknowledgement is
 *    `bts:subscription_succeeded` per channel; a bad subscription string answers with
 *    `bts:error`; a duplicate subscription is re-acknowledged without interrupting the stream
 *    (all measured live). `bts:request_reconnect` is the venue asking for a fresh socket.
 *  - The depth channel is a DIFF feed: each `data` frame carries only the changed levels as
 *    `[price, amount]` pairs with an absolute amount, `0.00000000` removing the level, plus a
 *    matching-engine `microtimestamp` (microseconds). There is no sequence number and no
 *    checksum; the venue's own ordering fact is that microsecond clock.
 *  - Because the diff feed cannot bootstrap itself, the sync is the venue's own documented
 *    reconciliation algorithm (WebSocket API v2): subscribe first and buffer every diff, fetch the
 *    REST order book (`?group=1`, the documented default grouping the diff feed pairs with),
 *    discard every buffered diff at or before the snapshot's microtimestamp, replay the rest in
 *    order. This is why the adapter sends its own subscription frames inside connection
 *    preparation, before the REST fetch: the connection's fixed order is prepare-then-subscribe,
 *    and this venue must buffer before the snapshot.
 *  - Steady state: the diff stream must never move backwards in source time (a regression would
 *    re-apply an older diff over newer book state) - it fails closed until a new connection. An
 *    equal microtimestamp is not a stale frame and passes, as in v1.
 *  - Keep-alive is the client heartbeat `{"event":"bts:heartbeat"}` (answered with
 *    `{"event":"bts:heartbeat","channel":"","data":{"status":"success"}}`, measured live), sent
 *    after 15 s without a word from the venue - the same quiet-period cadence v1 kept with
 *    websocket-level pings.
 *
 * The frames below are the exact shapes the live stream sent (trimmed).
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import { createBitstampAdapter, createBitstampSynchronizer } from '../src/ingest/venues/bitstamp.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';

// ---------------------------------------------------------------------------------------------------
// Fixtures: exact shapes from the live read-only capture.
// ---------------------------------------------------------------------------------------------------

const DIFF_CHANNEL = 'diff_order_book_btcusd';
const TRADE_CHANNEL = 'live_trades_btcusd';

const ack = (channel) => ({ event: 'bts:subscription_succeeded', channel, data: {} });
const ackTrades = ack(TRADE_CHANNEL);
const ackDiffs = ack(DIFF_CHANNEL);

const diff = (us, bids = [['83463.08', '0.55048175']], asks = [['83473.08', '0.00000000']]) => ({
  data: { timestamp: String(Math.floor(us / 1e6)), microtimestamp: String(us), bids, asks },
  channel: DIFF_CHANNEL,
  event: 'data',
});

const trade = (overrides = {}) => ({
  data: {
    id: 650885871,
    timestamp: '1791403680',
    amount: 0.00274133,
    amount_str: '0.00274133',
    price: 83463.08,
    price_str: '83463.08',
    type: 1,
    microtimestamp: '1791403680349000',
    buy_order_id: 2058521371389952,
    sell_order_id: 2058521418514434,
    ...overrides,
  },
  channel: TRADE_CHANNEL,
  event: 'trade',
});

const heartbeatAck = { event: 'bts:heartbeat', channel: '', data: { status: 'success' } };
const errorFrame = { event: 'bts:error', channel: '', data: { code: null, message: 'Bad subscription string.' } };
const reconnect = { event: 'bts:request_reconnect', channel: '', data: null };

const snapshotPayload = (us, bids = [['83463.08', '0.65048175']], asks = [['83473.08', '0.20000000']]) => ({
  timestamp: String(Math.floor(us / 1e6)),
  microtimestamp: String(us),
  bids,
  asks,
});

const parse = (adapter, payload) => adapter.parse(typeof payload === 'string' ? payload : JSON.stringify(payload));
const changesOf = (adapter, payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

// ---------------------------------------------------------------------------------------------------
// The wire contract: endpoint, channels, the acknowledgement, and the keep-alive
// ---------------------------------------------------------------------------------------------------

test('the adapter names the Bitstamp endpoint, its two channels and their ack keys', () => {
  const adapter = createBitstampAdapter();
  assert.equal(adapter.market, 'bitstamp_spot');
  assert.equal(adapter.symbol, 'BTC/USD');
  assert.equal(adapter.stream, 'trades');
  assert.equal(adapter.url, 'wss://ws.bitstamp.net');
  assert.equal(adapter.restUrl, 'https://www.bitstamp.net/api/v2/order_book/btcusd/?group=1');
  assert.equal(adapter.ackMode, 'explicit');
  assert.equal(adapter.boundary, 'sequence');
  assert.deepEqual(adapter.expectedSubscriptions(), [TRADE_CHANNEL, DIFF_CHANNEL]);
  assert.deepEqual(
    adapter.subscribeMessages(),
    [],
    'the adapter sends its subscriptions inside connection preparation - subscribing must precede the REST snapshot',
  );
});

test('the keep-alive is the client heartbeat, sent after 15 s of venue silence', () => {
  const adapter = createBitstampAdapter();
  const plan = adapter.keepAlive();
  assert.equal(plan.noActivityMs, 15_000);
  assert.deepEqual(JSON.parse(plan.payload()), { event: 'bts:heartbeat' });
});

test('the acknowledgement is read per channel, and the venue events have their kinds', () => {
  const adapter = createBitstampAdapter();
  assert.deepEqual(parse(adapter, ackDiffs), { kind: 'subscription', key: DIFF_CHANNEL, ok: true, detail: '' });
  assert.deepEqual(parse(adapter, ackTrades), { kind: 'subscription', key: TRADE_CHANNEL, ok: true, detail: '' });

  assert.deepEqual(parse(adapter, heartbeatAck), { kind: 'heartbeat', answered: true });
  const error = parse(adapter, errorFrame);
  assert.equal(error.kind, 'protocol-error');
  assert.match(error.reason, /Bad subscription string/);
  const coded = parse(adapter, { event: 'bts:error', channel: '', data: { code: 4009, message: 'Connection is unauthorized.' } });
  assert.match(coded.reason, /code 4009/);
  const asked = parse(adapter, reconnect);
  assert.equal(asked.kind, 'shutdown');
  assert.throws(() => adapter.parse('not json'), /unrecognised Bitstamp/);
  assert.throws(() => parse(adapter, { event: 'bts:unsubscription_succeeded', channel: TRADE_CHANNEL, data: {} }), /unrecognised Bitstamp/);
});

// ---------------------------------------------------------------------------------------------------
// The diff feed: absolute quantities, zero removes, the microsecond clock
// ---------------------------------------------------------------------------------------------------

test('a diff frame carries absolute quantities; zero removes; the microtimestamp is the ordering', () => {
  const adapter = createBitstampAdapter();
  const frame = diff(1791403680093950);
  assert.deepEqual(parse(adapter, frame), { kind: 'data', depth: true });
  assert.equal(adapter.venueSeqOf(JSON.stringify(frame)), 1791403680093950);
  assert.deepEqual(changesOf(adapter, frame), {
    replace: false,
    changes: [
      { side: 'bid', price: 83463.08, size: 0.55048175 },
      { side: 'ask', price: 83473.08, size: 0 },
    ],
  });
  assert.equal(adapter.venueSeqOf(JSON.stringify(trade())), null, 'a trade carries no book time for the board');
});

test('a malformed diff frame is refused, never applied', () => {
  const adapter = createBitstampAdapter();
  const broken = [
    { ...diff(1), data: { ...diff(1).data, microtimestamp: undefined } },
    { ...diff(1), data: { ...diff(1).data, microtimestamp: 'x' } },
    { ...diff(1), data: { ...diff(1).data, microtimestamp: '0' } },
    { ...diff(1), data: { ...diff(1).data, bids: 'nope' } },
    { ...diff(1), data: { ...diff(1).data, bids: [], asks: [] } },
    { ...diff(1), data: { ...diff(1).data, bids: [['x', '1']] } },
    { ...diff(1), data: { ...diff(1).data, bids: [['-1', '1']] } },
    { ...diff(1), data: { ...diff(1).data, bids: [[['1'], '1']] } },
    { ...diff(1), data: { ...diff(1).data, asks: [['1', '-1']] } },
    { ...diff(1), data: { ...diff(1).data, asks: [['1']] } },
  ];
  for (const frame of broken) {
    const parsed = parse(adapter, frame);
    assert.equal(parsed.kind, 'protocol-error', `parse refuses ${JSON.stringify(frame.data).slice(0, 70)}`);
  }
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(diff(2))).status, 'resync', 'with no snapshot, nothing is accepted');
});

test('trades are classified as trades and produce no board changes', () => {
  const adapter = createBitstampAdapter();
  assert.deepEqual(parse(adapter, trade()), { kind: 'data', trade: true });
  assert.deepEqual(changesOf(adapter, trade()), { replace: false, changes: [] });
  const bad = [
    trade({ price: 'x' }),
    trade({ amount: '-1' }),
    trade({ amount: 0 }),
    trade({ type: 7 }),
    trade({ microtimestamp: 'x' }),
  ];
  for (const frame of bad) {
    assert.throws(() => parse(adapter, frame), /malformed Bitstamp trade frame/);
  }
});

// ---------------------------------------------------------------------------------------------------
// The snapshot boundary and the diff continuity
// ---------------------------------------------------------------------------------------------------

test('the snapshot boundary accepts diffs after it, discards diffs at or inside it', () => {
  const subject = createBitstampAdapter();
  subject.syncSnapshot(snapshotPayload(100, [['49999.00', '1.0']], [['50001.00', '2.0']]));
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(99))).status, 'discarded', 'inside the snapshot');
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(100))).status, 'discarded',
    'at the boundary: the venue algorithm treats it as inside');
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(101))).status, 'applied');

  const event = diff(101, [['50000.00', '1.25']], [['50001.00', '2.50']]);
  assert.deepEqual(changesOf(subject, event), {
    replace: true,
    levels: [
      { side: 'bid', price: 49999, size: 1 },
      { side: 'ask', price: 50001, size: 2.5 },
      { side: 'bid', price: 50000, size: 1.25 },
    ],
  }, 'the first accepted frame is the snapshot as one replacement, carrying the frame with it');
  assert.deepEqual(changesOf(subject, event), {
    replace: false,
    changes: [
      { side: 'bid', price: 50000, size: 1.25 },
      { side: 'ask', price: 50001, size: 2.5 },
    ],
  }, 'and only once');
});

test('a diff regressing in source time fails closed until a new connection', () => {
  const subject = createBitstampAdapter();
  subject.syncSnapshot(snapshotPayload(100));
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(102))).status, 'applied');
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(102))).status, 'applied', 'an equal microtimestamp is not stale');
  const regression = subject.acceptDepthEvent(JSON.stringify(diff(101)));
  assert.equal(regression.status, 'resync', 'a regression is stale');
  assert.match(regression.reason, /regressed/);
  assert.equal(subject.needsResync, true);
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(103))).status, 'resync', 'the refusal persists');
});

test('a late diff inside the boundary is stale news, not a failure', () => {
  const subject = createBitstampAdapter();
  subject.syncSnapshot(snapshotPayload(100));
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(102))).status, 'applied');
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(99))).status, 'discarded',
    'behind the cursor but inside the snapshot: stale news, quietly discarded');
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(103))).status, 'applied',
    'and the stream continues');
  assert.equal(subject.connects({ current: { raw: JSON.stringify(diff(100)) } }), true,
    'a frame the snapshot already covers is not a broken stream: reception discards it before stamping');
});

test('the sync replays what the snapshot does not cover, and refetches a regressing buffer', async () => {
  let calls = 0;
  const sync = createBitstampSynchronizer({
    fetchSnapshot: async () => {
      calls += 1;
      return snapshotPayload(100);
    },
  });
  const result = await sync.sync([diff(100), diff(102)]);
  assert.equal(result.status, 'synced');
  assert.equal(calls, 1, 'a diff at the boundary is inside the snapshot: no refetch');

  let regressCalls = 0;
  const regressSync = createBitstampSynchronizer({
    fetchSnapshot: async () => {
      regressCalls += 1;
      return snapshotPayload(regressCalls === 1 ? 100 : 104);
    },
  });
  const regressResult = await regressSync.sync([diff(102), diff(101)]);
  assert.equal(regressResult.status, 'synced');
  assert.equal(regressCalls, 2, 'a regressing buffer forces one refetch that provably covers both');
});

test('the sync gives up bounded when the buffer keeps regressing', async () => {
  const sync = createBitstampSynchronizer({
    fetchSnapshot: async () => snapshotPayload(100),
  });
  await assert.rejects(() => sync.sync([diff(102), diff(101)]), /regress|could not be proven/i);
  assert.equal(sync.needsResync, true, 'giving up leaves the stream unsynchronized');
  assert.equal(sync.accept(diff(300)).status, 'resync', 'and a direct accept must refuse');
});

test('the sync discards equal-boundary diffs in the buffer before judging order', async () => {
  let calls = 0;
  const sync = createBitstampSynchronizer({
    fetchSnapshot: async () => {
      calls += 1;
      return snapshotPayload(100);
    },
  });
  const result = await sync.sync([diff(101), diff(100)]);
  assert.equal(result.status, 'synced',
    'the equal-boundary diff is inside the snapshot: it is skipped, not a regression');
  assert.equal(calls, 1, 'no refetch was needed');
  assert.equal(sync.accept(diff(101)).status, 'applied');
});

test('a new connection resets synchronization and unsynchronized diffs are refused', () => {
  const subject = createBitstampAdapter();
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(200))).status, 'resync', 'before any sync');
  subject.syncSnapshot(snapshotPayload(100));
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(101))).status, 'applied');
  subject.resetConnection();
  assert.equal(subject.needsResync, true);
  assert.equal(subject.acceptDepthEvent(JSON.stringify(diff(102))).status, 'resync');
});

test('the book-side rule refuses a regressing diff and lets trades through', () => {
  const subject = createBitstampAdapter();
  subject.syncSnapshot(snapshotPayload(100));
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: subject.venueSeqOf(JSON.stringify(frame)) } });
  assert.equal(subject.connects({ current: env(diff(101)) }), true);
  assert.equal(subject.connects({ current: env(diff(102)) }), true);
  assert.equal(subject.connects({ current: env(diff(101)) }), false, 'the book refuses a regression too');
  assert.equal(subject.connects({ current: env(diff(103)) }), false, 'and the refusal persists');
  subject.resetConnection();
  subject.syncSnapshot(snapshotPayload(300));
  assert.equal(subject.connects({ current: env(trade()) }), true, 'a trade carries no book time: nothing to connect');
});

// ---------------------------------------------------------------------------------------------------
// The preparation seam: subscribe first, fetch, buffer, replay
// ---------------------------------------------------------------------------------------------------

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

async function until(predicate, { timeoutMs = 3000, stepMs = 5, label = 'the condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return true;
    if (Date.now() >= deadline) throw new Error(`timed out (${timeoutMs} ms) waiting for ${label}`);
    await new Promise((done) => setTimeout(done, stepMs));
  }
}

test('preparation subscribes first, then fetches; buffered frames replay against the snapshot', async () => {
  let releaseSnapshot;
  const pending = new Promise((resolve) => {
    releaseSnapshot = resolve;
  });
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/',
    fetchImpl: async () => ({ ok: true, json: async () => pending }),
  });
  const { sockets, impl } = fakeSockets();
  const envelopes = [];
  const connection = createReceiveConnection({
    adapter,
    market: 'bitstamp_spot',
    venue: 'bitstamp_spot',
    runId: 'run-prep',
    webSocketImpl: impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();

  assert.deepEqual(
    sockets[0].sent.map((message) => JSON.parse(message).data.channel).sort(),
    [DIFF_CHANNEL, TRADE_CHANNEL],
    'the subscriptions go out before the REST fetch is released',
  );

  sockets[0].deliver(JSON.stringify(ackTrades));
  sockets[0].deliver(JSON.stringify(ackDiffs));
  sockets[0].deliver(JSON.stringify(diff(99)));
  sockets[0].deliver(JSON.stringify(diff(101)));
  sockets[0].deliver(JSON.stringify(trade()));
  sockets[0].deliver(JSON.stringify(diff(102)));

  releaseSnapshot(snapshotPayload(100));
  await until(() => envelopes.length === 2, { label: 'the accepted diffs to be stamped' });
  assert.deepEqual(
    envelopes.map((envelope) => envelope.meta.venue_seq),
    [101, 102],
    'the diff inside the snapshot boundary is discarded; the later ones replay; the trade does not enter',
  );
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'the redelivered acks to establish' });
  assert.equal(connection.generation, 1);
  connection.stop();
});

test('a missing acknowledgement leaves the link pending while the diffs still flow', async () => {
  let releaseSnapshot;
  const pending = new Promise((resolve) => {
    releaseSnapshot = resolve;
  });
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/?group=1',
    fetchImpl: async () => ({ ok: true, json: async () => pending }),
  });
  const { sockets, impl } = fakeSockets();
  const envelopes = [];
  const connection = createReceiveConnection({
    adapter,
    market: 'bitstamp_spot',
    venue: 'bitstamp_spot',
    runId: 'run-missing-ack',
    webSocketImpl: impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  connection.start();
  sockets[0].onopen();
  sockets[0].deliver(JSON.stringify(ackTrades));
  sockets[0].deliver(JSON.stringify(diff(101)));
  releaseSnapshot(snapshotPayload(100));
  await until(() => envelopes.length === 1, { label: 'the diff to be stamped' });
  assert.equal(connection.subscriptionState, 'pending',
    'one ack is not the whole set - and no deadline is armed in this configuration, so the link waits');
  connection.stop();
});

test('a stale REST response from an old generation cannot synchronize the replacement', async () => {
  const pending = [];
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/',
    fetchImpl: () => new Promise((resolve) => pending.push({ resolve })),
  });
  const first = adapter.onConnectionOpen({ connectionId: 'run:bitstamp:1', generation: 1, socket: null });
  const second = adapter.onConnectionOpen({ connectionId: 'run:bitstamp:2', generation: 2, socket: null });
  pending[1].resolve({ ok: true, json: async () => snapshotPayload(200) });
  await second;
  pending[0].resolve({ ok: true, json: async () => snapshotPayload(100) });
  await first;
  assert.equal(adapter.boundaryUs, 200, 'the newer generation owns the boundary');
});

test('a preparation buffer beyond the cap fails the sync instead of growing forever', async () => {
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/?group=1',
    fetchImpl: () =>
      new Promise((resolve) => setTimeout(() => resolve({ ok: true, json: async () => snapshotPayload(100) }), 30)),
  });
  const opening = adapter.onConnectionOpen({});
  for (let i = 0; i < 10_001; i += 1) adapter.bufferDuringPreparation(JSON.stringify(diff(200 + i)));
  await assert.rejects(() => opening, /buffer overflowed/);
  assert.equal(adapter.needsResync, true, 'the overflowed stream is unsynchronized');
});

test('a hanging REST fetch is bounded by the deadline and leaves the stream unsynchronized', async () => {
  let aborted = 0;
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/?group=1',
    restTimeoutMs: 50,
    fetchImpl: (url, options) =>
      new Promise((resolve, reject) => {
        options?.signal?.addEventListener('abort', () => {
          aborted += 1;
          reject(new Error('aborted by the REST deadline'));
        });
      }),
  });
  await assert.rejects(() => adapter.syncDepth(), /aborted|deadline/);
  assert.equal(aborted, 3, 'each of the bounded attempts carries its own deadline');
  assert.equal(adapter.needsResync, true, 'giving up leaves the stream unsynchronized');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(diff(300))).status, 'resync',
    'and a direct accept must refuse');
});

test('REST snapshot sync uses a deterministic local HTTP server and official request shape', async () => {
  const requests = [];
  const server = createServer((request, response) => {
    requests.push(request.url);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(snapshotPayload(100, [['50000.00', '1.0']], [['50001.00', '2.0']])));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    const adapter = createBitstampAdapter({
      url: 'ws://127.0.0.1:1/',
      restUrl: `http://127.0.0.1:${port}/api/v2/order_book/btcusd/`,
    });
    await adapter.syncDepth();
    assert.deepEqual(requests, ['/api/v2/order_book/btcusd/']);
    assert.equal(adapter.needsResync, false);
    assert.equal(adapter.boundaryUs, 100);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('a malformed REST snapshot cannot synchronize and is retried bounded', async () => {
  let calls = 0;
  const adapter = createBitstampAdapter({
    url: 'ws://127.0.0.1:1/',
    restUrl: 'http://127.0.0.1:1/order_book/btcusd/',
    fetchImpl: async () => {
      calls += 1;
      return { ok: true, json: async () => ({ bids: [], asks: [] }) };
    },
  });
  await assert.rejects(() => adapter.syncDepth(), /microtimestamp|incomplete/i);
  assert.equal(calls, 3, 'bounded attempts, never an unbounded refetch');
  assert.equal(adapter.needsResync, true, 'giving up leaves the stream unsynchronized');
});

// ---------------------------------------------------------------------------------------------------
// The entrance's configuration registers the market
// ---------------------------------------------------------------------------------------------------

test('the configuration registers bitstamp_spot as a buildable venue', () => {
  assert.equal(knownVenues().includes('bitstamp_spot'), true);
  const adapter = adapterFor({ venue: 'bitstamp_spot', market: 'bitstamp_spot', symbol: 'BTC/USD', stream: 'trades' });
  assert.equal(adapter.market, 'bitstamp_spot');
  assert.equal(adapter.stream, 'trades');
  assert.throws(
    () => adapterFor({ venue: 'bitstamp_spot', market: 'bitstamp_spot', symbol: 'BTC/USD', stream: 'book' }),
    /carries the trades stream|stream/,
  );
});

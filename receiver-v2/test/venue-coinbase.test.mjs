/**
 * Coinbase Advanced Trade public WS: the spot market this receiver admits - BTC-USD.
 *
 * The fixtures below are the exact shapes the real public stream sent (read-only capture on
 * `wss://advanced-trade-ws.coinbase.com`): the subscribe acknowledgement is a `subscriptions` frame
 * carrying the whole acknowledged set at once (a bad product answers with an empty set, silently);
 * `l2_data` pushes a full `snapshot` and then `update`s whose `new_quantity` is the level's
 * post-update quantity (0 removes it); `market_trades` carries trades; `heartbeats` is the venue's
 * own liveness channel (the documented keep-alive - there is no client ping to send).
 *
 * The sequence rule is the venue's own: `sequence_num` is a book-change counter and the server
 * coalesces changes, so forward skips of 2-3 are normal delivery (measured live). Continuity is:
 * strictly forward, within the coalescing allowance (32, v1's calibrated ceiling); a repeat, a
 * backwards step, a skip beyond the allowance, or an update before any snapshot fails closed.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createCoinbaseAdapter } from '../src/ingest/venues/coinbase.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

// ---------------------------------------------------------------------------------------------------
// Fixtures: exact shapes from the live read-only capture (level lists trimmed, fields kept verbatim).
// ---------------------------------------------------------------------------------------------------

const PRODUCT = 'BTC-USD';

const ack = (subscriptions) => ({
  channel: 'subscriptions',
  timestamp: '2026-10-07T11:59:37.822235563Z',
  sequence_num: 5,
  events: [{ subscriptions }],
});
const ackLevel2 = ack({ level2: [PRODUCT] });
const ackAll = ack({ heartbeats: ['heartbeats'], level2: [PRODUCT], market_trades: [PRODUCT] });
const ackEmpty = { ...ack({}), timestamp: '2026-10-07T11:59:57.405279524Z', sequence_num: 0 };

const snapshot = (seq, updates = [
  { side: 'bid', event_time: '2026-10-07T12:03:48.252184Z', price_level: '83603.05', new_quantity: '0.30237105' },
  { side: 'bid', event_time: '2026-10-07T12:03:48.252184Z', price_level: '83602.65', new_quantity: '0.14' },
  { side: 'offer', event_time: '2026-10-07T12:03:48.252184Z', price_level: '83603.06', new_quantity: '1.5' },
  { side: 'offer', event_time: '2026-10-07T12:03:48.252184Z', price_level: '83605.15', new_quantity: '0.00717677' },
]) => ({
  channel: 'l2_data',
  timestamp: '2026-10-07T12:03:48.252184Z',
  sequence_num: seq,
  events: [{ type: 'snapshot', product_id: PRODUCT, updates }],
});
const update = (seq, updates = [
  { side: 'offer', event_time: '2026-10-07T12:03:48.29989Z', price_level: '83603.06', new_quantity: '0' },
  { side: 'offer', event_time: '2026-10-07T12:03:48.29989Z', price_level: '83605.15', new_quantity: '0.00717677' },
  { side: 'bid', event_time: '2026-10-07T12:03:48.29989Z', price_level: '83604.76', new_quantity: '0.25688833' },
  { side: 'bid', event_time: '2026-10-07T12:03:48.29989Z', price_level: '83604.05', new_quantity: '0' },
]) => ({
  channel: 'l2_data',
  timestamp: '2026-10-07T12:03:48.29989Z',
  sequence_num: seq,
  events: [{ type: 'update', product_id: PRODUCT, updates }],
});

const trades = (seq, type = 'snapshot') => ({
  channel: 'market_trades',
  timestamp: '2026-10-07T12:03:48.27226Z',
  sequence_num: seq,
  events: [
    {
      type,
      trades: [
        { product_id: PRODUCT, trade_id: '1103340916', price: '83603.05', size: '0.00000006', time: '2026-10-07T12:03:48.27226Z', side: 'BUY' },
        { product_id: PRODUCT, trade_id: '1103340915', price: '83603.06', size: '0.000597', time: '2026-10-07T12:03:48.134006Z', side: 'SELL' },
      ],
    },
  ],
});

const heartbeat = {
  channel: 'heartbeats',
  timestamp: '2026-10-07T12:03:48.753495258Z',
  sequence_num: 13,
  events: [{ current_time: '2026-10-07 12:03:48.750958988 +0000 UTC m=+66999.250987985', heartbeat_counter: 66999 }],
};

const parse = (adapter, payload) => adapter.parse(typeof payload === 'string' ? payload : JSON.stringify(payload));
const changesOf = (adapter, payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

// ---------------------------------------------------------------------------------------------------
// The wire contract: endpoint, channels, the cumulative acknowledgement, and the keep-alive
// ---------------------------------------------------------------------------------------------------

test('the adapter names the Advanced Trade endpoint, its three channels and their ack keys', () => {
  const adapter = createCoinbaseAdapter();
  assert.equal(adapter.market, 'coinbase_spot');
  assert.equal(adapter.symbol, PRODUCT);
  assert.equal(adapter.stream, 'trades');
  assert.equal(adapter.url, 'wss://advanced-trade-ws.coinbase.com');
  assert.equal(adapter.restUrl, `https://api.exchange.coinbase.com/products/${PRODUCT}/book?level=3`);
  assert.equal(adapter.ackMode, 'explicit');
  assert.equal(adapter.boundary, 'sequence');
  assert.deepEqual(adapter.expectedSubscriptions(), [
    `level2:${PRODUCT}`,
    `market_trades:${PRODUCT}`,
    'heartbeats:heartbeats',
  ]);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { type: 'subscribe', product_ids: [PRODUCT], channel: 'level2' },
    { type: 'subscribe', product_ids: [PRODUCT], channel: 'market_trades' },
    { type: 'subscribe', channel: 'heartbeats' },
  ]);
});

test('there is nothing to send as a keep-alive: the venue pushes heartbeats', () => {
  const adapter = createCoinbaseAdapter();
  assert.equal(adapter.keepAlive(), null, 'no client ping is documented; heartbeats keep the link open');
});

test('the cumulative acknowledgement is read as the whole set it carries, per channel and product', () => {
  const adapter = createCoinbaseAdapter();
  const single = parse(adapter, ackLevel2);
  assert.equal(single.kind, 'subscription');
  assert.equal(single.ok, true);
  assert.equal(single.full, true, 'the acknowledgement is the whole state, not a delta');
  assert.deepEqual(single.keys, [`level2:${PRODUCT}`], 'the first ack carries only the first channel');

  const all = parse(adapter, ackAll);
  assert.deepEqual(all.keys, [`heartbeats:heartbeats`, `level2:${PRODUCT}`, `market_trades:${PRODUCT}`],
    'a later ack carries the accumulated set');

  const empty = parse(adapter, ackEmpty);
  assert.deepEqual(empty, { kind: 'subscription', keys: [], full: true, ok: true, detail: '' },
    'an empty set is a state too: it acknowledges nothing');
});

test('heartbeats are liveness, trades are trades, and an error frame is a protocol error', () => {
  const adapter = createCoinbaseAdapter();
  assert.deepEqual(parse(adapter, heartbeat), { kind: 'heartbeat', answered: false });
  assert.deepEqual(parse(adapter, trades(3)), { kind: 'data', trade: true });
  const error = parse(adapter, { type: 'error', message: 'Invalid product' });
  assert.equal(error.kind, 'protocol-error');
  assert.match(error.reason, /Invalid product/);
  assert.throws(() => adapter.parse('not json'), /unrecognised Coinbase frame/);
});

// ---------------------------------------------------------------------------------------------------
// Board changes and the sequence rule
// ---------------------------------------------------------------------------------------------------

test('a snapshot replaces the board; an update carries post-update quantities and 0 removes', () => {
  const adapter = createCoinbaseAdapter();
  assert.deepEqual(changesOf(adapter, snapshot(0)), {
    replace: true,
    levels: [
      { side: 'bid', price: 83603.05, size: 0.30237105 },
      { side: 'bid', price: 83602.65, size: 0.14 },
      { side: 'ask', price: 83603.06, size: 1.5 },
      { side: 'ask', price: 83605.15, size: 0.00717677 },
    ],
  });
  assert.deepEqual(changesOf(adapter, update(1)), {
    replace: false,
    changes: [
      { side: 'ask', price: 83603.06, size: 0 },
      { side: 'ask', price: 83605.15, size: 0.00717677 },
      { side: 'bid', price: 83604.76, size: 0.25688833 },
      { side: 'bid', price: 83604.05, size: 0 },
    ],
  });
});

test('the sequence advances within the coalescing allowance and fails closed outside it', () => {
  const adapter = createCoinbaseAdapter();
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(0))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(1))).status, 'applied', 'normal +1');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(4))).status, 'applied',
    'a +3 skip is measured normal delivery: the server coalesces changes into one frame');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(36))).status, 'applied',
    'a skip of exactly the allowance (32) is still normal delivery');

  const repeat = adapter.acceptDepthEvent(JSON.stringify(update(36)));
  assert.equal(repeat.status, 'resync', 'a repeated sequence_num is not forward');
  assert.match(repeat.reason, /sequence/i);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(37))).status, 'resync',
    'once failed, the stream stays failed until a new connection');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(1000))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(1000))).status, 'resync', 'a held seq does not advance');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(1000))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(999))).status, 'resync', 'a backwards step fails closed');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(1000))).status, 'applied');
  const beyond = adapter.acceptDepthEvent(JSON.stringify(update(1033)));
  assert.equal(beyond.status, 'resync', 'a skip beyond the allowance is a suspected loss');
  assert.match(beyond.reason, /allowance|loss/i);

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(5))).status, 'resync', 'an update before any snapshot');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(7))).status, 'resync',
    'and a snapshot does not reopen a failed stream');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(7))).status, 'applied', 'a snapshot anchors a fresh stream');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(8))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(3))).status, 'applied', 'a later snapshot re-anchors');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(4))).status, 'applied');
});

test('the tolerance is the venue-calibrated 32 and can be overridden for re-calibration', () => {
  const adapter = createCoinbaseAdapter();
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(0))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(33))).status, 'resync', 'default: 33 is beyond 32');

  const wide = createCoinbaseAdapter({ seqSkipTolerance: 64 });
  wide.onConnectionOpen();
  assert.equal(wide.acceptDepthEvent(JSON.stringify(snapshot(0))).status, 'applied');
  assert.equal(wide.acceptDepthEvent(JSON.stringify(update(33))).status, 'applied', 'with the override, 33 is inside');
});

// ---------------------------------------------------------------------------------------------------
// Malformed frames and product validation: fail closed
// ---------------------------------------------------------------------------------------------------

test('a malformed l2 frame is refused, never applied', () => {
  const adapter = createCoinbaseAdapter();
  adapter.onConnectionOpen();
  const broken = [
    { ...snapshot(0), sequence_num: undefined },
    { ...snapshot(0), sequence_num: 'x' },
    { ...snapshot(0), events: 'nope' },
    { ...snapshot(0), events: [] },
    { ...snapshot(0), events: [{ type: 'nope', product_id: PRODUCT, updates: [] }] },
    { ...snapshot(0), events: [{ type: 'snapshot', product_id: PRODUCT, updates: [{ side: 'x', event_time: 't', price_level: '1', new_quantity: '1' }] }] },
    { ...snapshot(0), events: [{ type: 'snapshot', product_id: PRODUCT, updates: [{ side: 'bid', event_time: 't', price_level: 'abc', new_quantity: '1' }] }] },
    { ...snapshot(0), events: [{ type: 'snapshot', product_id: PRODUCT, updates: [{ side: 'bid', event_time: 't', price_level: '1', new_quantity: '-1' }] }] },
    { ...snapshot(0), events: [{ type: 'snapshot', product_id: PRODUCT, updates: [{ side: 'bid', event_time: '', price_level: '1', new_quantity: '1' }] }] },
    { ...snapshot(0), events: [{ type: 'snapshot', product_id: PRODUCT, updates: 'nope' }] },
    { ...update(1), events: [{ type: 'update', product_id: PRODUCT, updates: [{ side: 'bid', event_time: 't', price_level: '1' }] }] },
  ];
  for (const frame of broken) {
    const parsed = parse(adapter, frame);
    assert.equal(parsed.kind, 'protocol-error', `parse refuses ${JSON.stringify(frame.events).slice(0, 60)}`);
    assert.equal(adapter.acceptDepthEvent(JSON.stringify(frame)).status, 'malformed');
  }
});

test('frames for another product cannot enter our stream', () => {
  const adapter = createCoinbaseAdapter();
  const foreign = { ...snapshot(0), events: [{ type: 'snapshot', product_id: 'ETH-USD', updates: [] }] };
  assert.throws(() => parse(adapter, foreign), /unrecognised Coinbase/);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(foreign)).status, 'malformed');

  const foreignTrade = { ...trades(1), events: [{ type: 'snapshot', trades: [{ product_id: 'ETH-USD', trade_id: '1', price: '1', size: '1', time: 't', side: 'BUY' }] }] };
  assert.throws(() => parse(adapter, foreignTrade), /unrecognised Coinbase|malformed Coinbase/);

  assert.throws(() => createCoinbaseAdapter({ symbol: 'ETH-USD' }), /unsupported|instrument/i);
});

// ---------------------------------------------------------------------------------------------------
// Trades carry no book changes; the sequence belongs to the book frames only
// ---------------------------------------------------------------------------------------------------

test('trades are classified as trades and produce no board changes', () => {
  const adapter = createCoinbaseAdapter();
  assert.deepEqual(parse(adapter, trades(3, 'update')), { kind: 'data', trade: true });
  assert.deepEqual(changesOf(adapter, trades(3)), { replace: false, changes: [] });
  assert.equal(adapter.venueSeqOf(JSON.stringify(trades(3))), null, 'a trade has no book sequence for the board');
  assert.equal(adapter.venueSeqOf(JSON.stringify(snapshot(0))), 0, 'an l2 frame carries its sequence_num');
  assert.equal(adapter.venueSeqOf(JSON.stringify(heartbeat)), null, 'a heartbeat has no book sequence');
});

// ---------------------------------------------------------------------------------------------------
// The connection rule the book resolves (sequence: forward, within the allowance)
// ---------------------------------------------------------------------------------------------------

test('the connection rule requires forward steps within the allowance, and a refusal persists', () => {
  const adapter = createCoinbaseAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: adapter.venueSeqOf(JSON.stringify(frame)) } });
  assert.equal(adapter.connects({ previous: null, current: env(snapshot(0)) }), true);
  assert.equal(adapter.connects({ previous: env(snapshot(0)), current: env(update(1)) }), true);
  assert.equal(adapter.connects({ previous: env(update(1)), current: env(update(4)) }), true, 'coalescing is normal');
  assert.equal(adapter.connects({ previous: env(update(4)), current: env(update(4)) }), false, 'a repeat is refused');
  assert.equal(adapter.connects({ previous: env(update(4)), current: env(update(5)) }), false,
    'and the refusal persists until a new connection, as reception refuses');
  assert.equal(adapter.connects({ previous: env(update(4)), current: env(trades(6)) }), true,
    'a trade carries no book sequence: nothing to connect, nothing refused');

  adapter.onConnectionOpen();
  assert.equal(adapter.connects({ previous: null, current: env(snapshot(0)) }), true, 'a new connection re-anchors');
  assert.equal(adapter.connects({ previous: env(snapshot(0)), current: env(update(100)) }), false, 'beyond the allowance');
  assert.equal(adapter.connects({ previous: env(update(100)), current: env(update(101)) }), false, 'still refused');

  const fresh = createCoinbaseAdapter();
  fresh.onConnectionOpen();
  assert.equal(fresh.connects({ previous: null, current: env(update(1)) }), false,
    'an update with nothing proven before it is refused');
});

test('a coinbase snapshot and update drive the single-process board to serving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'coinbase-serving-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-coinbase' });
  const structure = createStructure({
    market: 'coinbase_spot',
    stream: 'trades',
    adapter: createCoinbaseAdapter(),
    durability: store,
    webSocketImpl: function unused() {
      throw new Error('this test feeds frames directly');
    },
    spoolDir: null,
  });
  structure.accept('conn-1');
  try {
    const envelope = (seq, frame) =>
      makeEnvelope({
        market: 'coinbase_spot',
        stream: 'trades',
        connectionId: 'conn-1',
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: JSON.stringify(frame),
        meta: { first_seq: 1, venue_seq: frame.sequence_num ?? null },
      });
    const first = structure.feed(envelope(1, snapshot(0)));
    assert.equal(first.applied, true, `the snapshot was applied: ${first.reason ?? ''}`);
    const second = structure.feed(envelope(2, update(1)));
    assert.equal(second.applied, true, `the update was applied: ${second.reason ?? ''}`);
    assert.equal(structure.book.isRunning, true, 'the board serves');
    assert.equal(structure.stats.applied, 2);
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// The production connection: cumulative acknowledgement, partial failure, fail-closed replacement
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

function openConnection({ adapter, sockets, envelopes = [], diagnostics = [], ackDeadlineMs = 60_000 }) {
  return createReceiveConnection({
    adapter,
    market: adapter.market,
    venue: 'coinbase',
    runId: 'run-coinbase',
    webSocketImpl: sockets.impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs,
  });
}

test('one cumulative acknowledgement frame establishes the link, and the live frames flow', async () => {
  const adapter = createCoinbaseAdapter();
  const sockets = fakeSockets();
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  assert.deepEqual(
    sockets.sockets[0].sent.map((message) => JSON.parse(message).channel),
    ['level2', 'market_trades', 'heartbeats'],
    'each channel is asked for in its own frame',
  );
  sockets.sockets[0].deliver(JSON.stringify(ackAll));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver(JSON.stringify(snapshot(0)));
  sockets.sockets[0].deliver(JSON.stringify(trades(3)));
  sockets.sockets[0].deliver(JSON.stringify(update(1)));
  sockets.sockets[0].deliver(JSON.stringify(heartbeat));
  sockets.sockets[0].deliver(JSON.stringify(ackEmpty));
  await until(() => envelopes.length === 2, { label: 'the book frames to be stamped' });
  assert.deepEqual(envelopes.map((envelope) => envelope.meta.venue_seq), [0, 1],
    'the snapshot and the update carry their sequence_num; nothing else is stamped');
  assert.equal(connection.generation, 1);
  connection.stop();
});

test('a partial acknowledgement fails the link at the deadline', async () => {
  const adapter = createCoinbaseAdapter();
  const sockets = fakeSockets();
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, diagnostics, ackDeadlineMs: 150 });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackLevel2));
  assert.equal(connection.subscriptionState, 'pending', 'asking is not agreeing');
  await until(() => connection.subscriptionState === 'failed', { label: 'the ack deadline' });
  assert.equal(diagnostics.some(({ reason }) => /subscription failed: the subscription ack deadline passed/.test(reason)), true);
  connection.stop();
});

test('the acknowledged set is a state: a shrink or an emptying fails the link', async () => {
  // A later ack that no longer names level2 must not leave the link established on keys that
  // merely appeared once: the whole set is the truth.
  const shrinkAdapter = createCoinbaseAdapter();
  const shrinkSockets = fakeSockets();
  const shrinkDiagnostics = [];
  const shrink = openConnection({ adapter: shrinkAdapter, sockets: shrinkSockets, diagnostics: shrinkDiagnostics });
  shrink.start();
  shrinkSockets.sockets[0].onopen();
  shrinkSockets.sockets[0].deliver(JSON.stringify(ackLevel2));
  shrinkSockets.sockets[0].deliver(JSON.stringify(ack({ market_trades: [PRODUCT], heartbeats: ['heartbeats'] })));
  await until(() => shrink.subscriptionState === 'failed', { label: 'the vanished subscription' });
  assert.equal(shrinkDiagnostics.some(({ reason }) => /left the acknowledged set/.test(reason)), true);
  shrinkSockets.sockets[0].deliver(JSON.stringify(ackAll));
  assert.equal(shrink.subscriptionState, 'failed',
    'a later full set does not talk the link back: replacing the connection is the recovery');
  shrink.stop();

  // And an emptied set after establishment is a lost link, not a quiet one.
  const emptyAdapter = createCoinbaseAdapter();
  const emptySockets = fakeSockets();
  const emptyDiagnostics = [];
  const emptied = openConnection({ adapter: emptyAdapter, sockets: emptySockets, diagnostics: emptyDiagnostics });
  emptied.start();
  emptySockets.sockets[0].onopen();
  emptySockets.sockets[0].deliver(JSON.stringify(ackAll));
  await until(() => emptied.subscriptionState === 'acknowledged', { label: 'establishment' });
  emptySockets.sockets[0].deliver(JSON.stringify(ackEmpty));
  await until(() => emptied.subscriptionState === 'failed', { label: 'the emptied set' });
  assert.equal(emptyDiagnostics.some(({ reason }) => /left the acknowledged set/.test(reason)), true);
  emptySockets.sockets[0].deliver(JSON.stringify(ackAll));
  assert.equal(emptied.subscriptionState, 'failed', 'and it stays failed when the set is returned');
  emptied.stop();
});

test('a skip beyond the allowance replaces the connection fail-closed instead of reaching the board', async () => {
  const adapter = createCoinbaseAdapter();
  const sockets = fakeSockets();
  const envelopes = [];
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, envelopes, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackAll));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });
  sockets.sockets[0].deliver(JSON.stringify(snapshot(0)));
  sockets.sockets[0].deliver(JSON.stringify(update(1)));
  await until(() => envelopes.length === 2, { label: 'the first two frames' });

  sockets.sockets[0].deliver(JSON.stringify(update(500)));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(envelopes.length, 2, 'the lossy frame never became an envelope');
  assert.equal(diagnostics.some(({ reason }) => /sequence\/checksum failure/.test(reason)), true);
  connection.stop();
});

test('an unclassifiable frame is counted, not silently dropped', async () => {
  const adapter = createCoinbaseAdapter();
  const sockets = fakeSockets();
  const diagnostics = [];
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, diagnostics, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackAll));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver('this is not json');
  sockets.sockets[0].deliver(JSON.stringify({ ...snapshot(0), events: [{ type: 'snapshot', product_id: 'ETH-USD', updates: [] }] }));
  await until(() => diagnostics.filter(({ reason }) => reason === 'unparsable frame').length === 2, {
    label: 'the counted frames',
  });
  assert.equal(envelopes.length, 0, 'nothing unclassifiable became a frame');
  assert.equal(connection.generation, 1, 'and the link is not torn down for it');
  connection.stop();
});

// ---------------------------------------------------------------------------------------------------
// The entrance's configuration registers the market
// ---------------------------------------------------------------------------------------------------

test('the configuration registers coinbase_spot as a buildable venue', () => {
  assert.equal(knownVenues().includes('coinbase_spot'), true);
  const adapter = adapterFor({ venue: 'coinbase_spot', market: 'coinbase_spot', symbol: PRODUCT, stream: 'trades' });
  assert.equal(adapter.market, 'coinbase_spot');
  assert.equal(adapter.stream, 'trades');
  assert.throws(
    () => adapterFor({ venue: 'coinbase_spot', market: 'coinbase_spot', symbol: PRODUCT, stream: 'book' }),
    /carries the trades stream|stream/,
  );
});

/**
 * Bybit v5 public WS: the two markets this receiver admits - USDT perpetual (linear) and Spot.
 *
 * The frames used as fixtures here were captured from the real public stream read-only
 * (subscribe + snapshot + delta + trade + ping/pong), and the rules they are judged by are the
 * official ones: a snapshot replaces the local book, a delta is a diff with size 0 meaning the
 * level is gone, `u` is the update id that must move forward (backwards/duplicate fails closed,
 * forward jumps are the venue's own right - u is not guaranteed +1), `u=1` is a service-restart
 * snapshot that overwrites the book, and the subscribe ack echoes the request's `req_id` (so
 * each topic is asked for in its own request and acknowledged by its own key).
 *
 * Trades are classified and never applied to the book: reception drops them, as it does for every
 * venue here (a trade is not a recorded frame). Liquidation frames are classified and recorded -
 * they travel as frames carrying no level changes. A frame this adapter cannot classify is thrown,
 * which reception counts as an unparsable frame instead of dropping it in silence.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createBybitPerpAdapter, createBybitSpotAdapter } from '../src/ingest/venues/bybit.mjs';
import { createReceiveConnection, DEFAULT_SILENCE_DEADLINE_MS } from '../src/ingest/connection.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

// ---------------------------------------------------------------------------------------------------
// Fixtures: exact shapes from the live read-only capture (levels trimmed, fields kept verbatim).
// ---------------------------------------------------------------------------------------------------

const PERP_BOOK = 'orderbook.1000.BTCUSDT';
const SPOT_BOOK = 'orderbook.200.BTCUSDT';
const PERP_TRADE = 'publicTrade.BTCUSDT';
const PERP_LIQ = 'allLiquidation.BTCUSDT';

const perpSnapshot = (u, b = [['83769.90', '3.380'], ['83769.80', '1.114']], a = [['83770.00', '19.788'], ['83770.10', '0.002']]) => ({
  topic: PERP_BOOK,
  type: 'snapshot',
  ts: 1791365004404,
  cts: 1791365004403,
  data: { s: 'BTCUSDT', b, a, u, seq: 821670555418 },
});
const perpDelta = (u, b = [['83766.70', '0'], ['83766.50', '0.002']], a = [['83773.20', '0.002']]) => ({
  topic: PERP_BOOK,
  type: 'delta',
  ts: 1791365004603,
  data: { s: 'BTCUSDT', b, a, u, seq: 821670555855 },
});
const spotSnapshot = (u, b = [['83805', '0.150276']], a = [['83805.1', '0.993847']]) => ({
  topic: SPOT_BOOK,
  type: 'snapshot',
  ts: 1791365007116,
  cts: 1791365007112,
  data: { s: 'BTCUSDT', b, a, u, seq: 115058638774 },
});
const spotDelta = (u, b = [['83797.3', '0.11934']], a = [['83821.1', '0.023873']]) => ({
  topic: SPOT_BOOK,
  type: 'delta',
  ts: 1791365007216,
  data: { s: 'BTCUSDT', b, a, u, seq: 115058638880 },
});

/** The perpetual trade frame as it arrived: L / BT / RPI / uuid i are all real fields. */
const perpTrade = {
  topic: PERP_TRADE,
  type: 'snapshot',
  ts: 1791365008335,
  data: [
    {
      T: 1791365008334,
      s: 'BTCUSDT',
      S: 'Buy',
      v: '0.001',
      p: '83766.30',
      L: 'PlusTick',
      i: '68bc1c35-dcc0-58e8-a4c5-f59a6b790814',
      BT: false,
      RPI: false,
      seq: 821670598155,
    },
  ],
};

/** The spot trade frame as it arrived: `i` is a numeric string and there is no `L`. */
const spotTrade = {
  topic: PERP_TRADE,
  ts: 1791365009231,
  type: 'snapshot',
  data: [
    {
      i: '2290000001225843671',
      T: 1791365009230,
      p: '83805.1',
      v: '0.001466',
      S: 'Buy',
      seq: 115058640184,
      s: 'BTCUSDT',
      BT: false,
      RPI: false,
    },
  ],
};

/** The documented allLiquidation shape: Buy means a long position was liquidated. */
const perpLiquidation = {
  topic: PERP_LIQ,
  type: 'snapshot',
  ts: 1791365008335,
  data: { T: 1791365008334, s: 'BTCUSDT', S: 'Sell', v: '0.050', p: '83000.00' },
};

const linearAck = (reqId) => ({
  success: true,
  ret_msg: '',
  conn_id: 'db292md48aephea0qcg0-cg8d',
  req_id: reqId,
  op: 'subscribe',
});
const spotAck = (reqId) => ({
  success: true,
  ret_msg: 'subscribe',
  conn_id: 'd9avt13c3kkqi46e1u9g-ge1kf',
  req_id: reqId,
  op: 'subscribe',
});
const alreadyAck = (reqId) => ({
  success: false,
  ret_msg: `error:already subscribed,topic:${reqId}`,
  conn_id: 'db29396o6jg9l5np2l4g-cyzk',
  req_id: reqId,
  op: 'subscribe',
});
const refusedAck = (reqId) => ({
  success: false,
  ret_msg: 'error:handler not found,topic:orderbook.1000.NOPEUSDT',
  conn_id: 'db29396o6jg9l5np2l4g-cyzk',
  req_id: reqId,
  op: 'subscribe',
});
const pong = (reqId = '') => ({
  success: true,
  ret_msg: 'pong',
  conn_id: 'db29396o6jg9l5np2l4g-cyzk',
  req_id: reqId,
  op: 'ping',
});

const parse = (adapter, payload) => adapter.parse(JSON.stringify(payload));
const changesOf = (adapter, payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

// ---------------------------------------------------------------------------------------------------
// The wire contract: URLs, subscriptions, acknowledgement keys
// ---------------------------------------------------------------------------------------------------

test('the perpetual adapter names the linear endpoint, its topics and their own ack keys', () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  assert.equal(adapter.market, 'bybit_perp');
  assert.equal(adapter.symbol, 'BTCUSDT');
  assert.equal(adapter.stream, 'trades');
  assert.equal(adapter.url, 'wss://stream.bybit.com/v5/public/linear');
  assert.equal(adapter.restUrl, 'https://api.bybit.com/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=1000');
  assert.equal(adapter.ackMode, 'explicit');
  assert.deepEqual(adapter.expectedSubscriptions(), [PERP_TRADE, PERP_BOOK, PERP_LIQ]);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { op: 'subscribe', req_id: PERP_TRADE, args: [PERP_TRADE] },
    { op: 'subscribe', req_id: PERP_BOOK, args: [PERP_BOOK] },
    { op: 'subscribe', req_id: PERP_LIQ, args: [PERP_LIQ] },
  ]);
});

test('the spot adapter names the spot endpoint and its two topics', () => {
  const adapter = createBybitSpotAdapter({ market: 'bybit_spot' });
  assert.equal(adapter.market, 'bybit_spot');
  assert.equal(adapter.url, 'wss://stream.bybit.com/v5/public/spot');
  assert.equal(adapter.restUrl, 'https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200');
  assert.deepEqual(adapter.expectedSubscriptions(), [PERP_TRADE, SPOT_BOOK]);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { op: 'subscribe', req_id: PERP_TRADE, args: [PERP_TRADE] },
    { op: 'subscribe', req_id: SPOT_BOOK, args: [SPOT_BOOK] },
  ]);
});

test('the keep-alive is the documented JSON ping, sent inside the silence deadline', () => {
  const adapter = createBybitPerpAdapter();
  const keepAlive = adapter.keepAlive();
  assert.equal(keepAlive.noActivityMs, 10_000);
  assert.equal(
    keepAlive.noActivityMs < DEFAULT_SILENCE_DEADLINE_MS,
    true,
    'the ping must land before the connection judges the link silent',
  );
  assert.deepEqual(JSON.parse(keepAlive.payload()), { op: 'ping' });
});

test('a live acknowledgement is matched by the req_id it echoes, not by its wording', () => {
  const adapter = createBybitPerpAdapter();
  const linear = parse(adapter, linearAck(PERP_BOOK));
  assert.equal(linear.kind, 'subscription');
  assert.equal(linear.ok, true);
  assert.equal(linear.key, PERP_BOOK, 'the linear ack carries an empty ret_msg; the req_id is the identity');
  const spot = parse(adapter, spotAck(SPOT_BOOK));
  assert.equal(spot.ok, true);
  assert.equal(spot.key, SPOT_BOOK, 'the spot ack says "subscribe" in ret_msg; the req_id is still the identity');
});

test('a refused subscription is a failure and "already subscribed" is the state we wanted', () => {
  const adapter = createBybitPerpAdapter();
  const already = parse(adapter, alreadyAck(PERP_TRADE));
  assert.equal(already.kind, 'subscription');
  assert.equal(already.ok, true, 'the venue says the topic is already in the state we asked for');
  assert.match(already.detail, /already subscribed/);

  const refused = parse(adapter, refusedAck('orderbook.1000.NOPEUSDT'));
  assert.equal(refused.ok, false);
  assert.match(refused.detail, /handler not found/);
});

test('the pong answers the ping and is liveness, not data', () => {
  const adapter = createBybitPerpAdapter();
  const pongFrame = parse(adapter, pong());
  assert.equal(pongFrame.kind, 'heartbeat');
  assert.equal(pongFrame.answered, true);
  assert.equal(parse(adapter, { ...pong(), ret_msg: '' }).answered, false, 'a ping we did not see answered yet');
});

// ---------------------------------------------------------------------------------------------------
// Board changes: snapshot / delta / u
// ---------------------------------------------------------------------------------------------------

test('a snapshot replaces the board; a delta is a diff and size 0 removes a level', () => {
  const adapter = createBybitPerpAdapter();
  assert.deepEqual(changesOf(adapter, perpSnapshot(100, [['100.0', '1.5']], [['101.0', '0.5']])), {
    replace: true,
    levels: [
      { side: 'bid', price: 100, size: 1.5 },
      { side: 'ask', price: 101, size: 0.5 },
    ],
  });
  assert.deepEqual(changesOf(adapter, perpDelta(101, [['100.0', '0']], [])), {
    replace: false,
    changes: [{ side: 'bid', price: 100, size: 0 }],
  });
});

test('the live snapshot and delta frames derive their documented changes', () => {
  const adapter = createBybitPerpAdapter();
  const snapshot = changesOf(adapter, perpSnapshot(32881660));
  assert.equal(snapshot.replace, true);
  assert.deepEqual(snapshot.levels, [
    { side: 'bid', price: 83769.9, size: 3.38 },
    { side: 'bid', price: 83769.8, size: 1.114 },
    { side: 'ask', price: 83770, size: 19.788 },
    { side: 'ask', price: 83770.1, size: 0.002 },
  ]);
  const delta = changesOf(adapter, perpDelta(32881661));
  assert.equal(delta.replace, false);
  assert.deepEqual(delta.changes, [
    { side: 'bid', price: 83766.7, size: 0 },
    { side: 'bid', price: 83766.5, size: 0.002 },
    { side: 'ask', price: 83773.2, size: 0.002 },
  ]);
});

test('u must move forward: a duplicate or backwards update fails closed, a forward jump does not', () => {
  const adapter = createBybitPerpAdapter();
  adapter.onConnectionOpen();
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(100))), { status: 'applied' });
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify(perpDelta(101))), { status: 'applied' });
  assert.deepEqual(adapter.acceptDepthEvent(JSON.stringify(perpDelta(105))), { status: 'applied' },
    'u is an update id, not a counter: a forward jump is the venue\'s own right');

  const duplicate = adapter.acceptDepthEvent(JSON.stringify(perpDelta(105)));
  assert.equal(duplicate.status, 'resync');
  assert.match(duplicate.reason, /backwards|duplicate/i);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpDelta(106))).status, 'resync',
    'once failed, the stream stays failed until a new connection');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(200))).status, 'applied');
  const backwards = adapter.acceptDepthEvent(JSON.stringify(perpDelta(199)));
  assert.equal(backwards.status, 'resync');
});

test('a delta before any snapshot is not applied', () => {
  const adapter = createBybitPerpAdapter();
  adapter.onConnectionOpen();
  const verdict = adapter.acceptDepthEvent(JSON.stringify(perpDelta(5)));
  assert.equal(verdict.status, 'resync');
  assert.match(verdict.reason, /snapshot/i);
});

test('u=1: a snapshot overwrites the book, a delta claiming the restart id fails closed', () => {
  const adapter = createBybitPerpAdapter();

  // A snapshot carrying u=1 is the documented service-restart snapshot: it is the whole book.
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(1))).status, 'applied');
  assert.deepEqual(changesOf(adapter, perpSnapshot(1)).replace, true, 'and it overwrites the local order book');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpDelta(2))).status, 'applied');

  // A delta carrying u=1 is not trusted to be a complete book: believing it could replace the
  // board with a fragment, so it fails closed and a fresh snapshot is what recovers.
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(100))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpDelta(101))).status, 'applied');
  const refused = adapter.acceptDepthEvent(JSON.stringify(perpDelta(1, [['100.0', '2.0']], [['101.0', '2.0']])));
  assert.equal(refused.status, 'resync');
  assert.match(refused.reason, /restart|snapshot/i);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(200))).status, 'resync',
    'the stream stays failed until a new connection');
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(200))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpDelta(201))).status, 'applied');
});

// ---------------------------------------------------------------------------------------------------
// Malformed frames and symbol validation: fail closed
// ---------------------------------------------------------------------------------------------------

test('a malformed book frame is refused, never applied', () => {
  const adapter = createBybitPerpAdapter();
  adapter.onConnectionOpen();
  const broken = [
    { ...perpSnapshot(100), data: { s: 'BTCUSDT', b: [['100.0', '1.0']], a: [], seq: 1 } }, // no u
    { ...perpSnapshot(100), data: { s: 'BTCUSDT', b: [['100.0', '1.0']], a: [], u: 'x', seq: 1 } }, // non-integer u
    { ...perpSnapshot(100), data: { s: 'BTCUSDT', b: [['100.0']], a: [], u: 100, seq: 1 } }, // level too short
    { ...perpSnapshot(100), data: { s: 'BTCUSDT', b: [['100.0', 'x']], a: [], u: 100, seq: 1 } }, // size not a number
    { ...perpSnapshot(100), data: { s: 'BTCUSDT', b: 'nope', a: [], u: 100, seq: 1 } }, // b not an array
  ];
  for (const frame of broken) {
    const parsed = parse(adapter, frame);
    assert.equal(parsed.kind, 'protocol-error', `parse refuses ${JSON.stringify(frame.data).slice(0, 60)}`);
    assert.equal(adapter.acceptDepthEvent(JSON.stringify(frame)).status, 'malformed');
  }
  assert.throws(() => adapter.parse('not json'), /unrecognised Bybit frame/);
  assert.equal(adapter.parse(JSON.stringify({ op: 'subscribe' })).ok, false, 'an ack without success is a refusal');
  assert.throws(
    () => adapter.parse(JSON.stringify({ ...perpTrade, data: [{ ...perpTrade.data[0], v: 'not-a-number' }] })),
    /malformed Bybit trade frame/,
    'a malformed trade is thrown, not dropped in silence',
  );
});

test('a frame for another symbol or an unknown topic cannot enter the stream', () => {
  const adapter = createBybitPerpAdapter();
  const foreignBook = { ...perpSnapshot(100), topic: 'orderbook.1000.ETHUSDT', data: { ...perpSnapshot(100).data, s: 'ETHUSDT' } };
  assert.throws(() => parse(adapter, foreignBook), /unrecognised Bybit topic/);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(foreignBook)).status, 'malformed');

  // Our own topic, the wrong symbol: the stream is changing shape and the connection resynchronizes.
  const wrongSymbol = { ...perpSnapshot(100), data: { ...perpSnapshot(100).data, s: 'ETHUSDT' } };
  assert.equal(parse(adapter, wrongSymbol).kind, 'protocol-error');

  const foreignTrade = { ...perpTrade, topic: 'publicTrade.ETHUSDT', data: [{ ...perpTrade.data[0], s: 'ETHUSDT' }] };
  assert.throws(() => parse(adapter, foreignTrade), /unrecognised Bybit topic/);
  assert.throws(
    () => parse(adapter, { topic: 'kline.1.BTCUSDT', type: 'snapshot', data: {} }),
    /unrecognised Bybit topic/,
    'a topic we did not subscribe',
  );

  const spot = createBybitSpotAdapter();
  assert.throws(() => parse(spot, perpSnapshot(100)), /unrecognised Bybit topic/, 'the spot adapter does not carry the linear depth topic');
  assert.equal(parse(spot, spotSnapshot(1)).kind, 'data');
});

test('the constructors refuse a symbol or market they are not built for', () => {
  assert.throws(() => createBybitPerpAdapter({ symbol: 'ETHUSDT' }), /unsupported|symbol/i);
  assert.throws(() => createBybitSpotAdapter({ symbol: 'ETHUSDT' }), /unsupported|symbol/i);
});

// ---------------------------------------------------------------------------------------------------
// Trades and liquidations: classified and recorded, never board changes
// ---------------------------------------------------------------------------------------------------

test('trades are classified as trades and produce no board changes', () => {
  const adapter = createBybitPerpAdapter();
  assert.deepEqual(parse(adapter, perpTrade), { kind: 'data', trade: true });
  assert.deepEqual(parse(adapter, spotTrade), { kind: 'data', trade: true }, 'the spot trade shape parses too');
  assert.deepEqual(changesOf(adapter, perpTrade), { replace: false, changes: [] });
  assert.equal(adapter.venueSeqOf(JSON.stringify(perpTrade)), null, 'a trade has no update id for the board');
});

test('liquidation frames are classified and recorded without moving the book', () => {
  const adapter = createBybitPerpAdapter();
  assert.deepEqual(parse(adapter, perpLiquidation), { kind: 'data', liquidation: true });
  assert.deepEqual(changesOf(adapter, perpLiquidation), { replace: false, changes: [] });

  // The shape a live connection actually sent: `data` is an array, not the single object the docs
  // show. Both are accepted, because the venue decides its own batching.
  const liveLiquidation = {
    topic: PERP_LIQ,
    type: 'snapshot',
    ts: 1791365634482,
    data: [
      { T: 1791365634263, s: 'BTCUSDT', S: 'Buy', v: '0.013', p: '83446.80' },
      { T: 1791365634404, s: 'BTCUSDT', S: 'Buy', v: '0.001', p: '83440.00' },
    ],
  };
  assert.deepEqual(parse(adapter, liveLiquidation), { kind: 'data', liquidation: true });
  assert.deepEqual(changesOf(adapter, liveLiquidation), { replace: false, changes: [] });

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpSnapshot(100))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpLiquidation)).status, 'applied',
    'a liquidation is part of the stream: it is accepted without touching the update id');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpDelta(101))).status, 'applied',
    'the u continuity is judged between book frames, not broken by a liquidation');
});

// ---------------------------------------------------------------------------------------------------
// The connection rule the book resolves (sequence: u must increase over the proven range)
// ---------------------------------------------------------------------------------------------------

test('the connection rule accepts a first frame, requires u to increase, and lets a replacement re-anchor', () => {
  const adapter = createBybitPerpAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: frame.data?.u ?? null } });
  assert.equal(adapter.connects({ previous: null, current: env(perpSnapshot(100)) }), true, 'the first frame builds the anchor');
  assert.equal(adapter.connects({ previous: env(perpSnapshot(100)), current: env(perpDelta(101)) }), true);
  assert.equal(adapter.connects({ previous: env(perpDelta(101)), current: env(perpDelta(101)) }), false, 'duplicate');
  assert.equal(adapter.connects({ previous: env(perpDelta(101)), current: env(perpDelta(100)) }), false, 'backwards');
  assert.equal(adapter.connects({ previous: env(perpDelta(101)), current: env(perpSnapshot(1)) }), true,
    'a snapshot with u=1 is a replacement and re-anchors');
  assert.equal(adapter.connects({ previous: env(perpDelta(101)), current: env(perpDelta(1)) }), false,
    'a delta claiming the restart id is not a continuation');
  assert.equal(adapter.connects({ previous: env(perpDelta(101)), current: env(perpLiquidation) }), true,
    'a liquidation carries no update id: nothing to connect, nothing refused');
});

test('a non-book frame between book frames does not erase what the rule proved', () => {
  const adapter = createBybitPerpAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: frame.data?.u ?? null } });
  assert.equal(adapter.connects({ previous: env(perpSnapshot(100)), current: env(perpLiquidation) }), true);
  assert.equal(
    adapter.connects({ previous: env(perpLiquidation), current: env(perpDelta(99)) }),
    false,
    'the backwards update after a liquidation is still refused (the audit restoration series)',
  );
  assert.equal(adapter.connects({ previous: env(perpLiquidation), current: env(perpDelta(101)) }), true);
});

test('a bybit snapshot and delta drive the single-process board to serving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bybit-serving-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-bybit' });
  const structure = createStructure({
    market: 'bybit_perp',
    stream: 'trades',
    adapter: createBybitPerpAdapter({ market: 'bybit_perp' }),
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
        market: 'bybit_perp',
        stream: 'trades',
        connectionId: 'conn-1',
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: JSON.stringify(frame),
        meta: { first_seq: 1, venue_seq: frame.data?.u ?? null },
      });
    const first = structure.feed(envelope(1, perpSnapshot(100, [['100.0', '1.0']], [['101.0', '1.0']])));
    assert.equal(first.applied, true, `the snapshot was applied: ${first.reason ?? ''}`);
    const second = structure.feed(envelope(2, perpDelta(101, [['102.0', '2.0']], [])));
    assert.equal(second.applied, true, `the delta was applied: ${second.reason ?? ''}`);
    assert.equal(structure.book.isRunning, true, 'the board serves');
    assert.equal(structure.stats.applied, 2);
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// The production connection: establishment, live flow, and fail-closed replacement
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

function openConnection({ adapter, sockets, envelopes = [], diagnostics = [] }) {
  const connection = createReceiveConnection({
    adapter,
    market: adapter.market,
    venue: 'bybit',
    runId: 'run-bybit',
    webSocketImpl: sockets.impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
  return connection;
}

test('the connection admits bybit when every topic is acknowledged, and the live frames flow', async () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const sockets = fakeSockets();
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  assert.deepEqual(
    sockets.sockets[0].sent.map((message) => JSON.parse(message).req_id),
    [PERP_TRADE, PERP_BOOK, PERP_LIQ],
    'each topic was asked for under its own req_id',
  );
  for (const topic of [PERP_TRADE, PERP_BOOK, PERP_LIQ]) {
    sockets.sockets[0].deliver(JSON.stringify(linearAck(topic)));
  }
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver(JSON.stringify(perpSnapshot(100)));
  sockets.sockets[0].deliver(JSON.stringify(perpTrade));
  sockets.sockets[0].deliver(JSON.stringify(perpDelta(101)));
  sockets.sockets[0].deliver(JSON.stringify(perpLiquidation));
  await until(() => envelopes.length === 3, { label: 'the data frames to be stamped' });
  assert.deepEqual(envelopes.map((envelope) => envelope.meta.venue_seq), [100, 101, null],
    'book frames carry their update id; the recorded liquidation carries none');
  assert.equal(envelopes[0].meta.changes, undefined, 'changes are attached by ingest, not reception');
  assert.equal(connection.generation, 1);
  connection.stop();
});

test('a partial subscription failure fails the link closed', async () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const sockets = fakeSockets();
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(linearAck(PERP_TRADE)));
  sockets.sockets[0].deliver(JSON.stringify(linearAck(PERP_BOOK)));
  sockets.sockets[0].deliver(JSON.stringify(refusedAck(PERP_LIQ)));
  await until(() => connection.subscriptionState === 'failed', { label: 'the failed subscription' });
  assert.equal(connection.subscriptionFailure.length > 0, true);
  assert.equal(diagnostics.some(({ reason }) => /subscription failed/.test(reason)), true);
  connection.stop();
});

test('a duplicate update replaces the connection fail-closed instead of reaching the board', async () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const sockets = fakeSockets();
  const envelopes = [];
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, envelopes, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  for (const topic of [PERP_TRADE, PERP_BOOK, PERP_LIQ]) sockets.sockets[0].deliver(JSON.stringify(linearAck(topic)));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });
  sockets.sockets[0].deliver(JSON.stringify(perpSnapshot(100)));
  sockets.sockets[0].deliver(JSON.stringify(perpDelta(101)));
  await until(() => envelopes.length === 2, { label: 'the first two frames' });

  sockets.sockets[0].deliver(JSON.stringify(perpDelta(101)));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(envelopes.length, 2, 'the duplicate never became a frame');
  assert.equal(diagnostics.some(({ reason }) => /sequence\/checksum failure/.test(reason)), true);
  connection.stop();
});

test('a malformed book frame replaces the connection fail-closed', async () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const sockets = fakeSockets();
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  for (const topic of [PERP_TRADE, PERP_BOOK, PERP_LIQ]) sockets.sockets[0].deliver(JSON.stringify(linearAck(topic)));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver(JSON.stringify({ ...perpSnapshot(100), data: { s: 'BTCUSDT', b: [], a: [], seq: 1 } }));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(envelopes.length, 0, 'the malformed frame was never stamped');
  connection.stop();
});

test('an unclassifiable frame is counted, not silently dropped', async () => {
  const adapter = createBybitPerpAdapter({ market: 'bybit_perp' });
  const sockets = fakeSockets();
  const diagnostics = [];
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, diagnostics, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  for (const topic of [PERP_TRADE, PERP_BOOK, PERP_LIQ]) sockets.sockets[0].deliver(JSON.stringify(linearAck(topic)));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver('this is not json');
  sockets.sockets[0].deliver(JSON.stringify({ topic: 'kline.1.BTCUSDT', type: 'snapshot', data: {} }));
  await until(() => diagnostics.filter(({ reason }) => reason === 'unparsable frame').length === 2, {
    label: 'the counted frames',
  });
  assert.equal(envelopes.length, 0, 'nothing unclassifiable became a frame');
  assert.equal(connection.generation, 1, 'and the link is not torn down for it');
  connection.stop();
});

// ---------------------------------------------------------------------------------------------------
// The entrance's configuration registers both markets
// ---------------------------------------------------------------------------------------------------

test('the configuration registers bybit_perp and bybit_spot as buildable venues', () => {
  assert.deepEqual(knownVenues().filter((venue) => venue.startsWith('bybit')), ['bybit_perp', 'bybit_spot']);
  for (const venue of ['bybit_perp', 'bybit_spot']) {
    const adapter = adapterFor({ venue, market: venue, symbol: 'BTCUSDT', stream: 'trades' });
    assert.equal(adapter.market, venue);
    assert.equal(adapter.stream, 'trades');
  }
  assert.throws(
    () => adapterFor({ venue: 'bybit_perp', market: 'bybit_perp', symbol: 'BTCUSDT', stream: 'book' }),
    /carries the trades stream|stream/,
    'a stream the adapter does not carry is refused',
  );
});

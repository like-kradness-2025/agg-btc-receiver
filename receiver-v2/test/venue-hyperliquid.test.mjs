/**
 * Hyperliquid public WS: the perpetual market this receiver admits - hyperliquid_perp (BTC).
 *
 * The wire contract here is the official one, checked against the live public stream on
 * `wss://api.hyperliquid.xyz/ws`:
 *
 *  - Two channels are asked for, each in its own frame: `l2Book` (the book) and `trades`. The
 *    acknowledgement is a `subscriptionResponse` frame echoing the subscription (with its default
 *    fields filled in); one ack per subscription. A duplicate subscription answers with an `error`
 *    channel frame (`{channel:"error",data:"Already subscribed: ..."}`), measured live; an unknown
 *    coin is answered with a silent close (measured live - no error frame at all).
 *  - `l2Book` pushes are full replacements, never diffs: every frame carries the whole book
 *    (`levels = [bids, asks]`, each level `{px, sz, n}` with string prices/sizes) and a `time`
 *    stamp - milliseconds on the live stream, nanoseconds in the v1-era fixtures (normalised by
 *    magnitude, `n > 1e15` is ns). The live cadence measured 2.7-5.6 s between pushes, 20 levels
 *    per side, strictly increasing times, no repeats and no byte-identical resends in the window.
 *  - Hyperliquid publishes no sequence number, no checksum and no replay cursor for market data.
 *    The ordering the stream does carry is the frame time itself, so that is the proof this
 *    adapter claims: the book time must not move backwards. A step backwards is a stale frame
 *    that would walk the whole book back and fails closed until a new connection; an equal time
 *    is not out-of-order (the stamp is millisecond-granular) and is accepted. There is no skip
 *    allowance to set, because there is no counter to skip: the forward distance is unbounded by
 *    design (pushes are block-driven, not clock-driven).
 *  - Keep-alive is a client ping: `{"method":"ping"}` - the server replies `{channel:"pong"}`.
 *    The venue closes a connection that has carried nothing for 60 s, so the ping is sent after
 *    30 s without a word from the venue - a margin kept in the adapter, not assumed from the
 *    venue. There is no server-side ping in the contract.
 *  - Hyperliquid has no public liquidation stream; that is the venue's contract, not a gap here.
 *
 * The frames below are the exact shapes the live stream and the v1 golden fixtures carry
 * (trimmed to a few levels).
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createHyperliquidAdapter } from '../src/ingest/venues/hyperliquid.mjs';
import { createReceiveConnection } from '../src/ingest/connection.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

// ---------------------------------------------------------------------------------------------------
// Fixtures: exact shapes from the live read-only probe and the v1 golden fixtures.
// ---------------------------------------------------------------------------------------------------

const COIN = 'BTC';

const ackL2Book = {
  channel: 'subscriptionResponse',
  data: {
    method: 'subscribe',
    subscription: { type: 'l2Book', coin: COIN, nSigFigs: null, mantissa: null, fast: false },
  },
};
const ackTrades = {
  channel: 'subscriptionResponse',
  data: { method: 'subscribe', subscription: { type: 'trades', coin: COIN } },
};
const ackActiveAssetCtx = {
  channel: 'subscriptionResponse',
  data: { method: 'subscribe', subscription: { type: 'activeAssetCtx', coin: COIN } },
};

const row = (px, sz, n = 1) => ({ px, sz, n });
const book = (time, bids = [row('83457.0', '1.54301', 4), row('83456.0', '0.01793')], asks = [
  row('83458.0', '0.37464', 2),
  row('83459.0', '0.00012'),
]) => ({ channel: 'l2Book', data: { coin: COIN, time, levels: [bids, asks] } });

const tradeRow = {
  coin: COIN,
  side: 'A',
  px: '83459.0',
  sz: '0.00012',
  time: 1_791_400_828_182,
  hash: '0xdf134706123cfe13e08d04460eff5302021400ebad301ce582dbf258d130d7fe',
  tid: 468_409_263_219_607,
  users: ['0x4c6988b39fc0904628c2959e7606ef5f8518c152'],
};
const trades = (rows = [tradeRow]) => ({ channel: 'trades', data: rows });

const pong = { channel: 'pong' };
const errorFrame = {
  channel: 'error',
  data: 'Already subscribed: {"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}',
};

const parse = (adapter, payload) => adapter.parse(typeof payload === 'string' ? payload : JSON.stringify(payload));
const changesOf = (adapter, payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

// ---------------------------------------------------------------------------------------------------
// The wire contract: endpoint, channels, the acknowledgement, and the keep-alive
// ---------------------------------------------------------------------------------------------------

test('the adapter names the Hyperliquid endpoint, its channels and their ack keys', () => {
  const adapter = createHyperliquidAdapter();
  assert.equal(adapter.market, 'hyperliquid_perp');
  assert.equal(adapter.symbol, COIN);
  assert.equal(adapter.stream, 'trades');
  assert.equal(adapter.url, 'wss://api.hyperliquid.xyz/ws');
  assert.equal(adapter.restUrl, 'https://api.hyperliquid.xyz/info');
  assert.equal(adapter.ackMode, 'explicit');
  assert.equal(adapter.boundary, 'sequence', 'the frame time is the ordering this stream carries');
  assert.deepEqual(adapter.expectedSubscriptions(), [`l2Book:${COIN}`, `trades:${COIN}`, `activeAssetCtx:${COIN}`]);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { method: 'subscribe', subscription: { type: 'l2Book', coin: COIN } },
    { method: 'subscribe', subscription: { type: 'trades', coin: COIN } },
    { method: 'subscribe', subscription: { type: 'activeAssetCtx', coin: COIN } },
  ]);
});

test('the keep-alive is the client ping, sent after 30 s of venue silence', () => {
  const adapter = createHyperliquidAdapter();
  const plan = adapter.keepAlive();
  assert.equal(plan.noActivityMs, 30_000, 'the venue closes a connection silent for 60 s; 30 s keeps the margin');
  assert.deepEqual(JSON.parse(plan.payload()), { method: 'ping' });
});

test('one ack per subscription; the echoed default fields do not confuse the key', () => {
  const adapter = createHyperliquidAdapter();
  assert.deepEqual(parse(adapter, ackL2Book), { kind: 'subscription', key: `l2Book:${COIN}`, ok: true, detail: '' });
  assert.deepEqual(parse(adapter, ackTrades), { kind: 'subscription', key: `trades:${COIN}`, ok: true, detail: '' });
});

test('a pong is liveness, an error frame is a protocol error, and garbage is thrown', () => {
  const adapter = createHyperliquidAdapter();
  assert.deepEqual(parse(adapter, pong), { kind: 'heartbeat', answered: true });
  const error = parse(adapter, errorFrame);
  assert.equal(error.kind, 'protocol-error');
  assert.match(error.reason, /Already subscribed/);
  assert.throws(() => adapter.parse('not json'), /unrecognised Hyperliquid/);
  assert.throws(() => parse(adapter, { channel: 'candle', data: {} }), /unrecognised Hyperliquid/);
});

// ---------------------------------------------------------------------------------------------------
// The book: full replacements, time normalisation, and the board changes they carry
// ---------------------------------------------------------------------------------------------------

test('an l2Book frame is a full replacement whose time is the book ordering', () => {
  const adapter = createHyperliquidAdapter();
  assert.deepEqual(parse(adapter, book(1_791_400_840_821)), { kind: 'data', book: true });
  assert.equal(adapter.venueSeqOf(JSON.stringify(book(1_791_400_840_821))), 1_791_400_840_821,
    'milliseconds pass through');

  const ns = 1_700_000_000_000_000_000; // the v1-era nanoseconds shape (exactly representable)
  assert.equal(adapter.venueSeqOf(JSON.stringify(book(ns))), 1_700_000_000_000, 'nanoseconds normalise to ms');

  assert.deepEqual(changesOf(adapter, book(1_791_400_840_821)), {
    replace: true,
    levels: [
      { side: 'bid', price: 83457, size: 1.54301 },
      { side: 'bid', price: 83456, size: 0.01793 },
      { side: 'ask', price: 83458, size: 0.37464 },
      { side: 'ask', price: 83459, size: 0.00012 },
    ],
  });
});

test('a malformed l2Book frame is refused, never applied', () => {
  const adapter = createHyperliquidAdapter();
  const data = (patch) => ({ channel: 'l2Book', data: { coin: COIN, time: 1_791_400_840_821, levels: [ [row('1', '1')], [row('2', '1')] ], ...patch } });
  const trim = (levels) => ({ channel: 'l2Book', data: { coin: COIN, time: 1_791_400_840_821, levels } });
  const broken = [
    { channel: 'l2Book', data: { coin: COIN, levels: [ [row('1', '1')], [row('2', '1')] ] } },             // no time
    data({ time: 'x' }),
    data({ time: -1 }),
    data({ levels: 'nope' }),
    data({ levels: [] }),
    data({ levels: [ [row('1', '1')] ] }),                                                                 // one side only
    data({ levels: [ 'nope', [] ] }),
    data({ levels: [ [], [] ] }),                                                                          // nothing at all
    data({ levels: [ [row('abc', '1')], [] ] }),
    data({ levels: [ [row('-1', '1')], [] ] }),
    data({ levels: [ [row('0', '1')], [] ] }),
    data({ levels: [ [row('1', '-1')], [] ] }),
    data({ levels: [ [{ px: '1', sz: '1' }, 'nope'], [] ] }),
    data({ levels: [ [{ px: '1' }, { px: '2', sz: '1' }], [] ] }),
  ];
  for (const frame of broken) {
    const parsed = parse(adapter, frame);
    assert.equal(parsed.kind, 'protocol-error', `parse refuses ${JSON.stringify(frame.data).slice(0, 70)}`);
    assert.equal(adapter.acceptDepthEvent(JSON.stringify(frame)).status, 'malformed');
  }
  assert.equal(parse(adapter, trim([ [row('1', '0')], [] ])).kind, 'data', 'a zero size is a level the board drops, not a malformed frame');
});

test('frames for another coin cannot enter our stream', () => {
  const adapter = createHyperliquidAdapter();
  const foreign = { channel: 'l2Book', data: { coin: 'ETH', time: 1_791_400_840_821, levels: [ [row('1', '1')], [row('2', '1')] ] } };
  assert.throws(() => parse(adapter, foreign), /unrecognised Hyperliquid/);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(foreign)).status, 'malformed');

  const foreignTrade = trades([{ ...tradeRow, coin: 'ETH' }]);
  assert.throws(() => parse(adapter, foreignTrade), /unrecognised Hyperliquid/);

  assert.throws(() => createHyperliquidAdapter({ symbol: 'ETH' }), /unsupported|instrument/i);
});

// ---------------------------------------------------------------------------------------------------
// Trades carry no book changes; the time belongs to the book frames only
// ---------------------------------------------------------------------------------------------------

test('trades are classified as trades and produce no board changes', () => {
  const adapter = createHyperliquidAdapter();
  assert.deepEqual(parse(adapter, trades()), { kind: 'data', trade: true });
  assert.deepEqual(changesOf(adapter, trades()), { replace: false, changes: [] });
  assert.equal(adapter.venueSeqOf(JSON.stringify(trades())), null, 'a trade has no book time for the board');

  const bad = [
    { data: [] },
    { data: [{ ...tradeRow, side: 'X' }] },
    { data: [{ ...tradeRow, px: 'abc' }] },
    { data: [{ ...tradeRow, sz: '-1' }] },
    { data: [{ ...tradeRow, time: 'x' }] },
    { data: 'nope' },
  ];
  for (const patch of bad) {
    assert.throws(() => parse(adapter, { channel: 'trades', ...patch }), /unrecognised Hyperliquid|malformed Hyperliquid/);
  }
});

// ---------------------------------------------------------------------------------------------------
// The time rule: never backwards, fail closed, until a new connection
// ---------------------------------------------------------------------------------------------------

test('the book time must not move backwards, and an equal time is not out-of-order', () => {
  const adapter = createHyperliquidAdapter();
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1000))).status, 'applied', 'the first frame anchors');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1005))).status, 'applied', 'forward');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1005))).status, 'applied',
    'an equal time is not a stale frame: the stamp is millisecond-granular');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1010))).status, 'applied', 'and the stream continues');

  const backwards = adapter.acceptDepthEvent(JSON.stringify(book(1009)));
  assert.equal(backwards.status, 'resync', 'a backwards step is stale and fails closed');
  assert.match(backwards.reason, /backwards/);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1011))).status, 'resync',
    'once failed, the stream stays failed until a new connection');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(trades())).status, 'applied',
    'a trade carries no book time: nothing to judge');
  assert.equal(adapter.acceptDepthEvent('not json').status, 'malformed', 'an unreadable frame is malformed');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(ackL2Book)).status, 'malformed',
    'a subscription frame is not a depth event');
});

test('the connection rule the book resolves: never backwards, persistent refusal, reset on a new connection', () => {
  const adapter = createHyperliquidAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: adapter.venueSeqOf(JSON.stringify(frame)) } });
  assert.equal(adapter.connects({ previous: null, current: env(book(1000)) }), true, 'the first frame is judged on its own');
  assert.equal(adapter.connects({ previous: env(book(1000)), current: env(book(1000)) }), true,
    'an equal time connects: reception stamps equal frames too');
  assert.equal(adapter.connects({ previous: env(book(1000)), current: env(book(1005)) }), true);
  assert.equal(adapter.connects({ previous: env(book(1005)), current: env(book(1004)) }), false, 'a backwards step is refused');
  assert.equal(adapter.connects({ previous: env(book(1005)), current: env(book(1010)) }), false,
    'and the refusal persists until a new connection');
  assert.equal(adapter.connects({ previous: env(book(1005)), current: env(trades()) }), true,
    'a trade carries no book time: nothing to connect, nothing refused');

  adapter.onConnectionOpen();
  assert.equal(adapter.connects({ previous: null, current: env(book(2000)) }), true, 'a new connection re-anchors');

  const fresh = createHyperliquidAdapter();
  fresh.onConnectionOpen();
  assert.equal(fresh.connects({ previous: null, current: env(book(1)) }), true,
    'with nothing before it, a book frame is its own proof');
});

test('a new connection releases both failures and re-anchors both cursors', () => {
  const adapter = createHyperliquidAdapter();
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(1000))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(999))).status, 'resync',
    'the reception side fails on a regression');
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: frame.data.time } });
  assert.equal(adapter.connects({ previous: null, current: env(book(1000)) }), true);
  assert.equal(adapter.connects({ previous: env(book(1000)), current: env(book(999)) }), false,
    'and so does the book side');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(book(500))).status, 'applied',
    'the reception failure and cursor are released: a fresh stream re-anchors anywhere forward');
  assert.equal(adapter.connects({ previous: null, current: env(book(400)) }), true,
    'and the book side re-anchors too');
});

test('the book adopts the last proven frame time before judging the next one', () => {
  const adapter = createHyperliquidAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: frame.data.time } });
  assert.equal(adapter.connects({ previous: null, current: env(book(1000)) }), true);
  assert.equal(
    adapter.connects({ previous: env(book(1005)), current: env(book(1004)) }),
    false,
    'a frame behind the last proven time is refused even though it moves forward over the older cursor',
  );
  assert.equal(adapter.connects({ previous: env(book(1005)), current: env(book(1006)) }), false,
    'and the refusal persists until a new connection');
});

test('a subscription frame with no usable identity is refused', () => {
  const adapter = createHyperliquidAdapter();
  const frame = (subscription, method = 'subscribe') => ({ channel: 'subscriptionResponse', data: { method, subscription } });
  assert.throws(() => parse(adapter, frame({ type: '', coin: COIN })), /unrecognised Hyperliquid subscription/);
  assert.throws(() => parse(adapter, frame({ type: 'l2Book', coin: '' })), /unrecognised Hyperliquid subscription/);
  assert.throws(() => parse(adapter, frame(undefined)), /unrecognised Hyperliquid subscription/);
  assert.throws(() => parse(adapter, frame({ type: 'l2Book', coin: COIN }, 'unsubscribe')), /unrecognised Hyperliquid subscription/);
  assert.deepEqual(parse(adapter, frame({ type: 'l2Book', coin: COIN })), {
    kind: 'subscription',
    key: `l2Book:${COIN}`,
    ok: true,
    detail: '',
  });
});

// ---------------------------------------------------------------------------------------------------
// The single-process board: a replacement stream drives it to serving
// ---------------------------------------------------------------------------------------------------

test('a hyperliquid book frame drives the single-process board to serving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'hyperliquid-serving-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-hyperliquid' });
  const structure = createStructure({
    market: 'hyperliquid_perp',
    stream: 'trades',
    adapter: createHyperliquidAdapter(),
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
        market: 'hyperliquid_perp',
        stream: 'trades',
        connectionId: 'conn-1',
        receiveSeq: seq,
        recvTsMs: 1_791_400_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: JSON.stringify(frame),
        meta: { first_seq: 1, venue_seq: frame.data.time },
      });
    const first = structure.feed(envelope(1, book(1_791_400_000_000)));
    assert.equal(first.applied, true, `the first replacement was applied: ${first.reason ?? ''}`);
    const second = structure.feed(envelope(2, book(1_791_400_005_200)));
    assert.equal(second.applied, true, `the next replacement was applied: ${second.reason ?? ''}`);
    assert.equal(structure.book.isRunning, true, 'the board serves');
    assert.equal(structure.stats.applied, 2);
  } finally {
    structure.stop();
    store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------------------
// The production connection: acks, fail-closed replacement, counted frames
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
    venue: 'hyperliquid',
    runId: 'run-hyperliquid',
    webSocketImpl: sockets.impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs,
  });
}

test('the two acks establish the link, and the live frames flow', async () => {
  const adapter = createHyperliquidAdapter();
  const sockets = fakeSockets();
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  assert.deepEqual(
    JSON.parse(sockets.sockets[0].sent[0]),
    { method: 'subscribe', subscription: { type: 'l2Book', coin: COIN } },
    'the book is asked for first',
  );
  assert.deepEqual(JSON.parse(sockets.sockets[0].sent[1]), { method: 'subscribe', subscription: { type: 'trades', coin: COIN } });
  assert.deepEqual(JSON.parse(sockets.sockets[0].sent[2]), { method: 'subscribe', subscription: { type: 'activeAssetCtx', coin: COIN } });
  assert.equal(sockets.sockets[0].sent.length, 3, 'nothing else is sent on open: the ping waits for the silence it answers');

  sockets.sockets[0].deliver(JSON.stringify(ackL2Book));
  assert.equal(connection.subscriptionState, 'pending', 'one ack is not the whole set');
  sockets.sockets[0].deliver(JSON.stringify(ackTrades));
  sockets.sockets[0].deliver(JSON.stringify(ackActiveAssetCtx));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver(JSON.stringify(book(1_700_000_000_000)));
  sockets.sockets[0].deliver(JSON.stringify(trades()));
  sockets.sockets[0].deliver(JSON.stringify(book(1_700_000_005_200)));
  sockets.sockets[0].deliver(JSON.stringify(book(1_700_000_005_200)));
  sockets.sockets[0].deliver(JSON.stringify(pong));
  await until(() => envelopes.length === 3, { label: 'the book frames to be stamped' });
  assert.deepEqual(envelopes.map((envelope) => envelope.meta.venue_seq), [1_700_000_000_000, 1_700_000_005_200, 1_700_000_005_200],
    'each book frame carries its time - an equal time included; nothing else is stamped');
  assert.equal(connection.generation, 1, 'an equal time does not replace the connection');
  connection.stop();
});

test('a partial acknowledgement fails the link at the deadline', async () => {
  const adapter = createHyperliquidAdapter();
  const sockets = fakeSockets();
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, diagnostics, ackDeadlineMs: 150 });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackL2Book));
  assert.equal(connection.subscriptionState, 'pending', 'asking is not agreeing');
  await until(() => connection.subscriptionState === 'failed', { label: 'the ack deadline' });
  assert.equal(diagnostics.some(({ reason }) => /subscription failed: the subscription ack deadline passed/.test(reason)), true);
  connection.stop();
});

test('a backwards book frame replaces the connection fail-closed instead of reaching the board', async () => {
  const adapter = createHyperliquidAdapter();
  const sockets = fakeSockets();
  const envelopes = [];
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, envelopes, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackL2Book));
  sockets.sockets[0].deliver(JSON.stringify(ackTrades));
  sockets.sockets[0].deliver(JSON.stringify(ackActiveAssetCtx));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });
  sockets.sockets[0].deliver(JSON.stringify(book(5000)));
  await until(() => envelopes.length === 1, { label: 'the first frame' });

  sockets.sockets[0].deliver(JSON.stringify(book(4990)));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(envelopes.length, 1, 'the stale frame never became an envelope');
  assert.equal(diagnostics.some(({ reason }) => /sequence\/checksum failure/.test(reason)), true);
  connection.stop();
});

test('an error frame replaces the connection, and an unclassifiable frame is counted', async () => {
  const adapter = createHyperliquidAdapter();
  const sockets = fakeSockets();
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ackL2Book));
  sockets.sockets[0].deliver(JSON.stringify(ackTrades));
  sockets.sockets[0].deliver(JSON.stringify(ackActiveAssetCtx));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver('this is not json');
  sockets.sockets[0].deliver(JSON.stringify({ channel: 'l2Book', data: { coin: 'ETH', time: 1, levels: [[], []] } }));
  await until(() => diagnostics.filter(({ reason }) => reason === 'unparsable frame').length === 2, {
    label: 'the counted frames',
  });
  assert.equal(connection.generation, 1, 'and the link is not torn down for it');

  sockets.sockets[0].deliver(JSON.stringify(errorFrame));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(diagnostics.some(({ reason }) => /venue protocol error/.test(reason)), true);
  connection.stop();
});

// ---------------------------------------------------------------------------------------------------
// The entrance's configuration registers the market
// ---------------------------------------------------------------------------------------------------

test('the configuration registers hyperliquid_perp as a buildable venue', () => {
  assert.equal(knownVenues().includes('hyperliquid_perp'), true);
  const adapter = adapterFor({ venue: 'hyperliquid_perp', market: 'hyperliquid_perp', symbol: COIN, stream: 'trades' });
  assert.equal(adapter.market, 'hyperliquid_perp');
  assert.equal(adapter.stream, 'trades');
  assert.throws(
    () => adapterFor({ venue: 'hyperliquid_perp', market: 'hyperliquid_perp', symbol: COIN, stream: 'book' }),
    /carries the trades stream|stream/,
  );
});

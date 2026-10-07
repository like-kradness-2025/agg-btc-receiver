/**
 * OKX v5 public WS: the two markets this receiver admits - USDT perpetual (BTC-USDT-SWAP) and Spot
 * (BTC-USDT).
 *
 * The fixtures below are the exact shapes the real public stream sent (read-only capture on the
 * post-8443 endpoint `wss://ws.okx.com/ws/v5/public`): the subscribe ack carries the channel's own
 * `arg`; a subscription error echoes the request's `id` (so each topic is asked for under its own
 * id and a refusal is mapped back to the topic it refused); the book pushes a full `snapshot`
 * (prevSeqId -1) and then `update`s whose `prevSeqId` is the previous `seqId`; a maintenance reset
 * arrives once as `seqId < prevSeqId`; and the client's text `ping` is answered by a text `pong`.
 *
 * The rules: `prevSeqId` must equal the last `seqId` (a mismatch fails closed into a resync), the
 * documented maintenance reset and the empty keep-alive update are accepted, `checksum` is
 * deprecated and not used. Trades are classified and dropped by reception; liquidation frames are
 * classified and recorded, but the liquidation channel is instType-wide - frames for other
 * instruments are expected traffic and are not ours to carry.
 */

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createOkxPerpAdapter, createOkxSpotAdapter } from '../src/ingest/venues/okx.mjs';
import { createReceiveConnection, DEFAULT_SILENCE_DEADLINE_MS } from '../src/ingest/connection.mjs';
import { adapterFor, knownVenues } from '../src/entry/config.mjs';
import { makeEnvelope } from '../src/envelope.mjs';
import { openDurability } from '../src/durability.mjs';
import { createStructure } from '../src/supervisor/structure.mjs';

// ---------------------------------------------------------------------------------------------------
// Fixtures: exact shapes from the live read-only capture (levels trimmed, fields kept verbatim).
// ---------------------------------------------------------------------------------------------------

const PERP = 'BTC-USDT-SWAP';
const SPOT = 'BTC-USDT';

const bookTopic = (symbol) => `books:${symbol}`;
const tradeTopic = (symbol) => `trades:${symbol}`;

const ack = (channel, extra, id = '1512') => ({ id, event: 'subscribe', arg: { channel, ...extra }, connId: 'bfcb4b8e' });
const refuse = (id, msg = 'Subscribe failed, wrong URL or channel:books,instId:NOPE-USDT doesn\'t exist.') => ({
  event: 'error',
  msg,
  code: '60018',
  connId: '5e1f2fe5',
  id,
});

const snapshot = (symbol, seqId, bids = [['83725.8', '7.69', '0', '5'], ['83725.4', '0.02', '0', '1']], asks = [['83725.9', '1712.41', '0', '92'], ['83726', '1.43', '0', '3']]) => ({
  arg: { channel: 'books', instId: symbol },
  action: 'snapshot',
  data: [{ bids, asks, ts: '1791369351001', seqId, prevSeqId: -1 }],
});
const update = (symbol, seqId, prevSeqId, bids = [['83723.6', '0', '0', '0'], ['83723.1', '0', '0', '0']], asks = [['83729.6', '70.03', '0', '4']]) => ({
  arg: { channel: 'books', instId: symbol },
  action: 'update',
  data: [{ bids, asks, ts: '1791369352001', seqId, prevSeqId }],
});
/** The documented keep-alive update: empty sides, seq held (prevSeqId == seqId). */
const keepAliveUpdate = (symbol, seqId) => update(symbol, seqId, seqId, [], []);

const perpTrade = {
  arg: { channel: 'trades', instId: PERP },
  data: [
    { instId: PERP, tradeId: '2978601964', px: '83725.9', sz: '5', side: 'buy', ts: '1791369350776', count: '1', source: '0', seqId: 344430848209 },
  ],
};
const spotTrade = {
  arg: { channel: 'trades', instId: SPOT },
  data: [
    { instId: SPOT, tradeId: '1067468269', px: '83753', sz: '0.000011', side: 'sell', ts: '1791369376364', count: '1', source: '0', seqId: 82083114838 },
  ],
};
/** The live liquidation shape: details[] inside an instId envelope; the channel is instType-wide. */
const perpLiquidation = (instId = PERP) => ({
  arg: { channel: 'liquidation-orders', instType: 'SWAP' },
  data: [
    {
      details: [{ bkLoss: '0', bkPx: '0.01968', ccy: '', posSide: 'short', side: 'buy', sz: '42', ts: '1791369351375' }],
      instFamily: instId === PERP ? 'BTC-USDT' : 'XDP-USDT',
      instId,
      instType: 'SWAP',
      uly: instId === PERP ? 'BTC-USDT' : 'XDP-USDT',
    },
  ],
});
const notice = { event: 'notice', code: '64008', msg: 'The connection will soon be closed for a service upgrade. Please reconnect.', connId: 'a4d3ae55' };

const parse = (adapter, payload) => adapter.parse(typeof payload === 'string' ? payload : JSON.stringify(payload));
const changesOf = (adapter, payload) => adapter.changesFor({ raw: Buffer.from(JSON.stringify(payload)) });

// ---------------------------------------------------------------------------------------------------
// The wire contract: URLs, subscriptions, acknowledgement keys
// ---------------------------------------------------------------------------------------------------

test('the perpetual adapter names the post-8443 endpoint, its topics and their own ack keys', () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  assert.equal(adapter.market, 'okx_perp');
  assert.equal(adapter.symbol, PERP);
  assert.equal(adapter.stream, 'trades');
  assert.equal(adapter.url, 'wss://ws.okx.com/ws/v5/public');
  assert.equal(adapter.restUrl, `https://openapi.okx.com/api/v5/market/books?instId=${PERP}&sz=400`);
  assert.equal(adapter.ackMode, 'explicit');
  assert.deepEqual(adapter.expectedSubscriptions(), [bookTopic(PERP), tradeTopic(PERP), 'liquidation-orders:SWAP']);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { id: 'ob1', op: 'subscribe', args: [{ channel: 'books', instId: PERP }] },
    { id: 'tr1', op: 'subscribe', args: [{ channel: 'trades', instId: PERP }] },
    { id: 'lq1', op: 'subscribe', args: [{ channel: 'liquidation-orders', instType: 'SWAP' }] },
  ]);
});

test('the spot adapter carries the two spot topics', () => {
  const adapter = createOkxSpotAdapter({ market: 'okx_spot' });
  assert.equal(adapter.market, 'okx_spot');
  assert.equal(adapter.symbol, SPOT);
  assert.equal(adapter.url, 'wss://ws.okx.com/ws/v5/public');
  assert.equal(adapter.restUrl, `https://openapi.okx.com/api/v5/market/books?instId=${SPOT}&sz=400`);
  assert.deepEqual(adapter.expectedSubscriptions(), [bookTopic(SPOT), tradeTopic(SPOT)]);
  const messages = adapter.subscribeMessages().map((message) => JSON.parse(message));
  assert.deepEqual(messages, [
    { id: 'ob1', op: 'subscribe', args: [{ channel: 'books', instId: SPOT }] },
    { id: 'tr1', op: 'subscribe', args: [{ channel: 'trades', instId: SPOT }] },
  ]);
});

test('the keep-alive is the documented text ping, sent inside the silence deadline', () => {
  const adapter = createOkxPerpAdapter();
  const keepAlive = adapter.keepAlive();
  assert.equal(keepAlive.noActivityMs, 10_000);
  assert.equal(keepAlive.noActivityMs < DEFAULT_SILENCE_DEADLINE_MS, true);
  assert.equal(keepAlive.payload(), 'ping');
});

test('a live acknowledgement is matched by the arg it carries, and a refusal by the id it echoes', () => {
  const adapter = createOkxPerpAdapter();
  const bookAck = parse(adapter, ack('books', { instId: PERP }));
  assert.equal(bookAck.kind, 'subscription');
  assert.equal(bookAck.ok, true);
  assert.equal(bookAck.key, bookTopic(PERP));
  const liqAck = parse(adapter, ack('liquidation-orders', { instType: 'SWAP' }));
  assert.equal(liqAck.ok, true);
  assert.equal(liqAck.key, 'liquidation-orders:SWAP', 'the liquidation ack carries no instId');

  const refused = parse(adapter, refuse('ob1'));
  assert.equal(refused.kind, 'subscription');
  assert.equal(refused.ok, false);
  assert.equal(refused.key, bookTopic(PERP), 'the echoed id maps the refusal back to the topic');
  assert.match(refused.detail, /60018/);
});

test('the pong answers our ping and the service-upgrade notice is a shutdown', () => {
  const adapter = createOkxPerpAdapter();
  assert.deepEqual(parse(adapter, 'pong'), { kind: 'heartbeat', answered: true });
  assert.equal(parse(adapter, 'ping').kind, 'heartbeat', 'a server text ping is liveness, not data');
  const upgrade = parse(adapter, notice);
  assert.equal(upgrade.kind, 'shutdown');
  assert.match(upgrade.detail, /64008/);
});

// ---------------------------------------------------------------------------------------------------
// Board changes and the seqId/prevSeqId rule
// ---------------------------------------------------------------------------------------------------

test('a snapshot replaces the board; an update is a diff and size 0 removes a level', () => {
  const adapter = createOkxPerpAdapter();
  assert.deepEqual(changesOf(adapter, snapshot(PERP, 100, [['100.0', '1.5', '0', '1']], [['101.0', '0.5', '0', '1']])), {
    replace: true,
    levels: [
      { side: 'bid', price: 100, size: 1.5 },
      { side: 'ask', price: 101, size: 0.5 },
    ],
  });
  assert.deepEqual(changesOf(adapter, update(PERP, 101, 100, [['100.0', '0', '0', '0']], [])), {
    replace: false,
    changes: [{ side: 'bid', price: 100, size: 0 }],
  });
});

test('the live snapshot and update derive their documented changes', () => {
  const adapter = createOkxPerpAdapter();
  const snap = changesOf(adapter, snapshot(PERP, 344430848984));
  assert.equal(snap.replace, true);
  assert.deepEqual(snap.levels, [
    { side: 'bid', price: 83725.8, size: 7.69 },
    { side: 'bid', price: 83725.4, size: 0.02 },
    { side: 'ask', price: 83725.9, size: 1712.41 },
    { side: 'ask', price: 83726, size: 1.43 },
  ]);
  const upd = changesOf(adapter, update(PERP, 344430849237, 344430848984));
  assert.equal(upd.replace, false);
  assert.deepEqual(upd.changes, [
    { side: 'bid', price: 83723.6, size: 0 },
    { side: 'bid', price: 83723.1, size: 0 },
    { side: 'ask', price: 83729.6, size: 70.03 },
  ]);
});

test('prevSeqId must equal the last seqId; a mismatch fails closed, the reset and keep-alive do not', () => {
  const adapter = createOkxPerpAdapter();
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 100))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 101, 100))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(keepAliveUpdate(PERP, 101))).status, 'applied',
    'the empty keep-alive update holds the seq and is accepted');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 105, 101))).status, 'applied',
    'a forward jump whose prevSeqId bridges is the venue\'s own numbering');

  const gap = adapter.acceptDepthEvent(JSON.stringify(update(PERP, 200, 150)));
  assert.equal(gap.status, 'resync');
  assert.match(gap.reason, /prevSeqId|sequence/i);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 201, 200))).status, 'resync',
    'once failed, the stream stays failed until a new connection');

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 100))).status, 'applied');
  const stale = adapter.acceptDepthEvent(JSON.stringify(update(PERP, 99, 98)));
  assert.equal(stale.status, 'resync', 'a stale/duplicate update does not bridge');

  // A reset that does not bridge is not the documented reset: it is a gap wearing its shape.
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 100))).status, 'applied');
  const forgedReset = adapter.acceptDepthEvent(JSON.stringify(update(PERP, 3, 999)));
  assert.equal(forgedReset.status, 'resync', 'a reset must bridge like every other update');
  assert.match(forgedReset.reason, /prevSeqId/);

  // A same-seq update that carries levels is not the keep-alive.
  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 100))).status, 'applied');
  const carrying = adapter.acceptDepthEvent(JSON.stringify(update(PERP, 100, 100, [['99.0', '1.0', '0', '1']], [])));
  assert.equal(carrying.status, 'resync', 'a held seq is only the empty keep-alive');
  assert.match(carrying.reason, /level changes/);

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 500))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 3, 500))).status, 'applied',
    'the documented maintenance reset (seqId < prevSeqId, bridging) is accepted');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 4, 3))).status, 'applied',
    'and the normal rule resumes');
});

test('an update before any snapshot is not applied', () => {
  const adapter = createOkxPerpAdapter();
  adapter.onConnectionOpen();
  const verdict = adapter.acceptDepthEvent(JSON.stringify(update(PERP, 5, 4)));
  assert.equal(verdict.status, 'resync');
  assert.match(verdict.reason, /snapshot/i);
});

// ---------------------------------------------------------------------------------------------------
// Malformed frames and symbol validation: fail closed
// ---------------------------------------------------------------------------------------------------

test('a malformed book frame is refused, never applied', () => {
  const adapter = createOkxPerpAdapter();
  adapter.onConnectionOpen();
  const broken = [
    { ...snapshot(PERP, 100), data: [{ bids: [], asks: [], ts: '1' }] }, // no seqId
    { ...snapshot(PERP, 100), data: [{ bids: [], asks: [], ts: '1', seqId: 'x', prevSeqId: -1 }] },
    { ...snapshot(PERP, 100), data: [{ bids: [['100.0']], asks: [], ts: '1', seqId: 100, prevSeqId: -1 }] },
    { ...snapshot(PERP, 100), data: [{ bids: [['100.0', 'x']], asks: [], ts: '1', seqId: 100, prevSeqId: -1 }] },
    { ...snapshot(PERP, 100), data: [{ bids: 'nope', asks: [], ts: '1', seqId: 100, prevSeqId: -1 }] },
    { ...update(PERP, 101, 100), data: [{ bids: [], asks: [], ts: '1', seqId: 101 }] }, // update without prevSeqId
  ];
  for (const frame of broken) {
    const parsed = parse(adapter, frame);
    assert.equal(parsed.kind, 'protocol-error', `parse refuses ${JSON.stringify(frame.data).slice(0, 70)}`);
    assert.equal(adapter.acceptDepthEvent(JSON.stringify(frame)).status, 'malformed');
  }
  assert.throws(() => adapter.parse('not json'), /unrecognised OKX frame/);
});

test('frames for other instruments cannot enter our stream', () => {
  const adapter = createOkxPerpAdapter();
  // The liquidation channel is instType-wide: another instrument's liquidation is expected
  // traffic, not ours to carry, and is not an anomaly.
  assert.equal(parse(adapter, perpLiquidation('XDP-USDT-SWAP')), null);
  assert.deepEqual(parse(adapter, perpLiquidation(PERP)), { kind: 'data', liquidation: true });

  // A row without an instrument is not another instrument's noise: the frame is refused.
  const shapeless = { ...perpLiquidation(), data: [null, {}] };
  assert.throws(() => parse(adapter, shapeless), /malformed OKX liquidation/);
  assert.throws(() => parse(adapter, { ...perpLiquidation(), data: [] }), /malformed OKX liquidation/);

  // Books/trades were subscribed per instrument: another instId there is not expected.
  const foreignBook = { ...snapshot('ETH-USDT-SWAP', 100), arg: { channel: 'books', instId: 'ETH-USDT-SWAP' } };
  assert.throws(() => parse(adapter, foreignBook), /unrecognised OKX/);
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(foreignBook)).status, 'malformed');

  const spot = createOkxSpotAdapter();
  assert.throws(() => parse(spot, snapshot(PERP, 100)), /unrecognised OKX/, 'the spot adapter does not carry the swap');
  assert.equal(parse(spot, snapshot(SPOT, 1)).kind, 'data');
});

test('the constructors refuse an instrument they are not built for', () => {
  assert.throws(() => createOkxPerpAdapter({ symbol: 'ETH-USDT-SWAP' }), /unsupported|instrument/i);
  assert.throws(() => createOkxSpotAdapter({ symbol: 'ETH-USDT' }), /unsupported|instrument/i);
});

// ---------------------------------------------------------------------------------------------------
// Trades and liquidations: classified and recorded, never board changes
// ---------------------------------------------------------------------------------------------------

test('trades are classified as trades and produce no board changes', () => {
  const adapter = createOkxPerpAdapter();
  assert.deepEqual(parse(adapter, perpTrade), { kind: 'data', trade: true });
  const spot = createOkxSpotAdapter();
  assert.deepEqual(parse(spot, spotTrade), { kind: 'data', trade: true }, 'the spot trade shape parses on the spot adapter');
  assert.deepEqual(changesOf(adapter, perpTrade), { replace: false, changes: [] });
  assert.equal(adapter.venueSeqOf(JSON.stringify(perpTrade)), null, 'a trade has no book sequence for the board');
});

test('liquidation frames are classified and recorded without moving the book', () => {
  const adapter = createOkxPerpAdapter();
  assert.deepEqual(parse(adapter, perpLiquidation()), { kind: 'data', liquidation: true });
  assert.deepEqual(changesOf(adapter, perpLiquidation()), { replace: false, changes: [] });

  adapter.onConnectionOpen();
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(snapshot(PERP, 100))).status, 'applied');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(perpLiquidation())).status, 'applied',
    'a liquidation is part of the stream and does not touch the book sequence');
  assert.equal(adapter.acceptDepthEvent(JSON.stringify(update(PERP, 101, 100))).status, 'applied');
});

// ---------------------------------------------------------------------------------------------------
// The connection rule the book resolves (sequence: prevSeqId must bridge the proven range)
// ---------------------------------------------------------------------------------------------------

test('the connection rule accepts a first frame, requires the seq to bridge, and lets a snapshot re-anchor', () => {
  const adapter = createOkxPerpAdapter();
  adapter.onConnectionOpen();
  const env = (frame) => ({ raw: JSON.stringify(frame), meta: { venue_seq: frame.data?.[0]?.seqId ?? null } });
  assert.equal(adapter.connects({ previous: null, current: env(snapshot(PERP, 100)) }), true);
  assert.equal(adapter.connects({ previous: env(snapshot(PERP, 100)), current: env(update(PERP, 101, 100)) }), true);
  assert.equal(adapter.connects({ previous: env(update(PERP, 101, 100)), current: env(update(PERP, 200, 150)) }), false,
    'a prevSeqId that does not bridge is refused');
  assert.equal(adapter.connects({ previous: env(update(PERP, 102, 101)), current: env(snapshot(PERP, 1)) }), true,
    'a snapshot re-anchors');
  assert.equal(adapter.connects({ previous: env(update(PERP, 102, 101)), current: env(perpLiquidation()) }), true,
    'a liquidation carries no book sequence: nothing to connect, nothing refused');

  // The reset must bridge on this side too; a same-seq update with levels is refused.
  assert.equal(adapter.connects({ previous: env(update(PERP, 500, 499)), current: env(update(PERP, 3, 999)) }), false,
    'a reset that does not bridge is refused');
  assert.equal(adapter.connects({ previous: env(update(PERP, 500, 499)), current: env(update(PERP, 3, 500)) }), true,
    'a bridging reset re-anchors');
  assert.equal(adapter.connects({ previous: env(update(PERP, 3, 500)), current: env(keepAliveUpdate(PERP, 3)) }), true,
    'the empty keep-alive connects without moving the seq');
  assert.equal(
    adapter.connects({ previous: env(keepAliveUpdate(PERP, 3)), current: env(update(PERP, 3, 3, [['99.0', '1.0', '0', '1']], [])) }),
    false,
    'a same-seq update with levels is refused',
  );
  assert.equal(adapter.connects({ previous: env(update(PERP, 3, 3, [['99.0', '1.0', '0', '1']], [])), current: env(update(PERP, 4, 3)) }), true,
    'and a normal update connects again');

  const fresh = createOkxPerpAdapter();
  fresh.onConnectionOpen();
  assert.equal(fresh.connects({ previous: null, current: env(update(PERP, 5, 4)) }), false,
    'an update with nothing proven before it is refused');
});

test('an okx snapshot and update drive the single-process board to serving', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'okx-serving-'));
  const store = openDurability({ path: join(dir, 'state.sqlite'), runId: 'run-okx' });
  const structure = createStructure({
    market: 'okx_perp',
    stream: 'trades',
    adapter: createOkxPerpAdapter({ market: 'okx_perp' }),
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
        market: 'okx_perp',
        stream: 'trades',
        connectionId: 'conn-1',
        receiveSeq: seq,
        recvTsMs: 1_792_000_000_000 + seq,
        recvMonoNs: 1_000_000 + seq,
        raw: JSON.stringify(frame),
        meta: { first_seq: 1, venue_seq: frame.data?.[0]?.seqId ?? null },
      });
    const first = structure.feed(envelope(1, snapshot(PERP, 100, [['100.0', '1.0', '0', '1']], [['101.0', '1.0', '0', '1']])));
    assert.equal(first.applied, true, `the snapshot was applied: ${first.reason ?? ''}`);
    const second = structure.feed(envelope(2, update(PERP, 101, 100, [['102.0', '2.0', '0', '1']], [])));
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
// The production connection: establishment, partial failure, fail-closed replacement
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
  return createReceiveConnection({
    adapter,
    market: adapter.market,
    venue: 'okx',
    runId: 'run-okx',
    webSocketImpl: sockets.impl,
    onGeneration: ({ settle }) => settle(true),
    onEnvelope: (envelope) => envelopes.push(envelope),
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
    silenceDeadlineMs: 60_000,
    ackDeadlineMs: 60_000,
  });
}

test('the connection admits okx when every topic is acknowledged, and the live frames flow', async () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  const sockets = fakeSockets();
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  assert.deepEqual(
    sockets.sockets[0].sent.map((message) => JSON.parse(message).id),
    ['ob1', 'tr1', 'lq1'],
    'each topic was asked for under its own id',
  );
  sockets.sockets[0].deliver(JSON.stringify(ack('books', { instId: PERP })));
  sockets.sockets[0].deliver(JSON.stringify(ack('trades', { instId: PERP })));
  sockets.sockets[0].deliver(JSON.stringify(ack('liquidation-orders', { instType: 'SWAP' })));
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver(JSON.stringify(snapshot(PERP, 100)));
  sockets.sockets[0].deliver(JSON.stringify(perpTrade));
  sockets.sockets[0].deliver(JSON.stringify(update(PERP, 101, 100)));
  sockets.sockets[0].deliver(JSON.stringify(perpLiquidation()));
  sockets.sockets[0].deliver(JSON.stringify(perpLiquidation('XDP-USDT-SWAP')));
  await until(() => envelopes.length === 3, { label: 'the data frames to be stamped' });
  assert.deepEqual(envelopes.map((envelope) => envelope.meta.venue_seq), [100, 101, null],
    'book frames carry their seqId; the recorded liquidation carries none');
  assert.equal(connection.generation, 1);
  connection.stop();
});

test('a partial subscription failure fails the link closed', async () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  const sockets = fakeSockets();
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  sockets.sockets[0].deliver(JSON.stringify(ack('books', { instId: PERP })));
  sockets.sockets[0].deliver(JSON.stringify(ack('trades', { instId: PERP })));
  sockets.sockets[0].deliver(JSON.stringify(refuse('lq1', 'liquidation-orders refused')));
  await until(() => connection.subscriptionState === 'failed', { label: 'the failed subscription' });
  assert.equal(diagnostics.some(({ reason }) => /subscription failed/.test(reason)), true);
  connection.stop();
});

test('a sequence gap replaces the connection fail-closed instead of reaching the board', async () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  const sockets = fakeSockets();
  const envelopes = [];
  const diagnostics = [];
  const connection = openConnection({ adapter, sockets, envelopes, diagnostics });
  connection.start();
  sockets.sockets[0].onopen();
  for (const [channel, extra] of [['books', { instId: PERP }], ['trades', { instId: PERP }], ['liquidation-orders', { instType: 'SWAP' }]]) {
    sockets.sockets[0].deliver(JSON.stringify(ack(channel, extra)));
  }
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });
  sockets.sockets[0].deliver(JSON.stringify(snapshot(PERP, 100)));
  sockets.sockets[0].deliver(JSON.stringify(update(PERP, 101, 100)));
  await until(() => envelopes.length === 2, { label: 'the first two frames' });

  sockets.sockets[0].deliver(JSON.stringify(update(PERP, 200, 150)));
  await until(() => connection.generation === 2, { label: 'the replacement socket' });
  assert.equal(envelopes.length, 2, 'the gap frame never became an envelope');
  assert.equal(diagnostics.some(({ reason }) => /sequence\/checksum failure/.test(reason)), true);
  connection.stop();
});

test('an unclassifiable frame is counted, not silently dropped', async () => {
  const adapter = createOkxPerpAdapter({ market: 'okx_perp' });
  const sockets = fakeSockets();
  const diagnostics = [];
  const envelopes = [];
  const connection = openConnection({ adapter, sockets, diagnostics, envelopes });
  connection.start();
  sockets.sockets[0].onopen();
  for (const [channel, extra] of [['books', { instId: PERP }], ['trades', { instId: PERP }], ['liquidation-orders', { instType: 'SWAP' }]]) {
    sockets.sockets[0].deliver(JSON.stringify(ack(channel, extra)));
  }
  await until(() => connection.subscriptionState === 'acknowledged', { label: 'establishment' });

  sockets.sockets[0].deliver('this is not json');
  sockets.sockets[0].deliver(JSON.stringify({ arg: { channel: 'books', instId: 'ETH-USDT-SWAP' }, action: 'update', data: [] }));
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

test('the configuration registers okx_perp and okx_spot as buildable venues', () => {
  assert.deepEqual(knownVenues().filter((venue) => venue.startsWith('okx')), ['okx_perp', 'okx_spot']);
  for (const [venue, symbol] of [['okx_perp', PERP], ['okx_spot', SPOT]]) {
    const adapter = adapterFor({ venue, market: venue, symbol, stream: 'trades' });
    assert.equal(adapter.market, venue);
    assert.equal(adapter.stream, 'trades');
  }
  assert.throws(
    () => adapterFor({ venue: 'okx_perp', market: 'okx_perp', symbol: PERP, stream: 'book' }),
    /carries the trades stream|stream/,
  );
});

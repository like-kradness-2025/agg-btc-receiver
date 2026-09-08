// test/ingress-metadata.test.mjs — Issue #12 ingress metadata contract tests
//
// Verifies that receive time / receive_seq are stamped at the WebSocket
// message boundary (BaseConnector._captureIngress) and flow UNCHANGED through
// buffer→replay and worker persistence:
//   (a) receive_seq is monotonic within one socket generation and resets on
//       connect() / new socket generation
//   (b) buffer→replay keeps the ORIGINAL recv_ts_ms (Bitstamp _pendingDepth,
//       Coinbase _replayRingBufAfterSnapshot) even after a >500ms buffer
//   (c) missing/invalid source timestamps become source_event_ts_ms:null
//       (events without an exchange event time still pass; malformed trade
//       timestamps fail-closed and never masquerade as the current time)
//   (d) buildRawDbEnvelope persists existing ingress metadata verbatim and
//       never re-stamps recv_ts_ms (missing stays null — no Date.now())
//
// Fixtures are inline (matching existing connector test style); the only
// fixture dir (test/fixtures/burst-v1) has no raw-frame fixtures to reuse.

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';
import { BitstampConnector } from '../lib/bitstamp-connector.mjs';
import { CoinbaseConnector } from '../lib/coinbase-connector.mjs';
// Import is main-thread safe: the worker only registers its parentPort IPC
// handler when parentPort is non-null (worker context).
import { buildRawDbEnvelope } from '../lib/orderflow-worker.mjs';

// ====== Mock WebSocket (ws-compatible, EventEmitter-based) ======

class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
    this.sent = [];
  }

  send(frame) { this.sent.push(frame); }
  ping() {}
  close() { this.readyState = 3; }
  terminate() { this.readyState = 3; }
}

/** Track created connectors so after() can clear timers (no test hang). */
const _cleanup = [];
const register = (conn) => { _cleanup.push(conn); return conn; };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

after(() => {
  for (const conn of _cleanup) {
    try { conn._clearTimers(); } catch { /* ignore */ }
  }
  _cleanup.length = 0;
});

/** connect() helper: resolves once the mock socket emits 'open'. */
async function connectConn(conn) {
  const p = conn.connect();
  setImmediate(() => {
    conn._ws.readyState = 1; // OPEN
    conn._ws.emit('open');
  });
  await p;
}

/** Emit one raw WS frame through the connector's real socket handler. */
function feedFrame(conn, obj) {
  conn._ws.emit('message', Buffer.from(JSON.stringify(obj)));
}

// ====== BaseConnector: receive_seq / connection generation (a) ======

describe('BaseConnector ingress metadata (a): receive_seq & socket generations', () => {
  /** Harness that records the ingress snapshot of every WS frame. */
  class IngressProbeConnector extends BaseConnector {
    constructor() {
      super({}, { market: 'probe', wsUrl: 'ws://localhost:1', restUrl: '' });
      this.frames = [];
    }

    subscribe() { /* no-op */ }

    _onMessage(data) {
      this.frames.push({ ...this._ingress });
    }
  }

  function createProbe() {
    const conn = new IngressProbeConnector();
    conn._setWebSocket(MockWebSocket);
    register(conn);
    return conn;
  }

  it('receive_seq is monotonic within one connection; resets on new socket generation', async () => {
    const conn = createProbe();
    await connectConn(conn);
    const firstConnectionId = conn._connectionId;
    assert.ok(firstConnectionId);

    for (let i = 0; i < 3; i++) feedFrame(conn, { i });

    const gen1 = conn.frames.slice();
    assert.deepStrictEqual(gen1.map((f) => f.receive_seq), [1, 2, 3]);
    assert.ok(gen1.every((f) => f.connection_id === firstConnectionId));
    // recv_mono_ns is guaranteed strictly increasing (guard in _monoNow).
    assert.ok(gen1[0].recv_mono_ns !== null);
    assert.ok(gen1[0].recv_mono_ns < gen1[1].recv_mono_ns
      && gen1[1].recv_mono_ns < gen1[2].recv_mono_ns);
    // recv_ts_ms is a wall clock at capture time: non-decreasing across the burst.
    assert.ok(gen1[2].recv_ts_ms >= gen1[0].recv_ts_ms);

    // New socket generation (reconnect): counter restarts at 1, new identity.
    conn._ws = null;
    await connectConn(conn);
    const secondConnectionId = conn._connectionId;
    assert.notStrictEqual(secondConnectionId, firstConnectionId);

    conn.frames = [];
    feedFrame(conn, { i: 10 });
    feedFrame(conn, { i: 11 });
    const gen2 = conn.frames;
    assert.deepStrictEqual(gen2.map((f) => f.receive_seq), [1, 2]);
    assert.ok(gen2.every((f) => f.connection_id === secondConnectionId));
  });
});

// ====== Bitstamp _pendingDepth: buffer→replay keeps recv_ts_ms (b) ======

describe('Bitstamp ingress metadata (b): _pendingDepth replay keeps original recv_ts_ms', () => {
  const SNAPSHOT_BODY = { bids: [['65000', '1.0']], asks: [['65001', '2.0']] };
  const depthFrame = (bids, asks, micro = '1700000000000000') => ({
    event: 'data',
    channel: 'diff_order_book_btcusd',
    data: { bids, asks, microtimestamp: micro },
  });

  function stubFetch(delayMs) {
    const original = globalThis.fetch;
    globalThis.fetch = async () => {
      if (delayMs > 0) await sleep(delayMs);
      return { ok: true, json: async () => ({ ...SNAPSHOT_BODY }) };
    };
    return original;
  }

  it('replays a buffered diff with its ORIGINAL recv_ts_ms after a >500ms REST sync', async () => {
    const originalFetch = stubFetch(600);
    const conn = new BitstampConnector({ restUrl: 'https://example.test/book' });
    conn._setWebSocket(MockWebSocket);
    register(conn);
    try {
      await connectConn(conn);

      const emitted = [];
      conn.on('depth', (event) => emitted.push(event));

      // _syncBook sets _depthSyncing=true synchronously, then awaits fetch.
      const syncPromise = conn._syncBook();
      assert.strictEqual(conn._depthSyncing, true);

      // Diff frame arrives while the REST snapshot is still in flight.
      feedFrame(conn, depthFrame([['65000', '1.5']], [['65001', '0']]));
      const buffered = conn._pendingDepth[0].ingress;
      assert.ok(buffered, 'buffered diff must carry an ingress snapshot');
      const bufferedRecvTs = buffered.recv_ts_ms;
      assert.strictEqual(buffered.receive_seq, 1);
      assert.strictEqual(buffered.connection_id, conn._connectionId);

      await syncPromise;

      assert.strictEqual(emitted.length, 2);
      assert.strictEqual(emitted[0].type, 'snapshot');
      assert.strictEqual(emitted[1].type, 'update');

      // REST snapshot is a local capture: no socket receive_seq, and the
      // source event time is explicitly unknown (not the local wall clock).
      assert.strictEqual(emitted[0].receive_seq, null);
      assert.strictEqual(emitted[0].source_event_ts_ms, null);
      assert.strictEqual(emitted[0].source_event_time_known, false);

      // The replayed diff keeps the recv time stamped at buffer time (600ms
      // earlier), not the replay/enqueue moment.
      const update = emitted[1];
      assert.strictEqual(update.recv_ts_ms, bufferedRecvTs);
      assert.strictEqual(update.receive_seq, 1);
      assert.strictEqual(update.connection_id, buffered.connection_id);
      assert.strictEqual(update.source_event_ts_ms, 1700000000000);
      assert.strictEqual(update.source_event_time_known, true);
      assert.ok(emitted[0].recv_ts_ms - bufferedRecvTs >= 400,
        `replay recv_ts_ms drifted to replay time: snapshot=${emitted[0].recv_ts_ms} buffered=${bufferedRecvTs}`);

      // Live update after sync: the non-syncing _handleDepth('update', ...)
      // call site inherits this._ingress implicitly (socket-frame metadata).
      feedFrame(conn, depthFrame([['65000', '2.0']], []));
      const live = emitted[2];
      assert.strictEqual(live.type, 'update');
      assert.strictEqual(live.receive_seq, 2);
      assert.strictEqual(live.connection_id, conn._connectionId);
      assert.strictEqual(live.source_event_ts_ms, 1700000000000);
      assert.ok(live.recv_ts_ms >= emitted[0].recv_ts_ms);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

// ====== Coinbase ring buffer: replay keeps original recv_ts_ms (b) ======

describe('Coinbase ingress metadata (b): _replayRingBufAfterSnapshot keeps original recv_ts_ms', () => {
  function makeL2Frame(sequenceNum, events) {
    return { channel: 'l2_data', sequence_num: sequenceNum, events };
  }
  const update = (side, price, qty) => ({ type: 'update', updates: [{ side, price_level: price, new_quantity: qty }] });

  it('replays ring-buffered updates with ORIGINAL recv_ts_ms after a >500ms buffer', async () => {
    const conn = new CoinbaseConnector({});
    conn._setWebSocket(MockWebSocket);
    register(conn);
    await connectConn(conn);
    assert.strictEqual(conn._wsSnapshotReceived, false);

    const emitted = [];
    conn.on('depth', (event) => emitted.push(event));

    // Two l2_data updates arrive before any snapshot: ring-buffered with the
    // frame's own ingress snapshot (non-enumerable on the buffered object).
    feedFrame(conn, makeL2Frame(101, [update('bid', '65000', '3.0')]));
    feedFrame(conn, makeL2Frame(102, [update('ask', '65002', '1.5')]));

    assert.strictEqual(conn._ringBuf.length, 2);
    const buf0 = conn._ringBuf[0]._ingress;
    const buf1 = conn._ringBuf[1]._ingress;
    assert.ok(buf0 && buf1, 'buffered frames must carry a non-enumerable _ingress snapshot');
    assert.strictEqual(buf0.receive_seq, 1);
    assert.strictEqual(buf1.receive_seq, 2);
    // Non-enumerable: never leaks into JSON / payload spreads.
    assert.ok(!JSON.stringify(conn._ringBuf[0]).includes('_ingress'));

    // Buffer for well over 500ms, then the WS snapshot arrives.
    await sleep(600);
    feedFrame(conn, makeL2Frame(100, [{
      type: 'snapshot',
      updates: [
        { side: 'bid', price_level: '65000', new_quantity: '1.0' },
        { side: 'ask', price_level: '65001', new_quantity: '2.0' },
      ],
    }]));

    assert.strictEqual(conn._ringBuf.length, 0);
    assert.strictEqual(emitted.length, 3);
    assert.deepStrictEqual(emitted.map((e) => e.type), ['snapshot', 'update', 'update']);

    const snap = emitted[0];
    const upd1 = emitted[1];
    const upd2 = emitted[2];
    assert.strictEqual(snap.receive_seq, 3); // snapshot frame itself is a socket frame
    // Coinbase l2_data has no exchange event time: source time explicitly null.
    assert.strictEqual(upd1.source_event_ts_ms, null);
    assert.strictEqual(upd1.source_event_time_known, false);
    assert.strictEqual(upd2.source_event_ts_ms, null);

    // Replayed updates carry the ORIGINAL capture times, >500ms before the
    // snapshot's own receive time — never the replay moment.
    assert.strictEqual(upd1.recv_ts_ms, buf0.recv_ts_ms);
    assert.strictEqual(upd1.receive_seq, 1);
    assert.strictEqual(upd1.connection_id, buf0.connection_id);
    assert.strictEqual(upd2.recv_ts_ms, buf1.recv_ts_ms);
    assert.strictEqual(upd2.receive_seq, 2);
    assert.ok(snap.recv_ts_ms - upd1.recv_ts_ms >= 400,
      `replayed recv_ts_ms drifted: snapshot=${snap.recv_ts_ms} update=${upd1.recv_ts_ms}`);

    // Ingress metadata is never part of the emitted payload itself.
    for (const ev of emitted) {
      assert.ok(!Object.prototype.hasOwnProperty.call(ev, '_ingress'));
      assert.ok(!JSON.stringify(ev).includes('_ingress'));
    }
  });
});

// ====== Source timestamp missing / invalid (c) ======

describe('Ingress metadata (c): missing/invalid source timestamps never fake "now"', () => {
  function createConn() {
    const conn = new BaseConnector({}, { market: 'test_market', wsUrl: 'ws://localhost:1', restUrl: '' });
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    register(conn);
    return conn;
  }

  it('Bitstamp trade with unparseable source ts is dropped (no Date.now() fallback)', () => {
    const conn = new BitstampConnector({});
    conn._ws = { send: () => {} };
    conn._setState('running');
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage({
      event: 'trade',
      channel: 'live_trades_btcusd',
      data: { id: 1, price: '65000', amount: '0.1', type: 0, microtimestamp: 'not-a-time' },
    });

    assert.strictEqual(emitted.length, 0);
    assert.strictEqual(conn._stats.droppedTradeCount, 1);
  });

  it('Coinbase trade with unparseable time is dropped (no Date.now() fallback)', () => {
    const conn = new CoinbaseConnector({});
    conn._ws = { send: () => {} };
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage({
      channel: 'market_trades',
      events: [{ trades: [{ price: '65000', size: '0.1', side: 'SELL', time: 'garbage', trade_id: 't1' }] }],
    });

    assert.strictEqual(emitted.length, 0);
  });

  it('liquidation with missing source_ts passes with source_event_ts_ms=null; invalid stays fail-closed', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({
      exchange: 'okx', symbol: 'BTC-USDT-SWAP', side: 'buy',
      price: 65000, qty: 1, notional: 65000,
      raw_type: 'liquidation-orders', trade_id: null,
      source_ts: null,
    });
    conn._emitLiquidation({
      exchange: 'okx', symbol: 'BTC-USDT-SWAP', side: 'buy',
      price: 65200, qty: 1, notional: 65200,
      raw_type: 'liquidation-orders', trade_id: null,
      source_ts: 1700000000000,
    });
    // Pre-existing fail-closed contract (asserted in base-connector.test.mjs):
    // a non-finite source_ts drops the liquidation entirely — it must never
    // be masked as a current-time fake.
    conn._emitLiquidation({
      exchange: 'okx', symbol: 'BTC-USDT-SWAP', side: 'sell',
      price: 65100, qty: 1, notional: 65100,
      raw_type: 'liquidation-orders', trade_id: null,
      source_ts: NaN,
    });

    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[0].source_event_ts_ms, null);
    assert.strictEqual(emitted[0].source_event_time_known, false);
    // Valid source ts is preserved (not substituted, not dropped).
    assert.strictEqual(emitted[1].source_event_ts_ms, 1700000000000);
    assert.strictEqual(emitted[1].source_event_time_known, true);
    assert.strictEqual(conn._stats.droppedLiquidationCount, 1);
  });
});

// ====== buildRawDbEnvelope: worker never re-stamps receive time (d) ======

describe('buildRawDbEnvelope (d): worker persists ingress metadata verbatim, never re-stamps', () => {
  const base = {
    obj: { ts: 1700000000000 },
    eventTimestampMs: 1700000000000,
    storage: 'sqlite',
    market: 'bitstamp_spot',
    kind: 'trades',
    workerId: 'w0',
    workerSeq: 7,
  };

  it('keeps existing recv_ts_ms / recv_mono_ns / receive_seq / connection_id unchanged', () => {
    const envelope = buildRawDbEnvelope({
      ...base,
      obj: {
        ts: 1700000000000,
        recv_ts_ms: 111222333,
        recv_mono_ns: 42,
        receive_seq: 5,
        connection_id: 'conn-1',
        source_event_ts_ms: 1700000000000,
        source_event_time_known: true,
      },
    });
    assert.strictEqual(envelope.recv_ts_ms, 111222333);
    assert.strictEqual(envelope.recv_mono_ns, 42);
    assert.strictEqual(envelope.receive_seq, 5);
    assert.strictEqual(envelope.connection_id, 'conn-1');
    assert.strictEqual(envelope.source_event_ts_ms, 1700000000000);
    assert.strictEqual(envelope.source_event_time_known, true);
    assert.strictEqual(envelope.event_ts_ms, 1700000000000);
  });

  it('missing recv_ts_ms stays null — enqueue time is never invented as receive time', () => {
    const before = Date.now();
    const envelope = buildRawDbEnvelope({ ...base, obj: { ts: 1700000000000 } });
    assert.strictEqual(envelope.recv_ts_ms, null);
    assert.strictEqual(envelope.recv_mono_ns, null);
    assert.strictEqual(envelope.receive_seq, null);
    assert.strictEqual(envelope.event_ts_ms, 1700000000000);
    // Guard against a regression to Date.now() re-stamping at enqueue:
    const after = Date.now();
    assert.ok(envelope.recv_ts_ms === null || envelope.recv_ts_ms < before || envelope.recv_ts_ms > after);
  });

  it('invalid recv/source timestamps normalize to null (never Date.now())', () => {
    const envelope = buildRawDbEnvelope({
      ...base,
      obj: {
        ts: 'garbage',
        recv_ts_ms: NaN,
        recv_mono_ns: 'x',
        source_event_ts_ms: 'not-a-time',
      },
    });
    assert.strictEqual(envelope.recv_ts_ms, null);
    assert.strictEqual(envelope.recv_mono_ns, null);
    assert.strictEqual(envelope.event_ts_ms, null);
    assert.strictEqual(envelope.source_event_ts_ms, null);
    assert.strictEqual(envelope.source_event_time_known, false);
  });

  it('legacy event without source fields falls back to event ts only (back-compat)', () => {
    const envelope = buildRawDbEnvelope({ ...base, obj: { ts: 1700000000000 } });
    assert.strictEqual(envelope.source_event_ts_ms, 1700000000000);
    assert.strictEqual(envelope.source_event_time_known, true);
  });

  it('payload never leaks the non-enumerable _ingress snapshot', () => {
    const obj = { ts: 1700000000000, recv_ts_ms: 111222333 };
    Object.defineProperty(obj, '_ingress', {
      value: { recv_ts_ms: 999, receive_seq: 1, connection_id: 'leak' },
      enumerable: false,
    });
    const envelope = buildRawDbEnvelope({ ...base, obj });
    assert.strictEqual(envelope.recv_ts_ms, 111222333);
    assert.ok(!JSON.stringify(envelope.payload).includes('_ingress'));
    assert.strictEqual(envelope.payload.worker_seq, 7);
    assert.strictEqual(envelope.payload.connection_id, null);
  });
});

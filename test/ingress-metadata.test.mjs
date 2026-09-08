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
//   (e) Astra-audit regressions: OKX replay honors emit rejection (fail-closed
//       resync, no book pollution); Gemini depth socket has its own
//       connection id / receive_seq; _ingressFields keeps the ingress
//       snapshot's connection_id across generations; strict timestamp
//       coercion never fabricates ts=0/-1 from null/''/false.
//   (f) Astra re-audit regressions: an awaiting OKX _syncBook never reports a
//       replay-rejection resync as success (stays 'reconnecting', no forced
//       'running'); Gemini depth-socket frames from a superseded generation
//       are ignored; Gemini depth-trade timestamps are type-checked before
//       Number() so false/''/blank never become ts:0.
//
// Fixtures are inline (matching existing connector test style); the only
// fixture dir (test/fixtures/burst-v1) has no raw-frame fixtures to reuse.

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';
import { BitstampConnector } from '../lib/bitstamp-connector.mjs';
import { CoinbaseConnector } from '../lib/coinbase-connector.mjs';
import { OkxConnector } from '../lib/okx-connector.mjs';
import { GeminiConnector } from '../lib/gemini-connector.mjs';
import { BitfinexConnector } from '../lib/bitfinex-connector.mjs';
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
  // REST snapshot carries its matching-engine microtimestamp as-of
  // 1700000000000 ms (issue #13 boundary).
  const SNAPSHOT_BODY = {
    bids: [['65000', '1.0']],
    asks: [['65001', '2.0']],
    microtimestamp: '1700000000000000',
    timestamp: '1700000000',
  };
  // Buffered diff is provably NEWER than the snapshot boundary so the
  // boundary partition replays it (issue #13).
  const depthFrame = (bids, asks, micro = '1700000000500000') => ({
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

      // REST snapshot is a local capture: no socket receive_seq. Since the
      // snapshot response carries the exchange microtimestamp, its source
      // event time IS known (issue #13) — distinct from feeds without one.
      assert.strictEqual(emitted[0].receive_seq, null);
      assert.strictEqual(emitted[0].source_event_ts_ms, 1700000000000);
      assert.strictEqual(emitted[0].source_event_time_known, true);
      assert.strictEqual(emitted[0].snapshot_asof_ts_ms, 1700000000000);

      // The replayed diff keeps the recv time stamped at buffer time (600ms
      // earlier), not the replay/enqueue moment.
      const update = emitted[1];
      assert.strictEqual(update.recv_ts_ms, bufferedRecvTs);
      assert.strictEqual(update.receive_seq, 1);
      assert.strictEqual(update.connection_id, buffered.connection_id);
      assert.strictEqual(update.source_event_ts_ms, 1700000000500);
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
      assert.strictEqual(live.source_event_ts_ms, 1700000000500);
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

  it('(e) Astra P2-3: ""/false/-1/whitespace junk normalizes to null — never a known 0/-1', () => {
    // Number('')===0, Number(false)===0 and Number('-1')===-1 would have
    // fabricated "known" timestamps; strict type-first coercion nulls them.
    const envelope = buildRawDbEnvelope({
      ...base,
      obj: {
        event_ts_ms: '',          // → null (not 0)
        ts: false,                // non-numeric type → null
        recv_ts_ms: '   ',        // whitespace-only string → null
        recv_mono_ns: -1,         // negative clock → null
        source_event_ts_ms: '-1', // numeric-string junk → null
      },
    });
    assert.strictEqual(envelope.event_ts_ms, null);
    assert.strictEqual(envelope.recv_ts_ms, null);
    assert.strictEqual(envelope.recv_mono_ns, null);
    assert.strictEqual(envelope.source_event_ts_ms, null);
    assert.strictEqual(envelope.source_event_time_known, false);
    assert.strictEqual(envelope.receive_seq, null);
  });

  it('(e) Astra P2-3: numeric strings and valid numbers still coerce (control)', () => {
    const envelope = buildRawDbEnvelope({
      ...base,
      obj: {
        event_ts_ms: '1700000000123.9',  // numeric string, truncated to ms
        recv_ts_ms: 1700000000456,
        recv_mono_ns: '1234567890',
        source_event_ts_ms: 1700000000000,
        source_event_time_known: true,
      },
    });
    assert.strictEqual(envelope.event_ts_ms, 1700000000123);
    assert.strictEqual(envelope.recv_ts_ms, 1700000000456);
    assert.strictEqual(envelope.recv_mono_ns, 1234567890);
    assert.strictEqual(envelope.source_event_ts_ms, 1700000000000);
    assert.strictEqual(envelope.source_event_time_known, true);
  });
});

// ====== Astra P1: OKX replay honors emit rejection (e) ======

describe('OKX replay (Astra P1): rejected replayed diff never touches book/seq', () => {
  function createOkx() {
    const conn = new OkxConnector({});
    conn._setState('connected');
    conn._ws = { send: () => {}, close: () => {} };
    conn._ringBuf = [];
    conn._wsSnapshotReceived = false;
    // Record the fail-closed resync trigger without arming real reconnect
    // timers (these tests only assert resync was entered). Mirrors the real
    // _scheduleReconnect state move so in-flight _syncBook() callers observe
    // the same 'reconnecting' transition they would in production.
    conn.resyncScheduled = false;
    conn._scheduleReconnect = () => {
      conn.resyncScheduled = true;
      conn._setState('reconnecting');
    };
    register(conn);
    return conn;
  }

  it('abandons replay and resyncs when the replayed diff is emit-rejected (missing ts)', () => {
    const conn = createOkx();
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // Buffered pre-snapshot: a diff with NO exchange ts. On replay its emit is
    // rejected (fail-closed) — it must not be applied to the book, and the
    // sequence must not advance past what the downstream stream saw.
    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 11, prevSeqId: 10, bids: [['65000', '1']], asks: [] }],
    });
    assert.strictEqual(conn._ringBuf.length, 1);

    conn._handleDepth({
      action: 'snapshot',
      data: [{ seqId: 10, ts: '1700000000000', bids: [['65000', '1.5']], asks: [['65001', '2.0']] }],
    });

    // Downstream saw the snapshot only — the rejected update was never emitted.
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'snapshot');
    // Fail-closed: emit rejection routes into the sequence-gap resync instead
    // of silently applying a diff the persisted stream never received.
    assert.strictEqual(conn._stats.droppedDepthCount, 1);
    assert.strictEqual(conn.resyncScheduled, true);
    assert.ok(errors.length >= 1 && /seq gap: OKX replay emit rejected/.test(errors[0].message));
    assert.strictEqual(conn._wsSnapshotReceived, false);
    assert.strictEqual(conn.book.bids.size, 0, 'rejected diff must not pollute the book');
    assert.strictEqual(conn.book._lastSeq, null, 'sequence must not advance on a rejected frame');
  });

  it('still applies a VALID replayed diff (control: replay behavior preserved)', () => {
    const conn = createOkx();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 11, prevSeqId: 10, ts: '1700000000011', bids: [['65000', '1']], asks: [] }],
    });
    assert.strictEqual(conn._ringBuf.length, 1);

    conn._handleDepth({
      action: 'snapshot',
      data: [{ seqId: 10, ts: '1700000000000', bids: [['65000', '1.5']], asks: [['65001', '2.0']] }],
    });

    assert.strictEqual(conn.resyncScheduled, false);
    assert.strictEqual(emitted.length, 2);
    assert.strictEqual(emitted[1].type, 'update');
    assert.strictEqual(emitted[1].source_event_ts_ms, 1700000000011);
    assert.strictEqual(emitted[1].source_event_time_known, true);
    assert.strictEqual(conn.book.bids.get('65000'), '1'); // diff applied over snapshot
    assert.strictEqual(conn.book._lastSeq, 11);
    assert.strictEqual(conn._wsSnapshotReceived, true);
  });

  it('(f) Astra P2: awaiting _syncBook does NOT force running when replay rejection derails the sync', async () => {
    const conn = createOkx();
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // _syncBook arms the WS-snapshot waiter (state syncing) — buffer an
    // update with NO exchange ts AFTER it, so the update rides the ring
    // buffer (pre-snapshot) instead of being cleared by _beginWsSnapshotSync.
    const syncPromise = conn._syncBook();
    assert.strictEqual(conn.getState(), 'syncing');
    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 11, prevSeqId: 10, bids: [['65000', '1']], asks: [] }],
    });
    assert.strictEqual(conn._ringBuf.length, 1);

    // Snapshot resolves the waiter; the replay then rejects the buffered
    // diff (missing ts) and routes into _handleSequenceGap → book cleared,
    // _wsSnapshotReceived reset, reconnect scheduled (state reconnecting).
    conn._handleDepth({
      action: 'snapshot',
      data: [{ seqId: 10, ts: '1700000000000', bids: [['65000', '1.5']], asks: [['65001', '2.0']] }],
    });

    await syncPromise;

    assert.strictEqual(conn.resyncScheduled, true);
    assert.ok(errors.length >= 1 && /seq gap: OKX replay emit rejected/.test(errors[0].message));
    // Fail-closed: a sync derailed into reconnect must NOT be reported as
    // success — the resumed _syncBook leaves the state machine alone.
    assert.strictEqual(conn.getState(), 'reconnecting',
      'derailed sync must not flip the state back to running');
    assert.strictEqual(conn._wsSnapshotReceived, false);
    assert.strictEqual(conn.book.bids.size, 0, 'book stays empty until a real snapshot');
    assert.strictEqual(conn._stats.resyncCount, 0, 'failed sync must not count as a resync');
    // Downstream only ever saw the snapshot — nothing after the rejection.
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'snapshot');
  });

  it('(f) Astra P2 control: a clean snapshot sync still finalizes to running', async () => {
    const conn = createOkx();
    const syncPromise = conn._syncBook();

    conn._handleDepth({
      action: 'update',
      data: [{ seqId: 11, prevSeqId: 10, ts: '1700000000011', bids: [['65000', '1']], asks: [] }],
    });
    conn._handleDepth({
      action: 'snapshot',
      data: [{ seqId: 10, ts: '1700000000000', bids: [['65000', '1.5']], asks: [['65001', '2.0']] }],
    });
    await syncPromise;

    assert.strictEqual(conn.resyncScheduled, false);
    assert.strictEqual(conn.getState(), 'running');
    assert.strictEqual(conn._wsSnapshotReceived, true);
    assert.strictEqual(conn._stats.resyncCount, 1);
    assert.strictEqual(conn.book.bids.get('65000'), '1'); // replayed diff applied
    assert.strictEqual(conn.book._lastSeq, 11);
  });
});

// ====== Astra P2-1: Gemini depth socket owns its ingress stream (e) ======

describe('Gemini depth socket (Astra P2-1): independent connection id / receive_seq', () => {
  function feedDepthFrame(conn, obj) {
    conn._depthWs.emit('message', Buffer.from(JSON.stringify(obj)));
  }

  async function createGemini() {
    const conn = new GeminiConnector({});
    conn._setWebSocket(MockWebSocket);
    register(conn);
    await connectConn(conn); // open → subscribe() → _connectDepthWs()
    const t0 = Date.now();
    while (!conn._depthWs && Date.now() - t0 < 2000) await sleep(5);
    assert.ok(conn._depthWs, 'depth WS never created by subscribe()');
    assert.ok(conn._depthConnectionId, 'depth connection id not assigned');
    return conn;
  }

  it('depth frames carry their own connection id and a receive_seq starting at 1', async () => {
    const conn = await createGemini();
    const mainId = conn._connectionId;
    const depthId = conn._depthConnectionId;
    assert.ok(mainId && depthId);
    assert.notStrictEqual(depthId, mainId, 'depth socket must be its own generation');

    // Advance the MAIN socket's frame counter first (heartbeats are ignored
    // payloads but still counted at the ingress boundary).
    feedFrame(conn, { type: 'heartbeat' });
    feedFrame(conn, { type: 'heartbeat' });
    assert.strictEqual(conn._receiveSeq, 2);

    const depthEvents = [];
    conn.on('depth', (ev) => depthEvents.push(ev));
    const depthFrame = (changes) => ({ type: 'l2_updates', changes, trades: [] });

    feedDepthFrame(conn, depthFrame([{ side: 'bid', price: '65000', remaining: '1.5' }]));
    feedDepthFrame(conn, depthFrame([{ side: 'ask', price: '65001', remaining: '2.0' }]));

    assert.strictEqual(depthEvents.length, 2);
    // Depth stream starts at 1 — it must NOT continue the main counter (3, 4).
    assert.deepStrictEqual(depthEvents.map((ev) => ev.receive_seq), [1, 2]);
    // ...and every depth event carries the depth socket's own connection id.
    assert.ok(depthEvents.every((ev) => ev.connection_id === depthId));
    assert.ok(depthEvents.every((ev) => ev.connection_id !== mainId));
    assert.ok(depthEvents.every((ev) => typeof ev.recv_mono_ns === 'number'));
    // Gemini book frames carry no exchange event time: still explicit.
    assert.strictEqual(depthEvents[0].source_event_ts_ms, null);
    assert.strictEqual(depthEvents[0].source_event_time_known, false);

    // Depth traffic never advanced the main socket's counter or identity.
    assert.strictEqual(conn._receiveSeq, 2);
    assert.strictEqual(conn._connectionId, mainId);
    assert.strictEqual(conn._receiveSeq + conn._depthReceiveSeq, 4);

    // Main socket frames after depth traffic still count independently.
    feedFrame(conn, { type: 'heartbeat' });
    assert.strictEqual(conn._receiveSeq, 3);
  });

  it('(f) Astra P2: frames from a superseded depth socket are ignored — never stamped with the new generation', async () => {
    const conn = await createGemini();
    const depthEvents = [];
    conn.on('depth', (ev) => depthEvents.push(ev));
    const depthFrame = (changes, trades = []) => ({ type: 'l2_updates', changes, trades });

    // One frame on the ORIGINAL depth socket.
    feedDepthFrame(conn, depthFrame([{ side: 'bid', price: '65000', remaining: '1.5' }]));
    assert.strictEqual(depthEvents.length, 1);
    const oldWs = conn._depthWs;
    const oldId = conn._depthConnectionId;
    assert.ok(oldWs && oldId);

    // A reconnect cycle replaces the depth socket with a new generation
    // (new connection id, receive_seq reset). _closeDepthWs nulls the socket
    // first, so wait until the async _connectDepthWs has installed the new one.
    conn.subscribe();
    const t0 = Date.now();
    while ((!conn._depthWs || conn._depthWs === oldWs) && Date.now() - t0 < 2000) await sleep(5);
    assert.ok(conn._depthWs && conn._depthWs !== oldWs, 'depth WS was not recreated');
    const newId = conn._depthConnectionId;
    assert.notStrictEqual(newId, oldId, 'reconnect must assign a new depth connection id');
    assert.strictEqual(conn._depthReceiveSeq, 0, 'reconnect resets the depth counter');

    // Late frame from the OLD socket arrives after the new socket took over:
    // the generation guard must drop it — it must NOT be recorded under the
    // new generation's connection id / receive_seq.
    oldWs.emit('message', Buffer.from(JSON.stringify(depthFrame([{ side: 'ask', price: '65001', remaining: '2.0' }]))));
    assert.strictEqual(depthEvents.length, 1, 'stale depth frame must not emit a depth event');
    assert.strictEqual(conn._depthReceiveSeq, 0, 'stale frame must not advance the counter');

    // Control: the NEW socket's first frame starts at seq 1 under the new id.
    feedDepthFrame(conn, depthFrame([{ side: 'bid', price: '65002', remaining: '3.0' }]));
    assert.strictEqual(depthEvents.length, 2);
    assert.strictEqual(depthEvents[1].connection_id, newId);
    assert.strictEqual(depthEvents[1].receive_seq, 1);
  });

  it('(f) Astra P2: depth trades with invalid timestamps are dropped — never fabricated as ts:0/known', async () => {
    const conn = await createGemini();
    const trades = [];
    conn.on('trade', (ev) => trades.push(ev));
    const depthFrame = (changes, tradeRows) => ({ type: 'l2_updates', changes, trades: tradeRows });
    const baseTrade = { price: '65000', quantity: '0.1', side: 'sell', event_id: 900, timestamp: 1700000000123 };

    // Number(false)===0 / Number('')===0 / Number('   ')===0 would fabricate
    // ts:0 with source_event_time_known:true — strict type-first coercion
    // must null them out and drop the trades fail-closed.
    // Astra R3 (P2 regression): Date.parse('-1') yields a positive 2001 epoch
    // (978274800000), so a negative numeric string used to slip past the
    // numeric gate and emit a trade with a FABRICATED known source time.
    // Finite negatives (number and numeric string) must be rejected in place.
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 901, timestamp: false }]));
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 902, timestamp: '' }]));
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 903, timestamp: '   ' }]));
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 904, timestamp: '-1' }]));
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 905, timestamp: -1 }]));
    feedDepthFrame(conn, depthFrame([], [{ ...baseTrade, event_id: 906, timestamp: '-1000' }]));
    assert.strictEqual(trades.length, 0, 'invalid depth-trade timestamps must not emit ts:0/known trades');

    // Control: a valid timestamp still emits with the known source time.
    feedDepthFrame(conn, depthFrame([], [baseTrade]));
    assert.strictEqual(trades.length, 1);
    assert.strictEqual(trades[0].ts, 1700000000123);
    assert.strictEqual(trades[0].source_event_ts_ms, 1700000000123);
    assert.strictEqual(trades[0].source_event_time_known, true);
  });
});

// ====== Astra P2-2: connection_id follows the ingress snapshot (e) ======

describe('_ingressFields (Astra P2-2): connection_id follows the ingress snapshot', () => {
  function createConn() {
    const conn = new BaseConnector({}, { market: 'p22', wsUrl: 'ws://localhost:1', restUrl: '' });
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    register(conn);
    return conn;
  }

  it('old-generation replay snapshot keeps its OWN connection_id (no mixed heritage)', () => {
    const conn = createConn();
    conn._connectionId = 'gen-2-current';
    conn._receiveSeq = 5;
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // Ingress captured on an OLD socket generation, replayed now: recv_ts_ms,
    // receive_seq AND connection_id must all stay with the old generation —
    // never "old recv_ts/receive_seq + new connection_id".
    conn._emitDepth('update', [['65000', '1']], [], 1700000000000, 9, {}, {
      recv_ts_ms: 111111, recv_mono_ns: 42, connection_id: 'gen-1-old', receive_seq: 7,
    });

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].connection_id, 'gen-1-old');
    assert.strictEqual(emitted[0].receive_seq, 7);
    assert.strictEqual(emitted[0].recv_ts_ms, 111111);
    assert.strictEqual(emitted[0].recv_mono_ns, 42);
    assert.strictEqual(emitted[0].source_event_ts_ms, 1700000000000);
    assert.strictEqual(emitted[0].source_event_time_known, true);
  });

  it('ingress without connection_id (or none at all) falls back to the current connection id', () => {
    const conn = createConn();
    conn._connectionId = 'gen-2-current';
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // Legacy-shaped ingress snapshot (no connection_id field) → current gen.
    conn._emitDepth('update', [['65000', '1']], [], 1700000000000, 1, {},
      { recv_ts_ms: 222, recv_mono_ns: null, receive_seq: 3 });
    // No ingress at all (direct emit outside a socket callback) → current gen.
    conn._emitDepth('update', [['65000', '2']], [], 1700000000001, 2, {});

    assert.strictEqual(emitted[0].connection_id, 'gen-2-current');
    assert.strictEqual(emitted[0].receive_seq, 3);
    assert.strictEqual(emitted[0].recv_ts_ms, 222);
    assert.strictEqual(emitted[1].connection_id, 'gen-2-current');
    assert.strictEqual(emitted[1].receive_seq, null);
  });
});

// ====== Astra P2-3: Bitfinex missing source ts stays null (e) ======

describe('Bitfinex (Astra P2-3): missing source ts is null — never fabricated ts=0', () => {
  function createConn() {
    const conn = new BitfinexConnector({});
    conn._ws = { send: () => {} };
    conn._setState('running');
    return conn;
  }

  it('array trade with null mts is dropped (fail-closed), not emitted as ts=0/known', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // [chanId, 'tu', [id, mts, amount, price]] with a missing (null) mts.
    conn._onMessage([5, 'tu', [123, null, 0.1, '65000']]);

    assert.strictEqual(emitted.length, 0);
    assert.strictEqual(conn._stats.droppedTradeCount, 1);
  });

  it("object trade with ''/false mts is dropped — never ts=0/source_event_time_known=true", () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage({ type: 'trade', data: { id: 124, mts: '', amount: -0.2, price: '65001' } });
    conn._onMessage({ type: 'trade', data: { id: 125, mts: false, amount: 0.3, price: '65002' } });

    assert.strictEqual(emitted.length, 0);
    assert.strictEqual(conn._stats.droppedTradeCount, 2);
  });

  it('valid mts still emits with known source time (control)', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._onMessage([5, 'tu', [126, 1700000000123, -0.2, '65003']]);

    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 1700000000123);
    assert.strictEqual(emitted[0].source_event_ts_ms, 1700000000123);
    assert.strictEqual(emitted[0].source_event_time_known, true);
    assert.strictEqual(emitted[0].side, 'sell');
    assert.strictEqual(emitted[0].qty, 0.2);
  });
});

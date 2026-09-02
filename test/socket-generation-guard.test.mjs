// test/socket-generation-guard.test.mjs — socket generation guard tests
//
// Root cause: 2.0x raw trade duplication on binance/bybit/bitfinex/bitstamp
// markets (CVD inflation report 2026-09-02). The base connector had no
// generation guard: a stale socket's delayed close/error callback could
// schedule a reconnect after a newer socket was already running, leaving two
// live sockets feeding the same market. These tests assert:
//   1. connect() advances a generation counter.
//   2. connect() removes listeners / terminates the previous socket.
//   3. A stale-socket close (fired after a newer generation) schedules no
//      reconnect and mutates no state.

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

/** ws-compatible mock that records listener removal / termination. */
class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
    this.listenerRemoved = [];
    this.terminateCalls = 0;
  }
  close(_code, _reason) { this.readyState = 3; }
  removeAllListeners(event) {
    if (event) this.listenerRemoved.push(event);
    return super.removeAllListeners(event);
  }
  terminate() { this.terminateCalls++; this.readyState = 3; }
}

const _cleanup = [];

function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {};
  _cleanup.push(conn);
  return conn;
}

after(() => {
  for (const conn of _cleanup) conn._clearTimers();
  _cleanup.length = 0;
});

describe('BaseConnector socket generation guard', () => {
  it('connect() advances _wsGeneration on each call', async () => {
    const conn = createTestConnector();
    const g0 = conn._wsGeneration;
    const p1 = conn.connect();
    setImmediate(() => { conn._ws.readyState = 1; conn._ws.emit('open'); });
    await p1;
    assert.strictEqual(conn._wsGeneration, g0 + 1);
    const p2 = conn.connect();
    setImmediate(() => { conn._ws.readyState = 1; conn._ws.emit('open'); });
    await p2;
    assert.strictEqual(conn._wsGeneration, g0 + 2);
  });

  it('connect() terminates and detaches listeners of the previous socket', async () => {
    const conn = createTestConnector();
    const p1 = conn.connect();
    setImmediate(() => { conn._ws.readyState = 1; conn._ws.emit('open'); });
    await p1;
    const ws1 = conn._ws;
    const p2 = conn.connect();
    setImmediate(() => { conn._ws.readyState = 1; conn._ws.emit('open'); });
    await p2;
    const ws2 = conn._ws;
    assert.notStrictEqual(ws1, ws2, 'must create a fresh socket');
    assert.strictEqual(ws1.terminateCalls, 1, 'old socket terminated');
    for (const ev of ['message', 'open', 'error', 'close']) {
      assert.ok(ws1.listenerRemoved.includes(ev), `listener ${ev} removed from old socket`);
    }
  });

  it('a stale-socket close after a newer generation schedules no reconnect', async () => {
    const conn = createTestConnector();
    // Capture each socket instance so we can fire the OLD one's close late.
    const sockets = [];
    const origGetWs = conn._getWsImpl.bind(conn);
    conn._getWsImpl = async () => {
      const Cls = await origGetWs();
      return class extends Cls {
        constructor(url) { super(url); sockets.push(this); }
      };
    };

    let reconnectSchedules = 0;
    const origSchedule = conn._scheduleReconnect.bind(conn);
    conn._scheduleReconnect = (...a) => { reconnectSchedules++; return origSchedule(...a); };

    const p1 = conn.connect();
    setImmediate(() => { sockets[0].readyState = 1; sockets[0].emit('open'); });
    await p1;
    assert.strictEqual(conn.getState(), 'connected');

    const p2 = conn.connect();
    setImmediate(() => { sockets[1].readyState = 1; sockets[1].emit('open'); });
    await p2;
    assert.strictEqual(conn.getState(), 'connected');

    // Now the BUG scenario: stale socket (sockets[0]) closes LATE, after the
    // newer socket (sockets[1]) is already running. The generation guard must
    // make this a no-op: no reconnect, no state change, no error emit.
    const errors = [];
    conn.on('error', (e) => errors.push(e));
    const reconnectsBefore = reconnectSchedules;
    sockets[0].emit('close', 1006, 'stale-late');
    assert.strictEqual(conn.getState(), 'connected', 'state untouched by stale close');
    assert.strictEqual(reconnectSchedules, reconnectsBefore, 'no reconnect scheduled by stale close');

    // And the current socket still delivers trades normally.
    const trades = [];
    conn.on('trade', (ev) => trades.push(ev));
    conn._emitTrade(65000, 0.5, 'buy', Date.now(), 12345);
    assert.strictEqual(trades.length, 1);
  });
});

// test/base-connector.test.mjs — BaseConnector connect() settle-once unit tests
//
// Verifies that connect() resolves on open and rejects on error/close before open,
// preventing the startup hang bug where an unresolved promise blocks
// Promise.allSettled(startPromises).

import { describe, it, after } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

// ====== Mock WebSocket ======

/**
 * Minimal ws-compatible mock using EventEmitter.
 * Emit events manually to simulate open/close/error in tests.
 */
class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
  }

  close(_code, _reason) {
    this.readyState = 3; // CLOSED
    // In real ws, close does not emit the event itself — the remote side does.
  }
}

// ====== Helpers ======

/** Track created connectors for cleanup. */
const _cleanup = [];

/**
 * Create a BaseConnector suitable for connect() testing.
 * Stubs subscribe() so _onOpen() doesn't throw.
 */
function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {}; // no-op to avoid throw in _onOpen
  _cleanup.push(conn);
  return conn;
}

after(() => {
  // Clear all pending reconnect timers so the test runner does not hang.
  for (const conn of _cleanup) {
    conn._clearTimers();
  }
  _cleanup.length = 0;
});

// ====== Tests ======

describe('BaseConnector connect() settle-once', () => {
  it('should resolve on open and set state to connected', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    // Simulate open after a tick
    setImmediate(() => {
      conn._ws.readyState = 1; // OPEN
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');
  });

  it('should reject on error before open, emit error, and schedule reconnect', async () => {
    const conn = createTestConnector();

    const errors = [];
    conn.on('error', (ev) => errors.push(ev));

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.emit('error', new Error('connection refused'));
    });

    await assert.rejects(connectPromise, /error before open/);
    // Verify error event was emitted
    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('connection refused'));
    // State should be reconnecting (scheduled reconnect on pre-open error)
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should reject on close before open and schedule reconnect', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.emit('close');
    });

    await assert.rejects(connectPromise, /closed before open/);
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should not reject if error fires after open has already resolved', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    // Open first
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');

    // Now fire error after open — should NOT reject (promise already resolved)
    // and should follow runtime error path (emit error + reconnect)
    const errors = [];
    conn.on('error', (ev) => errors.push(ev));
    conn._ws.readyState = 3; // CLOSED
    conn._ws.emit('error', new Error('runtime error'));
    assert.strictEqual(conn.getState(), 'reconnecting');
    assert.ok(errors.length > 0);
    assert.ok(errors[0].message.includes('runtime error'));
  });

  it('should not reject if close fires after open has already resolved', async () => {
    const conn = createTestConnector();

    const connectPromise = conn.connect();

    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });

    await assert.doesNotReject(connectPromise);
    assert.strictEqual(conn.getState(), 'connected');

    // Fire close after open — should trigger reconnect but not reject
    conn._ws.emit('close');
    assert.strictEqual(conn.getState(), 'reconnecting');
  });

  it('should complete all tests within timeout (no hang)', async () => {
    // This test ensures that multiple connect() calls with various
    // failure modes all settle promptly without hanging.
    const conn1 = createTestConnector();
    const conn2 = createTestConnector();
    const conn3 = createTestConnector();

    // Suppress error events to avoid ERR_UNHANDLED_ERROR
    conn1.on('error', () => {});
    conn2.on('error', () => {});

    const p1 = conn1.connect();
    const p2 = conn2.connect();
    const p3 = conn3.connect();

    // Trigger different settle paths
    setImmediate(() => {
      conn1._ws.emit('error', new Error('err1'));
      conn2._ws.emit('close');
      conn3._ws.readyState = 1;
      conn3._ws.emit('open');
    });

    const results = await Promise.allSettled([p1, p2, p3]);

    assert.strictEqual(results[0].status, 'rejected');
    assert.strictEqual(results[1].status, 'rejected');
    assert.strictEqual(results[2].status, 'fulfilled');
    assert.ok(results[0].reason.message.includes('error before open'));
    assert.ok(results[1].reason.message.includes('closed before open'));
  });
});

describe('BaseConnector periodic raw snapshots', () => {
  it('emits a full book snapshot while running', async () => {
    const conn = createTestConnector();
    conn.config.raw_snapshot_interval_ms = 5;
    conn.book = {
      isEmpty: () => false,
      toSnapshot: (ts) => ({ ts, seq: 42, bids: [['1', '2']], asks: [['2', '3']] }),
    };
    const snapshots = [];
    conn.on('depth', (event) => snapshots.push(event));
    conn._setState('running');
    await new Promise((resolve) => setTimeout(resolve, 15));
    conn._clearTimers();
    assert.ok(snapshots.length >= 1);
    assert.equal(snapshots[0].type, 'snapshot');
    assert.equal(snapshots[0].snapshot_origin, 'periodic_book');
    assert.equal(snapshots[0].seq, 42);
    assert.deepEqual(snapshots[0].bids, [['1', '2']]);
  });
});

describe('BaseConnector depth metadata and connection identity', () => {
  it('preserves depth metadata and assigns a new id per websocket', async () => {
    const conn = createTestConnector();
    const firstConnect = conn.connect();
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });
    await firstConnect;
    const firstId = conn._connectionId;
    const depth = [];
    const raw = [];
    conn.on('depth', (event) => depth.push(event));
    conn.on('rawDepth', (event) => raw.push(event));
    conn._emitDepth('snapshot', [], [], 1700000000000, 7, {
      snapshot_origin: 'rest_sync',
      provenance: 'test',
    });
    conn._emitRawDepth([], [], 1700000000000, 7, { seq_start: 7, seq_end: 7 });

    conn._ws = null;
    const secondConnect = conn.connect();
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });
    await secondConnect;

    assert.ok(firstId);
    assert.notStrictEqual(conn._connectionId, firstId);
    assert.strictEqual(depth[0].snapshot_origin, 'rest_sync');
    assert.strictEqual(depth[0].provenance, 'test');
    assert.strictEqual(raw[0].connection_id, firstId);
    conn._clearTimers();
  });
});

describe('BaseConnector _emitTrade side validation', function () {
  it('should emit trade with valid side "buy"', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].side, 'buy');
  });

  it('should emit trade with valid side "sell"', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'sell', 1700000000000, 't2');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].side, 'sell');
  });

  it('should drop trade with invalid side "Buy"', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'Buy', 1700000000000, 't3');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade with invalid side "SELL"', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'SELL', 1700000000000, 't4');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade with invalid side null', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, null, 1700000000000, 't5');
    assert.strictEqual(emitted.length, 0);
  });
});

describe('BaseConnector _emitDepth ts validation', function () {
  function createConn() {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    return conn;
  }

  it('should emit depth with valid positive ts', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], 1700000000000, null);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 1700000000000);
  });

  it('should emit depth with ts = 0', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], 0, null);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 0);
  });

  it('should drop depth when ts is undefined', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], undefined, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is null', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], null, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is a string', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], '1700000000000', null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is NaN', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], NaN, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is Infinity', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], Infinity, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is -Infinity', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], -Infinity, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop depth when ts is negative', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], -1, null);
    assert.strictEqual(emitted.length, 0);
  });

  it('should not update depthMsgCount or lastDepthMsgAt for invalid ts', () => {
    const conn = createConn();
    conn.on('depth', () => {});

    const before = conn._stats.depthMsgCount;
    conn._emitDepth('snapshot', [], [], NaN, null);
    assert.strictEqual(conn._stats.depthMsgCount, before);
  });

  it('should emit subsequent valid depth after a dropped invalid one', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    conn._emitDepth('snapshot', [], [], NaN, null);
    conn._emitDepth('snapshot', [], [], 1700000000000, null);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 1700000000000);
  });
});

describe('BaseConnector _emitTrade ts validation', function () {
  function createConn() {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    conn._setWebSocket(MockWebSocket);
    conn.subscribe = () => {};
    return conn;
  }

  it('should emit trade with valid positive ts', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', 1700000000000, 't1');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 1700000000000);
  });

  it('should emit trade with ts = 0', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', 0, 't2');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 0);
  });

  it('should drop trade when ts is undefined', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'sell', undefined, 't3');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is null', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', null, 't4');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is a string', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'sell', '1700000000000', 't5');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is NaN', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', NaN, 't6');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is Infinity', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'sell', Infinity, 't7');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is -Infinity', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', -Infinity, 't8');
    assert.strictEqual(emitted.length, 0);
  });

  it('should drop trade when ts is negative', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'sell', -1000, 't9');
    assert.strictEqual(emitted.length, 0);
  });

  it('should not update tradeMsgCount for invalid ts', () => {
    const conn = createConn();
    conn.on('trade', () => {});

    const before = conn._stats.tradeMsgCount;
    conn._emitTrade(65000, 1.0, 'buy', NaN, 't10');
    assert.strictEqual(conn._stats.tradeMsgCount, before);
  });

  it('should still reject invalid side after valid ts', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    // side is invalid even though ts is valid
    conn._emitTrade(65000, 1.0, 'Buy', 1700000000000, 't11');
    assert.strictEqual(emitted.length, 0);
  });

  it('should emit subsequent valid trade after a dropped invalid one', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(65000, 1.0, 'buy', NaN, 't12');
    conn._emitTrade(65000, 1.0, 'sell', 1700000000000, 't13');
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].ts, 1700000000000);
  });

  it('should drop non-finite or non-positive price and quantity', () => {
    const conn = createConn();
    const emitted = [];
    conn.on('trade', (ev) => emitted.push(ev));

    conn._emitTrade(NaN, 1, 'buy', 1700000000000, 'bad-price');
    conn._emitTrade(65000, Infinity, 'sell', 1700000000000, 'bad-qty');
    conn._emitTrade(0, 1, 'buy', 1700000000000, 'zero-price');
    conn._emitTrade(65000, -1, 'sell', 1700000000000, 'negative-qty');

    assert.strictEqual(emitted.length, 0);
    assert.strictEqual(conn._stats.droppedTradeCount, 4);
  });
});

describe('BaseConnector _emitLiquidation validation', function () {
  it('should reject malformed liquidation values', () => {
    const conn = new BaseConnector(
      {},
      { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: '' },
    );
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    conn._emitLiquidation({ side: 'buy', price: NaN, qty: 1 });
    conn._emitLiquidation({ side: 'buy', price: 65000, qty: 1, source_ts: NaN });

    assert.strictEqual(emitted.length, 0);
    assert.strictEqual(conn._stats.droppedLiquidationCount, 2);
  });
});

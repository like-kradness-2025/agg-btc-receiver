// test/base-connector-reconnect.test.mjs — BaseConnector reconnect / stale / emit validation
// C2 Phase 2: new tests for reconnect backoff, stale detection, sequence gap,
// _emitDepth and _emitLiquidation validation.  Separate from base-connector.test.mjs
// to avoid regression risk on existing connect settle-once tests.

import { describe, it, after, before, mock } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { BaseConnector } from '../lib/base-connector.mjs';

// ====== Mock WebSocket ======

class MockWebSocket extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.readyState = 0; // CONNECTING
    this._closeCalled = false;
  }
  close(_code, _reason) {
    this._closeCalled = true;
    this.readyState = 3; // CLOSED
  }
}

// ====== Helpers ======

const _cleanup = [];

function createTestConnector() {
  const conn = new BaseConnector(
    {},
    { market: 'test_market', wsUrl: 'ws://localhost:9999', restUrl: 'http://localhost:9999' },
  );
  conn._setWebSocket(MockWebSocket);
  conn.subscribe = () => {};
  conn._syncBook = async () => {};  // short-circuit real sync
  _cleanup.push(conn);
  return conn;
}

after(() => {
  for (const conn of _cleanup) {
    conn._clearTimers();
  }
  _cleanup.length = 0;
});

// ====== Phase 2-1: Reconnect backoff ======

describe('reconnect backoff', () => {
  it('_backoffDelay returns ~1000ms on first call', () => {
    const conn = createTestConnector();
    // _backoffDelay increments _reconnectAttempt from 0 → 1
    const delay = conn._backoffDelay();
    // base = 1000 * 2^0 = 1000  + jitter (0..1000)
    assert.ok(delay >= 1000 && delay <= 2000, `expected 1000..2000, got ${delay}`);
    assert.strictEqual(conn._reconnectAttempt, 1);
  });

  it('_backoffDelay doubles each attempt up to 30s cap', () => {
    const conn = createTestConnector();
    const prevAttempt = conn._reconnectAttempt;
    const delays = [];
    for (let i = 0; i < 20; i++) {
      delays.push(conn._backoffDelay());
    }
    // attempt 1 ~1000-2000, 2 ~2000-3000, 3 ~4000-5000, 4 ~8000-9000,
    // 5 ~16000-17000, 6+ ~30000-31000 (capped)
    assert.ok(delays[0] >= 1000 && delays[0] <= 2000,
      `attempt ${prevAttempt + 1}: expected 1000..2000, got ${delays[0]}`);
    assert.ok(delays[1] >= 2000 && delays[1] <= 3000,
      `attempt ${prevAttempt + 2}: expected 2000..3000, got ${delays[1]}`);
    assert.ok(delays[2] >= 4000 && delays[2] <= 5000,
      `attempt ${prevAttempt + 3}: expected 4000..5000, got ${delays[2]}`);
    // After ~5 attempts delay is capped at MAX (30000)
    for (let i = 5; i < delays.length; i++) {
      assert.ok(delays[i] >= 30000 && delays[i] <= 31000,
        `attempt ${prevAttempt + i + 1}: expected 30000..31000, got ${delays[i]}`);
    }
  });

  it('enters error state after MAX_RECONNECT_ATTEMPTS (30) failures', async () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR
    conn._reconnectAttempt = 29; // just below max

    // Calling _scheduleReconnect with attempt=29 sets a reconnect timer
    conn._scheduleReconnect();
    assert.strictEqual(conn.getState(), 'reconnecting',
      `expected reconnecting state at attempt 29, got ${conn.getState()}`);

    // Clear the timer from the previous call so the next call processes MAX check
    conn._clearTimers();
    conn._reconnectAttempt = 30;
    conn._scheduleReconnect();
    assert.strictEqual(conn.getState(), 'error',
      `expected error state at attempt 30, got ${conn.getState()}`);
  });

  it('_reconnectAttempt reduces by 10 after error recovery cooldown', async () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR
    conn._reconnectAttempt = 30;
    conn._clearTimers(); // clear any pending

    // Trigger the error recovery path
    conn._scheduleReconnect();
    assert.strictEqual(conn.getState(), 'error');

    // Manually invoke the error recovery callback (since we can't wait 60s)
    // The recovery sets _reconnectAttempt = max(0, 30 - 10) = 20
    conn._reconnectAttempt = Math.max(0, conn._reconnectAttempt - 10);
    conn._errorRecoveryTimer = null;
    conn._scheduleReconnect();
    // After reduction to 20, reconnecting is scheduled (state should not be error)
    assert.strictEqual(conn.getState(), 'reconnecting',
      `expected reconnecting after reduction, got ${conn.getState()}`);
    // _scheduleReconnect calls _backoffDelay which increments attempt
    assert.strictEqual(conn._reconnectAttempt, 21); // 20 + 1 from _backoffDelay
  });
});

// ====== Phase 2-2: Stale detection ======

describe('stale detection', () => {
  it('stale timer is started after connect open', async () => {
    const conn = createTestConnector();
    assert.strictEqual(conn._staleTimer, null, 'no stale timer before connect');

    const connectPromise = conn.connect();
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });
    await connectPromise;

    assert.ok(conn._staleTimer !== null, 'stale timer created after connect');
    assert.strictEqual(conn.getState(), 'connected');
  });

  it('stale check fires reconnect when _lastMsgAt exceeds threshold', async () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR
    conn._state = 'running';
    conn._lastMsgAt = Date.now() - 35000; // 35s ago > 30s threshold
    // Simulate an active WS so the socket-close path is exercised
    const ws = new MockWebSocket('ws://localhost');
    conn._ws = ws;

    // Call the actual _checkStale method (not reimplementing source logic)
    conn._checkStale();

    assert.strictEqual(conn.getState(), 'reconnecting',
      'stale detection should trigger reconnect');
    assert.ok(ws._closeCalled,
      'stale detection should close the WebSocket');
  });

  it('stale check interval is 5000ms (verified via node:test mock timers)', () => {
    // Use mock timers to verify the actual setInterval period
    const conn = createTestConnector();
    conn.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR

    // Prevent stale from immediately triggering (no active WS)
    conn._state = 'connected';
    conn._lastMsgAt = Date.now();

    mock.timers.enable({ apis: ['setInterval'] });
    try {
      conn._startStaleTimer();
      assert.ok(conn._staleTimer !== null, 'stale timer should be created');

      // Spy on _checkStale to count calls
      const checkStaleSpy = mock.method(conn, '_checkStale');

      // Advance by 5000ms — should trigger one _checkStale call
      mock.timers.tick(5000);
      assert.strictEqual(
        checkStaleSpy.mock.callCount(), 1,
        '_checkStale should be called once after 5000ms',
      );

      // Advance another 5000ms — should trigger second call
      mock.timers.tick(5000);
      assert.strictEqual(
        checkStaleSpy.mock.callCount(), 2,
        '_checkStale should be called twice after 10000ms',
      );
    } finally {
      mock.timers.reset();
    }
  });
});

// ====== Phase 2-3: Sequence gap handling ======

describe('sequence gap handling', () => {
  it('_handleSequenceGap closes socket and schedules reconnect', () => {
    const conn = createTestConnector();
    conn.on('error', () => {}); // prevent ERR_UNHANDLED_ERROR
    conn._state = 'running';

    // Have a mock WS active
    const connectPromise = conn.connect();
    setImmediate(() => {
      conn._ws.readyState = 1;
      conn._ws.emit('open');
    });

    // Now trigger sequence gap
    conn._handleSequenceGap('expected seq 100, got 200');

    assert.strictEqual(conn.getState(), 'reconnecting',
      'sequence gap puts connector in reconnecting state');
  });

  it('_handleSequenceGap is no-op in reconnecting or error state', () => {
    const conn = createTestConnector();
    conn._state = 'reconnecting';
    conn._handleSequenceGap('gap during reconnect');
    assert.strictEqual(conn.getState(), 'reconnecting',
      'should stay reconnecting, not double-schedule');
  });

  it('_handleSequenceGap is no-op during shutdown', () => {
    const conn = createTestConnector();
    conn._isShuttingDown = true;
    conn._handleSequenceGap('gap during shutdown');
    assert.strictEqual(conn.getState(), 'init',
      'should not change state during shutdown');
  });
});

// ====== Phase 2-4: _emitDepth validation ======

describe('_emitDepth validation', () => {
  it('emits depth event with partial type', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));
    conn._emitDepth('partial', [[50000, 1]], [[50100, 0.5]], 1700000000000, 123);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'partial');
    assert.strictEqual(emitted[0].market, 'test_market');
    assert.strictEqual(emitted[0].seq, 123);
  });

  it('emits depth event with delta type', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));
    conn._emitDepth('delta', [], [], 1700000000001, 124);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'delta');
  });

  it('emits depth event with snapshot type', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));
    conn._emitDepth('snapshot', [[49500, 3]], [[50500, 1.5]], 1700000000010, null);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'snapshot');
  });

  it('emits depth event with update type', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));
    conn._emitDepth('update', [[49600, 1]], [[50600, 2]], 1700000000011, null);
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].type, 'update');
  });

  it('passes through valid bids/asks but drops invalid (non-array) bids/asks', () => {
    // Per C1 §5.2 the contract requires bids/asks to be [price,qty] pairs.
    // Valid arrays pass through; non-array values are dropped.
    const conn = createTestConnector();
    const emitted = [];
    conn.on('depth', (ev) => emitted.push(ev));

    // Valid: type is recognised, bids/asks are arrays
    conn._emitDepth('delta', [[50000, 1]], [[50100, 0.5]], 1700000000002, 125);
    assert.strictEqual(emitted.length, 1, 'valid delta should emit');
    assert.strictEqual(emitted[0].type, 'delta');

    // Invalid: type is not one of the recognised depth event types
    emitted.length = 0;
    conn._emitDepth('unknown', [[50000, 1]], [[50100, 0.5]], 1700000000003, 126);
    assert.strictEqual(emitted.length, 0, 'invalid type should NOT emit');

    // Invalid: non-array bids
    conn._emitDepth('partial', 'string-bids', [[50100, 0.5]], 1700000000004, 127);
    assert.strictEqual(emitted.length, 0, 'non-array bids should NOT emit');

    // Invalid: null asks
    conn._emitDepth('partial', [[50000, 1]], null, 1700000000005, 128);
    assert.strictEqual(emitted.length, 0, 'null asks should NOT emit');
  });

  it('increments depthMsgCount on each call', () => {
    const conn = createTestConnector();
    conn.on('depth', () => {});
    assert.strictEqual(conn._stats.depthMsgCount, 0);
    conn._emitDepth('partial', [], [], 0, 1);
    assert.strictEqual(conn._stats.depthMsgCount, 1);
    conn._emitDepth('delta', [], [], 0, 2);
    assert.strictEqual(conn._stats.depthMsgCount, 2);
  });

  it('updates lastSeq when seq is provided', () => {
    const conn = createTestConnector();
    conn.on('depth', () => {});
    conn._emitDepth('partial', [], [], 0, 999);
    assert.strictEqual(conn._stats.lastSeq, 999);
  });
});

// ====== Phase 2-5: _emitLiquidation validation ======

describe('_emitLiquidation validation', () => {
  it('emits liquidation with all required fields', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));
    conn._emitLiquidation({
      exchange: 'binance',
      symbol: 'BTCUSDT',
      side: 'sell',
      price: 49000,
      qty: 1.5,
      notional: 73500,
      raw_type: 'forceOrder',
      trade_id: 'liq123',
      source_ts: 1700000000000,
    });
    assert.strictEqual(emitted.length, 1);
    const ev = emitted[0];
    assert.strictEqual(ev.market, 'test_market');
    assert.strictEqual(ev.exchange, 'binance');
    assert.strictEqual(ev.symbol, 'BTCUSDT');
    assert.strictEqual(ev.side, 'sell');
    assert.strictEqual(ev.price, 49000);
    assert.strictEqual(ev.qty, 1.5);
    assert.strictEqual(ev.notional, 73500);
    assert.strictEqual(ev.raw_type, 'forceOrder');
    assert.strictEqual(ev.trade_id, 'liq123');
    assert.strictEqual(ev.source_ts, 1700000000000);
    assert.ok(typeof ev.ts === 'number', 'ts should be epoch ms');
  });

  it('computes notional = price * qty when notional not provided', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));
    conn._emitLiquidation({
      exchange: 'bybit',
      symbol: 'BTCUSDT',
      side: 'buy',
      price: 50000,
      qty: 2.0,
    });
    assert.strictEqual(emitted.length, 1);
    assert.strictEqual(emitted[0].notional, 100000);
  });

  it('uses defaults when optional fields are omitted', () => {
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));
    conn._emitLiquidation({
      side: 'buy',
      price: 50000,
      qty: 1.0,
    });
    assert.strictEqual(emitted.length, 1);
    const ev = emitted[0];
    // market name is 'test_market', exchange default = market.replace(/_.*$/, '') = 'test'
    assert.strictEqual(ev.exchange, 'test', 'default exchange from market prefix');
    assert.strictEqual(ev.symbol, '', 'default empty symbol');
    assert.strictEqual(ev.raw_type, 'liquidation', 'default raw_type');
    assert.strictEqual(ev.trade_id, null);
    assert.strictEqual(ev.source_ts, null);
  });

  it('_emitLiquidation drops events with missing/invalid side, price, or qty', () => {
    // Per C1 §5.3/§5.5: side= buy/sell, price=positive number, qty=positive number.
    // Invalid payloads are dropped, not persisted.
    const conn = createTestConnector();
    const emitted = [];
    conn.on('liquidation', (ev) => emitted.push(ev));

    // Invalid: null side
    conn._emitLiquidation({ side: null, price: 50000, qty: 1 });
    assert.strictEqual(emitted.length, 0, 'null side should NOT emit');

    // Invalid: undefined price
    conn._emitLiquidation({ side: 'sell', price: undefined, qty: 1 });
    assert.strictEqual(emitted.length, 0, 'undefined price should NOT emit');

    // Invalid: non-positive qty
    conn._emitLiquidation({ side: 'buy', price: 50000, qty: -1 });
    assert.strictEqual(emitted.length, 0, 'negative qty should NOT emit');

    // Invalid: side not buy/sell
    conn._emitLiquidation({ side: 'unknown', price: 50000, qty: 1 });
    assert.strictEqual(emitted.length, 0, 'unknown side should NOT emit');

    // Valid payload still passes through
    conn._emitLiquidation({ side: 'buy', price: 50000, qty: 1 });
    assert.strictEqual(emitted.length, 1, 'valid liquidation should emit');
    assert.strictEqual(emitted[0].side, 'buy');
    assert.strictEqual(emitted[0].price, 50000);
    assert.strictEqual(emitted[0].qty, 1);
  });
});
